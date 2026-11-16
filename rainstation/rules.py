import json
import math
from datetime import datetime
from collections import defaultdict
from datetime import timedelta,timezone
from .store import connect
from .ingest import timestamp,sequence_inventory
from .identity import model_for_device
from .timeline import (corrected_time,local_hour,drift_bounds,anchor_spread_ok,
                       maintenance_portion,touched_local_hours,hour_utc_span,gap_windows)

VERSION='2'

HARD_PROBLEMS=('clock_drift','physical_spike','coefficient_missing',
               'neighbor_disagreement','unknown_device_model')
SOFT_PROBLEMS=('sequence_gap','heartbeat_gap','heartbeat_missing',
               'no_pulses','clock_anchor_missing')

HISTORY_DDL="""CREATE TABLE IF NOT EXISTS decision_history(
  subject TEXT NOT NULL,run_id INTEGER NOT NULL REFERENCES run(id),
  status TEXT NOT NULL,details TEXT NOT NULL,rule_version TEXT NOT NULL,
  PRIMARY KEY(subject,run_id))"""


def _iso(dt): return dt.astimezone(timezone.utc).isoformat()


def _coefficient_at(coefficients, model, when):
    """Coefficient effective at an instant. Effective instants are inclusive,
    so an event exactly on an effective boundary selects the new coefficient."""
    active=[(eff,mm) for eff,m,mm in coefficients if m==model and eff<=when]
    if not active: return None
    return max(active,key=lambda item:item[0])[1]


def _round(value): return round(value+0.0,6)


def evaluate(db_path, policy='conservative'):
    if policy not in ('conservative','interval'):
        raise ValueError('unknown_policy')
    db=connect(db_path)
    db.execute(HISTORY_DDL)
    cfg=json.loads(db.execute("SELECT value FROM meta WHERE key='config_json'").fetchone()['value'])
    stations={s['id']:dict(s) for s in cfg['stations']}
    source_of={r['id']:r['source_id'] for r in db.execute('SELECT id,source_id FROM event')}
    by=defaultdict(list)
    for row in db.execute('SELECT * FROM event ORDER BY time,id'):
        by[row['subject']].append(dict(row))
    results={}
    with db:
        run_id=db.execute('INSERT INTO run(started_at,status) VALUES (?,?)',
                          (datetime.now(timezone.utc).isoformat(),'running')).lastrowid
        db.execute('DELETE FROM evidence WHERE rule_version=?',(VERSION,))
        db.execute("DELETE FROM issue WHERE code NOT IN ('parse_error','identity_conflict')")
        for spec in stations.values():
            site=spec['id']
            events=by.pop(site,[])
            hours,problems = _evaluate_station(spec,events,source_of,policy)
            hard=any(p in HARD_PROBLEMS for p in problems)
            uncertain=any(not h['exact'] for h in hours.values())
            if hard:
                status='review'
            elif uncertain or any(p in SOFT_PROBLEMS for p in problems):
                status='incomplete'
            else:
                status='trusted'
            previous=db.execute('SELECT * FROM decision WHERE subject=?',(site,)).fetchone()
            diff=_recompute_diff(previous,hours)
            if previous:
                db.execute('INSERT OR REPLACE INTO decision_history VALUES (?,?,?,?,?)',
                           (site,previous['run_id'],previous['status'],
                            previous['details'],previous['rule_version']))
            hourly_mm={h['hour']:h['measured_mm'] for h in hours.values() if h['measured_mm']}
            detail=json.dumps({
                'policy':policy,
                'rule_version':VERSION,
                'hourly_mm':hourly_mm,
                'hours':[hours[k] for k in sorted(hours)],
                'issues':sorted(set(problems)),
                'evidence':sorted({eid for h in hours.values() for eid in h['evidence_event_ids']} |
                                  {e['id'] for e in events}),
                'recompute_diff':diff,
            },ensure_ascii=False)
            db.execute('INSERT OR REPLACE INTO decision VALUES (?,?,?,?,?)',
                       (site,status,detail,VERSION,run_id))
            db.execute('DELETE FROM evidence WHERE subject=? AND rule_version=?',(site,VERSION))
            for e in events:
                db.execute('INSERT OR IGNORE INTO evidence VALUES (?,?,?,?)',(site,e['id'],VERSION,run_id))
            for code in sorted(set(problems)):
                db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',(code,site))
            results[site]=status
        for site in by:
            db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',('unknown_station',site))
        db.execute('UPDATE run SET completed_at=?,status=? WHERE id=?',
                   (datetime.now(timezone.utc).isoformat(),'done',run_id))
    db.close()
    return results


def _recompute_diff(previous, hours):
    if previous is None:
        return {'previous_run_id':None,'changes':[],'added':sorted(hours),'removed':[]}
    old=json.loads(previous['details'])
    old_hours={h['hour']:(h['mm_lower'],h['mm_upper']) for h in old.get('hours',[])}
    new_hours={k:(h['mm_lower'],h['mm_upper']) for k,h in hours.items()}
    changes=[]
    for key in sorted(set(old_hours)|set(new_hours)):
        if key in old_hours and key in new_hours and old_hours[key]!=new_hours[key]:
            before=old_hours[key];after=new_hours[key]
            changes.append({'hour':key,'before':list(before),'after':list(after),
                            'delta_lower':_round(after[0]-before[0]),
                            'delta_upper':_round(after[1]-before[1])})
    return {'previous_run_id':previous['run_id'],
            'previous_rule_version':previous['rule_version'],
            'changes':changes,
            'added':sorted(set(new_hours)-set(old_hours)),
            'removed':sorted(set(old_hours)-set(new_hours))}


def _evaluate_station(spec, events, source_of, policy):
    tz=timezone(timedelta(minutes=spec['offset_minutes']))
    max_drift=float(spec['max_clock_drift_seconds'])
    max_rate=float(spec['max_mm_per_hour'])
    threshold=spec.get('dispatch_threshold_mm')
    coefficients=[(timestamp(c['effective']),c['model'],float(c['mm_per_pulse']))
                  for c in spec['coefficients']]
    maintenance=sorted((timestamp(v['start']),timestamp(v['end']))
                       for e in events if e['kind']=='maintenance'
                       for v in [json.loads(e['payload'])])
    anchor_events=defaultdict(list)
    for e in events:
        if e['kind']=='anchor':
            v=json.loads(e['payload'])
            anchor_events[v.get('device','')].append((timestamp(e['time']),float(v['offset_seconds'])))
    for device in anchor_events: anchor_events[device].sort()
    bad_devices={d for d,vals in anchor_events.items() if not anchor_spread_ok(vals,max_drift)}
    problems=[]
    heartbeats=[]
    hours={}
    reserved_mm=defaultdict(float)

    def hour_state(hour):
        key=hour.astimezone(tz).isoformat()
        state=hours.get(key)
        if state is None:
            state={'hour':key,'measured_mm':0.0,'unknown_mm_lower':0.0,'unknown_mm_upper':0.0,
                   'mm_lower':0.0,'mm_upper':0.0,'exact':True,'unbounded':False,
                   'observed_event_ids':[],'gap_ids':[],'bounds':[],
                   'evidence_event_ids':set(),'evidence_source_ids':set()}
            hours[key]=state
        return state

    # --- observed pulses: deterministic measured part of every bound ---------
    for e in events:
        v=json.loads(e['payload']);kind=e['kind'];device=v.get('device','')
        when=timestamp(e['time'])
        if kind=='heartbeat': heartbeats.append(when)
        if kind!='pulse': continue
        if device in bad_devices:
            problems.append('clock_drift');continue
        model=model_for_device(spec,device)
        if model is None:
            problems.append('unknown_device_model');continue
        corrected=corrected_time(when,anchor_events[device])
        if corrected is None:
            problems.append('clock_anchor_missing');continue
        if any(start<=corrected<end for start,end in maintenance):
            continue
        coef=_coefficient_at(coefficients,model,local_hour(corrected,tz))
        if coef is None:
            problems.append('coefficient_missing');continue
        state=hour_state(local_hour(corrected,tz))
        mm=_round(int(v.get('count',1))*coef)
        state['measured_mm']=_round(state['measured_mm']+mm)
        state['observed_event_ids'].append(e['id'])
        state['evidence_event_ids'].add(e['id'])
        state['evidence_source_ids'].add(source_of.get(e['id']))

    for state in hours.values():
        if state['measured_mm']>max_rate: problems.append('physical_spike')
        neighbor=spec.get('neighbor_hourly_mm',{}).get(state['hour'])
        if neighbor is not None and abs(state['measured_mm']-float(neighbor))>spec.get('neighbor_tolerance_mm',50):
            problems.append('neighbor_disagreement')

    # --- missing seqs: one bounded unknown block per gap, split across hours --
    gaps=sequence_inventory(events)
    if gaps: problems.append('sequence_gap')
    event_by_id={e['id']:e for e in events}
    for gap in gaps:
        _allocate_gap(gap,event_by_id,anchor_events,bad_devices,maintenance,coefficients,
                      model_for_device(spec,gap['device']),tz,max_drift,max_rate,
                      hours,hour_state,reserved_mm,problems,source_of)

    if not heartbeats: problems.append('heartbeat_missing')
    if any((b-a).total_seconds()>spec.get('max_heartbeat_gap_seconds',3600)
           for a,b in zip(heartbeats,heartbeats[1:])):
        problems.append('heartbeat_gap')
    if not hours or not any(h['observed_event_ids'] for h in hours.values()):
        problems.append('no_pulses')

    for state in hours.values():
        state['mm_lower']=_round(state['measured_mm']+state['unknown_mm_lower'])
        state['mm_upper']=_round(state['measured_mm']+state['unknown_mm_upper'])
        if state['unbounded']: state['exact']=False
        state['policies']=_policies(state,policy,threshold)
        state['evidence_event_ids']=sorted(state['evidence_event_ids'])
        state['evidence_source_ids']=sorted(s for s in state['evidence_source_ids'] if s is not None)
    return hours,sorted(set(problems))


def _policies(state, policy, threshold):
    lo,hi=state['mm_lower'],state['mm_upper']
    if state['exact']:
        conservative={'state':'published','value':lo}
        interval={'state':'usable','range':[lo,hi]}
    else:
        # Never emit a midpoint: conservative withholds the whole hour.
        conservative={'state':'review','value':None}
        if threshold is None:
            interval={'state':'pending','range':[lo,hi]}
        elif lo<threshold<hi:
            interval={'state':'pending','range':[lo,hi]}
        else:
            interval={'state':'usable','range':[lo,hi]}
    return {'conservative':conservative,'interval':interval,
            'selected':policy,'threshold_mm':threshold}


def _allocate_gap(gap,event_by_id,anchor_events,bad_devices,maintenance,coefficients,
                  model,tz,max_drift,max_rate,hours,hour_state,reserved_mm,
                  problems,source_of):
    device=gap['device'];n=int(gap['missing_count'])
    before=event_by_id[gap['before_event_id']]
    after=event_by_id[gap['after_event_id']]
    gap_id=f"{before['subject']}:{device}:seqs-{gap['missing_seqs'][0]}-{gap['missing_seqs'][-1]}"
    evidence={gap['before_event_id'],gap['after_event_id']}
    sources={source_of.get(i) for i in list(evidence)}
    if device in bad_devices:
        problems.append('clock_drift')
    if model is None:
        problems.append('unknown_device_model')
    b0=drift_bounds(timestamp(before['time']),anchor_events[device],max_drift)
    b1=drift_bounds(timestamp(after['time']),anchor_events[device],max_drift)
    if b0 is None or b1 is None:
        # Unlocatable gap: no hour boundary can be defended, withhold values.
        problems.append('clock_anchor_missing')
        for flank in (before,after):
            state=hour_state(local_hour(timestamp(flank['time']),tz))
            state['unbounded']=True;state['gap_ids'].append(gap_id)
            state['evidence_event_ids']|=evidence
            state['evidence_source_ids']|=sources
        return
    earliest,latest=b0[0],b1[1]
    touched=touched_local_hours(earliest,latest,tz)
    hour_cuts=set()
    for hour in touched:
        h_start,h_end=hour_utc_span(hour,tz)
        if earliest<h_start<latest: hour_cuts.add(h_start)
        if earliest<h_end<latest: hour_cuts.add(h_end)
    segments=gap_windows(earliest,latest,maintenance,sorted(hour_cuts))

    caps={}
    free_hours=set()
    segment_audit=[]
    for lo_t,hi_t,inside in segments:
        hour=local_hour(lo_t+(hi_t-lo_t)/2,tz)
        key=hour.astimezone(tz).isoformat()
        coef=_coefficient_at(coefficients,model,hour) if model else None
        if coef is None:
            problems.append('coefficient_missing');coef=0.0
        length=(hi_t-lo_t).total_seconds()
        maint_len=maintenance_portion(lo_t,hi_t,maintenance).total_seconds()
        free=not (inside or maint_len>=length)
        if free: free_hours.add(key)
        # Physical-rate headroom left for unknown pulses in this hour, after
        # measured pulses and bounds already reserved by earlier gaps.
        measured=hours[key]['measured_mm'] if key in hours else 0.0
        avail_mm=max_rate-measured-reserved_mm.get(key,0.0)
        # Segments wholly inside a maintenance half-open window cannot hold
        # natural-rainfall unknown pulses.
        cap=0 if not free else (max(0,math.floor(avail_mm/coef)) if coef>0 else 0)
        caps.setdefault(key,0)
        caps[key]=max(caps[key],cap)
        segment_audit.append({'start_utc':_iso(lo_t),'end_utc':_iso(hi_t),
                              'inside_maintenance':not free,
                              'maintenance_seconds':maint_len,
                              'hour':key,'mm_per_pulse':coef,
                              'max_pulses':cap})
    # Allocate the single block of n unknown pulses across touched hours.
    # Each unknown pulse is counted once: both bounds total <= n.
    # Bounds are the extremes of every feasible allocation x_h with
    # sum(x_h)=n, 0<=x_h<=cap_h (maintenance-only hours have cap 0):
    #   upper_h = min(cap_h, n)
    #   lower_h = max(0, n - sum(cap of other hours))
    lo_p={key:0 for key in caps};hi_p={key:0 for key in caps}
    total_cap=sum(caps.values())
    if total_cap<n:
        if total_cap==0 and not free_hours:
            # The whole uncertainty window is covered by maintenance: the
            # missing block is test activity, contributing zero rainfall.
            pass
        else:
            # The physical-rate envelope cannot physically hold the missing
            # block outside maintenance; the hour values are not defensible.
            problems.append('physical_spike')
    for key in sorted(caps):
        hi_p[key]=min(caps[key],n)
        lo_p[key]=max(0,min(n,total_cap)-(total_cap-caps[key]))
        if lo_p[key]>hi_p[key]: problems.append('physical_spike')
    coef_by_hour={seg['hour']:seg['mm_per_pulse'] for seg in segment_audit}
    # A gap wholly inside maintenance contributes zero unknown pulses; the
    # touched hours still carry the gap's audit evidence.
    for hour in touched:
        caps.setdefault(hour.astimezone(tz).isoformat(),0)
    for key in sorted(caps):
        state=hour_state(timestamp(key))
        c=coef_by_hour.get(key,0.0)
        state['unknown_mm_lower']=_round(state['unknown_mm_lower']+lo_p.get(key,0)*c)
        state['unknown_mm_upper']=_round(state['unknown_mm_upper']+hi_p.get(key,0)*c)
        reserved_mm[key]=_round(reserved_mm.get(key,0.0)+hi_p.get(key,0)*c)
        state['gap_ids'].append(gap_id)
        state['evidence_event_ids']|=evidence
        state['evidence_source_ids']|=sources
        state['exact']=state['exact'] and lo_p.get(key,0)==hi_p.get(key,0)
    anchor_ids=[e['id'] for e in event_by_id.values()
                if e['kind']=='anchor' and json.loads(e['payload']).get('device')==device]
    maint_ids=[e['id'] for e in event_by_id.values() if e['kind']=='maintenance']
    bound={'gap_id':gap_id,'device':device,'missing_seqs':gap['missing_seqs'],
           'unknown_pulses':n,
           'before_event_id':gap['before_event_id'],'after_event_id':gap['after_event_id'],
           'anchor_event_ids':anchor_ids,'maintenance_event_ids':maint_ids,
           'source_ids':sorted(s for s in sources if s is not None),
           'envelope_utc':{'earliest':_iso(earliest),'latest':_iso(latest)},
           'lower_pulse_total':sum(lo_p.values()),'upper_pulse_total':sum(hi_p.values()),
           'missing_pulses':n,
           'hours':sorted(caps),'segments':segment_audit}
    for key in sorted(caps):
        hours[key]['bounds'].append(bound)
