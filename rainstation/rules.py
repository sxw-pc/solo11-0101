import json
from datetime import datetime
from collections import defaultdict
from datetime import timedelta,timezone
from .store import connect
from .ingest import timestamp
from .identity import model_for_device
from .timeline import corrected_time,local_hour

VERSION='1'
def evaluate(db_path):
    db=connect(db_path);cfg=json.loads(db.execute("SELECT value FROM meta WHERE key='config_json'").fetchone()['value'])
    stations={s['id']:s for s in cfg['stations']};by=defaultdict(list)
    for row in db.execute('SELECT * FROM event ORDER BY time,id'):by[row['subject']].append(dict(row))
    results={}
    with db:
        run_id=db.execute('INSERT INTO run(started_at,status) VALUES (?,?)',(datetime.now(timezone.utc).isoformat(),'running')).lastrowid
        db.execute('DELETE FROM evidence')
        db.execute('DELETE FROM decision');db.execute("DELETE FROM issue WHERE code NOT IN ('parse_error','identity_conflict')")
        for site,spec in stations.items():
            events=by.pop(site,[]);problems=[];pulses=defaultdict(int);seqs=defaultdict(list);anchors=defaultdict(list);heartbeats=[]
            tz=timezone(timedelta(minutes=spec['offset_minutes']))
            maintenance=[(timestamp(v['start']),timestamp(v['end'])) for e in events if e['kind']=='maintenance' for v in [json.loads(e['payload'])]]
            anchor_events=defaultdict(list)
            for e in events:
                if e['kind']=='anchor':anchor_events[json.loads(e['payload']).get('device','')].append((timestamp(e['time']),float(json.loads(e['payload'])['offset_seconds'])))
            bad_devices={device for device,values in anchor_events.items() if values and max(v for _,v in values)-min(v for _,v in values)>spec['max_clock_drift_seconds']}
            for e in events:
                v=json.loads(e['payload']);kind=e['kind'];device=v.get('device','');time=timestamp(e['time'])
                if 'seq' in v:seqs[device].append(int(v['seq']))
                if kind=='anchor':anchors[device].append(float(v['offset_seconds']))
                elif kind=='maintenance':pass
                elif kind=='heartbeat':heartbeats.append(time)
                elif kind=='pulse':
                    if device in bad_devices:continue
                    model=model_for_device(spec,device)
                    if model is None:problems.append('unknown_device_model');continue
                    corrected=corrected_time(time,anchor_events[device])
                    if corrected is None:problems.append('clock_anchor_missing');continue
                    if not any(start<=corrected<end for start,end in maintenance):
                        pulses[(local_hour(corrected,tz),model)]+=int(v.get('count',1))
            for device,values in seqs.items():
                unique=set(values)
                if unique and max(unique)-min(unique)+1!=len(unique):problems.append('sequence_gap')
            for values in anchors.values():
                if values and max(values)-min(values)>spec['max_clock_drift_seconds']:problems.append('clock_drift')
            hourly={}
            for (hour,model),count in sorted(pulses.items()):
                date=hour.astimezone(tz).isoformat()
                active=[x for x in spec['coefficients'] if x.get('model')==model and timestamp(x['effective'])<=hour]
                if not active:problems.append('coefficient_missing');continue
                selected=max(active,key=lambda x:timestamp(x['effective']))
                mm=round(count*selected['mm_per_pulse'],6);hourly[date]=round(hourly.get(date,0)+mm,6)
                if hourly[date]>spec['max_mm_per_hour']:problems.append('physical_spike')
                neighbor=spec.get('neighbor_hourly_mm',{}).get(date)
                if neighbor is not None and abs(mm-float(neighbor))>spec.get('neighbor_tolerance_mm',50):problems.append('neighbor_disagreement')
            if not heartbeats:problems.append('heartbeat_missing')
            if any((b-a).total_seconds()>spec.get('max_heartbeat_gap_seconds',3600) for a,b in zip(heartbeats,heartbeats[1:])):problems.append('heartbeat_gap')
            if not pulses:problems.append('no_pulses')
            status='review' if any(p in problems for p in ('clock_drift','physical_spike','coefficient_missing','neighbor_disagreement','unknown_device_model')) else ('incomplete' if problems else 'trusted')
            detail=json.dumps({'hourly_mm':hourly,'issues':sorted(set(problems)),'evidence':[e['id'] for e in events]},ensure_ascii=False)
            db.execute('INSERT INTO decision VALUES (?,?,?,?,?)',(site,status,detail,VERSION,run_id))
            for e in events:db.execute('INSERT INTO evidence VALUES (?,?,?,?)',(site,e['id'],VERSION,run_id))
            for code in sorted(set(problems)):db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',(code,site))
            results[site]=status
        for site in by:db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',('unknown_station',site))
        db.execute('UPDATE run SET completed_at=?,status=? WHERE id=?',(datetime.now(timezone.utc).isoformat(),'done',run_id))
    db.close();return results
