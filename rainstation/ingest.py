import hashlib,json
from datetime import datetime,timezone
from pathlib import Path
from .store import connect

def hash_bytes(data): return hashlib.sha256(data).hexdigest()
def timestamp(text):
    dt=datetime.fromisoformat(text.replace('Z','+00:00'))
    if dt.tzinfo is None: raise ValueError('timezone_required')
    return dt
def freeze_config(db_path,config):
    raw=Path(config).read_bytes();digest=hash_bytes(raw); db=connect(db_path)
    old=db.execute("SELECT value FROM meta WHERE key='config_hash'").fetchone()
    if old and old['value']!=digest:
        db.close()
        raise ValueError('frozen_config_conflict')
    parsed=json.loads(raw)
    subjects=parsed.get('boxes',parsed.get('stations',parsed.get('recipients',[])))
    ids=[item.get('box',item.get('id')) for item in subjects]
    if not ids or any(not item for item in ids) or len(ids)!=len(set(ids)):
        raise ValueError('duplicate_or_missing_subject')
    with db:
        old=db.execute("SELECT value FROM meta WHERE key='config_hash'").fetchone()
        if old and old['value']!=digest: raise ValueError('frozen_config_conflict')
        db.execute("INSERT OR IGNORE INTO meta VALUES ('config_hash',?)",(digest,))
        db.execute("INSERT OR IGNORE INTO meta VALUES ('config_json',?)",(raw.decode('utf-8'),))
        db.execute("INSERT OR IGNORE INTO batch VALUES ('batch-1',?)",(digest,))
        for item in subjects:
            db.execute("INSERT OR IGNORE INTO subject VALUES (?,?)",(item.get('box',item.get('id')),'batch-1'))
    db.close();return digest

def _seq_key(identity,row):
    device=row.get('device')
    if device is None:
        head=identity.split(':',1)[0]
        device=head if head else identity
    return str(device)

def sequence_inventory(events):
    """Group events carrying a seq per device key.

    The inventory is derived only from immutable event rows, so it can be
    recomputed identically after any config/anchor re-run. A gap is a missing
    integer between the observed min/max seq of a device; every missing seq is
    reported exactly once together with its flanking observed events.
    """
    seen={}
    for e in events:
        v=json.loads(e['payload'])
        if 'seq' not in v: continue
        try: seq=int(v['seq'])
        except (TypeError,ValueError): continue
        key=_seq_key(e['identity'],v)
        bucket=seen.setdefault(key,{})
        bucket[seq]=e['id']
    gaps=[]
    for device,bucket in sorted(seen.items()):
        ordered=sorted(bucket)
        lo,hi=ordered[0],ordered[-1]
        present=set(ordered)
        missing=[s for s in range(lo,hi+1) if s not in present]
        # Contiguous missing seqs form ONE unknown block: n consecutive pulses
        # that must be allocated as a group, never counted independently.
        runs=[]
        for seq in missing:
            if runs and seq==runs[-1][-1]+1:
                runs[-1].append(seq)
            else:
                runs.append([seq])
        for i,run in enumerate(runs):
            before=max(s for s in ordered if s<run[0])
            after=min(s for s in ordered if s>run[-1])
            gaps.append({'device':device,'missing_seqs':run,
                         'missing_count':len(run),
                         'before_event_id':bucket[before],
                         'after_event_id':bucket[after],
                         'gap_index':i,'gap_total':len(runs)})
    return gaps

def import_file(db_path,path):
    raw=Path(path).read_bytes(); digest=hash_bytes(raw);db=connect(db_path)
    if db.execute('SELECT 1 FROM source WHERE sha256=?',(digest,)).fetchone():
        db.close();return {'imported':0,'duplicate_file':True}
    count=0
    with db:
        sid=db.execute('INSERT INTO source(path,sha256,raw) VALUES (?,?,?)',(str(path),digest,raw)).lastrowid
        for line_no,line in enumerate(raw.decode('utf-8-sig').splitlines(),1):
            if not line.strip(): continue
            try:
                row=json.loads(line)
                kind=str(row['kind']); subject=str(row['subject']);ts=str(row['time'])
                normalized=timestamp(ts).astimezone(timezone.utc).isoformat()
                identity=str(row.get('identity') or f"{row['device']}:{int(row['seq'])}")
                if not kind or not subject: raise ValueError('missing_kind_or_subject')
                canonical=json.dumps(row,sort_keys=True,ensure_ascii=False,separators=(',',':'))
                h=hash_bytes(canonical.encode())
                old=db.execute('SELECT sha256 FROM event WHERE identity=?',(identity,)).fetchone()
                if old:
                    if old['sha256']!=h: db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',('identity_conflict',identity))
                    continue
                db.execute('INSERT INTO event(source_id,line_no,identity,subject,kind,time,payload,sha256) VALUES (?,?,?,?,?,?,?,?)',(sid,line_no,identity,subject,kind,normalized,canonical,h));count+=1
            except (ValueError,TypeError,KeyError,json.JSONDecodeError) as exc:
                db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',('parse_error',json.dumps({'source_id':sid,'line_no':line_no,'error':str(exc),'raw_line':line},ensure_ascii=False)))
    db.close();return {'imported':count,'duplicate_file':False}
