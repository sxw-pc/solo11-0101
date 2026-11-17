"""Freeze a field crew's synthetic station fault tasks without routing them."""
import hashlib,json
from pathlib import Path
from .store import connect

def register(db_path,case_id,fixture):
    if not case_id:raise ValueError('missing_dispatch_case_id')
    source=Path(fixture).resolve();raw=source.read_bytes();doc=json.loads(raw)
    if not doc.get('tasks') or not doc.get('travel_minutes') or not doc.get('crew'):
        raise ValueError('bad_dispatch_contract')
    db=connect(db_path)
    try:
        with db:
            run=db.execute('SELECT id,status FROM run ORDER BY id DESC LIMIT 1').fetchone()
            if not run or run['status']!='done':raise ValueError('dispatch_run_missing')
            frozen=[]
            for task in doc['tasks']:
                row=db.execute('SELECT status,details FROM decision WHERE subject=? AND run_id=?',(task['station'],run['id'])).fetchone()
                if not row or task['issue_code'] not in json.loads(row['details'])['issues']:
                    raise ValueError('dispatch_issue_missing')
                evidence=[r['sha256'] for r in db.execute('SELECT e.sha256 FROM evidence v JOIN event e ON e.id=v.event_id WHERE v.subject=? AND v.run_id=? ORDER BY e.id',(task['station'],run['id']))]
                frozen.append((task['station'],row['status'],task['issue_code'],evidence))
            payload=json.dumps({'fixture':str(source),'digest':hashlib.sha256(raw).hexdigest(),
                                'doc':doc,'run_id':run['id'],'frozen':frozen},sort_keys=True,separators=(',',':'))
            old=db.execute('SELECT payload FROM dispatch_case WHERE id=?',(case_id,)).fetchone()
            if old and old['payload']!=payload:raise ValueError('dispatch_case_conflict')
            db.execute('INSERT OR IGNORE INTO dispatch_case VALUES (?,?)',(case_id,payload))
    finally:db.close()
    return {'case_id':case_id,'digest':hashlib.sha256(raw).hexdigest(),'tasks':len(frozen)}
