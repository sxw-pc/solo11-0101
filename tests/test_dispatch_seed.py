from pathlib import Path
from rainstation.ingest import freeze_config,import_file
from rainstation.rules import evaluate
from rainstation.dispatch import register
from rainstation.store import connect
import pytest

FIX=Path(__file__).resolve().parents[1]/'fixtures'

def test_freezes_real_health_issues_without_route(tmp_path):
    db=tmp_path/'rain.db';freeze_config(db,FIX/'dispatch-config.json')
    assert import_file(db,FIX/'dispatch-events.jsonl')['imported']==12
    assert evaluate(db)=={'S0':'incomplete','S1':'incomplete','S2':'review'}
    got=register(db,'crew-01',FIX/'dispatch-case.json')
    assert got['tasks']==3 and register(db,'crew-01',FIX/'dispatch-case.json')==got
    conn=connect(db)
    try:assert conn.execute('SELECT COUNT(*) FROM dispatch_case').fetchone()[0]==1
    finally:conn.close()

def test_unrun_dispatch_rejected(tmp_path):
    db=tmp_path/'rain.db'
    with pytest.raises(ValueError,match='dispatch_run_missing'):register(db,'crew-01',FIX/'dispatch-case.json')
