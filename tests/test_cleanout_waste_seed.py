from pathlib import Path

import pytest

from rainstation.cleanout_waste import register
from rainstation.ingest import freeze_config, import_file
from rainstation.rules import evaluate
from rainstation.store import connect


FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def test_cleanout_case_from_real_maintenance_and_sealed_containers(tmp_path):
    db = tmp_path / 'rain.db'
    freeze_config(db, FIX / 'config.json')
    assert import_file(db, FIX / 'cleanout_events.jsonl')['imported'] == 5
    assert evaluate(db)['S1'] == 'trusted'
    got = register(db, 'CLEAN-1', FIX / 'cleanout_case.json')
    assert got['containers'] == 2
    assert register(db, 'CLEAN-1', FIX / 'cleanout_case.json') == got
    con = connect(db)
    try:
        assert con.execute('SELECT COUNT(*) FROM cleanout_container').fetchone()[0] == 2
    finally:
        con.close()


def test_cleanout_requires_imported_maintenance(tmp_path):
    db = tmp_path / 'rain.db'
    freeze_config(db, FIX / 'config.json')
    with pytest.raises(ValueError, match='cleanout_event_missing'):
        register(db, 'CLEAN-1', FIX / 'cleanout_case.json')
