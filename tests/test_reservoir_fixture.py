from pathlib import Path
import hashlib
import json

from rainstation.ingest import freeze_config, import_file
from rainstation.rules import evaluate
from rainstation.store import connect


def test_reservoir_fixture_has_two_trusted_rain_hours_and_feedback(tmp_path):
    p = Path(__file__).parents[1] / 'fixtures'
    case = json.loads((p / 'reservoir_plan.json').read_text(encoding='utf-8'))
    raw_feedback = (p / 'reservoir_feedback.json').read_bytes()
    assert hashlib.sha256(raw_feedback).hexdigest() == (p / 'reservoir_feedback.sha256').read_text().strip()
    db = tmp_path / 'station.db'
    freeze_config(db, p / 'reservoir_config.json')
    assert import_file(db, p / 'reservoir_rain_events.jsonl')['imported'] == 8
    assert evaluate(db)['S1'] == 'trusted'
    con = connect(db)
    details = json.loads(con.execute("SELECT details FROM decision WHERE subject='S1'").fetchone()['details'])
    assert sorted(details['hourly_mm'].values()) == [40, 60]
    assert case['initial_storage'] + 60 - 30 == 90
    assert json.loads(raw_feedback)['observed_storage_after_slot'] == 95
    con.close()
