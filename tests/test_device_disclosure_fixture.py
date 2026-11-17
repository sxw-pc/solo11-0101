from pathlib import Path
import json

from rainstation.ingest import freeze_config, import_file
from rainstation.rules import evaluate
from rainstation.report import export


def test_device_disclosure_fixture_has_composition_risk(tmp_path):
    p = Path(__file__).parents[1] / 'fixtures'
    req = json.loads((p / 'device_disclosure_requests.json').read_text(encoding='utf-8'))
    db = tmp_path / 'stations.db'
    freeze_config(db, p / 'device_disclosure_config.json')
    assert import_file(db, p / 'device_disclosure_events.jsonl')['imported'] == 12
    assert len(evaluate(db)) == 4
    payload = export(db, tmp_path / 'report.json').read_text(encoding='utf-8')
    assert 'device_model' in payload and 'T2' in payload
    assert req['previous_release']['count'] == 1
    assert req['new_request']['stations'] == ['S1', 'S2', 'S3']
