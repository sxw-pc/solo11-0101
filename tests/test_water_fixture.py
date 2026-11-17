from pathlib import Path
import hashlib
import json

from rainstation.ingest import freeze_config, import_file
from rainstation.rules import evaluate
from rainstation.store import connect


def test_water_fixture_has_trusted_rain_and_closed_tank(tmp_path):
    p = Path(__file__).parents[1] / 'fixtures'
    contract = json.loads((p / 'water_attribute_contract.json').read_text(encoding='utf-8'))
    tank = json.loads((p / 'water_tank_log.json').read_text(encoding='utf-8'))
    assert hashlib.sha256((p / contract['file']).read_bytes()).hexdigest() == contract['sha256']
    db = tmp_path / 'rain.db'
    freeze_config(db, p / 'water_config.json')
    assert import_file(db, p / 'water_events.jsonl')['imported'] == 4
    assert evaluate(db)['S1'] == 'trusted'
    con = connect(db)
    detail = json.loads(con.execute("SELECT details FROM decision WHERE subject='S1'").fetchone()['details'])
    assert list(detail['hourly_mm'].values()) == [20]
    assert 20 * contract['catchment_area_m2'] / 1000 == tank['closing_m3'] + tank['overflow_m3']
    con.close()
