from pathlib import Path
import hashlib
import json

from rainstation.ingest import freeze_config
from rainstation.store import connect


def test_transfer_fixture_links_frozen_stations_and_real_certificates(tmp_path):
    p = Path(__file__).parents[1] / 'fixtures'
    case = json.loads((p / 'station_transfer.json').read_text(encoding='utf-8'))
    db = tmp_path / 'stations.db'
    freeze_config(db, p / 'transfer_config.json')
    con = connect(db)
    assert {r['id'] for r in con.execute('SELECT id FROM subject')} == {s for x in case['requests'] for s in x['stations']}
    con.close()
    for logger in case['loggers']:
        assert hashlib.sha256((p / 'logger_certificates' / logger['file']).read_bytes()).hexdigest() == logger['sha256']
