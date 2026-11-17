from pathlib import Path
import hashlib
import json

from rainstation.ingest import freeze_config, import_file
from rainstation.store import connect


def test_maintenance_snapshot_fixture_has_two_real_versions(tmp_path):
    p = Path(__file__).parents[1] / 'fixtures'
    case = json.loads((p / 'maintenance_snapshot_mutation.json').read_text(encoding='utf-8'))
    initial = (p / case['initial_file']).read_bytes()
    revised = (p / case['revised_file']).read_bytes()
    assert hashlib.sha256(initial).hexdigest() == case['initial_sha256']
    assert hashlib.sha256(revised).hexdigest() == case['revised_sha256']
    assert initial != revised
    db = tmp_path / 'stations.db'
    freeze_config(db, p / 'config.json')
    assert import_file(db, p / case['initial_file'])['imported'] == 2048
    con = connect(db)
    assert con.execute('SELECT raw FROM source').fetchone()['raw'] == initial
    assert con.execute('SELECT COUNT(*) FROM event').fetchone()[0] == 2048
    con.close()
