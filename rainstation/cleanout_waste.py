"""Freeze a real gauge cleanout and measured sealed liquids; no disposition yet."""

import csv
import hashlib
import json
from decimal import Decimal
from pathlib import Path

from .store import connect


def register(db_path, case_id, fixture):
    path = Path(fixture).resolve()
    raw = path.read_bytes()
    doc = json.loads(raw)
    if not case_id or not doc.get('station') or not doc.get('maintenance_identity') or not doc.get('contracts'):
        raise ValueError('bad_cleanout_case')
    src = (path.parent / doc['containers_file']).resolve()
    source_raw = src.read_bytes()
    rows = list(csv.DictReader(source_raw.decode('utf-8-sig').splitlines()))
    if not rows:
        raise ValueError('cleanout_measurements_missing')
    db = connect(db_path)
    try:
        with db:
            maintenance = db.execute(
                '''SELECT e.id,e.payload,s.sha256 AS source_sha256 FROM event e
                   JOIN source s ON s.id=e.source_id
                   WHERE e.identity=? AND e.subject=? AND e.kind='maintenance' ''',
                (doc['maintenance_identity'], doc['station']),
            ).fetchone()
            decision = db.execute(
                'SELECT status,details FROM decision WHERE subject=?', (doc['station'],)
            ).fetchone()
            if not maintenance or json.loads(maintenance['payload']).get('operation') != 'bucket_cleanout':
                raise ValueError('cleanout_event_missing')
            if not decision or decision['status'] != 'trusted':
                raise ValueError('cleanout_station_not_verified')
            for row in rows:
                gross = Decimal(row['gross_kg'])
                contaminant = Decimal(row['contaminant_kg'])
                if not 0 <= contaminant <= gross:
                    raise ValueError('invalid_cleanout_mass')
                old = db.execute(
                    'SELECT maintenance_identity,gross_kg,contaminant_kg,seal FROM cleanout_container WHERE id=?',
                    (row['container'],),
                ).fetchone()
                expected = {'maintenance_identity': doc['maintenance_identity'],
                            'gross_kg': row['gross_kg'], 'contaminant_kg': row['contaminant_kg'],
                            'seal': row['seal']}
                if old and dict(old) != expected:
                    raise ValueError('cleanout_container_conflict')
                db.execute(
                    'INSERT OR IGNORE INTO cleanout_container VALUES (?,?,?,?,?)',
                    (row['container'], doc['maintenance_identity'], row['gross_kg'],
                     row['contaminant_kg'], row['seal']),
                )
            digest = hashlib.sha256(raw + source_raw).hexdigest()
            payload = json.dumps(
                {'fixture': str(path), 'case': doc, 'containers': rows,
                 'measurements_sha256': hashlib.sha256(source_raw).hexdigest(),
                 'maintenance': dict(maintenance), 'station_decision': dict(decision)},
                sort_keys=True, separators=(',', ':'),
            )
            old = db.execute('SELECT digest FROM cleanout_case WHERE id=?', (case_id,)).fetchone()
            if old and old['digest'] != digest:
                raise ValueError('cleanout_case_conflict')
            db.execute('INSERT OR IGNORE INTO cleanout_case VALUES (?,?,?)', (case_id, payload, digest))
    finally:
        db.close()
    return {'case_id': case_id, 'digest': digest, 'containers': len(rows)}
