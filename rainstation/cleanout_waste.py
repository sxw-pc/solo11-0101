"""Sealed cleanout waste: per-container physical custody, disposition and receipts.

The rainfall ``decision`` only proves the maintenance happened and the hourly
rainfall is trustworthy; it is a rainwater attribute credential, never a waste
handover.  Physical movement of the sealed cleaning waste lives in
``cleanout_event``/``cleanout_batch``/``cleanout_manifest``/``cleanout_receipt``
and changes real custody state, so every kilogram is auditable from the station
and maintenance event M1 to its final custodian.  Masses are exact decimals
(0.001 kg resolution) stored as text; re-runs are idempotent via dedupe keys.
"""

import csv
import hashlib
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .ingest import timestamp
from .store import connect

KG = Decimal('0.001')
ZERO = Decimal('0.000')
CONTRACT_KINDS = ('closed_loop_sep', 'sealed_ship')


def _mass(value, code='invalid_cleanout_mass'):
    """Parse an exact decimal kilogram mass at 0.001 kg resolution."""
    try:
        d = Decimal(str(value).strip())
        if not d.is_finite():
            raise ValueError(code)
        quantized = d.quantize(KG)
    except (InvalidOperation, ValueError):
        raise ValueError(code)
    if quantized != d:
        raise ValueError('cleanout_mass_precision')
    return d


def _kg(d):
    return str(d.quantize(KG))


def _canonical(obj):
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def _dedupe(kind, case_id, payload):
    return hashlib.sha256(f'{kind}|{case_id}|{_canonical(payload)}'.encode()).hexdigest()


def _parse_time(value, code):
    try:
        return timestamp(str(value))
    except (ValueError, TypeError):
        raise ValueError(code)


def _validate_contracts(doc):
    """Both disposition path contracts must be frozen, authorized and complete."""
    ids = []
    for contract in doc['contracts']:
        cid = contract.get('id')
        if cid not in CONTRACT_KINDS or not contract.get('authorized_by') or cid in ids:
            raise ValueError('cleanout_contract_invalid')
        ids.append(cid)
        if cid == 'closed_loop_sep':
            if not contract.get('process_cert') or not contract.get('cert_valid_until'):
                raise ValueError('cleanout_contract_incomplete')
            _parse_time(contract['cert_valid_until'], 'cleanout_contract_invalid')
        if cid == 'sealed_ship':
            if not contract.get('carrier_license') or not contract.get('license_valid_until') \
                    or not contract.get('manifest_id'):
                raise ValueError('cleanout_contract_incomplete')
            _parse_time(contract['license_valid_until'], 'cleanout_contract_invalid')
            receipt = contract.get('receipt')
            if receipt is not None:
                for field in ('id', 'carrier', 'signed_by', 'accepted_containers', 'accepted_kg', 'at'):
                    if not receipt.get(field):
                        raise ValueError('cleanout_contract_incomplete')
                _mass(receipt['accepted_kg'], 'cleanout_contract_invalid')
                _parse_time(receipt['at'], 'cleanout_contract_invalid')
    if set(ids) != set(CONTRACT_KINDS):
        raise ValueError('cleanout_contract_incomplete')


def _case(db, case_id):
    row = db.execute('SELECT payload FROM cleanout_case WHERE id=?', (case_id,)).fetchone()
    if not row:
        raise ValueError('cleanout_case_missing')
    return json.loads(row['payload'])


def _record_review(db, case_id, reason, detail, at):
    """Flag the whole lot as pending verification; idempotent per problem."""
    key = hashlib.sha256(f'review|{case_id}|{reason}|{detail}'.encode()).hexdigest()
    db.execute(
        'INSERT OR IGNORE INTO cleanout_review(case_id,reason,detail,at,dedupe_key) VALUES (?,?,?,?,?)',
        (case_id, reason, str(detail), at, key),
    )


def _insert_event(db, case_id, kind, at, actor, payload, key):
    return db.execute(
        'INSERT INTO cleanout_event(case_id,kind,at,actor,payload,dedupe_key) VALUES (?,?,?,?,?,?)',
        (case_id, kind, at, actor, _canonical(payload), key),
    ).lastrowid


def _replay(db, kind, case_id, input_payload):
    """Return the stored result of an identical earlier call, or None."""
    key = _dedupe(kind, case_id, input_payload)
    row = db.execute('SELECT payload FROM cleanout_event WHERE dedupe_key=?', (key,)).fetchone()
    return (key, json.loads(row['payload'])['result']) if row else (key, None)


def register(db_path, case_id, fixture):
    path = Path(fixture).resolve()
    raw = path.read_bytes()
    doc = json.loads(raw)
    if not case_id or not doc.get('station') or not doc.get('maintenance_identity') \
            or not doc.get('contracts') or not doc.get('containers_file'):
        raise ValueError('bad_cleanout_case')
    inspection_at = _parse_time(doc.get('inspection_at'), 'bad_cleanout_case').isoformat()
    inspector = doc.get('inspector') or ''
    _validate_contracts(doc)
    src = (path.parent / doc['containers_file']).resolve()
    source_raw = src.read_bytes()
    rows = list(csv.DictReader(source_raw.decode('utf-8-sig').splitlines()))
    if not rows:
        raise ValueError('cleanout_measurements_missing')
    seen = set()
    masses = {}
    for row in rows:
        cid = (row.get('container') or '').strip()
        if not cid:
            raise ValueError('cleanout_container_missing')
        if cid in seen:
            raise ValueError('cleanout_duplicate_container')
        seen.add(cid)
        if not (row.get('seal') or '').strip():
            raise ValueError('cleanout_seal_missing')
        if not (row.get('gross_kg') or '').strip() or not (row.get('contaminant_kg') or '').strip():
            raise ValueError('cleanout_weighing_missing')
        gross = _mass(row['gross_kg'])
        contaminant = _mass(row['contaminant_kg'])
        if not ZERO <= contaminant <= gross:
            raise ValueError('invalid_cleanout_mass')
        masses[cid] = (gross, contaminant)
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
            measurements_sha256 = hashlib.sha256(source_raw).hexdigest()
            payload = json.dumps(
                {'fixture': str(path), 'case': doc, 'containers': rows,
                 'measurements_sha256': measurements_sha256,
                 'maintenance': dict(maintenance), 'station_decision': dict(decision)},
                sort_keys=True, separators=(',', ':'),
            )
            old = db.execute('SELECT digest FROM cleanout_case WHERE id=?', (case_id,)).fetchone()
            if old and old['digest'] != digest:
                raise ValueError('cleanout_case_conflict')
            db.execute('INSERT OR IGNORE INTO cleanout_case VALUES (?,?,?)', (case_id, payload, digest))
            # Physical registration: one parent batch per sealed container, on site.
            event_payload = {'station': doc['station'], 'maintenance_identity': doc['maintenance_identity'],
                             'containers': [dict(r) for r in rows],
                             'measurements_sha256': measurements_sha256, 'digest': digest}
            key = _dedupe('register', case_id, event_payload)
            if not db.execute('SELECT 1 FROM cleanout_event WHERE dedupe_key=?', (key,)).fetchone():
                eid = _insert_event(db, case_id, 'register', inspection_at, inspector, event_payload, key)
                for row in rows:
                    gross, contaminant = masses[row['container']]
                    db.execute(
                        '''INSERT INTO cleanout_batch(id,case_id,parent_id,container_id,kind,status,custody,
                           gross_kg,contaminant_kg,seal,maintenance_identity,weigh_sha256,recheck_id,
                           created_by,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                        (f'{case_id}:{row["container"]}', case_id, None, row['container'], 'parent',
                         'on_site', f'site:{doc["station"]}', _kg(gross), _kg(contaminant), row['seal'],
                         doc['maintenance_identity'], measurements_sha256, '', eid, inspection_at),
                    )
    finally:
        db.close()
    return {'case_id': case_id, 'digest': digest, 'containers': len(rows)}


def dispose(db_path, case_id, contract_id, at, actor, recheck=None):
    """Atomically execute one frozen contract: on-site separation or sealed shipment."""
    moment = _parse_time(at, 'cleanout_time_invalid')
    at_iso = moment.isoformat()
    db = connect(db_path)
    try:
        payload = _case(db, case_id)
        doc = payload['case']
        station = doc['station']
        contracts = {c['id']: c for c in doc['contracts']}
        contract = contracts.get(contract_id)
        if contract is None:
            raise ValueError('cleanout_contract_missing')
        if not contract.get('authorized_by'):
            raise ValueError('cleanout_unauthorized')
        input_payload = {'contract_id': contract_id, 'at': at_iso, 'actor': actor, 'recheck': recheck}
        key, replayed = _replay(db, 'dispose', case_id, input_payload)
        if replayed is not None:
            return replayed
        parents = db.execute(
            "SELECT * FROM cleanout_batch WHERE case_id=? AND kind='parent' ORDER BY container_id",
            (case_id,),
        ).fetchall()
        problems = []
        selected = db.execute(
            'SELECT contract_id FROM cleanout_contract WHERE case_id=?', (case_id,)
        ).fetchone()
        if selected and selected['contract_id'] != contract_id:
            problems.append(('cleanout_contract_conflict', selected['contract_id']))
        elif selected:
            problems.append(('cleanout_dispose_conflict', contract_id))
        if not parents:
            problems.append(('cleanout_weighing_missing', case_id))
        for p in parents:
            if p['status'] != 'on_site':
                problems.append(('cleanout_parent_disposed', p['container_id']))
            if not p['gross_kg'] or not p['weigh_sha256']:
                problems.append(('cleanout_weighing_missing', p['container_id']))
                continue
            gross, contaminant = Decimal(p['gross_kg']), Decimal(p['contaminant_kg'])
            if gross < ZERO or contaminant < ZERO:
                problems.append(('cleanout_invalid_mass', p['container_id']))
            elif contaminant > gross:
                problems.append(('cleanout_contaminant_exceeds', p['container_id']))
        if contract_id == 'closed_loop_sep':
            if moment > _parse_time(contract['cert_valid_until'], 'cleanout_contract_invalid'):
                problems.append(('cleanout_cert_expired', contract['process_cert']))
            if not recheck or not recheck.get('id') or not recheck.get('containers'):
                problems.append(('cleanout_recheck_missing', case_id))
            else:
                recheck_map = {r.get('container'): r for r in recheck['containers']}
                for p in parents:
                    r = recheck_map.get(p['container_id'])
                    if r is None:
                        problems.append(('cleanout_recheck_missing', p['container_id']))
                        continue
                    if r.get('seal') != p['seal']:
                        problems.append(('cleanout_seal_mismatch', p['container_id']))
                        continue
                    try:
                        same = _mass(r.get('gross_kg')) == Decimal(p['gross_kg']) \
                            and _mass(r.get('contaminant_kg')) == Decimal(p['contaminant_kg'])
                    except ValueError:
                        same = False
                    if not same:
                        problems.append(('cleanout_recheck_failed', p['container_id']))
        if contract_id == 'sealed_ship':
            if moment > _parse_time(contract['license_valid_until'], 'cleanout_contract_invalid'):
                problems.append(('cleanout_license_expired', contract['carrier_license']))
        if problems:
            with db:
                for reason, detail in problems:
                    _record_review(db, case_id, reason, detail, at_iso)
            raise ValueError(problems[0][0])
        with db:
            db.execute(
                'INSERT INTO cleanout_contract VALUES (?,?,?,?,?)',
                (case_id, contract_id, contract['authorized_by'], at_iso, _canonical(contract)),
            )
            if contract_id == 'closed_loop_sep':
                plan = []
                for p in parents:
                    gross, contaminant = Decimal(p['gross_kg']), Decimal(p['contaminant_kg'])
                    clean, residue = gross - contaminant, contaminant
                    plan.append((p, clean, residue))
                children = [
                    {'id': f"{p['id']}:{suffix}", 'container': p['container_id'], 'kind': kind,
                     'gross_kg': _kg(mass), 'contaminant_kg': _kg(cont)}
                    for p, clean, residue in plan
                    for suffix, kind, mass, cont in (
                        ('clean', 'clean_liquid', clean, ZERO),
                        ('residue', 'residue', residue, residue),
                    )
                ]
                result = {'case_id': case_id, 'contract': contract_id, 'children': children,
                          'reused_kg': _kg(sum((clean for _, clean, _ in plan), ZERO)),
                          'residue_kg': _kg(sum((residue for _, _, residue in plan), ZERO))}
                full_payload = {**input_payload, 'process_cert': contract['process_cert'],
                                'recheck_id': recheck.get('id', ''), 'result': result}
                eid = _insert_event(db, case_id, 'dispose', at_iso, actor, full_payload, key)
                for p, clean, residue in plan:
                    db.execute(
                        "UPDATE cleanout_batch SET status='consumed', custody=? WHERE id=?",
                        (f"process:{contract['process_cert']}", p['id']),
                    )
                    for suffix, kind, status, mass, cont in (
                        ('clean', 'clean_liquid', 'reusable', clean, ZERO),
                        ('residue', 'residue', 'pending_shipment', residue, residue),
                    ):
                        db.execute(
                            '''INSERT INTO cleanout_batch(id,case_id,parent_id,container_id,kind,status,custody,
                               gross_kg,contaminant_kg,seal,maintenance_identity,weigh_sha256,recheck_id,
                               created_by,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                            (f"{p['id']}:{suffix}", case_id, p['id'], p['container_id'], kind, status,
                             f'site:{station}', _kg(mass), _kg(cont), p['seal'],
                             p['maintenance_identity'], p['weigh_sha256'], recheck.get('id', ''), eid, at_iso),
                        )
            else:
                carrier = contract.get('carrier') or contract['carrier_license']
                if db.execute('SELECT 1 FROM cleanout_manifest WHERE id=?',
                              (contract['manifest_id'],)).fetchone():
                    raise ValueError('cleanout_manifest_conflict')
                containers = [p['container_id'] for p in parents]
                result = {'case_id': case_id, 'contract': contract_id, 'manifest': contract['manifest_id'],
                          'carrier': carrier, 'containers': containers, 'reused_kg': _kg(ZERO),
                          'shipped_kg': _kg(sum((Decimal(p['gross_kg']) for p in parents), ZERO))}
                full_payload = {**input_payload, 'manifest_id': contract['manifest_id'],
                                'carrier': carrier, 'result': result}
                _insert_event(db, case_id, 'dispose', at_iso, actor, full_payload, key)
                db.execute(
                    'INSERT INTO cleanout_manifest VALUES (?,?,?,?,?,?,?)',
                    (contract['manifest_id'], case_id, carrier, contract['carrier_license'],
                     contract['license_valid_until'], at_iso, json.dumps(containers)),
                )
                for p in parents:
                    db.execute(
                        "UPDATE cleanout_batch SET status='in_transit', custody=? WHERE id=?",
                        (f'carrier:{carrier}', p['id']),
                    )
        return result
    finally:
        db.close()


def receive(db_path, case_id, receipt=None):
    """Record the licensed carrier's signed receipt against the shipment manifest.

    Containers the carrier did not accept keep their real on-site custody via an
    appended return event; the shipment event itself is never rewritten.
    """
    db = connect(db_path)
    try:
        payload = _case(db, case_id)
        doc = payload['case']
        station = doc['station']
        contracts = {c['id']: c for c in doc['contracts']}
        if receipt is None:
            receipt = (contracts.get('sealed_ship') or {}).get('receipt')
        if not receipt:
            raise ValueError('cleanout_receipt_missing')
        for field in ('id', 'carrier', 'signed_by', 'accepted_kg', 'at'):
            if not receipt.get(field):
                raise ValueError('cleanout_receipt_invalid')
        if not isinstance(receipt.get('accepted_containers'), list):
            raise ValueError('cleanout_receipt_invalid')
        moment = _parse_time(receipt['at'], 'cleanout_receipt_invalid')
        accepted_kg = _mass(receipt['accepted_kg'], 'cleanout_receipt_invalid')
        manifest = db.execute(
            'SELECT * FROM cleanout_manifest WHERE case_id=? ORDER BY shipped_at DESC', (case_id,)
        ).fetchone()
        if manifest is None:
            raise ValueError('cleanout_manifest_missing')
        key, replayed = _replay(db, 'receive', case_id, {'receipt': receipt})
        if replayed is not None:
            return replayed
        if db.execute('SELECT 1 FROM cleanout_receipt WHERE id=?', (receipt['id'],)).fetchone():
            raise ValueError('cleanout_receipt_conflict')
        parents = {p['container_id']: p for p in db.execute(
            "SELECT * FROM cleanout_batch WHERE case_id=? AND kind='parent'", (case_id,)).fetchall()}
        accepted = list(dict.fromkeys(receipt['accepted_containers']))
        if any(c not in parents for c in accepted):
            raise ValueError('cleanout_unknown_container')
        reviews = []
        if receipt['carrier'] != manifest['carrier']:
            reviews.append(('cleanout_carrier_mismatch', receipt['carrier']))
        signed_mass = sum((Decimal(parents[c]['gross_kg']) for c in accepted), ZERO)
        if signed_mass != accepted_kg:
            reviews.append(('cleanout_receipt_mass_mismatch',
                            f"{receipt['id']}:{_kg(signed_mass)}!={_kg(accepted_kg)}"))
        returned = [c for c in json.loads(manifest['containers']) if c not in accepted]
        result = {'case_id': case_id, 'receipt': receipt['id'], 'manifest': manifest['id'],
                  'accepted': accepted, 'returned': returned, 'received_kg': _kg(signed_mass),
                  'reviews': [reason for reason, _ in reviews]}
        with db:
            db.execute(
                'INSERT INTO cleanout_receipt VALUES (?,?,?,?,?,?)',
                (receipt['id'], manifest['id'], receipt['signed_by'], moment.isoformat(),
                 _kg(accepted_kg), json.dumps(accepted)),
            )
            _insert_event(db, case_id, 'receive', moment.isoformat(), receipt['signed_by'],
                          {'receipt': receipt, 'result': result}, key)
            for cid in accepted:
                p = parents[cid]
                if p['status'] == 'in_transit':
                    db.execute(
                        "UPDATE cleanout_batch SET status='received', custody=? WHERE id=?",
                        (f"carrier:{manifest['carrier']}", p['id']),
                    )
                elif p['status'] != 'received':
                    _record_review(db, case_id, 'cleanout_unexpected_container_state',
                                   f"{cid}:{p['status']}", moment.isoformat())
            for cid in returned:
                p = parents[cid]
                if p['status'] == 'in_transit':
                    db.execute(
                        "UPDATE cleanout_batch SET status='on_site', custody=? WHERE id=?",
                        (f'site:{station}', p['id']),
                    )
                    rkey = _dedupe('return', case_id,
                                   {'container': cid, 'receipt': receipt['id'], 'at': moment.isoformat()})
                    db.execute(
                        '''INSERT OR IGNORE INTO cleanout_event(case_id,kind,at,actor,payload,dedupe_key)
                           VALUES (?,?,?,?,?,?)''',
                        (case_id, 'return', moment.isoformat(), receipt['signed_by'],
                         _canonical({'container': cid, 'receipt': receipt['id'],
                                     'reason': 'carrier_rejected'}), rkey),
                    )
            for reason, detail in reviews:
                _record_review(db, case_id, reason, detail, moment.isoformat())
        return result
    finally:
        db.close()


def reweigh(db_path, case_id, correction):
    """Append a late weighing as a correction event; never rewrites batches or custody."""
    db = connect(db_path)
    try:
        _case(db, case_id)
        container = correction.get('container')
        batch = db.execute(
            "SELECT * FROM cleanout_batch WHERE case_id=? AND container_id=? AND kind='parent'",
            (case_id, container),
        ).fetchone()
        if batch is None:
            raise ValueError('cleanout_unknown_container')
        moment = _parse_time(correction.get('at'), 'cleanout_time_invalid')
        key, replayed = _replay(db, 'reweigh', case_id, {'correction': correction})
        if replayed is not None:
            return replayed
        reviews = []
        try:
            gross = _mass(correction.get('gross_kg'))
            contaminant = _mass(correction.get('contaminant_kg'))
            if gross < ZERO or contaminant < ZERO or contaminant > gross:
                reviews.append(('cleanout_invalid_mass', container))
            elif gross != Decimal(batch['gross_kg']) or contaminant != Decimal(batch['contaminant_kg']):
                reviews.append(('cleanout_reweigh_mismatch', container))
        except ValueError:
            reviews.append(('cleanout_invalid_mass', container))
        if correction.get('seal') and correction['seal'] != batch['seal']:
            reviews.append(('cleanout_seal_mismatch', container))
        if batch['status'] in ('consumed', 'received'):
            reviews.append(('cleanout_already_released', f"{container}:{batch['status']}"))
        result = {'case_id': case_id, 'container': container, 'recorded': True,
                  'reviews': [reason for reason, _ in reviews]}
        with db:
            _insert_event(db, case_id, 'reweigh', moment.isoformat(),
                          correction.get('inspector', ''), {'correction': correction, 'result': result}, key)
            for reason, detail in reviews:
                _record_review(db, case_id, reason, detail, moment.isoformat())
        return result
    finally:
        db.close()


def carrier_return(db_path, case_id, container, at, actor, reason='carrier_returned'):
    """Carrier hands a container back: append a return event, keep the ship event.

    A container already signed over (received) is never silently moved back to
    on-site inventory; the attempt is recorded and flagged for review instead.
    """
    db = connect(db_path)
    try:
        payload = _case(db, case_id)
        station = payload['case']['station']
        batch = db.execute(
            "SELECT * FROM cleanout_batch WHERE case_id=? AND container_id=? AND kind='parent'",
            (case_id, container),
        ).fetchone()
        if batch is None:
            raise ValueError('cleanout_unknown_container')
        moment = _parse_time(at, 'cleanout_time_invalid')
        input_payload = {'container': container, 'at': moment.isoformat(), 'actor': actor, 'reason': reason}
        key, replayed = _replay(db, 'return', case_id, input_payload)
        if replayed is not None:
            return replayed
        reviews = []
        applied = False
        if batch['status'] == 'in_transit':
            applied = True
        elif batch['status'] == 'received':
            reviews.append(('cleanout_already_released', container))
        else:
            raise ValueError('cleanout_not_in_transit')
        result = {'case_id': case_id, 'container': container, 'returned': applied,
                  'reviews': [r for r, _ in reviews]}
        with db:
            _insert_event(db, case_id, 'return', moment.isoformat(), actor,
                          {**input_payload, 'result': result}, key)
            if applied:
                db.execute(
                    "UPDATE cleanout_batch SET status='on_site', custody=? WHERE id=?",
                    (f'site:{station}', batch['id']),
                )
            for r, detail in reviews:
                _record_review(db, case_id, r, detail, moment.isoformat())
        return result
    finally:
        db.close()


def report(db_path, case_id, out=None):
    """JSON material-flow and manifest report with decimal conservation checks."""
    db = connect(db_path)
    try:
        payload = _case(db, case_id)
        doc = payload['case']
        batches = [dict(r) for r in db.execute(
            'SELECT * FROM cleanout_batch WHERE case_id=? ORDER BY id', (case_id,))]
        events = [{'id': r['id'], 'kind': r['kind'], 'at': r['at'], 'actor': r['actor'],
                   'payload': json.loads(r['payload'])}
                  for r in db.execute(
                      'SELECT * FROM cleanout_event WHERE case_id=? ORDER BY id', (case_id,))]
        manifests = []
        for m in db.execute(
                'SELECT * FROM cleanout_manifest WHERE case_id=? ORDER BY shipped_at', (case_id,)):
            receipts = [{**dict(r), 'containers': json.loads(r['containers'])}
                        for r in db.execute(
                            'SELECT * FROM cleanout_receipt WHERE manifest_id=? ORDER BY at', (m['id'],))]
            manifests.append({**dict(m), 'containers': json.loads(m['containers']),
                              'receipts': receipts})
        reviews = [dict(r) for r in db.execute(
            'SELECT id,reason,detail,at FROM cleanout_review WHERE case_id=? ORDER BY id', (case_id,))]
        contract_row = db.execute(
            'SELECT * FROM cleanout_contract WHERE case_id=?', (case_id,)).fetchone()
        parents = [b for b in batches if b['kind'] == 'parent']
        children = [b for b in batches if b['kind'] != 'parent']

        def total(items, col='gross_kg'):
            return sum((Decimal(b[col]) for b in items), ZERO)

        active = [b for b in batches if b['status'] != 'consumed']
        reused = [b for b in children if b['kind'] == 'clean_liquid']
        residue = [b for b in children if b['kind'] == 'residue']
        by_parent = {}
        for child in children:
            by_parent.setdefault(child['parent_id'], []).append(child)
        parent_child_balanced = True
        for p in parents:
            kids = by_parent.get(p['id'], [])
            if p['status'] == 'consumed':
                if total(kids) != Decimal(p['gross_kg']) \
                        or total(kids, 'contaminant_kg') != Decimal(p['contaminant_kg']):
                    parent_child_balanced = False
            elif kids:
                parent_child_balanced = False
        totals = {
            'registered_kg': _kg(total(parents)),
            'registered_contaminant_kg': _kg(total(parents, 'contaminant_kg')),
            'reused_kg': _kg(total(reused)),
            'hazardous_residue_kg': _kg(total(residue)),
            'shipped_kg': _kg(total([b for b in parents if b['status'] in ('in_transit', 'received')])),
            'received_kg': _kg(total([b for b in parents if b['status'] == 'received'])),
            'in_transit_kg': _kg(total([b for b in parents if b['status'] == 'in_transit'])),
            'on_site_kg': _kg(total([b for b in active if b['custody'].startswith('site:')])),
            'active_inventory_kg': _kg(total(active)),
            'active_contaminant_kg': _kg(total(active, 'contaminant_kg')),
        }
        result = {
            'case_id': case_id,
            'station': doc['station'],
            'maintenance_identity': doc['maintenance_identity'],
            'maintenance_event': payload['maintenance'],
            'station_decision': payload['station_decision'],
            'measurements_sha256': payload['measurements_sha256'],
            'contracts_available': [c['id'] for c in doc['contracts']],
            'contract': ({**dict(contract_row), 'payload': json.loads(contract_row['payload'])}
                         if contract_row else None),
            'batches': batches,
            'events': events,
            'manifests': manifests,
            'reviews': reviews,
            'totals': totals,
            'conservation': {
                'mass_conserved': total(active) == total(parents),
                'contaminant_conserved': total(active, 'contaminant_kg') == total(parents, 'contaminant_kg'),
                'reuse_contaminant_free': all(Decimal(b['contaminant_kg']) == ZERO for b in reused),
                'parent_child_balanced': parent_child_balanced,
            },
        }
        if out:
            target = Path(out)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
        return result
    finally:
        db.close()
