import hashlib
import json
import sys
import threading
from pathlib import Path

import pytest

from rainstation.__main__ import main
from rainstation.ingest import freeze_config, import_file
from rainstation.rules import evaluate
from rainstation.store import connect
from rainstation.water_attributes import (
    import_attribute_contract,
    import_tank_log,
    issue_attributes,
    list_certificates,
    recall_queue,
    transfer_certificate,
)

BASE = Path(__file__).parent.parent / 'fixtures'
HOUR = '2026-01-01T08:00:00Z'
HOUR_KEY = '2026-01-01T08:00:00+00:00'


def make_db(tmp_path, run_evaluate=True, with_tank=True, with_contract=True):
    db = tmp_path / 'water.db'
    freeze_config(db, BASE / 'water_config.json')
    import_file(db, BASE / 'water_events.jsonl')
    if run_evaluate:
        assert evaluate(db)['S1'] == 'trusted'
    if with_tank:
        import_tank_log(db, BASE / 'water_tank_log.json')
    if with_contract:
        import_attribute_contract(db, BASE / 'water_attribute_contract.json')
    return db


def write_json(path, payload):
    path.write_text(json.dumps(payload), encoding='utf-8')
    return path


def tank_payload(**over):
    tank = {'tank_id': 'TANK-01', 'station': 'S1', 'hour': HOUR, 'capacity_m3': 2.0,
            'opening_m3': 0.0, 'closing_m3': 1.6, 'outflow_m3': 0.0, 'overflow_m3': 0.4}
    tank.update(over)
    return tank


def import_tampered_contract(db, tmp_path):
    d = tmp_path / 'tampered'
    (d / 'water_contracts').mkdir(parents=True)
    (d / 'water_contracts' / 'WATER-001.txt').write_text('tampered agreement\n', encoding='utf-8')
    contract = json.loads((BASE / 'water_attribute_contract.json').read_text(encoding='utf-8'))
    write_json(d / 'water_attribute_contract.json', contract)  # 声明哈希仍是原文的
    return import_attribute_contract(db, d / 'water_attribute_contract.json')


def test_gross_title_issues_ten_transferable_certificates(tmp_path):
    db = make_db(tmp_path)
    r = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    assert r['status'] == 'issued'
    assert r['holder'] == 'WATERSHED-AGENCY'
    assert r['certificate_count'] == 10
    assert r['volume_m3'] == pytest.approx(2.0)
    assert r['remainder_m3'] == 0
    certs = r['certificates']
    assert len({c['id'] for c in certs}) == 10
    assert [c['seq'] for c in certs] == list(range(1, 11))
    assert all(c['holder'] == 'WATERSHED-AGENCY' for c in certs)
    assert all(c['share_m3'] == pytest.approx(0.2) for c in certs)
    assert all(c['station'] == 'S1' and c['hour'] == HOUR_KEY for c in certs)
    basis = r['basis']
    assert basis['decision']['rule_version'] == '1'
    assert len(basis['evidence']) == 4
    assert basis['tank_log_sha256'] and basis['contract_file_sha256'] and basis['config_hash']


def test_retained_title_covers_only_stored_water(tmp_path):
    db = make_db(tmp_path)
    r = issue_attributes(db, 'S1', HOUR, 'retained_water_title')
    assert r['status'] == 'issued'
    assert r['holder'] == 'FACILITY-OPERATOR'
    assert r['certificate_count'] == 8
    assert r['volume_m3'] == pytest.approx(1.6)  # 溢流 0.4m³ 不归设施方
    assert sum(c['share_m3'] for c in r['certificates']) == pytest.approx(1.6)


def test_two_titles_cannot_be_active_for_same_hour(tmp_path):
    db = make_db(tmp_path)
    assert issue_attributes(db, 'S1', HOUR, 'gross_rain_title')['status'] == 'issued'
    second = issue_attributes(db, 'S1', HOUR, 'retained_water_title')
    assert second['status'] == 'pending' and second['code'] == 'title_conflict'
    active = list_certificates(db, station='S1', status='active')
    assert len(active) == 10
    assert {c['title'] for c in active} == {'gross_rain_title'}


def test_concurrent_claims_leave_exactly_one_title_active(tmp_path):
    db = make_db(tmp_path)
    results = {}

    def claim(title):
        results[title] = issue_attributes(db, 'S1', HOUR, title)

    threads = [threading.Thread(target=claim, args=(t,))
               for t in ('gross_rain_title', 'retained_water_title')]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    statuses = sorted(r['status'] for r in results.values())
    assert statuses == ['issued', 'pending']
    loser = [r for r in results.values() if r['status'] == 'pending'][0]
    assert loser['code'] == 'title_conflict'
    active = list_certificates(db, station='S1', status='active')
    titles = {c['title'] for c in active}
    assert len(titles) == 1
    assert len(active) == (10 if titles == {'gross_rain_title'} else 8)


def test_missing_tank_log_goes_to_pending(tmp_path):
    db = make_db(tmp_path, with_tank=False)
    r = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    assert r['status'] == 'pending' and r['code'] == 'tank_log_missing'
    assert list_certificates(db, station='S1') == []


def test_untrusted_decision_goes_to_pending(tmp_path):
    db = make_db(tmp_path, run_evaluate=False)
    r = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    assert r['status'] == 'pending' and r['code'] == 'hour_not_trusted'
    assert list_certificates(db, station='S1') == []


def test_unobserved_hour_is_not_guessed_and_hours_stay_isolated(tmp_path):
    db = make_db(tmp_path)
    assert issue_attributes(db, 'S1', HOUR, 'gross_rain_title')['status'] == 'issued'
    r = issue_attributes(db, 'S1', '2026-01-01T09:00:00Z', 'gross_rain_title')
    assert r['status'] == 'pending' and r['code'] == 'hour_not_observed'
    active = list_certificates(db, station='S1', status='active')
    assert len(active) == 10
    assert {c['hour'] for c in active} == {HOUR_KEY}


def test_mass_balance_must_close(tmp_path):
    db = make_db(tmp_path)
    write_json(tmp_path / 'tank_bad.json', tank_payload(closing_m3=1.5))
    import_tank_log(db, tmp_path / 'tank_bad.json')
    r = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    assert r['status'] == 'pending' and r['code'] == 'mass_balance_broken'
    assert list_certificates(db, station='S1') == []


def test_capacity_bounds_are_enforced(tmp_path):
    db = make_db(tmp_path)
    # 1.0 + 2.0 = 2.2 + 0.8 质量闭合，但期末 2.2m³ 超过 2m³ 罐容
    write_json(tmp_path / 'tank_cap.json',
               tank_payload(opening_m3=1.0, closing_m3=2.2, overflow_m3=0.8))
    import_tank_log(db, tmp_path / 'tank_cap.json')
    r = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    assert r['status'] == 'pending' and r['code'] == 'capacity_exceeded'


def test_negative_tank_values_are_rejected(tmp_path):
    db = make_db(tmp_path)
    write_json(tmp_path / 'tank_neg.json', tank_payload(closing_m3=2.0, overflow_m3=-0.4))
    import_tank_log(db, tmp_path / 'tank_neg.json')
    r = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    assert r['status'] == 'pending' and r['code'] == 'tank_negative'


def test_retained_unavailable_when_storage_decreases(tmp_path):
    db = make_db(tmp_path)
    # 1.6 + 2.0 = 1.0 + 0.4 + 2.2 闭合，但罐中蓄水净减 0.6m³
    write_json(tmp_path / 'tank_drain.json',
               tank_payload(opening_m3=1.6, closing_m3=1.0, outflow_m3=2.2))
    import_tank_log(db, tmp_path / 'tank_drain.json')
    r = issue_attributes(db, 'S1', HOUR, 'retained_water_title')
    assert r['status'] == 'pending' and r['code'] == 'retained_unavailable'


def test_contract_tampering_goes_to_pending(tmp_path):
    db = make_db(tmp_path)
    import_tampered_contract(db, tmp_path)
    r = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    assert r['status'] == 'pending' and r['code'] == 'contract_sha_mismatch'
    assert list_certificates(db, station='S1') == []


def test_tamper_after_issuance_revokes_active_certificates(tmp_path):
    db = make_db(tmp_path)
    assert issue_attributes(db, 'S1', HOUR, 'gross_rain_title')['status'] == 'issued'
    import_tampered_contract(db, tmp_path)
    r = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    assert r['status'] == 'pending' and r['code'] == 'contract_sha_mismatch'
    certs = list_certificates(db, station='S1')
    assert len(certs) == 10
    assert {c['status'] for c in certs} == {'revoked'}


def test_rule_version_conflict_goes_to_pending(tmp_path):
    db = make_db(tmp_path)
    con = connect(db)
    con.execute("UPDATE evidence SET rule_version='99'")
    con.commit()
    con.close()
    r = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    assert r['status'] == 'pending' and r['code'] == 'rule_version_conflict'


def test_unknown_title_goes_to_pending(tmp_path):
    db = make_db(tmp_path)
    r = issue_attributes(db, 'S1', HOUR, 'vapor_title')
    assert r['status'] == 'pending' and r['code'] == 'unknown_title'


def test_same_snapshot_reissue_is_idempotent(tmp_path):
    db = make_db(tmp_path)
    first = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    second = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    assert first['status'] == 'issued' and second['status'] == 'existing'
    assert [c['id'] for c in second['certificates']] == [c['id'] for c in first['certificates']]
    assert len(list_certificates(db, station='S1')) == 10


def test_revision_recalls_transferred_and_reissues_atomically(tmp_path):
    db = make_db(tmp_path)
    first = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    moved = transfer_certificate(db, first['certificates'][0]['id'], 'BUYER-1')
    assert moved['holder'] == 'BUYER-1' and moved['transferred'] == 1
    write_json(tmp_path / 'tank_v2.json', tank_payload(closing_m3=1.8, overflow_m3=0.2))
    import_tank_log(db, tmp_path / 'tank_v2.json')
    second = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    assert second['status'] == 'issued'
    assert second['basis']['hash'] != first['basis']['hash']
    assert second['certificate_count'] == 10
    new_ids = {c['id'] for c in second['certificates']}
    assert first['certificates'][0]['id'] not in new_ids
    old = {c['id']: c for c in list_certificates(db, station='S1') if c['id'] not in new_ids}
    assert len(old) == 10
    transferred_id = first['certificates'][0]['id']
    assert old[transferred_id]['status'] == 'recall_pending'
    assert old[transferred_id]['holder'] == 'BUYER-1'  # 已转让凭证不静默换主
    assert all(c['status'] == 'revoked' for k, c in old.items() if k != transferred_id)
    recalls = recall_queue(db)
    assert len(recalls) == 1
    assert recalls[0]['cert_id'] == transferred_id and recalls[0]['holder'] == 'BUYER-1'
    active = list_certificates(db, station='S1', status='active')
    assert len(active) == 10
    assert {c['holder'] for c in active} == {'WATERSHED-AGENCY'}


def test_retained_revision_changes_certificate_count(tmp_path):
    db = make_db(tmp_path)
    first = issue_attributes(db, 'S1', HOUR, 'retained_water_title')
    assert first['certificate_count'] == 8
    write_json(tmp_path / 'tank_v3.json', tank_payload(closing_m3=1.2, overflow_m3=0.8))
    import_tank_log(db, tmp_path / 'tank_v3.json')
    second = issue_attributes(db, 'S1', HOUR, 'retained_water_title')
    assert second['status'] == 'issued'
    assert second['certificate_count'] == 6
    assert second['volume_m3'] == pytest.approx(1.2)
    assert len(list_certificates(db, station='S1', status='active')) == 6
    assert len(list_certificates(db, station='S1', status='revoked')) == 8


def test_contract_revision_reissues_on_new_basis(tmp_path):
    db = make_db(tmp_path)
    first = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    d = tmp_path / 'revised'
    (d / 'water_contracts').mkdir(parents=True)
    new_bytes = b'Synthetic water conservation attribute agreement; unit 0.2 m3; v2\n'
    (d / 'water_contracts' / 'WATER-001.txt').write_bytes(new_bytes)
    contract = json.loads((BASE / 'water_attribute_contract.json').read_text(encoding='utf-8'))
    contract['sha256'] = hashlib.sha256(new_bytes).hexdigest()
    write_json(d / 'water_attribute_contract.json', contract)
    import_attribute_contract(db, d / 'water_attribute_contract.json')
    second = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    assert second['status'] == 'issued'
    assert second['basis']['hash'] != first['basis']['hash']
    assert len(list_certificates(db, station='S1', status='revoked')) == 10
    assert len(list_certificates(db, station='S1', status='active')) == 10


def test_remainder_carries_over_only_within_same_contract(tmp_path):
    events = [
        {'kind': 'anchor', 'subject': 'S1', 'identity': 'D1:1', 'device': 'D1', 'seq': 1,
         'time': '2026-01-01T08:00:00Z', 'offset_seconds': 0},
        {'kind': 'heartbeat', 'subject': 'S1', 'identity': 'D1:2', 'device': 'D1', 'seq': 2,
         'time': '2026-01-01T08:00:00Z'},
        {'kind': 'pulse', 'subject': 'S1', 'identity': 'D1:3', 'device': 'D1', 'seq': 3,
         'time': '2026-01-01T08:10:00Z', 'count': 17},
        {'kind': 'heartbeat', 'subject': 'S1', 'identity': 'D1:4', 'device': 'D1', 'seq': 4,
         'time': '2026-01-01T08:30:00Z'},
        {'kind': 'heartbeat', 'subject': 'S1', 'identity': 'D1:5', 'device': 'D1', 'seq': 5,
         'time': '2026-01-01T09:00:00Z'},
        {'kind': 'pulse', 'subject': 'S1', 'identity': 'D1:6', 'device': 'D1', 'seq': 6,
         'time': '2026-01-01T09:05:00Z', 'count': 1},
        {'kind': 'heartbeat', 'subject': 'S1', 'identity': 'D1:7', 'device': 'D1', 'seq': 7,
         'time': '2026-01-01T09:30:00Z'},
    ]
    db = tmp_path / 'water.db'
    freeze_config(db, BASE / 'water_config.json')
    p = tmp_path / 'events.jsonl'
    p.write_text('\n'.join(json.dumps(e) for e in events) + '\n', encoding='utf-8')
    import_file(db, p)
    assert evaluate(db)['S1'] == 'trusted'
    import_attribute_contract(db, BASE / 'water_attribute_contract.json')
    # 08:00 集雨 1.7m³：8 张凭证 + 0.1m³ 零头，不得四舍五入出第 9 张
    write_json(tmp_path / 'tank_h1.json', tank_payload(closing_m3=1.7, overflow_m3=0.0))
    import_tank_log(db, tmp_path / 'tank_h1.json')
    r1 = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    assert r1['status'] == 'issued'
    assert r1['certificate_count'] == 8
    assert r1['remainder_m3'] == pytest.approx(0.1)
    con = connect(db)
    rows = con.execute('SELECT * FROM attribute_carryover').fetchall()
    con.close()
    assert len(rows) == 1 and rows[0]['remainder_m3'] == pytest.approx(0.1)
    again = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    assert again['status'] == 'existing' and again['certificate_count'] == 8
    # 09:00 集雨 0.1m³ + 同合同结转 0.1m³ = 0.2m³，发行 1 张
    write_json(tmp_path / 'tank_h2.json',
               tank_payload(hour='2026-01-01T09:00:00Z', opening_m3=1.7, closing_m3=1.8,
                            overflow_m3=0.0))
    import_tank_log(db, tmp_path / 'tank_h2.json')
    r2 = issue_attributes(db, 'S1', '2026-01-01T09:00:00Z', 'gross_rain_title')
    assert r2['status'] == 'issued'
    assert r2['certificate_count'] == 1
    assert r2['carried_in_m3'] == pytest.approx(0.1)
    assert r2['remainder_m3'] == 0
    con = connect(db)
    assert con.execute('SELECT count(*) FROM attribute_carryover').fetchone()[0] == 0
    con.close()
    assert len(list_certificates(db, station='S1', status='active')) == 9


def test_issuance_never_rewrites_rain_evidence(tmp_path):
    db = make_db(tmp_path)

    def snapshot():
        con = connect(db)
        data = {t: sorted(map(tuple, con.execute(f'SELECT * FROM {t}').fetchall()))
                for t in ('event', 'decision', 'evidence')}
        data['source'] = sorted(map(tuple, con.execute('SELECT id,path,sha256 FROM source').fetchall()))
        con.close()
        return data

    before = snapshot()
    first = issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    transfer_certificate(db, first['certificates'][0]['id'], 'BUYER-1')
    write_json(tmp_path / 'tank_v2.json', tank_payload(closing_m3=1.8, overflow_m3=0.2))
    import_tank_log(db, tmp_path / 'tank_v2.json')
    issue_attributes(db, 'S1', HOUR, 'gross_rain_title')
    assert snapshot() == before


def test_cli_issue_command(tmp_path, monkeypatch, capsys):
    db = tmp_path / 'water.db'
    freeze_config(db, BASE / 'water_config.json')
    import_file(db, BASE / 'water_events.jsonl')
    evaluate(db)
    monkeypatch.setattr(sys, 'argv', [
        'rainstation', 'issue', '--db', str(db), '--station', 'S1', '--hour', HOUR,
        '--title', 'retained_water_title',
        '--tank-log', str(BASE / 'water_tank_log.json'),
        '--contract', str(BASE / 'water_attribute_contract.json')])
    main()
    out = json.loads(capsys.readouterr().out)
    assert out['status'] == 'issued'
    assert out['certificate_count'] == 8
    assert out['holder'] == 'FACILITY-OPERATOR'
    assert out['certificates'][0]['id'].startswith('WAC-S1-')
    assert out['certificates'][0]['share_m3'] == pytest.approx(0.2)
