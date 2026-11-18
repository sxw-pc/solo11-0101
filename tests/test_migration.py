import copy
import json
import sys
from pathlib import Path

import pytest

from rainstation.__main__ import main
from rainstation.ingest import freeze_config, import_file
from rainstation.migration import cancel_transfer, list_reservations, plan_transfer
from rainstation.rules import evaluate
from rainstation.store import connect

BASE = Path(__file__).parent.parent / 'fixtures'
EFF = '2026-06-01T00:00:00Z'


def _raw_case():
    return json.loads((BASE / 'station_transfer.json').read_text(encoding='utf-8'))


def setup_case(tmp_path, *, effective_at=None, loggers=None, requests=None,
               contracts=('oldest_complete_basin', 'max_completed_basins'),
               tampered=()):
    """在独立临时目录构造冻结库、转配申请与证书副本（篡改只动副本字节）。"""
    db = tmp_path / 'ledger.db'
    freeze_config(db, BASE / 'transfer_config.json')
    case = _raw_case()
    if effective_at is not None:
        case['effective_at'] = effective_at
    if loggers is not None:
        case['loggers'] = loggers
    if requests is not None:
        case['requests'] = requests
    if contracts is not None:
        case['contracts'] = list(contracts)
    cert_dir = tmp_path / 'certs'
    cert_dir.mkdir()
    for lg in case['loggers']:
        data = (BASE / 'logger_certificates' / lg['file']).read_bytes()
        if lg['serial'] in tampered:
            data += b'tampered-byte\n'
        (cert_dir / lg['file']).write_bytes(data)
    transfer = tmp_path / 'transfer.json'
    transfer.write_text(json.dumps(case, ensure_ascii=False), encoding='utf-8')
    return db, transfer, cert_dir, case


def base_loggers():
    return copy.deepcopy(_raw_case()['loggers'])


def reservation_rows(db):
    con = connect(db)
    rows = [dict(r) for r in con.execute(
        "SELECT * FROM migration_reservation ORDER BY id")]
    con.close()
    return rows


def request_rows(db):
    con = connect(db)
    rows = [dict(r) for r in con.execute(
        "SELECT group_id,status,reject_reason FROM migration_request ORDER BY id")]
    con.close()
    return rows


# ---------- 固定 fixture 上的两种合同 ----------

def test_oldest_contract_awards_both_loggers_to_basin_a(tmp_path):
    db, transfer, cert_dir, _ = setup_case(tmp_path)
    out = plan_transfer(db, transfer, cert_dir, 'oldest_complete_basin')
    assert out['confirmed_groups'] == ['BASIN-A']
    assert set(out['rejected_groups']) == {'BASIN-B', 'BASIN-C'}
    sites = {row['new_site']: row['logger_serial'] for row in out['new_sites']}
    assert set(sites) == {'UPSTREAM-0', 'UPSTREAM-1'}
    assert set(sites.values()) == {'LOGGER-01', 'LOGGER-02'}
    # 未获配组不能显示已迁：B/C 没有任何预约行
    active = list_reservations(db)
    assert {r['group_id'] for r in active} == {'BASIN-A'}
    assert len(active) == 2
    for r in active:
        assert r['effective_from'] == EFF and r['effective_to'] == '2027-01-01T00:00:00Z'
        assert r['cert_sha256'] and r['logger_model'] == 'T1'


def test_max_contract_completes_b_and_c_with_distinct_loggers(tmp_path):
    db, transfer, cert_dir, _ = setup_case(tmp_path)
    out = plan_transfer(db, transfer, cert_dir, 'max_completed_basins')
    assert set(out['confirmed_groups']) == {'BASIN-B', 'BASIN-C'}
    assert out['rejected_groups'] == ['BASIN-A']
    sites = {row['new_site']: row['logger_serial'] for row in out['new_sites']}
    assert set(sites) == {'UPSTREAM-2', 'UPSTREAM-3'}
    assert set(sites.values()) == {'LOGGER-01', 'LOGGER-02'}
    active = list_reservations(db)
    assert {r['group_id'] for r in active} == {'BASIN-B', 'BASIN-C'}
    # A 被整组拒绝，无半组站址占用
    assert {r['new_site'] for r in active}.isdisjoint({'UPSTREAM-0', 'UPSTREAM-1'})
    rejection = next(r for r in out['rejections'] if r['group'] == 'BASIN-A')
    assert any('group_not_completable' in reason for reason in rejection['reasons'])


def test_default_contract_comes_from_transfer_file(tmp_path):
    db, transfer, cert_dir, _ = setup_case(tmp_path)
    out = plan_transfer(db, transfer, cert_dir)
    assert out['contract'] == 'oldest_complete_basin'
    assert out['confirmed_groups'] == ['BASIN-A']


# ---------- 并列申请的确定性破同分 ----------

def test_tied_requests_break_by_group_name(tmp_path):
    requests = [
        {'group': 'BASIN-Y', 'requested_at': '2026-02-01T08:00:00Z',
         'stations': ['S3'], 'new_sites': ['UPSTREAM-3']},
        {'group': 'BASIN-X', 'requested_at': '2026-02-01T08:00:00Z',
         'stations': ['S2'], 'new_sites': ['UPSTREAM-2']},
    ]
    # 只留一台合格记录仪
    db, transfer, cert_dir, _ = setup_case(
        tmp_path, requests=requests, tampered={'LOGGER-02'})
    oldest = plan_transfer(db, transfer, cert_dir, 'oldest_complete_basin')
    assert oldest['confirmed_groups'] == ['BASIN-X']
    assert oldest['rejected_groups'] == ['BASIN-Y']
    assert oldest['new_sites'][0]['logger_serial'] == 'LOGGER-01'

    db2, transfer2, cert2, _ = setup_case(
        tmp_path / 'maxcase', requests=requests, tampered={'LOGGER-02'})
    most = plan_transfer(db2, transfer2, cert2, 'max_completed_basins')
    assert most['confirmed_groups'] == ['BASIN-X']
    assert most['rejected_groups'] == ['BASIN-Y']


# ---------- 证书改动 / 过期 / 边界 ----------

def test_tampered_certificate_bytes_reject_whole_group(tmp_path):
    db, transfer, cert_dir, _ = setup_case(
        tmp_path, requests=[_raw_case()['requests'][0]],
        tampered={'LOGGER-02'})
    out = plan_transfer(db, transfer, cert_dir, 'oldest_complete_basin')
    assert out['confirmed_groups'] == []
    assert out['rejected_groups'] == ['BASIN-A']
    reasons = out['rejections'][0]['reasons']
    assert any('group_not_completable' in r for r in reasons)
    assert any('cert_hash_mismatch' in r for r in reasons)
    # 原子性：没有任何预约/半组占用
    assert reservation_rows(db) == []
    assert request_rows(db)[0]['status'] == 'rejected'


def test_all_certificates_tampered_blocks_every_group(tmp_path):
    db, transfer, cert_dir, _ = setup_case(
        tmp_path, tampered={'LOGGER-01', 'LOGGER-02'})
    out = plan_transfer(db, transfer, cert_dir, 'max_completed_basins')
    assert out['confirmed_groups'] == []
    assert set(out['rejected_groups']) == {'BASIN-A', 'BASIN-B', 'BASIN-C'}
    assert reservation_rows(db) == []


def test_certificate_expiry_uses_half_open_effective_boundary(tmp_path):
    loggers = base_loggers()
    for lg in loggers:
        lg['valid_to'] = EFF  # 生效时刻恰好等于 valid_to -> 已过期
    db, transfer, cert_dir, _ = setup_case(tmp_path, loggers=loggers)
    out = plan_transfer(db, transfer, cert_dir, 'oldest_complete_basin')
    assert out['confirmed_groups'] == []
    rejection = out['rejections'][0]
    assert any('cert_expired' in r for r in rejection['reasons'])


def test_certificate_valid_from_boundary_is_inclusive(tmp_path):
    loggers = base_loggers()
    for lg in loggers:
        lg['valid_from'] = EFF  # 生效时刻恰好等于 valid_from -> 仍有效
    db, transfer, cert_dir, _ = setup_case(tmp_path, loggers=loggers)
    out = plan_transfer(db, transfer, cert_dir, 'oldest_complete_basin')
    assert out['confirmed_groups'] == ['BASIN-A']
    for r in list_reservations(db):
        assert r['valid_from'] == EFF


def test_logger_model_must_match_each_station_frozen_device_models(tmp_path):
    # LOGGER-02 改成冻结配置里不存在的 T2 型号：S0/S1 只接受 T1
    loggers = base_loggers()
    loggers[1]['model'] = 'T2'
    requests = [{'group': 'BASIN-A', 'requested_at': '2026-02-01T08:00:00Z',
                 'stations': ['S0', 'S1'],
                 'new_sites': ['UPSTREAM-0', 'UPSTREAM-1']}]
    db, transfer, cert_dir, _ = setup_case(
        tmp_path, loggers=loggers, requests=requests)
    out = plan_transfer(db, transfer, cert_dir, 'oldest_complete_basin')
    assert out['confirmed_groups'] == []
    reasons = out['rejections'][0]['reasons']
    assert any('logger_model_mismatch' in r for r in reasons)
    assert reservation_rows(db) == []


def test_unknown_station_model_is_rejected_before_matching(tmp_path):
    # 冻结站 S2 的 device_models 是 D2->T1；记录仪全为 T1 时正常，
    # 这里把两台都改成 T2，使每个站点都型号不符
    loggers = base_loggers()
    for lg in loggers:
        lg['model'] = 'T2'
    db, transfer, cert_dir, _ = setup_case(tmp_path, loggers=loggers)
    out = plan_transfer(db, transfer, cert_dir, 'max_completed_basins')
    assert out['confirmed_groups'] == []
    for rejection in out['rejections']:
        assert any('logger_model_mismatch' in r for r in rejection['reasons'])
    assert reservation_rows(db) == []


# ---------- 单组原子性 ----------

def test_two_station_group_with_one_logger_leaves_no_half_reservation(tmp_path):
    db, transfer, cert_dir, _ = setup_case(
        tmp_path, requests=[_raw_case()['requests'][0]],
        tampered={'LOGGER-01'})
    for contract in ('oldest_complete_basin', 'max_completed_basins'):
        d = tmp_path / contract
        d.mkdir()
        db2, transfer2, cert2, _ = setup_case(
            d, requests=[_raw_case()['requests'][0]],
            tampered={'LOGGER-01'})
        out = plan_transfer(db2, transfer2, cert2, contract)
        assert out['confirmed_groups'] == []
        assert out['rejected_groups'] == ['BASIN-A']
        assert reservation_rows(db2) == []


# ---------- 序列号与站址重叠 ----------

def test_logger_serial_cannot_be_shared_across_groups(tmp_path):
    requests = [
        {'group': 'BASIN-B', 'requested_at': '2026-02-02T08:00:00Z',
         'stations': ['S2'], 'new_sites': ['UPSTREAM-2']},
        {'group': 'BASIN-C', 'requested_at': '2026-02-03T08:00:00Z',
         'stations': ['S3'], 'new_sites': ['UPSTREAM-3']},
    ]
    db, transfer, cert_dir, _ = setup_case(
        tmp_path, requests=requests, tampered={'LOGGER-02'})
    out = plan_transfer(db, transfer, cert_dir, 'oldest_complete_basin')
    assert out['confirmed_groups'] == ['BASIN-B']
    assert out['rejected_groups'] == ['BASIN-C']
    serials = [r['logger_serial'] for r in reservation_rows(db)]
    assert serials == ['LOGGER-01']  # 没有同序列号重复交付


def test_new_site_conflict_within_same_batch_is_rejected(tmp_path):
    requests = [
        {'group': 'BASIN-B', 'requested_at': '2026-02-02T08:00:00Z',
         'stations': ['S2'], 'new_sites': ['UPSTREAM-2']},
        {'group': 'BASIN-C', 'requested_at': '2026-02-03T08:00:00Z',
         'stations': ['S3'], 'new_sites': ['UPSTREAM-2']},
    ]
    db, transfer, cert_dir, _ = setup_case(tmp_path, requests=requests)
    out = plan_transfer(db, transfer, cert_dir, 'max_completed_basins')
    assert out['confirmed_groups'] == ['BASIN-B']
    rejection = next(r for r in out['rejections'] if r['group'] == 'BASIN-C')
    assert any('new_site_conflict' in r for r in rejection['reasons'])
    assert all(r['new_site'] != 'UPSTREAM-2' or r['group_id'] == 'BASIN-B'
               for r in list_reservations(db))


def test_new_site_and_station_conflict_against_existing_reservation(tmp_path):
    first = [{'group': 'BASIN-B', 'requested_at': '2026-02-02T08:00:00Z',
              'stations': ['S2'], 'new_sites': ['UPSTREAM-2']}]
    db, transfer, cert_dir, _ = setup_case(tmp_path, requests=first)
    plan_transfer(db, transfer, cert_dir, 'oldest_complete_basin')

    second = [{'group': 'BASIN-D', 'requested_at': '2026-03-01T08:00:00Z',
               'stations': ['S3'], 'new_sites': ['UPSTREAM-2']}]
    _, transfer2, cert2, _ = setup_case(tmp_path / 'second', requests=second)
    out = plan_transfer(db, transfer2, cert2, 'oldest_complete_basin')
    assert any('new_site_conflict' in r for r in out['rejections'][0]['reasons'])

    third = [{'group': 'BASIN-E', 'requested_at': '2026-03-02T08:00:00Z',
              'stations': ['S2'], 'new_sites': ['UPSTREAM-9']}]
    _, transfer3, cert3, _ = setup_case(tmp_path / 'third', requests=third)
    out = plan_transfer(db, transfer3, cert3, 'oldest_complete_basin')
    assert any('station_already_reserved' in r
               for r in out['rejections'][0]['reasons'])


# ---------- 未知/重复站点 ----------

def test_unknown_and_duplicate_stations_get_audit_reasons(tmp_path):
    requests = [
        {'group': 'BASIN-X', 'requested_at': '2026-02-01T08:00:00Z',
         'stations': ['S9'], 'new_sites': ['UPSTREAM-9']},
        {'group': 'BASIN-Y', 'requested_at': '2026-02-02T08:00:00Z',
         'stations': ['S0', 'S0'], 'new_sites': ['UPSTREAM-0', 'UPSTREAM-1']},
        {'group': 'BASIN-Z', 'requested_at': '2026-02-03T08:00:00Z',
         'stations': ['S2'], 'new_sites': ['UP-2', 'UP-3']},
    ]
    db, transfer, cert_dir, _ = setup_case(tmp_path, requests=requests)
    out = plan_transfer(db, transfer, cert_dir, 'oldest_complete_basin')
    reasons = {r['group']: r['reasons'] for r in out['rejections']}
    assert any('unknown_station' in r for r in reasons['BASIN-X'])
    assert 'duplicate_station_in_request' in reasons['BASIN-Y']
    assert 'station_site_count_mismatch' in reasons['BASIN-Z']
    assert reservation_rows(db) == []
    con = connect(db)
    audit = [r['action'] for r in con.execute(
        'SELECT action FROM migration_audit ORDER BY id')]
    con.close()
    assert audit == ['reject', 'reject', 'reject']


def test_plan_requires_frozen_config(tmp_path):
    db = tmp_path / 'empty.db'
    transfer = tmp_path / 'transfer.json'
    transfer.write_text((BASE / 'station_transfer.json').read_text())
    with pytest.raises(ValueError, match='config_not_frozen'):
        plan_transfer(db, transfer, BASE / 'logger_certificates')


# ---------- 幂等重跑 ----------

def test_same_application_rerun_is_idempotent(tmp_path):
    db, transfer, cert_dir, _ = setup_case(tmp_path)
    first = plan_transfer(db, transfer, cert_dir, 'oldest_complete_basin')
    second = plan_transfer(db, transfer, cert_dir, 'oldest_complete_basin')
    assert first['new_sites'] == second['new_sites']
    assert all(c['idempotent_rerun'] for c in second['confirmed'])
    assert all(r['idempotent_rerun'] for r in second['rejections'])
    # 台账没有新增预约/请求，审计只保留首次 reserve
    assert len(reservation_rows(db)) == 2
    assert len(request_rows(db)) == 3
    con = connect(db)
    assert con.execute(
        "SELECT count(*) FROM migration_audit WHERE action='reserve'").fetchone()[0] == 2
    con.close()


# ---------- 取消后可追溯的再分配 ----------

def test_cancel_frees_devices_and_reenables_reallocation(tmp_path):
    db, transfer, cert_dir, _ = setup_case(
        tmp_path, contracts=('max_completed_basins',))
    out = plan_transfer(db, transfer, cert_dir, 'max_completed_basins')
    assert set(out['confirmed_groups']) == {'BASIN-B', 'BASIN-C'}

    cancelled = cancel_transfer(db, 'BASIN-B')
    assert {r['logger_serial'] for r in cancelled['released']} <= {
        'LOGGER-01', 'LOGGER-02'}
    assert {r['group_id'] for r in list_reservations(db)} == {'BASIN-C'}
    # 取消记录保留在台账与审计中
    con = connect(db)
    statuses = [r['status'] for r in con.execute(
        "SELECT status FROM migration_reservation ORDER BY id")]
    assert 'cancelled' in statuses and 'active' in statuses
    assert con.execute(
        "SELECT count(*) FROM migration_audit WHERE action='cancel'").fetchone()[0] == 1
    con.close()

    # 同一份申请重跑：B 已取消，按新的资源现状重新获配
    rerun = plan_transfer(db, transfer, cert_dir, 'max_completed_basins')
    assert set(rerun['confirmed_groups']) == {'BASIN-B', 'BASIN-C'}
    b = next(c for c in rerun['confirmed'] if c['group'] == 'BASIN-B')
    c = next(c for c in rerun['confirmed'] if c['group'] == 'BASIN-C')
    b_serials = {a['logger_serial'] for a in b['assignments']}
    c_serials = {a['logger_serial'] for a in c['assignments']}
    assert b_serials.isdisjoint(c_serials)
    assert b_serials | c_serials == {'LOGGER-01', 'LOGGER-02'}
    con = connect(db)
    actions = [r['action'] for r in con.execute(
        "SELECT action FROM migration_audit ORDER BY id")]
    assert actions.count('reserve') == 3 and actions.count('cancel') == 1
    con.close()


def test_cancel_unknown_group_raises(tmp_path):
    db, transfer, cert_dir, _ = setup_case(tmp_path)
    plan_transfer(db, transfer, cert_dir, 'oldest_complete_basin')
    with pytest.raises(ValueError, match='group_not_reserved'):
        cancel_transfer(db, 'BASIN-B')


# ---------- 历史降雨不被重新解释 ----------

def test_transfer_never_touches_frozen_history(tmp_path):
    db = tmp_path / 'ledger.db'
    freeze_config(db, BASE / 'transfer_config.json')
    import_file(db, BASE / 'events.jsonl')
    before = evaluate(db)
    con = connect(db)
    config_hash = con.execute(
        "SELECT value FROM meta WHERE key='config_hash'").fetchone()[0]
    config_json = con.execute(
        "SELECT value FROM meta WHERE key='config_json'").fetchone()[0]
    events_before = [tuple(r) for r in con.execute(
        'SELECT identity,sha256,payload FROM event ORDER BY id')]
    sources_before = con.execute('SELECT count(*) FROM source').fetchone()[0]
    decision_before = dict(con.execute(
        'SELECT * FROM decision WHERE subject="S1"').fetchone())
    evidence_before = con.execute('SELECT count(*) FROM evidence').fetchone()[0]
    con.close()

    _, transfer, cert_dir, _ = setup_case(tmp_path)
    plan_transfer(db, transfer, cert_dir, 'max_completed_basins')
    after = evaluate(db)

    con = connect(db)
    assert con.execute(
        "SELECT value FROM meta WHERE key='config_hash'").fetchone()[0] == config_hash
    assert con.execute(
        "SELECT value FROM meta WHERE key='config_json'").fetchone()[0] == config_json
    events_after = [tuple(r) for r in con.execute(
        'SELECT identity,sha256,payload FROM event ORDER BY id')]
    assert events_after == events_before
    assert con.execute('SELECT count(*) FROM source').fetchone()[0] == sources_before
    decision_after = dict(con.execute(
        'SELECT * FROM decision WHERE subject="S1"').fetchone())
    assert decision_after['details'] == decision_before['details']
    assert decision_after['status'] == decision_before['status'] == before['S1']
    assert con.execute('SELECT count(*) FROM evidence').fetchone()[0] == evidence_before
    # 历史脉冲仍按冻结的 D1->T1 解释，与新记录仪序列号无关
    details = json.loads(decision_after['details'])
    assert details['hourly_mm'] == {'2026-01-01T08:00:00+00:00': 5.0}
    con.close()
    assert after['S1'] == 'trusted'


# ---------- CLI ----------

def test_cli_transfer_list_and_cancel(tmp_path, monkeypatch, capsys):
    db, transfer, cert_dir, _ = setup_case(tmp_path)
    monkeypatch.setattr(sys, 'argv', [
        'rainstation', 'transfer', '--db', str(db),
        '--transfer', str(transfer), '--cert-dir', str(cert_dir)])
    main()
    out = json.loads(capsys.readouterr().out)
    assert out['confirmed_groups'] == ['BASIN-A']
    assert len(out['new_sites']) == 2

    monkeypatch.setattr(sys, 'argv', [
        'rainstation', 'transfer-list', '--db', str(db)])
    main()
    listed = json.loads(capsys.readouterr().out)
    assert {r['new_site'] for r in listed} == {'UPSTREAM-0', 'UPSTREAM-1'}

    monkeypatch.setattr(sys, 'argv', [
        'rainstation', 'transfer-cancel', '--db', str(db),
        '--group', 'BASIN-A'])
    main()
    cancelled = json.loads(capsys.readouterr().out)
    assert cancelled['status'] == 'cancelled'
    assert len(cancelled['released']) == 2
    assert list_reservations(db) == []
