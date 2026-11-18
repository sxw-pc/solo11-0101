"""设备型号外部披露的安全发布测试。

覆盖：多份重叠集合的线性联立推断、二元全零/全一极端推断、最小安全扩展、
受众隔离、并发竞争串行化、文件/台账中断恢复、外部文件无单站字段、
内部报告授权隔离与请求重放幂等。
"""
import hashlib
import json
import threading
from pathlib import Path

import pytest

from rainstation.ingest import freeze_config, import_file
from rainstation.rules import evaluate
from rainstation.report import export, INTERNAL_AUDIT_ROLE
from rainstation.store import connect
from rainstation.disclosure import (
    DisclosureError, forced_stations, is_safe, feasible_assignments,
    seed_prior_release, publish_request, reconcile, ledger_view,
    audience_facts, t2_stations, build_external_payload, file_digest,
    DELIVERED, DENIED, PENDING,
)

FIXTURES = Path(__file__).parents[1] / 'fixtures'
# 外部文件禁止出现的单站级字段/标识。metric 固定为 T2_device_count（任务
# 明确允许的汇总指标名），因此按完整键名与设备标识检查，不做裸子串匹配。
FORBIDDEN_KEYS = ('device_model', 'device', 'payload', 'source', 'path',
                  'raw_line', 'line_no', 'sha256')
FORBIDDEN_TOKENS = ('D1', 'D2', 'D3', 'D4', '"T1"')


def assert_external_file_clean(text):
    data = json.loads(text)
    assert set(data) == {'release_id', 'stations', 't2_count',
                         'batch_id', 'rule_version', 'metric'}
    assert data['metric'] == 'T2_device_count'

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                assert key not in FORBIDDEN_KEYS, key
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(data)
    for token in FORBIDDEN_TOKENS:
        assert token not in text


def build_db(tmp_path, name='dd.db', config='device_disclosure_config.json',
             events='device_disclosure_events.jsonl'):
    db = tmp_path / name
    freeze_config(db, FIXTURES / config)
    if events:
        import_file(db, FIXTURES / events)
    evaluate(db)
    return db


def seed_vendor(db):
    spec = json.loads((FIXTURES / 'device_disclosure_requests.json').read_text())
    seed_prior_release(db, spec['audience'], spec['previous_release'])
    return spec


# ---------------------------------------------------------------------------
# 推断引擎：多份重叠线性推断与二元极端
# ---------------------------------------------------------------------------

def test_multi_overlap_linear_inference_forces_station():
    # {S1,S2}=1 与 {S1,S2,S3}=2 联立：S3 被唯一强制为 1（T2）。
    facts = [(('S1', 'S2'), 1), (('S1', 'S2', 'S3'), 2)]
    forced = forced_stations(('S1', 'S2', 'S3'), facts)
    assert forced == {'S3': 1}
    assert not is_safe(('S1', 'S2', 'S3'), facts)


def test_safe_overlapping_releases_do_not_force():
    # REL-01 之后发布 {S3,S4} 计数 1：两站恰有一个 T2，谁也定不出。
    facts = [(('S1', 'S2'), 1), (('S3', 'S4'), 1)]
    assert forced_stations(('S1', 'S2', 'S3', 'S4'), facts) == {}
    assert is_safe(('S1', 'S2', 'S3', 'S4'), facts)
    assert len(feasible_assignments(('S1', 'S2', 'S3', 'S4'), facts)) == 4


def test_binary_extreme_all_zero_group_is_detected():
    # 全集计数为 0：所有站点被强制为 0（全零极端）。
    facts = [(('S1', 'S2', 'S3'), 0)]
    assert forced_stations(('S1', 'S2', 'S3'), facts) == {
        'S1': 0, 'S2': 0, 'S3': 0}


def test_binary_extreme_all_one_group_is_detected():
    # 全集计数为集合大小：所有站点被强制为 1（全一极端）。
    facts = [(('S1', 'S2'), 2)]
    assert forced_stations(('S1', 'S2'), facts) == {'S1': 1, 'S2': 1}


def test_single_station_release_is_never_safe():
    # 单站集合的计数直接暴露该站二元取值。
    assert not is_safe(('S1', 'S2'), [(('S1',), 1)])
    assert forced_stations(('S1', 'S2'), [(('S1',), 0)]) == {'S1': 0}


def test_infeasible_fact_combination_raises():
    with pytest.raises(DisclosureError, match='infeasible'):
        forced_stations(('S1', 'S2'), [(('S1', 'S2'), 1), (('S1',), 1),
                                       (('S2',), 1)])


# ---------------------------------------------------------------------------
# 端到端：REQ-02 两条合规路线
# ---------------------------------------------------------------------------

def test_fixture_t2_truth(tmp_path):
    # S1/S3 为 T2，S2/S4 为 T1。
    assert t2_stations(build_db(tmp_path)) == {'S1', 'S3'}


def test_deny_unsafe_blocks_subtraction_leak(tmp_path):
    db = build_db(tmp_path)
    spec = seed_vendor(db)
    out = tmp_path / 'out'
    row = publish_request(db, spec['audience'], spec['new_request'], out,
                          'deny_unsafe', spec.get('allowed_expansion', ()),
                          spec['station_universe'])
    assert row['status'] == DENIED
    assert row['reason'] == 'inference_unsafe'
    assert row['release_id'] is None and row['t2_count'] is None
    # 拒绝不产生任何外部文件，也不向已披露知识追加事实。
    assert not out.exists() or not list(out.glob('REL-*.json'))
    facts = audience_facts(db, spec['audience'])
    assert facts == [(('S1', 'S2'), 1)]


def test_minimum_safe_coarsening_publishes_four_station_count(tmp_path):
    db = build_db(tmp_path)
    spec = seed_vendor(db)
    out = tmp_path / 'out'
    row = publish_request(db, spec['audience'], spec['new_request'], out,
                          'minimum_safe_coarsening',
                          spec['allowed_expansion'], spec['station_universe'])
    assert row['status'] == DELIVERED
    assert row['release_id'] == 'REL-02'
    assert json.loads(row['stations_json']) == ['S1', 'S2', 'S3', 'S4']
    assert row['t2_count'] == 2  # 真实四站 T2 数
    external = json.loads((out / 'REL-02.json').read_text(encoding='utf-8'))
    assert set(external) == {'release_id', 'stations', 't2_count',
                             'batch_id', 'rule_version', 'metric'}
    assert external['t2_count'] == 2 and external['stations'] == ['S1', 'S2', 'S3', 'S4']
    text = (out / 'REL-02.json').read_text(encoding='utf-8')
    assert_external_file_clean(text)
    # 发布后外部仍无法单独定出 S3：S3/S4 中恰有一个 T2。
    facts = audience_facts(db, spec['audience'])
    assert forced_stations(spec['station_universe'], facts) == {}


def test_coarsening_denies_when_no_safe_expansion(tmp_path):
    db = build_db(tmp_path)
    spec = seed_vendor(db)
    out = tmp_path / 'out'
    row = publish_request(db, spec['audience'], spec['new_request'], out,
                          'minimum_safe_coarsening', (), spec['station_universe'])
    assert row['status'] == DENIED
    assert row['reason'] == 'no_safe_expansion'
    assert not (out / 'REL-02.json').exists()


def _write_custom_db(tmp_path, models):
    """models: {station: model}，生成对应配置与事件并入库。"""
    stations = []
    lines = []
    for idx, (station, model) in enumerate(models.items(), 1):
        device = f'D{idx}'
        stations.append({
            'id': station, 'offset_minutes': 0,
            'device_models': {device: model},
            'coefficients': [{'effective': '2025-01-01T00:00:00Z',
                              'model': model, 'mm_per_pulse': 1}],
            'max_clock_drift_seconds': 60, 'max_mm_per_hour': 100})
        for seq, kind in enumerate(('anchor', 'heartbeat', 'pulse'), 1):
            event = {'kind': kind, 'subject': station,
                     'identity': f'{device}:{seq}', 'device': device, 'seq': seq,
                     'time': '2026-01-01T08:00:00Z', 'device_model': model}
            if kind == 'anchor':
                event['offset_seconds'] = 0
            if kind == 'pulse':
                event['time'] = '2026-01-01T08:10:00Z'
                event['count'] = 1
            lines.append(json.dumps(event))
    cfg = tmp_path / 'custom_config.json'
    cfg.write_text(json.dumps({'stations': stations}), encoding='utf-8')
    ev = tmp_path / 'custom_events.jsonl'
    ev.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    db = tmp_path / 'custom.db'
    freeze_config(db, cfg)
    import_file(db, ev)
    evaluate(db)
    return db


def test_coarsening_without_any_safe_expansion_is_denied(tmp_path):
    # 六站：T2={S1,S3,S5,S6}，T1={S2,S4}。REL-01: {S1,S2}=1。
    # 请求只有 {S3}（计数 1，直接强制 S3=1）。扩展池只给 T2 站 S5/S6：
    # 加入任意 T2 站后集合计数恒等于集合大小，永远全一强制，不存在安全
    # 扩展 -> 必须拒绝，不发布数值文件。
    db = _write_custom_db(tmp_path, {
        'S1': 'T2', 'S2': 'T1', 'S3': 'T2', 'S4': 'T1',
        'S5': 'T2', 'S6': 'T2'})
    audience = 'external-vendor'
    seed_prior_release(db, audience,
                       {'id': 'REL-01', 'stations': ['S1', 'S2'], 'count': 1})
    universe = ('S1', 'S2', 'S3', 'S4', 'S5', 'S6')
    out = tmp_path / 'out'
    row = publish_request(
        db, audience, {'id': 'REQ-02', 'stations': ['S3']},
        out, 'minimum_safe_coarsening', ('S5', 'S6'), universe)
    assert row['status'] == DENIED
    assert row['reason'] == 'no_safe_expansion'
    assert not (out / 'REL-02.json').exists()


def test_coarsening_picks_lexicographically_first_minimum_expansion(tmp_path):
    # 六站：T2={S1,S3}，其余 T1。REL-01: {S1,S2}=1。请求 {S3}（强制）。
    # 扩展池 [S6,S5]（注意逆序传入）：加任意一个 T1 即可让两站对称，
    # size=1 即最小；同数量候选按字典序应选 S5 而非 S6。
    db = _write_custom_db(tmp_path, {
        'S1': 'T2', 'S2': 'T1', 'S3': 'T2', 'S4': 'T1',
        'S5': 'T1', 'S6': 'T1'})
    audience = 'external-vendor'
    seed_prior_release(db, audience,
                       {'id': 'REL-01', 'stations': ['S1', 'S2'], 'count': 1})
    universe = ('S1', 'S2', 'S3', 'S4', 'S5', 'S6')
    out = tmp_path / 'out'
    row = publish_request(
        db, audience, {'id': 'REQ-02', 'stations': ['S3']},
        out, 'minimum_safe_coarsening', ('S6', 'S5'), universe)
    assert row['status'] == DELIVERED
    assert json.loads(row['stations_json']) == ['S3', 'S5']
    assert row['t2_count'] == 1
    assert forced_stations(universe, audience_facts(db, audience)) == {}


def test_already_safe_request_needs_no_expansion(tmp_path):
    # 与 REL-01 不构成单站强制时，原样发布三站集合。
    db = _write_custom_db(tmp_path, {
        'S1': 'T2', 'S2': 'T1', 'S3': 'T1', 'S4': 'T2'})
    audience = 'external-vendor'
    seed_prior_release(db, audience,
                       {'id': 'REL-01', 'stations': ['S1', 'S2'], 'count': 1})
    out = tmp_path / 'out'
    row = publish_request(
        db, audience, {'id': 'REQ-09', 'stations': ['S3', 'S4']},
        out, 'minimum_safe_coarsening', ('S1',),
        ('S1', 'S2', 'S3', 'S4'))
    assert row['status'] == DELIVERED
    assert json.loads(row['stations_json']) == ['S3', 'S4']
    assert row['t2_count'] == 1


# ---------------------------------------------------------------------------
# 授权与受众隔离
# ---------------------------------------------------------------------------

def test_unauthorized_station_is_denied_and_not_learned(tmp_path):
    db = build_db(tmp_path)
    spec = seed_vendor(db)
    out = tmp_path / 'out'
    row = publish_request(
        db, spec['audience'],
        {'id': 'REQ-99', 'stations': ['S1', 'S9']}, out, 'deny_unsafe',
        (), spec['station_universe'])
    assert row['status'] == DENIED and row['reason'] == 'unauthorized_station'
    assert audience_facts(db, spec['audience']) == [(('S1', 'S2'), 1)]
    assert not (out / 'REL-99.json').exists()


def test_audience_knowledge_is_isolated(tmp_path):
    db = build_db(tmp_path)
    spec = seed_vendor(db)  # external-vendor 拿到 REL-01
    other = 'other-vendor'
    # 另一受众发布过更敏感的集合；external-vendor 不得借用。
    seed_prior_release(db, other,
                       {'id': 'REL-X1', 'stations': ['S1', 'S2', 'S3'], 'count': 2})
    out_a, out_b = tmp_path / 'a', tmp_path / 'b'
    req = {'id': 'REQ-02', 'stations': ['S1', 'S2', 'S3']}
    row_a = publish_request(db, spec['audience'], req, out_a, 'deny_unsafe',
                            (), spec['station_universe'])
    row_b = publish_request(db, other, req, out_b, 'deny_unsafe',
                            (), spec['station_universe'])
    assert row_a['status'] == DENIED  # 仅联立本受众 REL-01 时不安全
    assert row_b['status'] == DELIVERED  # 其他受众的既有知识下该请求安全
    ledgers = {r['audience'] for r in
               connect(db).execute('SELECT DISTINCT audience FROM disclosure_ledger')}
    assert ledgers == {'external-vendor', 'other-vendor'}
    # 各受众文件目录互不可见对方交付物。
    assert not (out_a / 'REL-02.json').exists()
    assert (out_b / 'REL-02.json').exists()
    # 内部报告不泄漏给外部角色，但内部审计员可取完整证据。
    with pytest.raises(PermissionError):
        export(db, tmp_path / 'leak.json', role='external-vendor')
    report = json.loads(export(db, tmp_path / 'internal.json',
                               role=INTERNAL_AUDIT_ROLE).read_text())
    raw = json.dumps(report, ensure_ascii=False)
    assert 'device_model' in raw and 'T2' in raw
    assert any(e['subject'] == 'S3' for e in report['event'])


# ---------------------------------------------------------------------------
# 重放幂等
# ---------------------------------------------------------------------------

def test_same_request_replay_returns_same_outcome(tmp_path):
    db = build_db(tmp_path)
    spec = seed_vendor(db)
    out = tmp_path / 'out'
    first = publish_request(db, spec['audience'], spec['new_request'], out,
                            'minimum_safe_coarsening',
                            spec['allowed_expansion'], spec['station_universe'])
    delivered = list(out.glob('REL-*.json'))
    second = publish_request(db, spec['audience'], spec['new_request'], out,
                             'minimum_safe_coarsening',
                             spec['allowed_expansion'], spec['station_universe'])
    assert first['id'] == second['id']
    assert second['status'] == DELIVERED
    assert list(out.glob('REL-*.json')) == delivered  # 没有第二份文件
    rows = [r for r in ledger_view(db, spec['audience']) if r['request_id'] == 'REQ-02']
    assert len(rows) == 1
    # 拒绝同样幂等。
    db2 = build_db(tmp_path, 'dd2.db')
    spec2 = seed_vendor(db2)
    r1 = publish_request(db2, spec2['audience'], spec2['new_request'],
                         tmp_path / 'o2', 'deny_unsafe', (),
                         spec2['station_universe'])
    r2 = publish_request(db2, spec2['audience'], spec2['new_request'],
                         tmp_path / 'o2', 'deny_unsafe', (),
                         spec2['station_universe'])
    assert r1['status'] == r2['status'] == DENIED
    assert len([r for r in ledger_view(db2, spec2['audience'])
                if r['status'] == DENIED]) == 1


def test_replay_with_changed_policy_is_rejected(tmp_path):
    db = build_db(tmp_path)
    spec = seed_vendor(db)
    publish_request(db, spec['audience'], spec['new_request'],
                    tmp_path / 'out', 'deny_unsafe', (),
                    spec['station_universe'])
    with pytest.raises(DisclosureError, match='request_policy_conflict'):
        publish_request(db, spec['audience'], spec['new_request'],
                        tmp_path / 'out', 'minimum_safe_coarsening',
                        spec['allowed_expansion'], spec['station_universe'])


# ---------------------------------------------------------------------------
# 并发竞争：各自安全、合并泄露的两个请求必须串行知识检查
# ---------------------------------------------------------------------------

def test_concurrent_requests_are_serialized_and_only_safe_one_publishes(tmp_path):
    # 事实：S1=1,S2=0,S3=0,S4=1（两台 T2）。REL-01: {S1,S2}=1。
    db = _write_custom_db(tmp_path, {
        'S1': 'T2', 'S2': 'T1', 'S3': 'T1', 'S4': 'T2'})
    audience = 'external-vendor'
    seed_prior_release(db, audience,
                       {'id': 'REL-01', 'stations': ['S1', 'S2'], 'count': 1})
    universe = ('S1', 'S2', 'S3', 'S4')
    # 单独各自安全（都不强制任何单站）：
    assert is_safe(universe, audience_facts(db, audience) + [(('S3', 'S4'), 1)])
    assert is_safe(universe, audience_facts(db, audience) + [(('S2', 'S3', 'S4'), 1)])
    # 但两者合并后 S1/S2 被唯一强制（合并泄露）：
    combined = audience_facts(db, audience) + [
        (('S3', 'S4'), 1), (('S2', 'S3', 'S4'), 1)]
    assert forced_stations(universe, combined) == {'S1': 1, 'S2': 0}

    out = tmp_path / 'out'
    results = []
    errors = []

    def worker(req_id, stations):
        try:
            results.append(publish_request(
                db, audience, {'id': req_id, 'stations': stations},
                out, 'deny_unsafe', (), universe))
        except DisclosureError as exc:  # pragma: no cover - 串行锁不应抛错
            errors.append(exc)

    t1 = threading.Thread(target=worker, args=('REQ-A', ['S3', 'S4']))
    t2 = threading.Thread(target=worker, args=('REQ-B', ['S2', 'S3', 'S4']))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert not errors
    statuses = sorted(r['status'] for r in results)
    assert statuses == [DELIVERED, DENIED]
    delivered = [r for r in results if r['status'] == DELIVERED][0]
    denied = [r for r in results if r['status'] == DENIED][0]
    assert denied['reason'] == 'inference_unsafe'
    # 最终受众知识下仍无任何单站被强制。
    assert forced_stations(universe, audience_facts(db, audience)) == {}
    assert len(list(out.glob('REL-*.json'))) == 1
    assert (out / (delivered['release_id'] + '.json')).exists()


# ---------------------------------------------------------------------------
# 中断恢复：台账与文件的一致性
# ---------------------------------------------------------------------------

def _delivered_row(db, audience, request_id='REQ-02'):
    return next(r for r in ledger_view(db, audience)
                if r['request_id'] == request_id and r['status'] == DELIVERED)


def test_recovery_rebuilds_delivered_file_missing_after_crash(tmp_path):
    db = build_db(tmp_path)
    spec = seed_vendor(db)
    out = tmp_path / 'out'
    publish_request(db, spec['audience'], spec['new_request'], out,
                    'minimum_safe_coarsening', spec['allowed_expansion'],
                    spec['station_universe'])
    target = out / 'REL-02.json'
    digest = hashlib_of(target)
    target.unlink()  # 模拟崩溃后文件丢失
    reconcile(db, spec['audience'], out)
    assert target.exists()
    assert hashlib_of(target) == digest  # 重建的是同一文件，不是新内容
    assert _delivered_row(db, spec['audience'])['status'] == DELIVERED


def test_recovery_pending_without_file_gets_delivered(tmp_path):
    db = build_db(tmp_path)
    spec = seed_vendor(db)
    out = tmp_path / 'out'
    row = publish_request(db, spec['audience'], spec['new_request'], out,
                          'minimum_safe_coarsening', spec['allowed_expansion'],
                          spec['station_universe'])
    target = out / 'REL-02.json'
    digest = hashlib_of(target)
    # 模拟“台账已记 pending、文件尚未改名”的崩溃点。
    with connect(db) as con:
        con.execute('UPDATE disclosure_ledger SET status=? WHERE id=?',
                    (PENDING, row['id']))
    target.unlink()
    reconcile(db, spec['audience'], out)
    assert target.exists() and hashlib_of(target) == digest
    assert _delivered_row(db, spec['audience'])['status'] == DELIVERED


def test_recovery_pending_with_complete_file_is_only_bookkept(tmp_path):
    db = build_db(tmp_path)
    spec = seed_vendor(db)
    out = tmp_path / 'out'
    row = publish_request(db, spec['audience'], spec['new_request'], out,
                          'minimum_safe_coarsening', spec['allowed_expansion'],
                          spec['station_universe'])
    target = out / 'REL-02.json'
    digest = hashlib_of(target)
    with connect(db) as con:
        con.execute('UPDATE disclosure_ledger SET status=? WHERE id=?',
                    (PENDING, row['id']))
    reconcile(db, spec['audience'], out)  # 文件已在且摘要匹配
    assert hashlib_of(target) == digest
    assert _delivered_row(db, spec['audience'])['status'] == DELIVERED


def test_recovery_removes_orphan_files_without_ledger_record(tmp_path):
    db = build_db(tmp_path)
    spec = seed_vendor(db)
    out = tmp_path / 'out'
    publish_request(db, spec['audience'], spec['new_request'], out,
                    'minimum_safe_coarsening', spec['allowed_expansion'],
                    spec['station_universe'])
    orphan = out / 'REL-GHOST.json'
    orphan.write_text(json.dumps(build_external_payload(
        'REL-GHOST', ['S1'], 1, 'batch-1')), encoding='utf-8')
    # 原子改名前崩溃留下的 .tmp 残文件同样必须清掉。
    stale_tmp = out / 'REL-02.json.tmp'
    stale_tmp.write_text('partial', encoding='utf-8')
    reconcile(db, spec['audience'], out)
    assert not orphan.exists()  # 不允许已交付文件却无披露记录
    assert not stale_tmp.exists()
    assert (out / 'REL-02.json').exists()


def test_recovery_refuses_to_resend_different_file(tmp_path):
    db = build_db(tmp_path)
    spec = seed_vendor(db)
    out = tmp_path / 'out'
    publish_request(db, spec['audience'], spec['new_request'], out,
                    'minimum_safe_coarsening', spec['allowed_expansion'],
                    spec['station_universe'])
    target = out / 'REL-02.json'
    target.write_text(target.read_text(encoding='utf-8').replace('"t2_count": 2',
                                                                 '"t2_count": 3'))
    with pytest.raises(DisclosureError, match='digest_mismatch'):
        reconcile(db, spec['audience'], out)


def hashlib_of(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


# ---------------------------------------------------------------------------
# CLI 端到端
# ---------------------------------------------------------------------------

def test_cli_publish_both_policies_and_report_role(tmp_path, monkeypatch):
    from rainstation.__main__ import main

    request_file = FIXTURES / 'device_disclosure_requests.json'
    # 两条合规路线使用各自独立的工作区（同库重跑必须返回同一决定）。
    db_deny = build_db(tmp_path, name='deny.db')
    out_deny = tmp_path / 'deny'
    monkeypatch.setattr('sys.argv', [
        'rainstation', 'publish', '--db', str(db_deny),
        '--request', str(request_file), '--out-dir', str(out_deny),
        '--policy', 'deny_unsafe'])
    main()
    assert not (out_deny / 'REL-02.json').exists()

    db_coarse = build_db(tmp_path, name='coarse.db')
    out_coarse = tmp_path / 'coarse'
    monkeypatch.setattr('sys.argv', [
        'rainstation', 'publish', '--db', str(db_coarse),
        '--request', str(request_file), '--out-dir', str(out_coarse),
        '--policy', 'minimum_safe_coarsening'])
    main()
    external = json.loads((out_coarse / 'REL-02.json').read_text(encoding='utf-8'))
    assert external['release_id'] == 'REL-02' and external['t2_count'] == 2
    assert_external_file_clean(json.dumps(external))

    # 供应商角色无法走内部报告通道。
    monkeypatch.setattr('sys.argv', [
        'rainstation', 'report', '--db', str(db_coarse),
        '--out', str(tmp_path / 'leak.json'), '--role', 'external-vendor'])
    with pytest.raises(SystemExit, match='permission_denied'):
        main()

    monkeypatch.setattr('sys.argv', [
        'rainstation', 'ledger', '--db', str(db_coarse),
        '--audience', 'external-vendor'])
    main()  # 不抛错即台账可读


def test_external_payload_whitelist_is_strict():
    payload = build_external_payload('REL-X', ['S1', 'S2'], 1, 'batch-1')
    assert_external_file_clean(json.dumps(payload, ensure_ascii=False))
    # 摘要只由白名单字段的规范编码决定。
    assert file_digest(payload) == file_digest(json.loads(json.dumps(payload)))
