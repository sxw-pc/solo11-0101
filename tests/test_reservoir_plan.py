import itertools
import json
from pathlib import Path

import pytest

from rainstation import reservoir_plan as rp
from rainstation.__main__ import main
from rainstation.ingest import freeze_config, import_file
from rainstation.rules import evaluate
from rainstation.store import connect

BASE = Path(__file__).parent.parent / 'fixtures'
CASE = json.loads((BASE / 'reservoir_plan.json').read_text(encoding='utf-8'))


def make_db(tmp_path):
    db = tmp_path / 'station.db'
    freeze_config(db, BASE / 'reservoir_config.json')
    assert import_file(db, BASE / 'reservoir_rain_events.jsonl')['imported'] == 8
    assert evaluate(db)['S1'] == 'trusted'
    return db


def active_commands(doc):
    return [(c['device'], c['amount']) for s in doc['slots'] for c in s['commands']]


# ---------- 两份合同在固定 fixture 上的最优指令 ----------

def test_min_downstream_peak_contract(tmp_path):
    doc = rp.generate_plan(make_db(tmp_path), CASE, 'min_downstream_peak')
    assert active_commands(doc) == [('pump', 30), ('gate', 30)]
    assert [s['storage_end'] for s in doc['slots']] == [90, 100]
    assert [s['pond_end'] for s in doc['slots']] == [30, 30]
    assert doc['metrics']['downstream_peak'] == 30
    assert doc['metrics']['high_water_exposure'] == 190
    assert doc['metrics']['objective_key'] == [30, 190]
    # 传播时延:t0/t1 的放水分别到达时段 1/2,不能当作本时段就到
    assert [s['downstream']['arrival_slot'] for s in doc['slots']] == [1, 2]
    assert [s['downstream']['total'] for s in doc['slots']] == [30, 30]
    assert doc['slots'][1]['downstream']['gate_arrived'] == 30


def test_min_exposure_contract(tmp_path):
    doc = rp.generate_plan(make_db(tmp_path), CASE, 'min_reservoir_high_water_exposure')
    assert active_commands(doc) == [('gate', 40), ('pump', 30)]
    assert [s['storage_end'] for s in doc['slots']] == [80, 90]
    assert [s['pond_end'] for s in doc['slots']] == [0, 30]
    assert doc['metrics']['downstream_peak'] == 70
    assert doc['metrics']['high_water_exposure'] == 170
    assert doc['metrics']['objective_key'] == [170, 70]
    # 下游峰 = 自然流 30 + t0 闸 40 迟到一个时段到达
    assert doc['slots'][0]['downstream']['total'] == 70


def test_two_contracts_drive_different_devices(tmp_path):
    db = make_db(tmp_path)
    peak_first = rp.generate_plan(db, CASE, 'min_downstream_peak')
    exposure_first = rp.generate_plan(db, CASE, 'min_reservoir_high_water_exposure')
    assert active_commands(peak_first) != active_commands(exposure_first)
    assert peak_first['metrics']['downstream_peak'] < exposure_first['metrics']['downstream_peak']
    assert exposure_first['metrics']['high_water_exposure'] < peak_first['metrics']['high_water_exposure']


def test_plan_cites_rain_and_device_evidence(tmp_path):
    doc = rp.generate_plan(make_db(tmp_path), CASE, 'min_downstream_peak')
    ev = doc['rain_evidence']
    assert ev['station'] == 'S1' and ev['run_id'] and ev['config_sha256']
    assert [h['mm'] for h in ev['hourly']] == [60, 40]
    assert [h['units'] for h in ev['hourly']] == [60, 40]
    assert len(ev['event_ids']) == 8
    assert doc['case']['gate_max_per_slot'] == 40
    assert doc['case']['pump_max_per_slot'] == 30
    assert doc['case_sha256']


# ---------- 入流证据门槛:缺时段、待核、来源冲突不自动排程 ----------

def test_missing_decision_blocks_scheduling(tmp_path):
    db = tmp_path / 'x.db'
    freeze_config(db, BASE / 'reservoir_config.json')
    import_file(db, BASE / 'reservoir_rain_events.jsonl')
    with pytest.raises(rp.PlanError, match='rain_decision_missing'):
        rp.generate_plan(db, CASE, 'min_downstream_peak')


def test_untrusted_decision_blocks_scheduling(tmp_path):
    db = tmp_path / 'x.db'
    freeze_config(db, BASE / 'reservoir_config.json')
    events = [json.loads(l) for l in (BASE / 'reservoir_rain_events.jsonl').read_text().splitlines()]
    events[-1]['count'] = 1000  # 物理强度超限 → review
    p = tmp_path / 'spike.jsonl'
    p.write_text('\n'.join(json.dumps(e) for e in events), encoding='utf-8')
    import_file(db, p)
    assert evaluate(db)['S1'] == 'review'
    with pytest.raises(rp.PlanError, match='rain_evidence_not_trusted'):
        rp.generate_plan(db, CASE, 'min_downstream_peak')


def test_missing_rain_slot_blocks_scheduling(tmp_path):
    case = dict(CASE, start='2026-01-01T10:00:00Z')
    with pytest.raises(rp.PlanError, match='rain_slot_missing'):
        rp.generate_plan(make_db(tmp_path), case, 'min_downstream_peak')


def test_source_conflict_blocks_scheduling(tmp_path):
    db = make_db(tmp_path)
    first = json.loads((BASE / 'reservoir_rain_events.jsonl').read_text().splitlines()[0])
    first['offset_seconds'] = 5
    p = tmp_path / 'conflict.jsonl'
    p.write_text(json.dumps(first), encoding='utf-8')
    import_file(db, p)
    with pytest.raises(rp.PlanError, match='rain_source_conflict'):
        rp.generate_plan(db, CASE, 'min_downstream_peak')


def test_unknown_objective_rejected(tmp_path):
    with pytest.raises(rp.PlanError, match='unknown_objective'):
        rp.generate_plan(make_db(tmp_path), CASE, 'min_something_else')


def test_infeasible_case_refuses_scheduling(tmp_path):
    # 自然流已顶满硬上限,闸门任何放水都越限,塘容又不足以吞掉全部来水
    case = dict(CASE, downstream_base=[75, 75])
    with pytest.raises(rp.Infeasible, match='no_feasible_plan'):
        rp.generate_plan(make_db(tmp_path), case, 'min_downstream_peak')


# ---------- 硬约束边界 ----------

def test_mid_horizon_overflow_cannot_be_hidden_by_final(tmp_path):
    # 末时段库容 100 达标,但 t0 末 120 已溢库
    with pytest.raises(rp.Infeasible, match='storage_overflow'):
        rp.simulate(CASE, [60, 40], [{}, {'gate': 60}])
    with pytest.raises(rp.Infeasible, match='storage_overflow'):
        rp.simulate(CASE, [60, 40], [{}, {}])


def test_gate_cooldown_blocks_consecutive_slots():
    with pytest.raises(rp.Infeasible, match='gate_cooldown'):
        rp.simulate(CASE, [60, 40], [{'gate': 40}, {'gate': 40}])
    no_cooldown = dict(CASE, gate_cooldown_slots=0)
    traj = rp.simulate(no_cooldown, [60, 40], [{'gate': 40}, {'gate': 40}])
    assert traj['slots'][1]['storage_end'] == 80


def test_shared_power_forbids_simultaneous_devices():
    with pytest.raises(rp.Infeasible, match='shared_power'):
        rp.simulate(CASE, [60, 40], [{'gate': 10, 'pump': 10}, {'gate': 40}])
    dual = dict(CASE, shared_power_max_active_devices=2, gate_cooldown_slots=0)
    traj = rp.simulate(dual, [60, 40], [{'gate': 10, 'pump': 10}, {'gate': 40}])
    assert traj['slots'][0]['storage_end'] == 100


def test_downstream_delay_and_hard_limit():
    wide = dict(CASE, gate_max_per_slot=50)
    # t0 放 45 → 到达时段 30+45=75 恰好达标;放 46 则越限
    traj = rp.simulate(wide, [60, 40], [{'gate': 45}, {'pump': 15}])
    assert traj['slots'][0]['downstream_total'] == 75
    with pytest.raises(rp.Infeasible, match='downstream_limit'):
        rp.simulate(wide, [60, 40], [{'gate': 46}, {'pump': 15}])
    # t0 放水计入到达时段(t+travel)的流量,而不是 t0 本时段
    traj = rp.simulate(CASE, [60, 40], [{'gate': 40}, {'pump': 30}])
    assert traj['slots'][0]['gate_arrived'] == 40
    assert traj['slots'][0]['downstream_total'] == 70


def test_pond_capacity_enforced():
    with pytest.raises(rp.Infeasible, match='pond_overflow'):
        rp.simulate(CASE, [60, 40], [{'pump': 30}, {'pump': 30}])


def test_device_cap_enforced():
    with pytest.raises(rp.Infeasible, match='device_cap'):
        rp.simulate(CASE, [60, 40], [{'gate': 41}, {}])


# ---------- 四设备八时段生成场景:规划器与独立穷举核对 ----------

GENERATED_CASE = {
    'rain_station': 'S1', 'start': '2026-01-01T00:00:00Z', 'slot_minutes': 60,
    'inflow_units_per_mm': 1, 'initial_storage': 6, 'max_storage': 6,
    'initial_pond': 0, 'max_pond': 2, 'shared_power_max_active_devices': 1,
    'gate_cooldown_slots': 1, 'downstream_travel_slots': 2,
    'downstream_base': [0, 0, 0, 0, 0, 0, 0, 0], 'downstream_hard_limit': 75,
    'devices': [
        {'id': 'G1', 'kind': 'gate', 'max_per_slot': 1},
        {'id': 'G2', 'kind': 'gate', 'max_per_slot': 1},
        {'id': 'P1', 'kind': 'pump', 'max_per_slot': 1},
        {'id': 'P2', 'kind': 'pump', 'max_per_slot': 1},
    ],
    'objectives': ['min_downstream_peak', 'min_reservoir_high_water_exposure'],
}
GENERATED_INFLOWS = [0, 0, 0, 0, 0, 0, 0, 0]


def brute_force_best(case, inflows, objective):
    """独立穷举:不经过规划器,逐时段枚举全部原始命令组合并完整回放。"""
    devs = case['devices']
    n = len(inflows)
    power_ok = [combo for combo in
                itertools.product(*(range(d['max_per_slot'] + 1) for d in devs))
                if sum(1 for a in combo if a) <= case['shared_power_max_active_devices']]
    best = None

    def rec(t, storage, pond, cool, cmds, peak, expo):
        nonlocal best
        if t == n:
            ck = tuple(tuple((i, a) for i, a in enumerate(c) if a) for c in cmds)
            key = (peak, expo, ck) if objective == 'min_downstream_peak' else (expo, peak, ck)
            if best is None or key < best[0]:
                best = (key, tuple(cmds))
            return
        for combo in power_ok:
            if any(a and devs[i]['kind'] == 'gate' and cool[i] for i, a in enumerate(combo)):
                continue
            gates = sum(a for i, a in enumerate(combo) if devs[i]['kind'] == 'gate')
            pumps = sum(combo) - gates
            s2 = storage + inflows[t] - gates - pumps
            p2 = pond + pumps
            if not 0 <= s2 <= case['max_storage'] or not 0 <= p2 <= case['max_pond']:
                continue
            if case['downstream_base'][t] + gates > case['downstream_hard_limit']:
                continue
            c2 = tuple(case['gate_cooldown_slots'] if a and devs[i]['kind'] == 'gate'
                       else max(0, cool[i] - 1) for i, a in enumerate(combo))
            rec(t + 1, s2, p2, c2, cmds + [combo],
                max(peak, case['downstream_base'][t] + gates), expo + s2)

    rec(0, case['initial_storage'], case['initial_pond'], tuple(0 for _ in devs), [], 0, 0)
    return best


def to_combos(case, commands):
    order = {d['id']: i for i, d in enumerate(case['devices'])}
    out = []
    for cmd in commands:
        amounts = [0] * len(order)
        for dev, amt in cmd.items():
            amounts[order[dev]] = amt
        out.append(tuple(amounts))
    return tuple(out)


@pytest.mark.parametrize('objective', ['min_downstream_peak', 'min_reservoir_high_water_exposure'])
def test_generated_4_devices_8_slots_matches_brute_force(objective):
    result = rp.optimize(GENERATED_CASE, GENERATED_INFLOWS, objective)
    assert result is not None
    best = brute_force_best(GENERATED_CASE, GENERATED_INFLOWS, objective)
    assert best is not None
    brute_key, brute_cmds = best
    assert result['key'] == brute_key
    assert to_combos(GENERATED_CASE, result['commands']) == brute_cmds


def test_generated_scenario_objectives_diverge():
    peak_first = rp.optimize(GENERATED_CASE, GENERATED_INFLOWS, 'min_downstream_peak')
    expo_first = rp.optimize(GENERATED_CASE, GENERATED_INFLOWS, 'min_reservoir_high_water_exposure')
    # 先最小峰值:全程不用闸,峰值 0,暴露 33
    assert peak_first['commands'] == [{'P1': 1}, {'P1': 1}, {}, {}, {}, {}, {}, {}]
    assert peak_first['trajectory']['metrics']['downstream_peak'] == 0
    assert peak_first['trajectory']['metrics']['high_water_exposure'] == 33
    # 先最小暴露:逐时段泄放,双闸交替遵守冷却,暴露 15,峰值 1
    assert expo_first['commands'] == [{'G1': 1}, {'G2': 1}, {'G1': 1}, {'G2': 1},
                                      {'G1': 1}, {'G2': 1}, {}, {}]
    assert expo_first['trajectory']['metrics']['high_water_exposure'] == 15
    assert expo_first['trajectory']['metrics']['downstream_peak'] == 1


# ---------- 反馈:锁定前缀重算、幂等、待核、冲突 ----------

def test_feedback_locks_prefix_and_replans(tmp_path):
    db = make_db(tmp_path)
    original = rp.generate_plan(db, CASE, 'min_downstream_peak')
    result = rp.apply_feedback(db, (BASE / 'reservoir_feedback.json').read_bytes())
    assert result['status'] == 'replanned'
    doc = result['plan']
    assert doc['base_plan_id'] == original['plan_id']
    assert doc['revision'] == 1
    # t0 锁定为真实泵 25,不重写为理想 30;t1 改为闸 35,不沿用原闸 30
    assert doc['slots'][0]['commands'] == [{'device': 'pump', 'kind': 'pump', 'amount': 25}]
    assert doc['slots'][0]['executed'] is True
    assert doc['slots'][0]['feedback_identity'] == 'GATE-ACK-001'
    assert doc['slots'][0]['observed_storage_end'] == 95
    assert doc['slots'][1]['commands'] == [{'device': 'gate', 'kind': 'gate', 'amount': 35}]
    assert doc['slots'][1]['executed'] is False
    assert [s['storage_end'] for s in doc['slots']] == [95, 100]
    assert [s['pond_end'] for s in doc['slots']] == [25, 25]
    assert doc['metrics']['downstream_peak'] == 35
    assert doc['metrics']['high_water_exposure'] == 195
    assert doc['locked_prefix'][0]['commands'] == {'pump': 25}
    con = connect(db)
    assert con.execute('SELECT status FROM plan WHERE id=?', (original['plan_id'],)).fetchone()[0] == 'superseded'
    assert con.execute('SELECT status FROM plan WHERE id=?', (doc['plan_id'],)).fetchone()[0] == 'active'
    stored = con.execute("SELECT sha256 FROM plan_feedback WHERE identity='GATE-ACK-001'").fetchone()[0]
    assert stored == (BASE / 'reservoir_feedback.sha256').read_text().strip()


def test_repeated_feedback_is_idempotent(tmp_path):
    db = make_db(tmp_path)
    rp.generate_plan(db, CASE, 'min_downstream_peak')
    raw = (BASE / 'reservoir_feedback.json').read_bytes()
    assert rp.apply_feedback(db, raw)['status'] == 'replanned'
    again = rp.apply_feedback(db, raw)
    assert again['status'] == 'duplicate'
    con = connect(db)
    assert con.execute('SELECT count(*) FROM plan').fetchone()[0] == 2
    assert con.execute('SELECT count(*) FROM plan_feedback').fetchone()[0] == 1


def test_repeated_generation_is_idempotent(tmp_path):
    db = make_db(tmp_path)
    a = rp.generate_plan(db, CASE, 'min_downstream_peak')
    b = rp.generate_plan(db, CASE, 'min_downstream_peak')
    assert a['plan_id'] == b['plan_id']
    assert connect(db).execute('SELECT count(*) FROM plan').fetchone()[0] == 1


def test_contradictory_observation_goes_pending(tmp_path):
    db = make_db(tmp_path)
    rp.generate_plan(db, CASE, 'min_downstream_peak')
    bad = {'planned_policy': 'min_downstream_peak', 'slot': 0,
           'planned': {'device': 'pump', 'amount': 30}, 'actual': {'device': 'pump', 'amount': 25},
           'observed_storage_after_slot': 90, 'observed_pond_after_slot': 25, 'identity': 'GATE-ACK-X'}
    res = rp.apply_feedback(db, bad)
    assert res['status'] == 'pending' and res['reason'] == 'observation_mismatch'
    con = connect(db)
    assert con.execute("SELECT status FROM plan_feedback WHERE identity='GATE-ACK-X'").fetchone()[0] == 'pending'
    assert con.execute("SELECT count(*) FROM plan WHERE status='active'").fetchone()[0] == 1
    assert con.execute('SELECT count(*) FROM plan').fetchone()[0] == 1


def test_out_of_order_feedback_goes_pending(tmp_path):
    db = make_db(tmp_path)
    rp.generate_plan(db, CASE, 'min_downstream_peak')
    late = {'planned_policy': 'min_downstream_peak', 'slot': 1,
            'planned': {'device': 'gate', 'amount': 30}, 'actual': {'device': 'gate', 'amount': 30},
            'observed_storage_after_slot': 100, 'observed_pond_after_slot': 30, 'identity': 'GATE-ACK-LATE'}
    res = rp.apply_feedback(db, late)
    assert res['status'] == 'pending' and res['reason'] == 'slot_out_of_order'


def test_planned_mismatch_goes_pending(tmp_path):
    db = make_db(tmp_path)
    rp.generate_plan(db, CASE, 'min_downstream_peak')
    wrong = {'planned_policy': 'min_downstream_peak', 'slot': 0,
             'planned': {'device': 'gate', 'amount': 40}, 'actual': {'device': 'gate', 'amount': 40},
             'observed_storage_after_slot': 80, 'observed_pond_after_slot': 0, 'identity': 'GATE-ACK-W'}
    res = rp.apply_feedback(db, wrong)
    assert res['status'] == 'pending' and res['reason'] == 'planned_mismatch'


def test_feedback_identity_conflict(tmp_path):
    db = make_db(tmp_path)
    rp.generate_plan(db, CASE, 'min_downstream_peak')
    raw = json.loads((BASE / 'reservoir_feedback.json').read_text())
    assert rp.apply_feedback(db, raw)['status'] == 'replanned'
    changed = dict(raw, observed_pond_after_slot=24)
    res = rp.apply_feedback(db, changed)
    assert res['status'] == 'conflict'
    assert connect(db).execute(
        "SELECT count(*) FROM issue WHERE code='feedback_identity_conflict'").fetchone()[0] == 1


def test_rain_ledger_is_not_rewritten(tmp_path):
    db = make_db(tmp_path)
    con = connect(db)
    before = {t: [tuple(r) for r in con.execute(f'SELECT * FROM {t} ORDER BY 1')]
              for t in ('source', 'event', 'decision')}
    rp.generate_plan(db, CASE, 'min_downstream_peak')
    rp.apply_feedback(db, (BASE / 'reservoir_feedback.json').read_bytes())
    after = {t: [tuple(r) for r in con.execute(f'SELECT * FROM {t} ORDER BY 1')]
             for t in ('source', 'event', 'decision')}
    assert before == after


# ---------- 离线命令行 ----------

def test_cli_plan_and_feedback(tmp_path, capsys):
    db = make_db(tmp_path)
    main(['plan', '--db', str(db), '--case', str(BASE / 'reservoir_plan.json'),
          '--objective', 'min_downstream_peak'])
    doc = json.loads(capsys.readouterr().out)
    assert doc['metrics']['downstream_peak'] == 30
    assert active_commands(doc) == [('pump', 30), ('gate', 30)]
    main(['feedback', '--db', str(db), '--file', str(BASE / 'reservoir_feedback.json')])
    res = json.loads(capsys.readouterr().out)
    assert res['status'] == 'replanned'
    assert res['plan']['slots'][1]['commands'][0]['amount'] == 35
    main(['plan', '--db', str(db), '--case', str(BASE / 'reservoir_plan.json'),
          '--objective', 'min_reservoir_high_water_exposure', '--out', str(tmp_path / 'p.json')])
    doc2 = json.loads(capsys.readouterr().out)
    assert doc2['metrics']['high_water_exposure'] == 170
    assert json.loads((tmp_path / 'p.json').read_text())['plan_id'] == doc2['plan_id']


def test_cli_plan_refuses_without_trusted_evidence(tmp_path, capsys):
    db = tmp_path / 'x.db'
    freeze_config(db, BASE / 'reservoir_config.json')
    import_file(db, BASE / 'reservoir_rain_events.jsonl')
    with pytest.raises(SystemExit):
        main(['plan', '--db', str(db), '--case', str(BASE / 'reservoir_plan.json'),
              '--objective', 'min_downstream_peak'])
    err = json.loads(capsys.readouterr().err)
    assert err['status'] == 'error' and 'rain_decision_missing' in err['reason']
