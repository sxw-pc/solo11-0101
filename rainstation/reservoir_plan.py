"""离线闸门/调蓄泵班次指令规划。

只消费同一站点、已完成且 trusted 的小时雨量决定作为入流证据;缺时段、
待核或来源冲突则不自动排程。在完整可行时序上按合同目标字典序求最优
(可用小规模穷举核验),不按每时段独立贪心。设备真实反馈锁定已执行前缀
后重算未执行时段。全程离线,不接任何真实闸门/泵网络。

硬约束(逐时段):库容上下限、旁路塘容、单设备流量上限、共享电力同时
驱动设备数、闸门冷却时段、下游传播时延(放水隔 travel 时段才到达),
下游各到达时段流量(自然流 + 实际到达闸水)不得超过硬上限。中途溢库
不能用末时段库容达标掩盖。命令水量一律为整数入库单位。
"""
import hashlib
import itertools
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .store import connect

VERSION = '1'
OBJECTIVES = ('min_downstream_peak', 'min_reservoir_high_water_exposure')
GATE = 'gate'
PUMP = 'pump'


class PlanError(ValueError):
    """证据或合同输入不允许自动排程。"""


class Infeasible(PlanError):
    """命令违反硬约束,或不存在满足全部硬约束的完整时序。"""


def _canon(obj):
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _now():
    return datetime.now(timezone.utc).isoformat()


def _parse_ts(text):
    dt = datetime.fromisoformat(str(text).replace('Z', '+00:00'))
    if dt.tzinfo is None:
        raise PlanError('case_start_requires_timezone')
    return dt


def devices_of(case):
    """设备台账:显式 devices 列表,或由 gate/pump 标量上限合成两台设备。"""
    if 'devices' in case:
        devices = [dict(d) for d in case['devices']]
    else:
        for key in ('gate_max_per_slot', 'pump_max_per_slot'):
            if key not in case:
                raise PlanError(f'case_missing_field:{key}')
        devices = [{'id': 'gate', 'kind': GATE, 'max_per_slot': case['gate_max_per_slot']},
                   {'id': 'pump', 'kind': PUMP, 'max_per_slot': case['pump_max_per_slot']}]
    ids = [d.get('id') for d in devices]
    if not ids or len(ids) != len(set(ids)):
        raise PlanError('case_duplicate_or_missing_device')
    for d in devices:
        if d.get('kind') not in (GATE, PUMP):
            raise PlanError(f"case_bad_device_kind:{d.get('id')}")
        cap = d.get('max_per_slot')
        if not isinstance(cap, int) or isinstance(cap, bool) or cap <= 0:
            raise PlanError(f"case_bad_device_cap:{d.get('id')}")
    return devices


def load_case(source):
    """读取离线合同(dict 或文件路径);返回 (case, sha256)。"""
    if isinstance(source, dict):
        case = source
        raw = _canon(case).encode('utf-8')
    else:
        raw = Path(source).read_bytes()
        case = json.loads(raw)
    for key in ('rain_station', 'start', 'slot_minutes', 'inflow_units_per_mm',
                'initial_storage', 'max_storage', 'initial_pond', 'max_pond',
                'downstream_base', 'downstream_hard_limit'):
        if key not in case:
            raise PlanError(f'case_missing_field:{key}')
    _parse_ts(case['start'])
    if not case['downstream_base']:
        raise PlanError('case_empty_downstream_base')
    devices_of(case)
    return case, _sha(raw)


def simulate(case, inflows, commands):
    """逐时段校验并回放命令序列;违反任一硬约束抛 Infeasible。

    commands[t] 为 {设备id: 整数水量};允许只给前缀时段(用于反馈核对)。
    闸水在放水时段后 travel 个时段才到达下游,到达时段流量 = 自然流 + 到达闸水。
    """
    devices = devices_of(case)
    index = {d['id']: i for i, d in enumerate(devices)}
    n = len(inflows)
    if len(commands) > n:
        raise PlanError('commands_beyond_horizon')
    base = list(case['downstream_base']) + [0] * max(0, n - len(case['downstream_base']))
    limit = case['downstream_hard_limit']
    cooldown_slots = int(case.get('gate_cooldown_slots', 0))
    power = int(case.get('shared_power_max_active_devices', 1))
    storage = case['initial_storage']
    pond = case['initial_pond']
    cool = [0] * len(devices)
    slots = []
    peak = 0
    exposure = 0
    for t in range(len(commands)):
        amounts = [0] * len(devices)
        for dev, amt in dict(commands[t]).items():
            if dev not in index:
                raise PlanError(f'unknown_device:{dev}')
            if not isinstance(amt, int) or isinstance(amt, bool) or amt < 0:
                raise PlanError(f'bad_amount:{dev}')
            amounts[index[dev]] = amt
        for i, d in enumerate(devices):
            if amounts[i] > d['max_per_slot']:
                raise Infeasible(f"device_cap:{d['id']}")
            if amounts[i] and d['kind'] == GATE and cool[i]:
                raise Infeasible(f"gate_cooldown:{d['id']}")
        if sum(1 for a in amounts if a) > power:
            raise Infeasible('shared_power_limit')
        gates = sum(a for i, a in enumerate(amounts) if devices[i]['kind'] == GATE)
        pumps = sum(amounts) - gates
        storage += inflows[t] - gates - pumps
        pond += pumps
        if storage > case['max_storage']:
            raise Infeasible('storage_overflow')
        if storage < 0:
            raise Infeasible('storage_underflow')
        if pond > case['max_pond']:
            raise Infeasible('pond_overflow')
        total = base[t] + gates
        if total > limit:
            raise Infeasible('downstream_limit')
        peak = max(peak, total)
        exposure += storage
        cool = [cooldown_slots if (a and devices[i]['kind'] == GATE) else max(0, cool[i] - 1)
                for i, a in enumerate(amounts)]
        slots.append({'slot': t,
                      'commands': {devices[i]['id']: a for i, a in enumerate(amounts) if a},
                      'storage_end': storage, 'pond_end': pond,
                      'downstream_base': base[t], 'gate_arrived': gates,
                      'downstream_total': total})
    return {'slots': slots,
            'metrics': {'downstream_peak': peak, 'high_water_exposure': exposure,
                        'final_storage': storage, 'final_pond': pond}}


def optimize(case, inflows, objective, locked=None):
    """在完整可行时序上按合同目标字典序求最优;无可行时序返回 None。

    目标键:min_downstream_peak → (下游峰值, 高水位暴露, 命令键);
    min_reservoir_high_water_exposure → (高水位暴露, 下游峰值, 命令键)。
    命令键为逐时段激活设备的 (设备序号, 水量) 元组,同分时取字典序最小,
    保证结果唯一确定、可复算。locked 为已执行时段的真实命令,原样锁定。
    """
    if objective not in OBJECTIVES:
        raise PlanError(f'unknown_objective:{objective}')
    devices = devices_of(case)
    n = len(inflows)
    locked = [dict(c) for c in (locked or [])]
    if len(locked) > n:
        raise PlanError('locked_beyond_horizon')
    index = {d['id']: i for i, d in enumerate(devices)}
    base = list(case['downstream_base']) + [0] * max(0, n - len(case['downstream_base']))
    limit = case['downstream_hard_limit']
    cooldown_slots = int(case.get('gate_cooldown_slots', 0))
    power = int(case.get('shared_power_max_active_devices', 1))
    max_storage = case['max_storage']
    max_pond = case['max_pond']
    inflow_suffix = [0] * (n + 1)
    for t in range(n - 1, -1, -1):
        inflow_suffix[t] = inflow_suffix[t + 1] + inflows[t]
    caps = sorted((d['max_per_slot'] for d in devices), reverse=True)
    slot_cap = sum(caps[:power])
    out_suffix = [0] * (n + 1)
    for t in range(n - 1, -1, -1):
        out_suffix[t] = out_suffix[t + 1] + slot_cap

    locked_amounts = []
    for cmd in locked:
        amounts = [0] * len(devices)
        for dev, amt in cmd.items():
            if dev not in index:
                raise PlanError(f'unknown_device:{dev}')
            amounts[index[dev]] = amt
        locked_amounts.append(tuple(amounts))

    best = {'key': None, 'commands': None}

    def rec(t, storage, pond, cool, cmds, cmds_key, peak, exposure):
        if t == n:
            key = (peak, exposure, tuple(cmds_key)) if objective == 'min_downstream_peak' \
                else (exposure, peak, tuple(cmds_key))
            if best['key'] is None or key < best['key']:
                best['key'] = key
                best['commands'] = list(cmds)
            return
        if best['key'] is not None:
            current = (peak, exposure) if objective == 'min_downstream_peak' else (exposure, peak)
            if current > (best['key'][0], best['key'][1]):
                return  # 峰值与暴露只增不减,已超过当前最优
        if storage + inflow_suffix[t] - out_suffix[t] > max_storage:
            return  # 剩余时段全开也拦不住中途溢库
        if t < len(locked_amounts):
            options = [locked_amounts[t]]
        else:
            ranges = []
            for i, d in enumerate(devices):
                blocked = d['kind'] == GATE and cool[i] > 0
                ranges.append((0,) if blocked else range(d['max_per_slot'] + 1))
            options = [combo for combo in itertools.product(*ranges)
                       if sum(1 for a in combo if a) <= power]
            options.sort(key=lambda c: (-sum(c), tuple((i, a) for i, a in enumerate(c) if a)))
        for amounts in options:
            gates = sum(a for i, a in enumerate(amounts) if devices[i]['kind'] == GATE)
            pumps = sum(amounts) - gates
            s2 = storage + inflows[t] - gates - pumps
            if s2 < 0 or s2 > max_storage:
                continue
            p2 = pond + pumps
            if p2 > max_pond:
                continue
            if base[t] + gates > limit:
                continue
            cool2 = tuple(cooldown_slots if (a and devices[i]['kind'] == GATE) else max(0, cool[i] - 1)
                          for i, a in enumerate(amounts))
            rec(t + 1, s2, p2, cool2,
                cmds + [{devices[i]['id']: a for i, a in enumerate(amounts) if a}],
                cmds_key + [tuple((i, a) for i, a in enumerate(amounts) if a)],
                max(peak, base[t] + gates), exposure + s2)

    rec(0, case['initial_storage'], case['initial_pond'], tuple(0 for _ in devices), [], [], 0, 0)
    if best['commands'] is None:
        return None
    trajectory = simulate(case, inflows, best['commands'])
    return {'commands': best['commands'], 'trajectory': trajectory, 'key': best['key']}


def trusted_inflows(db, case):
    """只接受同一站点、已完成且 trusted 的小时雨量决定作为入流证据。

    缺时段、决定非 trusted、运行未完成或存在来源冲突时拒绝自动排程。
    """
    station = case['rain_station']
    row = db.execute('SELECT status,details,rule_version,run_id FROM decision WHERE subject=?',
                     (station,)).fetchone()
    if row is None:
        raise PlanError('rain_decision_missing')
    run = db.execute('SELECT status FROM run WHERE id=?', (row['run_id'],)).fetchone()
    if run is None or run['status'] != 'done':
        raise PlanError('rain_run_incomplete')
    if row['status'] != 'trusted':
        raise PlanError('rain_evidence_not_trusted')
    if db.execute("SELECT 1 FROM issue WHERE code='identity_conflict' LIMIT 1").fetchone():
        raise PlanError('rain_source_conflict')
    cfg = json.loads(db.execute("SELECT value FROM meta WHERE key='config_json'").fetchone()['value'])
    spec = next((s for s in cfg.get('stations', []) if s.get('id') == station), None)
    if spec is None:
        raise PlanError('rain_station_not_configured')
    tz = timezone(timedelta(minutes=int(spec['offset_minutes'])))
    details = json.loads(row['details'])
    hourly = details.get('hourly_mm') or {}
    start = _parse_ts(case['start'])
    delta = timedelta(minutes=int(case['slot_minutes']))
    per_mm = case['inflow_units_per_mm']
    n = len(case['downstream_base'])
    inflows = []
    hours = []
    for t in range(n):
        key = (start + t * delta).astimezone(tz).replace(minute=0, second=0, microsecond=0).isoformat()
        if key not in hourly:
            raise PlanError(f'rain_slot_missing:{key}')
        units = round(float(hourly[key]) * per_mm, 6)
        if not float(units).is_integer():
            raise PlanError('inflow_units_not_integral')
        inflows.append(int(units))
        hours.append({'slot': t, 'hour': key, 'mm': hourly[key], 'units': int(units)})
    evidence = {'station': station, 'rule_version': row['rule_version'], 'run_id': row['run_id'],
                'config_sha256': db.execute("SELECT value FROM meta WHERE key='config_hash'").fetchone()['value'],
                'hourly': hours, 'event_ids': details.get('evidence', [])}
    return inflows, evidence


def _slot_entries(case, inflows, traj_slots, locked_prefix):
    kinds = {d['id']: d['kind'] for d in devices_of(case)}
    travel = int(case.get('downstream_travel_slots', 0))
    start = _parse_ts(case['start'])
    delta = timedelta(minutes=int(case['slot_minutes']))
    slots = []
    for t, s in enumerate(traj_slots):
        executed = t < len(locked_prefix)
        entry = {'slot': t, 'time': (start + t * delta).isoformat(), 'inflow_units': inflows[t],
                 'commands': [{'device': dev, 'kind': kinds[dev], 'amount': amt}
                              for dev, amt in sorted(s['commands'].items())],
                 'executed': executed,
                 'storage_end': s['storage_end'], 'pond_end': s['pond_end'],
                 'downstream': {'arrival_slot': t + travel,
                                'time': (start + (t + travel) * delta).isoformat(),
                                'base': s['downstream_base'], 'gate_arrived': s['gate_arrived'],
                                'total': s['downstream_total']}}
        if executed:
            step = locked_prefix[t]
            entry['feedback_identity'] = step['feedback_identity']
            entry['observed_storage_end'] = step['observed_storage']
            entry['observed_pond_end'] = step['observed_pond']
        slots.append(entry)
    return slots


def _spec(case_sha, objective, inflows, locked_prefix, revision, base_id):
    return {'case_sha256': case_sha, 'objective': objective, 'inflows': inflows,
            'locked_prefix': locked_prefix, 'revision': revision, 'base_plan_id': base_id}


def _document(case, case_sha, objective, inflows, evidence, locked_prefix, revision, base_id, result):
    spec = _spec(case_sha, objective, inflows, locked_prefix, revision, base_id)
    spec_sha = _sha(_canon(spec).encode('utf-8'))
    m = result['trajectory']['metrics']
    doc = {'plan_id': 'plan-' + spec_sha[:16], 'revision': revision, 'base_plan_id': base_id,
           'status': 'active', 'planner_version': VERSION, 'objective': objective,
           'station': case['rain_station'], 'start': case['start'], 'slot_minutes': case['slot_minutes'],
           'case': case, 'case_sha256': case_sha, 'inflows': inflows, 'rain_evidence': evidence,
           'locked_prefix': locked_prefix,
           'slots': _slot_entries(case, inflows, result['trajectory']['slots'], locked_prefix),
           'metrics': {'downstream_peak': m['downstream_peak'],
                       'high_water_exposure': m['high_water_exposure'],
                       'final_storage': m['final_storage'], 'final_pond': m['final_pond'],
                       'objective_key': [result['key'][0], result['key'][1]]}}
    return doc, spec_sha


def _infeasible_document(case, case_sha, objective, inflows, evidence, locked_prefix, revision, base_id):
    spec = _spec(case_sha, objective, inflows, locked_prefix, revision, base_id)
    spec_sha = _sha(_canon(spec).encode('utf-8'))
    traj = simulate(case, inflows, [dict(s['commands']) for s in locked_prefix])
    doc = {'plan_id': 'plan-' + spec_sha[:16], 'revision': revision, 'base_plan_id': base_id,
           'status': 'infeasible', 'planner_version': VERSION, 'objective': objective,
           'station': case['rain_station'], 'start': case['start'], 'slot_minutes': case['slot_minutes'],
           'case': case, 'case_sha256': case_sha, 'inflows': inflows, 'rain_evidence': evidence,
           'locked_prefix': locked_prefix,
           'slots': _slot_entries(case, inflows, traj['slots'], locked_prefix),
           'metrics': None}
    return doc, spec_sha


def _persist(db, doc, spec_sha):
    db.execute('INSERT OR IGNORE INTO plan(id,revision,base_id,station,objective,status,spec_sha256,document,created_at)'
               ' VALUES (?,?,?,?,?,?,?,?,?)',
               (doc['plan_id'], doc['revision'], doc['base_plan_id'], doc['station'], doc['objective'],
                doc['status'], spec_sha, json.dumps(doc, ensure_ascii=False, sort_keys=True), _now()))


def generate_plan(db_path, case_source, objective):
    """离线生成班次指令并写入计划台账;证据不足或无可行时序时拒绝自动排程。

    同一输入重算得到同一 plan_id,重复执行幂等。
    """
    case, case_sha = load_case(case_source)
    if objective not in OBJECTIVES:
        raise PlanError(f'unknown_objective:{objective}')
    if objective not in case.get('objectives', OBJECTIVES):
        raise PlanError(f'objective_not_in_contract:{objective}')
    db = connect(db_path)
    try:
        inflows, evidence = trusted_inflows(db, case)
        result = optimize(case, inflows, objective)
        if result is None:
            raise Infeasible('no_feasible_plan')
        doc, spec_sha = _document(case, case_sha, objective, inflows, evidence, [], 0, None, result)
        with db:
            _persist(db, doc, spec_sha)
        return doc
    finally:
        db.close()


def apply_feedback(db_path, feedback_source):
    """录入设备真实反馈;一致则锁定已执行前缀并重算未执行时段。

    相同身份+相同内容幂等(duplicate);相同身份不同内容记冲突(issue);
    迟到/矛盾反馈待核(pending),不产生新修订;不改写原始降雨台账。
    """
    if isinstance(feedback_source, dict):
        data = feedback_source
        raw = _canon(data).encode('utf-8')
    elif isinstance(feedback_source, (bytes, bytearray)):
        raw = bytes(feedback_source)
        data = json.loads(raw)
    else:
        raw = Path(feedback_source).read_bytes()
        data = json.loads(raw)
    sha = _sha(raw)
    identity = data.get('identity')
    if not identity:
        raise PlanError('feedback_identity_missing')
    db = connect(db_path)
    try:
        old = db.execute('SELECT sha256 FROM plan_feedback WHERE identity=?', (identity,)).fetchone()
        if old:
            if old['sha256'] == sha:
                return {'status': 'duplicate', 'identity': identity}
            with db:
                db.execute("INSERT INTO issue(code,detail) VALUES ('feedback_identity_conflict',?)", (identity,))
            return {'status': 'conflict', 'identity': identity}
        plan_row = db.execute("SELECT * FROM plan WHERE objective=? AND status='active'"
                              ' ORDER BY revision DESC LIMIT 1', (data.get('planned_policy'),)).fetchone()

        def pend(reason):
            with db:
                db.execute('INSERT INTO plan_feedback(identity,plan_id,slot,sha256,payload,status,detail,created_at)'
                           ' VALUES (?,?,?,?,?,?,?,?)',
                           (identity, plan_row['id'] if plan_row else None, data.get('slot'), sha,
                            raw.decode('utf-8'), 'pending', reason, _now()))
            return {'status': 'pending', 'reason': reason, 'identity': identity}

        if plan_row is None:
            return pend('no_active_plan')
        doc = json.loads(plan_row['document'])
        case = doc['case']
        inflows = [int(u) for u in doc['inflows']]
        prefix = doc['locked_prefix']
        slot = data.get('slot')
        if slot != len(prefix):
            return pend('slot_out_of_order')
        planned = {(c['device'], c['amount']) for c in doc['slots'][slot]['commands']}
        fb_planned = data.get('planned') or {}
        expected = {(fb_planned.get('device'), fb_planned.get('amount'))} if fb_planned.get('amount') else set()
        if planned != expected:
            return pend('planned_mismatch')
        fb_actual = data.get('actual')
        if not isinstance(fb_actual, dict):
            return pend('actual_infeasible')
        try:
            amount = fb_actual['amount']
            if not isinstance(amount, int) or isinstance(amount, bool) or amount < 0:
                raise ValueError('bad_amount')
            actual_cmd = {str(fb_actual['device']): amount} if amount else {}
        except (KeyError, TypeError, ValueError):
            return pend('actual_infeasible')
        prefix_cmds = [dict(step['commands']) for step in prefix] + [actual_cmd]
        try:
            traj = simulate(case, inflows, prefix_cmds)
        except PlanError:
            return pend('actual_infeasible')
        last = traj['slots'][-1]
        if (last['storage_end'] != data.get('observed_storage_after_slot')
                or last['pond_end'] != data.get('observed_pond_after_slot')):
            return pend('observation_mismatch')
        step = {'slot': slot, 'commands': actual_cmd, 'observed_storage': last['storage_end'],
                'observed_pond': last['pond_end'], 'feedback_identity': identity}
        new_prefix = prefix + [step]
        result = optimize(case, inflows, doc['objective'],
                          locked=[dict(s['commands']) for s in new_prefix])
        with db:
            db.execute('INSERT INTO plan_feedback(identity,plan_id,slot,sha256,payload,status,detail,created_at)'
                       ' VALUES (?,?,?,?,?,?,?,?)',
                       (identity, plan_row['id'], slot, sha, raw.decode('utf-8'), 'applied', None, _now()))
            db.execute("UPDATE plan SET status='superseded' WHERE id=?", (plan_row['id'],))
            if result is None:
                db.execute("INSERT INTO issue(code,detail) VALUES ('replan_infeasible',?)", (plan_row['id'],))
                new_doc, spec_sha = _infeasible_document(case, doc['case_sha256'], doc['objective'], inflows,
                                                         doc['rain_evidence'], new_prefix,
                                                         doc['revision'] + 1, doc['plan_id'])
            else:
                new_doc, spec_sha = _document(case, doc['case_sha256'], doc['objective'], inflows,
                                              doc['rain_evidence'], new_prefix,
                                              doc['revision'] + 1, doc['plan_id'], result)
            _persist(db, new_doc, spec_sha)
        return {'status': 'replanned' if result is not None else 'infeasible',
                'identity': identity, 'plan': new_doc}
    finally:
        db.close()
