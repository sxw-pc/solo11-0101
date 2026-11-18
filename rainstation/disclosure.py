"""面向外部受众的设备型号汇总安全发布。

与内部审计通道（``rainstation.report.export``）完全隔离：本模块只输出
授权站点集合上的 T2 汇总计数，不接触、不输出任何单站型号、设备 ID、
``event.payload``、``source.path``、原始行或可反查地址。

安全模型：把每个站的型号视为二元取值（是否为 T2）。受众此前收到的每份
集合-计数发布都是一条线性约束。发布前枚举满足全部可见约束的所有二元
赋值，若任何单站在全部可行赋值中取值相同，则该站型号已被唯一强制推出，
本次（或该组合）发布不安全。该方法自然覆盖多份重叠集合的联立推断以及
全零/全一等极端情形，不依赖单份集合人数、最新两份重叠或一步差分。
"""
import hashlib
import json
import os
import threading
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path

from .store import connect

RULE_VERSION = '1'
DELIVERED = 'delivered'
DENIED = 'denied'
HISTORICAL = 'historical'
PENDING = 'pending'
POLICIES = ('deny_unsafe', 'minimum_safe_coarsening')
# 外部文件只允许出现这些字段，任何其他字段都不得写入。
EXTERNAL_FIELDS = frozenset(
    ('release_id', 'stations', 't2_count', 'batch_id', 'rule_version', 'metric')
)
METRIC = 'T2_device_count'


class DisclosureError(ValueError):
    """发布流程的确定性错误（非法请求、台账与文件不一致等）。"""


# 进程内按 (数据库, 受众) 串行化；跨进程由 SQLite BEGIN IMMEDIATE 串行化。
_LOCKS = {}
_LOCKS_GUARD = threading.Lock()


def _audience_lock(db_path, audience):
    key = (str(Path(db_path).resolve()), audience)
    with _LOCKS_GUARD:
        lock = _LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _LOCKS[key] = lock
        return lock


def _now():
    return datetime.now(timezone.utc).isoformat()


def _frozen_config(db):
    row = db.execute("SELECT value FROM meta WHERE key='config_json'").fetchone()
    if row is None:
        raise DisclosureError('config_not_frozen')
    return json.loads(row['value'])


def t2_stations(db_path):
    """依据冻结的站点契约返回 T2 站点集合（内部事实，绝不写入外部文件）。"""
    db = connect(db_path)
    try:
        cfg = _frozen_config(db)
        t2 = set()
        for spec in cfg['stations']:
            models = set(spec.get('device_models', {}).values())
            if any(m == 'T2' for m in models):
                t2.add(spec['id'])
        # 与真实导入事件的 device_model 交叉核对，避免契约与采录证据漂移。
        seen = {}
        for row in db.execute('SELECT subject,payload FROM event'):
            payload = json.loads(row['payload'])
            if 'device_model' in payload:
                prior = seen.get(row['subject'])
                if prior is not None and prior != payload['device_model']:
                    raise DisclosureError('inconsistent_device_model')
                seen[row['subject']] = payload['device_model']
        for station, model in seen.items():
            if station in t2 and model != 'T2':
                raise DisclosureError('inconsistent_device_model')
            if station not in t2 and model == 'T2':
                raise DisclosureError('inconsistent_device_model')
        return frozenset(t2)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 二元推断引擎
# ---------------------------------------------------------------------------

def _feasible(universe, facts, assignment):
    """检查部分/完整赋值是否满足全部集合-计数约束。"""
    for stations, count in facts:
        known = sum(1 for s in stations if s in assignment)
        unknown = len(stations) - known
        ones = sum(1 for s in stations if assignment.get(s) == 1)
        if ones > count or ones + unknown < count:
            return False
    return True


def feasible_assignments(universe, facts):
    """枚举满足所有可见事实的完整二元赋值（按站点排序，结果确定）。"""
    universe = sorted(universe)
    facts = [(sorted(stations), count) for stations, count in facts]
    results = []

    def search(assignment, index):
        if not _feasible(universe, facts, assignment):
            return
        if index == len(universe):
            results.append(dict(assignment))
            return
        station = universe[index]
        for value in (0, 1):
            assignment[station] = value
            search(assignment, index + 1)
        del assignment[station]

    search({}, 0)
    return results


def forced_stations(universe, facts):
    """返回被可见事实唯一强制推出型号的站点 -> 二元取值。

    任何出现在此结果中的站点都意味着单站型号可被外部反推，发布不安全。
    全零组（某站在所有可行赋值中均为 0）与全一组（均为 1）同样被识别。
    """
    assignments = feasible_assignments(universe, facts)
    if not assignments:
        raise DisclosureError('infeasible_disclosure_facts')
    forced = {}
    for station in sorted(universe):
        values = {a[station] for a in assignments}
        if len(values) == 1:
            forced[station] = values.pop()
    return forced


def is_safe(universe, facts):
    """没有任何单站取值被可见事实唯一强制时才安全。"""
    return not forced_stations(universe, facts)


# ---------------------------------------------------------------------------
# 台账与外部文件
# ---------------------------------------------------------------------------

def audience_facts(db_path, audience):
    """该受众已实际交付的全部集合-计数知识。

    只统计本受众 ``delivered``/``historical`` 记录；被拒请求、其他受众的
    内部/外部记录、未授权站点均不会进入该受众的已披露知识。
    """
    db = connect(db_path)
    try:
        rows = db.execute(
            "SELECT stations_json,t2_count FROM disclosure_ledger "
            "WHERE audience=? AND status IN (?,?) ORDER BY id",
            (audience, DELIVERED, HISTORICAL),
        ).fetchall()
        return [(tuple(json.loads(r['stations_json'])), r['t2_count']) for r in rows]
    finally:
        db.close()


def build_external_payload(release_id, stations, t2_count, batch_id, rule_version=RULE_VERSION):
    """构造外部文件内容。白名单字段，绝不携带单站级信息。"""
    payload = {
        'release_id': release_id,
        'stations': sorted(stations),
        't2_count': int(t2_count),
        'batch_id': batch_id,
        'rule_version': rule_version,
        'metric': METRIC,
    }
    if set(payload) != EXTERNAL_FIELDS:  # 防御：字段集合一旦漂移立即失败
        raise DisclosureError('external_field_violation')
    return payload


def _canonical_bytes(payload):
    """实际落盘字节（统一缩进格式），文件摘要始终对这段字节计算。"""
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode('utf-8')


def file_digest(payload):
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _atomic_write(path, payload):
    """临时文件 + fsync + 同目录原子改名，杜绝部分写出的交付文件。

    返回 (路径, 落盘字节摘要)；台账与重建校验使用同一个摘要函数。
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = _canonical_bytes(payload)
    digest = hashlib.sha256(data).hexdigest()
    tmp = path.with_name(path.name + '.tmp')
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)
    return path, digest


def _release_id(request_id):
    """由请求 ID 确定性派生发布 ID（REQ-02 -> REL-02）。"""
    if request_id.startswith('REQ-'):
        return 'REL-' + request_id[len('REQ-'):]
    return 'REL-' + request_id


def _batch_id(db):
    row = db.execute('SELECT id FROM batch ORDER BY id LIMIT 1').fetchone()
    if row is None:
        raise DisclosureError('batch_missing')
    return row['id']


def seed_prior_release(db_path, audience, release):
    """把该受众此前已在其他渠道发布过的历史事实冻结进台账。

    历史发布没有本地交付文件，状态为 ``historical``；同 (受众,请求) 重放
    必须与已记录事实完全一致，否则拒绝污染已披露知识。
    """
    stations = tuple(sorted(release['stations']))
    count = int(release['count'])
    request_id = 'SEED-' + release['id']
    with _audience_lock(db_path, audience):
        db = connect(db_path)
        db.isolation_level = None
        try:
            db.execute('BEGIN IMMEDIATE')
            batch = _batch_id(db)
            existing = db.execute(
                'SELECT status,stations_json,t2_count,release_id FROM disclosure_ledger '
                'WHERE audience=? AND request_id=?', (audience, request_id)
            ).fetchone()
            if existing is not None:
                if (existing['status'] != HISTORICAL
                        or tuple(json.loads(existing['stations_json'])) != stations
                        or existing['t2_count'] != count
                        or existing['release_id'] != release['id']):
                    raise DisclosureError('prior_release_conflict')
                db.execute('COMMIT')
                return
            db.execute(
                'INSERT INTO disclosure_ledger(audience,request_id,release_id,policy,'
                'stations_json,t2_count,request_json,file_path,file_sha256,status,'
                'reason,rule_version,batch_id,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (audience, request_id, release['id'], 'seed', json.dumps(list(stations)),
                 count, json.dumps({'seeded_from': release['id']}, ensure_ascii=False),
                 None, None, HISTORICAL, None, RULE_VERSION, batch, _now()))
            db.execute('COMMIT')
        except Exception:
            db.execute('ROLLBACK')
            raise
        finally:
            db.close()


def reconcile(db_path, audience, out_dir):
    """中断恢复：让台账与实际交付文件重新一致。

    - pending 且文件已存在且摘要匹配 -> 补记 delivered；
    - pending 但文件缺失/不完整 -> 按台账确定性重发同一文件（内容由发布
      参数决定，摘要不变）；
    -  delivered 但文件缺失 -> 同样按台账重建；摘要冲突则报错，绝不换文件
      重发；
    - 目录里没有台账记录的 REL-*.json 视为孤儿文件删除，不能有已交付
      文件却无披露记录。
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    with _audience_lock(db_path, audience):
        db = connect(db_path)
        db.isolation_level = None
        try:
            db.execute('BEGIN IMMEDIATE')
            rows = db.execute(
                'SELECT * FROM disclosure_ledger WHERE audience=? '
                'AND status IN (?,?,?) ORDER BY id',
                (audience, DELIVERED, DENIED, PENDING)).fetchall()
            known = set()
            for r in rows:
                if r['status'] == DENIED:
                    continue
                payload = build_external_payload(
                    r['release_id'], json.loads(r['stations_json']), r['t2_count'],
                    r['batch_id'], r['rule_version'])
                digest = file_digest(payload)
                target = out_dir / (r['release_id'] + '.json')
                known.add(target.name)
                if target.exists():
                    actual = hashlib.sha256(target.read_bytes()).hexdigest()
                    if actual != r['file_sha256'] or actual != digest:
                        raise DisclosureError('ledger_file_digest_mismatch')
                if r['status'] == PENDING:
                    if not target.exists():
                        _atomic_write(target, payload)
                    db.execute('UPDATE disclosure_ledger SET status=?,file_path=?,'
                               'file_sha256=? WHERE id=?',
                               (DELIVERED, str(target), digest, r['id']))
                elif r['status'] == DELIVERED and not target.exists():
                    _, written_digest = _atomic_write(target, payload)
                    if written_digest != digest:
                        raise DisclosureError('ledger_file_digest_mismatch')
            for stray in out_dir.glob('REL-*.json'):
                if stray.name not in known:
                    stray.unlink()
            # 原子改名前崩溃会留下 .tmp 残文件：没有任何台账引用它，清除。
            for stale_tmp in out_dir.glob('REL-*.json.tmp'):
                stale_tmp.unlink()
            db.execute('COMMIT')
        except Exception:
            db.execute('ROLLBACK')
            raise
        finally:
            db.close()


def publish_request(db_path, audience, request, out_dir, policy,
                    allowed_expansion=(), metric_universe=None):
    """对一个受众执行一次外部汇总发布（或确定性拒绝）。

    返回台账记录字典。同一请求重放返回同一发布/拒绝，不产生第二份文件、
    不重复泄露。调用方负责先用 :func:`seed_prior_release` 冻结历史发布。
    """
    if policy not in POLICIES:
        raise DisclosureError('unknown_policy')
    request_id = str(request['id'])
    requested = tuple(sorted(request['stations']))
    expansion_pool = tuple(sorted(allowed_expansion))
    with _audience_lock(db_path, audience):
        # 先做崩溃对账，保证恢复一致后再处理新请求。
        reconcile(db_path, audience, out_dir)
        # T2 真值来自冻结配置与不可变事件；用独立连接在写事务外读取，
        # 避免在 BEGIN IMMEDIATE 保留写锁时再开连接触发 SQLite 写锁竞争。
        t2 = t2_stations(db_path)
        db = connect(db_path)
        db.isolation_level = None
        try:
            db.execute('BEGIN IMMEDIATE')
            batch = _batch_id(db)
            cfg = _frozen_config(db)
            universe = tuple(sorted(
                metric_universe or [s['id'] for s in cfg['stations']]
            ))
            authorized = set(universe)
            prior_facts = [
                (tuple(json.loads(r['stations_json'])), r['t2_count'])
                for r in db.execute(
                    "SELECT stations_json,t2_count FROM disclosure_ledger "
                    "WHERE audience=? AND status IN (?,?) ORDER BY id",
                    (audience, DELIVERED, HISTORICAL)).fetchall()
            ]
            existing = db.execute(
                'SELECT * FROM disclosure_ledger WHERE audience=? AND request_id=?',
                (audience, request_id)).fetchone()

            def count_for(stations):
                return sum(1 for s in stations if s in t2)

            def record_deny(reason, stations_for_record):
                if existing is not None:  # 重放：必须与首次决定完全一致
                    if existing['status'] != DENIED or existing['reason'] != reason:
                        raise DisclosureError('request_decision_conflict')
                    db.execute('COMMIT')
                    return dict(existing)
                db.execute(
                    'INSERT INTO disclosure_ledger(audience,request_id,release_id,policy,'
                    'stations_json,t2_count,request_json,file_path,file_sha256,status,'
                    'reason,rule_version,batch_id,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (audience, request_id, None, policy,
                     json.dumps(list(stations_for_record)), None,
                     json.dumps(request, ensure_ascii=False), None, None,
                     DENIED, reason, RULE_VERSION, batch, _now()))
                row = db.execute(
                    'SELECT * FROM disclosure_ledger WHERE audience=? AND request_id=?',
                    (audience, request_id)).fetchone()
                db.execute('COMMIT')
                return dict(row)

            # 未授权站点：直接拒绝，不向该受众的已披露知识写入任何事实。
            unauthorized = [s for s in requested if s not in authorized]
            if unauthorized:
                return record_deny('unauthorized_station', requested)
            if any(s not in authorized for s in expansion_pool):
                raise DisclosureError('unauthorized_expansion_station')

            candidate_facts = prior_facts + [(requested, count_for(requested))]
            if existing is not None:
                # 确定性重放：校验请求与策略一致后原样返回，绝不二次发布。
                if existing['policy'] != policy:
                    raise DisclosureError('request_policy_conflict')
                if tuple(json.loads(existing['stations_json'])) not in (
                        requested, tuple(sorted(set(requested) | set(expansion_pool)))):
                    raise DisclosureError('request_stations_conflict')
                db.execute('COMMIT')
                return dict(existing)

            if policy == 'deny_unsafe':
                if not is_safe(universe, candidate_facts):
                    return record_deny('inference_unsafe', requested)
                chosen_stations = requested
            else:
                if is_safe(universe, candidate_facts):
                    chosen_stations = requested
                else:
                    # 只允许从 allowed_expansion 加入最少站点：按加入数量
                    # 从小到大、同数量按字典序枚举，取首个安全扩展。
                    chosen_stations = None
                    pool = expansion_pool
                    for size in range(1, len(pool) + 1):
                        for extra in combinations(pool, size):
                            stations = tuple(sorted(set(requested) | set(extra)))
                            facts = prior_facts + [(stations, count_for(stations))]
                            if is_safe(universe, facts):
                                chosen_stations = stations
                                break
                        if chosen_stations is not None:
                            break
                    if chosen_stations is None:
                        return record_deny('no_safe_expansion', requested)

            stations = tuple(chosen_stations)
            count = count_for(stations)
            release_id = _release_id(request_id)
            payload = build_external_payload(release_id, stations, count, batch)
            digest = file_digest(payload)
            target = Path(out_dir) / (release_id + '.json')
            # 先落 pending 台账（提交后再写文件），任何中断都可由 reconcile
            # 收敛到“台账与文件一致”，不会出现无记录的交付文件。
            cur = db.execute(
                'INSERT INTO disclosure_ledger(audience,request_id,release_id,policy,'
                'stations_json,t2_count,request_json,file_path,file_sha256,status,'
                'reason,rule_version,batch_id,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (audience, request_id, release_id, policy, json.dumps(list(stations)),
                 count, json.dumps(request, ensure_ascii=False), str(target), digest,
                 PENDING, None, RULE_VERSION, batch, _now()))
            ledger_id = cur.lastrowid
            db.execute('COMMIT')
        except Exception:
            db.execute('ROLLBACK')
            raise
        finally:
            db.close()
        # 事务已提交，崩溃点之后由 reconcile 负责补齐或重建。
        reconcile(db_path, audience, out_dir)
        db = connect(db_path)
        try:
            row = dict(db.execute('SELECT * FROM disclosure_ledger WHERE id=?',
                                  (ledger_id,)).fetchone())
        finally:
            db.close()
        if row['status'] != DELIVERED:
            raise DisclosureError('publish_reconcile_failed')
        return row


def ledger_view(db_path, audience):
    """供审计/测试查看某受众的完整台账（含拒绝记录）。"""
    db = connect(db_path)
    try:
        return [dict(r) for r in db.execute(
            'SELECT * FROM disclosure_ledger WHERE audience=? ORDER BY id',
            (audience,)).fetchall()]
    finally:
        db.close()
