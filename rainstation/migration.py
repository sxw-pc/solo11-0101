"""未来站址/记录仪预约台账：离线转配（迁站）求解。

预约与既有冻结证据严格分离：本模块只写 migration_request /
migration_reservation / migration_audit 三张台账，绝不改写
meta(config_json/config_hash)、source、event、issue、evidence、decision。

一台记录仪在同一个生效区间 [effective_at, effective_to) 内只能交付一个
新站址；一个新站址在同一生效区间内也只能被一个站点占用。一个申请组
（流域迁站组）必须整组同时拿到互不相同且合格的设备才确认，否则整组
拒绝，不扣用任何设备、不产生任何半组占用。
"""
import hashlib
import json
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path

from .ingest import hash_bytes, timestamp
from .store import connect

CONTRACTS = ('oldest_complete_basin', 'max_completed_basins')


def _canonical_request(req, effective_at, contract, loggers):
    payload = {
        'group': req['group'],
        'requested_at': req['requested_at'],
        'effective_at': effective_at,
        'contract': contract,
        'stations': list(req['stations']),
        'new_sites': list(req['new_sites']),
        'loggers': sorted(loggers, key=lambda lg: lg['serial']),
    }
    canonical = json.dumps(
        payload, sort_keys=True, ensure_ascii=False, separators=(',', ':')
    )
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _now():
    return datetime.now(timezone.utc).isoformat()


def verify_loggers(loggers, cert_dir):
    """逐台核验记录仪：序列号唯一、型号齐全、证书真实字节哈希与有效期。

    返回 (by_serial, defects)。defects: serial -> [reason,...]，
    reason 取值：cert_missing / cert_hash_mismatch /
    duplicate_logger_serial / logger_missing_model。
    有效期（cert_expired/cert_not_yet_valid）按迁站生效时刻在求解阶段判定。
    """
    cert_dir = Path(cert_dir)
    by_serial = {}
    defects = {}
    seen = set()
    for lg in loggers:
        serial = lg['serial']
        reasons = defects.setdefault(serial, [])
        if serial in seen:
            reasons.append('duplicate_logger_serial')
        seen.add(serial)
        if not lg.get('model'):
            reasons.append('logger_missing_model')
        cert_path = cert_dir / lg['file']
        try:
            actual = hash_bytes(cert_path.read_bytes())
        except OSError:
            reasons.append('cert_missing')
            continue
        if actual != lg['sha256']:
            reasons.append('cert_hash_mismatch')
        by_serial[serial] = lg
    return by_serial, defects


def _overlaps(a_from, a_to, b_from, b_to):
    return timestamp(a_from) < timestamp(b_to) and timestamp(b_from) < timestamp(a_to)


def _load_active(db):
    """读取仍占用资源的预约（active 占位，cancelled 已释放）。"""
    rows = db.execute(
        "SELECT * FROM migration_reservation WHERE status='active' ORDER BY id"
    ).fetchall()
    return rows


def _eligible_for_station(station, effective_at, interval_to, frozen_stations,
                          logger_by_serial, logger_defects, active_rows):
    """单站合格记录仪：型号须在该冻结站点的 device_models 内、证书真实字节
    哈希未篡改、生效时刻落在证书有效期 [valid_from, valid_to) 内、且与既有
    预约的生效区间不重叠。返回 (合格序列号, 被拦截原因)。"""
    required_models = set(frozen_stations[station].get('device_models', {}).values())
    eligible, blocked = [], {}
    eff = timestamp(effective_at)
    for serial, lg in logger_by_serial.items():
        reasons = list(logger_defects.get(serial, ()))
        if lg.get('model') not in required_models:
            reasons.append('logger_model_mismatch')
        if not (timestamp(lg['valid_from']) <= eff < timestamp(lg['valid_to'])):
            reasons.append('cert_expired')
        if any(r['logger_serial'] == serial
               and _overlaps(effective_at, interval_to,
                             r['effective_from'], r['effective_to'])
               for r in active_rows):
            reasons.append('logger_interval_conflict')
        if reasons:
            blocked[serial] = reasons
        else:
            eligible.append(serial)
    return sorted(eligible), blocked


def _joint_match(entries, eligible_by_station):
    """跨整组联立的二分匹配：所有入选组的站点必须拿到互不相同的合格记录仪，
    且新站址互不重复。返回 assignment {station: serial} 或 None。"""
    pool_sites = set()
    for e in entries:
        for site in e['req']['new_sites']:
            if site in pool_sites:
                return None
            pool_sites.add(site)
    assignment = {}

    def augment(station, tried):
        for serial in eligible_by_station[station]:
            if serial in tried:
                continue
            tried.add(serial)
            owner = next((s for s, v in assignment.items() if v == serial), None)
            if owner is None or augment(owner, tried):
                assignment[station] = serial
                return True
        return False

    for e in entries:
        for station in e['req']['stations']:
            if not augment(station, set()):
                return None
    return assignment


def plan_transfer(db_path, transfer_path, cert_dir=None, contract=None):
    """按转配申请文件联立求解真实转配，原子写入预约台账。

    同一申请（组、站点、新站址、申请/生效时刻、合同、记录仪清单的规范
    哈希相同）重跑幂等：直接回放台账中既有的确认/拒绝结果。
    """
    transfer_path = Path(transfer_path)
    case = json.loads(transfer_path.read_text(encoding='utf-8'))
    cert_dir = Path(cert_dir) if cert_dir else transfer_path.parent / 'logger_certificates'
    effective_at = case['effective_at']
    chosen_contract = contract or case['contracts'][0]
    if chosen_contract not in CONTRACTS:
        raise ValueError('unknown_contract:' + str(chosen_contract))

    db = connect(db_path)
    frozen = db.execute(
        "SELECT value FROM meta WHERE key='config_json'"
    ).fetchone()
    if frozen is None:
        db.close()
        raise ValueError('config_not_frozen')
    frozen_stations = {s['id']: s for s in json.loads(frozen['value'])['stations']}

    logger_by_serial, logger_defects = verify_loggers(case['loggers'], cert_dir)
    # 申请在生效时刻可能占用的最宽区间，用于与既有预约做区间重叠判断
    interval_to = max(lg['valid_to'] for lg in case['loggers'])
    active_rows = _load_active(db)

    prepared = []
    for req in case['requests']:
        req_hash = _canonical_request(req, effective_at, chosen_contract, case['loggers'])
        existing = db.execute(
            "SELECT * FROM migration_request WHERE request_hash=? "
            "AND status IN ('confirmed','rejected') ORDER BY id",
            (req_hash,),
        ).fetchone()
        entry = {'req': req, 'hash': req_hash, 'existing': existing}
        if existing is None:
            stations, new_sites = list(req['stations']), list(req['new_sites'])
            reasons = []
            if len(stations) != len(new_sites):
                reasons.append('station_site_count_mismatch')
            unknown = sorted({s for s in stations if s not in frozen_stations})
            if unknown:
                reasons.append('unknown_station:' + ','.join(unknown))
            if len(set(stations)) != len(stations):
                reasons.append('duplicate_station_in_request')
            if len(set(new_sites)) != len(new_sites):
                reasons.append('duplicate_new_site_in_request')
            station_clash = sorted({
                s for s in stations
                if any(r['station_id'] == s
                       and _overlaps(effective_at, interval_to,
                                     r['effective_from'], r['effective_to'])
                       for r in active_rows)})
            if station_clash:
                reasons.append('station_already_reserved:' + ','.join(station_clash))
            site_clash = sorted({
                site for site in new_sites
                if any(r['new_site'] == site
                       and _overlaps(effective_at, interval_to,
                                     r['effective_from'], r['effective_to'])
                       for r in active_rows)})
            if site_clash:
                reasons.append('new_site_conflict:' + ','.join(site_clash))
            if not reasons:
                eligible_by_station, blocked_by_station = {}, {}
                for station in stations:
                    elig, blk = _eligible_for_station(
                        station, effective_at, interval_to, frozen_stations,
                        logger_by_serial, logger_defects, active_rows)
                    eligible_by_station[station] = elig
                    blocked_by_station[station] = blk
                entry['eligible_by_station'] = eligible_by_station
                entry['blocked_by_station'] = blocked_by_station
            entry['reasons'] = reasons
        prepared.append(entry)

    # 同一批申请内部的站点/站址互斥（后到的申请带可审计冲突原因）
    seen_stations, seen_sites = set(), set()
    for entry in prepared:
        existing = entry['existing']
        if existing is not None:
            if existing['status'] == 'confirmed':
                for r in db.execute(
                        "SELECT station_id,new_site FROM migration_reservation "
                        "WHERE request_id=? AND status='active'",
                        (existing['id'],)):
                    seen_stations.add(r['station_id'])
                    seen_sites.add(r['new_site'])
            continue
        req = entry['req']
        extra = []
        for s in req['stations']:
            if s in seen_stations:
                extra.append('duplicate_station_in_request:' + s)
        for site in req['new_sites']:
            if site in seen_sites:
                extra.append('new_site_conflict:' + site)
        if extra:
            entry['reasons'].extend(sorted(set(extra)))
        else:
            seen_stations.update(req['stations'])
            seen_sites.update(req['new_sites'])

    fresh = [e for e in prepared if e['existing'] is None]
    valid = [e for e in fresh if not e['reasons']]
    eligible_map = {s: elig for e in valid for s, elig in
                    e['eligible_by_station'].items()}

    if chosen_contract == 'oldest_complete_basin':
        # 优先最早可完整履约的流域组；按申请时间（组名破同分）逐组贪心
        selected = []
        for e in sorted(valid, key=lambda x: (x['req']['requested_at'], x['req']['group'])):
            if _joint_match(selected + [e], eligible_map) is not None:
                selected.append(e)
    else:
        # 最大化可完整迁站的不同流域组数，再按申请时间破同分
        ordered = sorted(valid, key=lambda x: (x['req']['requested_at'], x['req']['group']))
        selected = None
        for size in range(len(ordered), 0, -1):
            for subset in combinations(range(len(ordered)), size):
                entries = [ordered[i] for i in subset]
                if _joint_match(entries, eligible_map) is not None:
                    selected = entries
                    break
            if selected is not None:
                break
        selected = selected or []

    selected_ids = {id(e) for e in selected}
    assignment = _joint_match(selected, eligible_map) or {}

    confirmed, rejections = [], []
    with db:
        # 幂等回放：台账里已有结论的申请原样返回
        for e in prepared:
            existing = e['existing']
            if existing is None:
                continue
            if existing['status'] == 'confirmed':
                rows = db.execute(
                    "SELECT * FROM migration_reservation WHERE request_id=? "
                    "AND status='active' ORDER BY new_site", (existing['id'],)
                ).fetchall()
                confirmed.append({
                    'group': existing['group_id'], 'status': 'confirmed',
                    'idempotent_rerun': True,
                    'assignments': [
                        {'station': r['station_id'], 'new_site': r['new_site'],
                         'logger_serial': r['logger_serial'],
                         'cert_sha256': r['cert_sha256'],
                         'effective_from': r['effective_from'],
                         'effective_to': r['effective_to']} for r in rows]})
            else:
                rejections.append({
                    'group': existing['group_id'], 'status': 'rejected',
                    'idempotent_rerun': True,
                    'reasons': json.loads(existing['reject_reason'])})

        # 结构性拒绝：未知/重复站点、站址冲突等，不扣用任何设备
        for e in fresh:
            if e['reasons']:
                _persist_reject(db, e, chosen_contract, effective_at, e['reasons'])
                rejections.append({
                    'group': e['req']['group'], 'status': 'rejected',
                    'idempotent_rerun': False, 'reasons': e['reasons']})

        # 合同求解后的整组确认（联立匹配保证整组同时拿到不同合格设备）
        for e in selected:
            req = e['req']
            rid = db.execute(
                'INSERT INTO migration_request(request_hash,group_id,requested_at,'
                'effective_at,contract,status,reject_reason,created_at) '
                'VALUES (?,?,?,?,?,?,?,?)',
                (e['hash'], req['group'], req['requested_at'], effective_at,
                 chosen_contract, 'confirmed', None, _now())).lastrowid
            group_out = []
            for station, site in zip(req['stations'], req['new_sites']):
                serial = assignment[station]
                lg = logger_by_serial[serial]
                db.execute(
                    'INSERT INTO migration_reservation(request_id,group_id,station_id,'
                    'new_site,logger_serial,logger_model,cert_sha256,cert_file,'
                    'effective_from,effective_to,valid_from,valid_to,status,created_at) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (rid, req['group'], station, site, serial, lg['model'],
                     lg['sha256'], lg['file'], effective_at, lg['valid_to'],
                     lg['valid_from'], lg['valid_to'], 'active', _now()))
                db.execute(
                    'INSERT INTO migration_audit(ts,action,request_id,group_id,'
                    'station_id,new_site,logger_serial,detail) VALUES (?,?,?,?,?,?,?,?)',
                    (_now(), 'reserve', rid, req['group'], station, site, serial,
                     json.dumps({'cert_sha256': lg['sha256'],
                                 'contract': chosen_contract}, ensure_ascii=False)))
                group_out.append({'station': station, 'new_site': site,
                                  'logger_serial': serial,
                                  'cert_sha256': lg['sha256'],
                                  'effective_from': effective_at,
                                  'effective_to': lg['valid_to']})
            confirmed.append({'group': req['group'], 'status': 'confirmed',
                              'idempotent_rerun': False,
                              'assignments': sorted(group_out, key=lambda x: x['new_site'])})

        # 合同求解下无法整组履约的申请：整组拒绝，无半组占用
        used_serials = set(assignment.values())
        for e in valid:
            if id(e) in selected_ids or any(
                    c['group'] == e['req']['group'] for c in confirmed):
                continue
            detail = ['group_not_completable:insufficient_qualified_loggers',
                      'required=%d' % len(e['req']['stations']),
                      'loggers_used_by_confirmed_groups=%s'
                      % (','.join(sorted(used_serials)) or 'none')]
            for station, elig in e['eligible_by_station'].items():
                detail.append('%s:eligible=%s' % (station, ','.join(elig) or 'none'))
            for station, blk in e['blocked_by_station'].items():
                for serial, rs in sorted(blk.items()):
                    detail.append('%s:%s=%s' % (station, serial, ','.join(rs)))
            _persist_reject(db, e, chosen_contract, effective_at, detail)
            rejections.append({'group': e['req']['group'], 'status': 'rejected',
                               'idempotent_rerun': False, 'reasons': detail})

    db.close()
    confirmed.sort(key=lambda c: c['group'])
    rejections.sort(key=lambda r: r['group'])
    new_sites_output = [
        {'new_site': a['new_site'], 'station': a['station'],
         'logger_serial': a['logger_serial'], 'group': c['group'],
         'effective_from': a['effective_from'], 'effective_to': a['effective_to']}
        for c in confirmed for a in c['assignments']]
    return {
        'contract': chosen_contract,
        'effective_at': effective_at,
        'confirmed_groups': [c['group'] for c in confirmed],
        'rejected_groups': [r['group'] for r in rejections],
        'confirmed': confirmed,
        'rejections': rejections,
        'new_sites': sorted(new_sites_output, key=lambda x: x['new_site']),
    }


def _persist_reject(db, entry, contract, effective_at, reasons):
    req = entry['req']
    rid = db.execute(
        'INSERT INTO migration_request(request_hash,group_id,requested_at,'
        'effective_at,contract,status,reject_reason,created_at) '
        'VALUES (?,?,?,?,?,?,?,?)',
        (entry['hash'], req['group'], req['requested_at'], effective_at,
         contract, 'rejected', json.dumps(reasons, ensure_ascii=False), _now())
    ).lastrowid
    db.execute(
        'INSERT INTO migration_audit(ts,action,request_id,group_id,station_id,'
        'new_site,logger_serial,detail) VALUES (?,?,?,?,?,?,?,?)',
        (_now(), 'reject', rid, req['group'], None, None, None,
         json.dumps({'reasons': reasons}, ensure_ascii=False)))
    return rid


def cancel_transfer(db_path, group):
    """取消整组预约：active 预约转 cancelled、请求转 cancelled，资源即时释放。

    台账行与审计全部保留，取消后的再分配可追溯。返回释放明细。
    """
    db = connect(db_path)
    req = db.execute(
        "SELECT * FROM migration_request WHERE group_id=? "
        "AND status='confirmed' ORDER BY id DESC", (group,)).fetchone()
    if req is None:
        db.close()
        raise ValueError('group_not_reserved:' + group)
    released = []
    with db:
        rows = db.execute(
            "SELECT * FROM migration_reservation WHERE request_id=? AND status='active'",
            (req['id'],)).fetchall()
        for r in rows:
            db.execute(
                "UPDATE migration_reservation SET status='cancelled',cancelled_at=? "
                "WHERE id=?", (_now(), r['id']))
            db.execute(
                "INSERT INTO migration_audit(ts,action,request_id,group_id,"
                "station_id,new_site,logger_serial,detail) VALUES (?,?,?,?,?,?,?,?)",
                (_now(), 'cancel', req['id'], group, r['station_id'], r['new_site'],
                 r['logger_serial'],
                 json.dumps({'cert_sha256': r['cert_sha256']}, ensure_ascii=False)))
            released.append({'station': r['station_id'], 'new_site': r['new_site'],
                             'logger_serial': r['logger_serial']})
        db.execute(
            "UPDATE migration_request SET status='cancelled' WHERE id=?", (req['id'],))
    db.close()
    return {'group': group, 'status': 'cancelled', 'released': released}


def list_reservations(db_path):
    """输出当前仍生效（active）的预约；取消记录在 migration_audit 可追溯。"""
    db = connect(db_path)
    rows = db.execute(
        "SELECT group_id,station_id,new_site,logger_serial,logger_model,"
        "cert_sha256,cert_file,effective_from,effective_to,valid_from,valid_to "
        "FROM migration_reservation WHERE status='active' ORDER BY new_site"
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]
