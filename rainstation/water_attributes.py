"""雨水蓄留项目的节水属性发行台账。

属性来源只能引用同一站点/小时已有的 trusted 雨量决策与独立蓄水罐记录：
期初 + 集雨体积 = 期末 + 溢流 + 流出 必须闭合，非负、罐容边界与时间窗
都须成立，合同文件字节哈希须与声明一致。缺罐记录、未可信小时、质量不
闭合、哈希不符或版本冲突一律进入待核，绝不把小时降雨直接当作实际蓄留。

每张凭证锚定联合基础（冻结配置、decision/rule_version、event/source/
evidence、罐记录、合同字节）的哈希；合同或罐记录修订使基础变化时，原子
撤销旧活跃凭证并按新基础重发，已转让凭证进入召回待办而不静默换主。同一
快照重跑不新发；同一小时两种权属不得并行有效。原始雨量记录不被改写。
"""
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .ingest import hash_bytes, timestamp
from .store import connect

MICRO = 1_000_000  # 以 1e-6 m³ 为整数单位，避免浮点取整误造凭证
TANK_FIELDS = ('capacity_m3', 'opening_m3', 'closing_m3', 'outflow_m3', 'overflow_m3')
VOLUME_RULES = {'gross_rain_title': 'collected', 'retained_water_title': 'retained'}


def _micro(value):
    return int(round(float(value) * MICRO))


def _m3(micro):
    return micro / MICRO


def _now():
    return datetime.now(timezone.utc).isoformat()


def _canonical(row):
    return json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def import_tank_log(db_path, path):
    """导入蓄水罐记录文件；内容相同的重复导入不产生新版本。"""
    raw = Path(path).read_bytes()
    try:
        row = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f'tank_log_parse_error: {exc}') from exc
    missing = [k for k in ('tank_id', 'station', 'hour', *TANK_FIELDS) if k not in row]
    if missing:
        raise ValueError(f'tank_log_incomplete: {missing}')
    try:
        hour_utc = (timestamp(str(row['hour'])).astimezone(timezone.utc)
                    .replace(minute=0, second=0, microsecond=0).isoformat())
        for key in TANK_FIELDS:
            float(row[key])
    except (TypeError, ValueError) as exc:
        raise ValueError(f'tank_log_invalid: {exc}') from exc
    canonical = _canonical(row)
    digest = hash_bytes(canonical.encode('utf-8'))
    db = connect(db_path)
    try:
        latest = db.execute(
            'SELECT sha256 FROM tank_log WHERE tank_id=? AND hour=? ORDER BY id DESC LIMIT 1',
            (str(row['tank_id']), hour_utc)).fetchone()
        if latest and latest['sha256'] == digest:
            return {'imported': False, 'duplicate': True, 'sha256': digest}
        with db:
            version = db.execute(
                'INSERT INTO tank_log(tank_id,station,hour,payload,sha256,raw_sha256,raw,imported_at)'
                ' VALUES (?,?,?,?,?,?,?,?)',
                (str(row['tank_id']), str(row['station']), hour_utc, canonical, digest,
                 hash_bytes(raw), raw, _now())).lastrowid
        return {'imported': True, 'duplicate': False, 'sha256': digest, 'version': version}
    finally:
        db.close()


def import_attribute_contract(db_path, path):
    """导入属性权属合同（JSON 与其引用的合同文件字节）。

    声明哈希与文件实际哈希是否一致在发行时核验；这里原样留存字节，
    使篡改成为可审计的新版本而不是被静默丢弃。
    """
    raw = Path(path).read_bytes()
    try:
        row = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f'contract_parse_error: {exc}') from exc
    missing = [k for k in ('station', 'tank_id', 'catchment_area_m2',
                           'unit_m3_per_certificate', 'file', 'sha256', 'contracts') if k not in row]
    if missing:
        raise ValueError(f'contract_incomplete: {missing}')
    file_path = Path(path).parent / str(row['file'])
    if not file_path.is_file():
        raise ValueError(f'contract_file_missing: {file_path}')
    file_raw = file_path.read_bytes()
    canonical = _canonical(row)
    digest = hash_bytes(canonical.encode('utf-8'))
    file_sha = hash_bytes(file_raw)
    db = connect(db_path)
    try:
        latest = db.execute(
            'SELECT sha256,file_sha256 FROM attribute_contract WHERE station=? ORDER BY id DESC LIMIT 1',
            (str(row['station']),)).fetchone()
        if latest and latest['sha256'] == digest and latest['file_sha256'] == file_sha:
            return {'imported': False, 'duplicate': True, 'sha256': digest, 'file_sha256': file_sha}
        with db:
            version = db.execute(
                'INSERT INTO attribute_contract(station,payload,sha256,file_path,file_sha256,'
                'declared_sha256,raw,file_raw,imported_at) VALUES (?,?,?,?,?,?,?,?,?)',
                (str(row['station']), canonical, digest, str(row['file']), file_sha,
                 str(row['sha256']), raw, file_raw, _now())).lastrowid
        return {'imported': True, 'duplicate': False, 'sha256': digest,
                'file_sha256': file_sha, 'version': version}
    finally:
        db.close()


def _basis(db, station, hour_key, decision, contract, tank):
    """联合基础：冻结配置 + 决策/规则版本 + 事件/来源/证据 + 罐记录 + 合同字节。"""
    evidence = [dict(r) for r in db.execute(
        'SELECT e.event_id AS event_id,e.rule_version AS rule_version,'
        ' ev.sha256 AS event_sha256,s.sha256 AS source_sha256'
        ' FROM evidence e JOIN event ev ON ev.id=e.event_id JOIN source s ON s.id=ev.source_id'
        ' WHERE e.subject=? ORDER BY e.event_id', (station,))]
    config_hash = db.execute("SELECT value FROM meta WHERE key='config_hash'").fetchone()['value']
    basis = {
        'station': station,
        'hour': hour_key,
        'config_hash': config_hash,
        'decision': {'status': decision['status'], 'rule_version': decision['rule_version'],
                     'details': decision['details']},
        'evidence': evidence,
        'tank_log_sha256': tank['sha256'],
        'attribute_contract_sha256': contract['sha256'],
        'contract_file_sha256': contract['file_sha256'],
    }
    basis['hash'] = hash_bytes(json.dumps(basis, sort_keys=True, ensure_ascii=False).encode('utf-8'))
    return basis


def _validate(db, station, hour_key, title, decision, contract, tank):
    """返回 (错误码, 明细, 计量上下文)；可发行时错误码为 None。"""
    if decision is None or decision['status'] != 'trusted':
        return 'hour_not_trusted', '站点决策缺失或未达到 trusted，不得发行', None
    hourly = json.loads(decision['details']).get('hourly_mm') or {}
    if hour_key not in hourly:
        return 'hour_not_observed', f'可信决策中没有小时 {hour_key}，缺测小时不得猜测', None
    rain_mm = float(hourly[hour_key])
    versions = {decision['rule_version']}
    versions.update(r[0] for r in db.execute(
        'SELECT DISTINCT rule_version FROM evidence WHERE subject=?', (station,)))
    if len(versions) != 1:
        return 'rule_version_conflict', f'决策与证据的规则版本不一致: {sorted(versions)}', None
    if contract is None:
        return 'contract_missing', '缺少属性权属合同', None
    c = json.loads(contract['payload'])
    actual = hash_bytes(contract['file_raw'])
    if str(c.get('sha256')) != actual:
        return 'contract_sha_mismatch', f'合同文件哈希不符: 声明 {c.get("sha256")} 实际 {actual}', None
    if str(c.get('station')) != station:
        return 'contract_station_mismatch', f'合同站点 {c.get("station")} 与 {station} 不符', None
    titles = c.get('contracts') or {}
    if title not in titles:
        return 'unknown_title', f'合同未定义权属 {title}', None
    if title not in VOLUME_RULES:
        return 'unsupported_title', f'权属 {title} 没有已实现的计量规则', None
    holder = (titles[title] or {}).get('holder')
    if not holder:
        return 'unknown_title', f'权属 {title} 缺少权利人', None
    try:
        area = float(c['catchment_area_m2'])
        unit = float(c['unit_m3_per_certificate'])
    except (TypeError, ValueError, KeyError) as exc:
        return 'invalid_contract_quantity', f'合同数量字段无效: {exc}', None
    if area <= 0 or unit <= 0 or _micro(unit) <= 0:
        return 'invalid_contract_quantity', '集雨面积与单位水量必须为正', None
    if tank is None:
        return 'tank_log_missing', '缺少该小时的独立蓄水罐记录', None
    t = json.loads(tank['payload'])
    if str(t.get('tank_id')) != str(c.get('tank_id')):
        return 'tank_contract_mismatch', f'罐 {t.get("tank_id")} 与合同罐 {c.get("tank_id")} 不符', None
    try:
        vals = {k: float(t[k]) for k in TANK_FIELDS}
    except (TypeError, ValueError, KeyError) as exc:
        return 'tank_payload_invalid', f'罐记录数值无效: {exc}', None
    if any(v < 0 for v in vals.values()):
        return 'tank_negative', f'罐记录出现负值: {vals}', None
    if vals['opening_m3'] > vals['capacity_m3'] or vals['closing_m3'] > vals['capacity_m3']:
        return 'capacity_exceeded', '期初或期末蓄水超过罐容边界', None
    collected = _micro(rain_mm * area / 1000.0)
    opening, closing = _micro(vals['opening_m3']), _micro(vals['closing_m3'])
    outflow, overflow = _micro(vals['outflow_m3']), _micro(vals['overflow_m3'])
    if opening + collected != closing + overflow + outflow:
        return 'mass_balance_broken', (f'质量不闭合: 期初{_m3(opening)}+集雨{_m3(collected)}'
                                       f' != 期末{_m3(closing)}+溢流{_m3(overflow)}+流出{_m3(outflow)}'), None
    volume = collected if VOLUME_RULES[title] == 'collected' else closing - opening
    if volume < 0:
        return 'retained_unavailable', '罐中蓄水净增量为负，无可发行蓄留属性', None
    ctx = {'rain_mm': rain_mm, 'holder': holder, 'area': area, 'unit': unit,
           'volume_micro': volume, 'collected_micro': collected,
           'contract_file_sha256': contract['file_sha256']}
    return None, None, ctx


def _cert_id(station, hour_key, title, seq, basis_hash):
    tag = ''.join(word[0] for word in title.split('_')).upper()
    stamp = timestamp(hour_key).strftime('%Y%m%dT%H')
    return f'WAC-{station}-{stamp}-{tag}-{seq:04d}-{basis_hash[:8]}'


def _cert_rows(db, issuance_id):
    return [dict(r) for r in db.execute(
        'SELECT * FROM attribute_certificate WHERE issuance_id=? ORDER BY seq', (issuance_id,))]


def _result(status, issuance, certs):
    return {
        'status': status,
        'issuance_id': issuance['id'],
        'station': issuance['station'],
        'hour': issuance['hour'],
        'title': issuance['title'],
        'holder': issuance['holder'],
        'volume_m3': issuance['volume_m3'],
        'carried_in_m3': issuance['carried_in_m3'],
        'unit_m3': issuance['unit_m3'],
        'certificate_count': issuance['certificates'],
        'remainder_m3': issuance['remainder_m3'],
        'certificates': certs,
        'basis': json.loads(issuance['basis_json']),
    }


def _pending(db, station, hour_key, title, code, detail):
    db.execute('DELETE FROM attribute_pending WHERE station=? AND hour IS ? AND title IS ? AND code=?',
               (station, hour_key, title, code))
    db.execute('INSERT INTO attribute_pending(station,hour,title,code,detail,created_at)'
               ' VALUES (?,?,?,?,?,?)', (station, hour_key, title, code, detail, _now()))
    return {'status': 'pending', 'code': code, 'detail': detail,
            'station': station, 'hour': hour_key, 'title': title}


def _revoke(db, issuance):
    """原子撤销一次发行的全部活跃凭证；已转让的进入召回待办，不静默换主。"""
    now = _now()
    certs = db.execute(
        "SELECT * FROM attribute_certificate WHERE issuance_id=? AND status='active'",
        (issuance['id'],)).fetchall()
    for cert in certs:
        if cert['transferred']:
            db.execute("UPDATE attribute_certificate SET status='recall_pending' WHERE id=?", (cert['id'],))
            db.execute('INSERT INTO attribute_recall(cert_id,holder,reason,status,created_at)'
                       ' VALUES (?,?,?,?,?)', (cert['id'], cert['holder'], 'basis_revision', 'pending', now))
        else:
            db.execute("UPDATE attribute_certificate SET status='revoked' WHERE id=?", (cert['id'],))
    db.execute("UPDATE attribute_issuance SET status='revoked' WHERE id=?", (issuance['id'],))
    db.execute('DELETE FROM attribute_carryover WHERE source_issuance_id=?', (issuance['id'],))


def _issue(db, station, hour_key, title, ctx, basis):
    unit_micro = _micro(ctx['unit'])
    file_sha = ctx['contract_file_sha256']
    carried_row = db.execute(
        'SELECT * FROM attribute_carryover WHERE station=? AND title=? AND contract_sha256=?',
        (station, title, file_sha)).fetchone()
    carried = _micro(carried_row['remainder_m3']) if carried_row else 0
    total = ctx['volume_micro'] + carried
    count = total // unit_micro  # 整除取地板，零头只结转，绝不四舍五入造凭证
    remainder = total % unit_micro
    iid = db.execute(
        'INSERT INTO attribute_issuance(station,hour,title,holder,basis_hash,basis_json,volume_m3,'
        'carried_in_m3,unit_m3,certificates,remainder_m3,status,created_at)'
        ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (station, hour_key, title, ctx['holder'], basis['hash'],
         json.dumps(basis, sort_keys=True, ensure_ascii=False),
         _m3(ctx['volume_micro']), _m3(carried), ctx['unit'], count, _m3(remainder),
         'active', _now())).lastrowid
    for seq in range(1, count + 1):
        cid = _cert_id(station, hour_key, title, seq, basis['hash'])
        db.execute(
            'INSERT INTO attribute_certificate(id,issuance_id,station,hour,title,holder,share_m3,'
            'seq,basis_hash,transferred,status) VALUES (?,?,?,?,?,?,?,?,?,0,?)',
            (cid, iid, station, hour_key, title, ctx['holder'], ctx['unit'], seq,
             basis['hash'], 'active'))
    db.execute('DELETE FROM attribute_carryover WHERE station=? AND title=? AND contract_sha256=?',
               (station, title, file_sha))
    if remainder:
        db.execute(
            'INSERT INTO attribute_carryover(station,title,contract_sha256,remainder_m3,'
            'source_hour,source_issuance_id) VALUES (?,?,?,?,?,?)',
            (station, title, file_sha, _m3(remainder), hour_key, iid))
    issuance = db.execute('SELECT * FROM attribute_issuance WHERE id=?', (iid,)).fetchone()
    return _result('issued', issuance, _cert_rows(db, iid))


def _issue_txn(db, station, hour, title):
    cfg_row = db.execute("SELECT value FROM meta WHERE key='config_json'").fetchone()
    if cfg_row is None:
        return _pending(db, station, None, title, 'config_missing', '请先 freeze_config')
    cfg = json.loads(cfg_row['value'])
    spec = {s['id']: s for s in cfg.get('stations', [])}.get(station)
    if spec is None:
        return _pending(db, station, None, title, 'unknown_station', f'配置中没有站点 {station}')
    tz = timezone(timedelta(minutes=spec['offset_minutes']))
    hour_key = (timestamp(hour).astimezone(tz)
                .replace(minute=0, second=0, microsecond=0).isoformat())
    hour_utc = timestamp(hour_key).astimezone(timezone.utc).isoformat()
    decision = db.execute('SELECT * FROM decision WHERE subject=?', (station,)).fetchone()
    contract = db.execute(
        'SELECT * FROM attribute_contract WHERE station=? ORDER BY id DESC LIMIT 1', (station,)).fetchone()
    tank = db.execute(
        'SELECT * FROM tank_log WHERE station=? AND hour=? ORDER BY id DESC LIMIT 1',
        (station, hour_utc)).fetchone()
    basis = _basis(db, station, hour_key, decision, contract, tank) \
        if decision and contract and tank else None
    active = db.execute(
        "SELECT * FROM attribute_issuance WHERE station=? AND hour=? AND status='active'",
        (station, hour_key)).fetchone()
    if active is not None and active['title'] != title:
        return _pending(db, station, hour_key, title, 'title_conflict',
                        f"该小时已有 {active['title']} 有效发行 #{active['id']}，两种权属不得并行有效")
    if active is not None and basis is not None and active['basis_hash'] == basis['hash']:
        return _result('existing', active, _cert_rows(db, active['id']))
    error, detail, ctx = _validate(db, station, hour_key, title, decision, contract, tank)
    if active is not None:  # 联合基础已变：先原子撤销旧凭证，再按新基础重发或待核
        _revoke(db, active)
    if error is not None:
        return _pending(db, station, hour_key, title, error, detail)
    return _issue(db, station, hour_key, title, ctx, basis)


def issue_attributes(db_path, station, hour, title):
    """按最新联合基础为站点/小时/权属发行凭证。

    返回 issued/existing/pending；业务性失败一律进入待核而不抛异常。
    并发申领由每行唯一索引兜底，同一小时只允许一种权属取得有效凭证。
    """
    db = connect(db_path)
    try:
        for _ in range(3):
            try:
                db.execute('BEGIN IMMEDIATE')
                result = _issue_txn(db, station, hour, title)
                db.execute('COMMIT')
                return result
            except sqlite3.IntegrityError:
                db.execute('ROLLBACK')  # 并发下唯一索引冲突，重读后按现状判定
        raise sqlite3.IntegrityError('issuance_race_unresolved')
    finally:
        db.close()


def transfer_certificate(db_path, cert_id, new_holder):
    """转让一张活跃凭证；转让事实入台账，召回时按现持有人处理。"""
    db = connect(db_path)
    try:
        db.execute('BEGIN IMMEDIATE')
        cert = db.execute('SELECT * FROM attribute_certificate WHERE id=?', (cert_id,)).fetchone()
        if cert is None:
            db.execute('ROLLBACK')
            raise ValueError('certificate_not_found')
        if cert['status'] != 'active':
            db.execute('ROLLBACK')
            raise ValueError('certificate_not_transferable')
        old_holder = cert['holder']
        db.execute('UPDATE attribute_certificate SET holder=?, transferred=1 WHERE id=?',
                   (str(new_holder), cert_id))
        db.execute('INSERT INTO attribute_transfer(cert_id,from_holder,to_holder,created_at)'
                   ' VALUES (?,?,?,?)', (cert_id, old_holder, str(new_holder), _now()))
        row = dict(db.execute('SELECT * FROM attribute_certificate WHERE id=?', (cert_id,)).fetchone())
        db.execute('COMMIT')
        return row
    finally:
        db.close()


def list_certificates(db_path, station=None, hour=None, status=None):
    sql = 'SELECT * FROM attribute_certificate WHERE 1=1'
    args = []
    if station is not None:
        sql += ' AND station=?'
        args.append(station)
    if hour is not None:
        sql += ' AND hour=?'
        args.append(hour)
    if status is not None:
        sql += ' AND status=?'
        args.append(status)
    sql += ' ORDER BY issuance_id,seq'
    db = connect(db_path)
    try:
        return [dict(r) for r in db.execute(sql, args)]
    finally:
        db.close()


def recall_queue(db_path):
    db = connect(db_path)
    try:
        return [dict(r) for r in db.execute('SELECT * FROM attribute_recall ORDER BY id')]
    finally:
        db.close()
