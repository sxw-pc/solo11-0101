import codecs
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from .store import (
    DEFAULT_CHUNK_SIZE,
    MAX_SNAPSHOT_ATTEMPTS,
    connect,
    reset_snapshot_staging,
    write_blob_chunk,
)

CONTRACTS = ('freeze_on_open', 'verify_then_retry_latest')


class SnapshotUnstableError(RuntimeError):
    """verify_then_retry_latest 在最大尝试次数内无法读到稳定版本。"""


def hash_bytes(data):
    return hashlib.sha256(data).hexdigest()


def timestamp(text):
    dt = datetime.fromisoformat(text.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        raise ValueError('timezone_required')
    return dt


def freeze_config(db_path, config):
    raw = Path(config).read_bytes()
    digest = hash_bytes(raw)
    db = connect(db_path)
    old = db.execute("SELECT value FROM meta WHERE key='config_hash'").fetchone()
    if old and old['value'] != digest:
        db.close()
        raise ValueError('frozen_config_conflict')
    parsed = json.loads(raw)
    subjects = parsed.get('boxes', parsed.get('stations', parsed.get('recipients', [])))
    ids = [item.get('box', item.get('id')) for item in subjects]
    if not ids or any(not item for item in ids) or len(ids) != len(set(ids)):
        raise ValueError('duplicate_or_missing_subject')
    with db:
        old = db.execute("SELECT value FROM meta WHERE key='config_hash'").fetchone()
        if old and old['value'] != digest:
            raise ValueError('frozen_config_conflict')
        db.execute("INSERT OR IGNORE INTO meta VALUES ('config_hash',?)", (digest,))
        db.execute("INSERT OR IGNORE INTO meta VALUES ('config_json',?)", (raw.decode('utf-8'),))
        db.execute("INSERT OR IGNORE INTO batch VALUES ('batch-1',?)", (digest,))
        for item in subjects:
            db.execute("INSERT OR IGNORE INTO subject VALUES (?,?)",
                       (item.get('box', item.get('id')), 'batch-1'))
    db.close()
    return digest


class JsonlStreamer:
    """有界块 JSONL 行分割器。

    配合 codecs 增量解码处理跨块 BOM 与多字节字符；行边界语义与
    ``decode('utf-8-sig').splitlines()`` 对齐：``\\r\\n``、孤立 ``\\r`` 与
    ``\\n`` 均为行结束，边界可跨块，末尾无换行也保留末行。调用方逐块喂入，
    行缓冲只保留不完整的最后一行，不积累完整文本或全量行数组。
    """

    def __init__(self, chunk_size=DEFAULT_CHUNK_SIZE):
        self.chunk_size = chunk_size
        self.decoder = codecs.getincrementaldecoder('utf-8-sig')()
        self.pending = ''
        # held: 上一块末尾 '\r' 已构成边界、但需看下一块首字符是否为 '\n'
        # 而暂时挂起的行文本（不含该 '\r'）。
        self.held = None
        self.line_no = 0

    def feed(self, chunk):
        return self._extract(self.pending + self.decoder.decode(chunk), False)

    def finish(self):
        lines = self._extract(self.pending + self.decoder.decode(b'', final=True), True)
        if self.pending:
            self.line_no += 1
            lines.append((self.line_no, self.pending))
            self.pending = ''
        return lines

    def _extract(self, text, final):
        lines = []
        if self.held is not None:
            swallow = bool(text) and text[0] == '\n'
            self.line_no += 1
            lines.append((self.line_no, self.held))
            self.held = None
            if swallow:
                text = text[1:]
        start = 0
        j = 0
        n = len(text)
        while j < n:
            c = text[j]
            if c == '\n':
                self.line_no += 1
                lines.append((self.line_no, text[start:j]))
                j += 1
                start = j
            elif c == '\r':
                if j + 1 < n:
                    self.line_no += 1
                    lines.append((self.line_no, text[start:j]))
                    j += 1 if text[j + 1] != '\n' else 2
                    start = j
                elif final:
                    self.line_no += 1
                    lines.append((self.line_no, text[start:j]))
                    start = n
                    break
                else:
                    # '\r' 落在块尾：边界文本先行挂起，下一块首字符若为
                    # '\n' 则吞掉，避免 CRLF 跨块时丢行或重复计数。
                    self.held = text[start:j]
                    start = n
                    break
            else:
                j += 1
        self.pending = text[start:]
        return lines


def _parse_line(db, snapshot_id, line_no, line):
    """逐行解析，坏行隔离与身份冲突语义同历史实现，但全部落在暂存表。"""
    if not line.strip():
        return 0
    try:
        row = json.loads(line)
        kind = str(row['kind'])
        subject = str(row['subject'])
        ts = str(row['time'])
        normalized = timestamp(ts).astimezone(timezone.utc).isoformat()
        identity = str(row.get('identity') or f"{row['device']}:{int(row['seq'])}")
        if not kind or not subject:
            raise ValueError('missing_kind_or_subject')
        canonical = json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
        h = hash_bytes(canonical.encode())
        # 身份冲突沿用历史全局语义：先查本快照暂存，再查正式事件表。
        old = db.execute(
            'SELECT sha256 FROM staging_event WHERE snapshot_id=? AND identity=?',
            (snapshot_id, identity)).fetchone()
        if old is None:
            old = db.execute('SELECT sha256 FROM event WHERE identity=?',
                             (identity,)).fetchone()
        if old:
            if old['sha256'] != h:
                db.execute(
                    'INSERT INTO staging_issue(snapshot_id,line_no,code,detail) VALUES (?,?,?,?)',
                    (snapshot_id, line_no, 'identity_conflict', identity))
            return 0
        db.execute(
            'INSERT INTO staging_event(snapshot_id,line_no,identity,subject,kind,time,payload,sha256)'
            ' VALUES (?,?,?,?,?,?,?,?)',
            (snapshot_id, line_no, identity, subject, kind, normalized, canonical, h))
        return 1
    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        db.execute(
            'INSERT INTO staging_issue(snapshot_id,line_no,code,detail) VALUES (?,?,?,?)',
            (snapshot_id, line_no, 'parse_error',
             json.dumps({'line_no': line_no, 'error': str(exc), 'raw_line': line},
                        ensure_ascii=False)))
        return 0


def _identity_of_path(path):
    return f'snap:{Path(path).resolve()}'


def _begin_or_resume(db, snapshot_id, logical_path, contract):
    """绑定/恢复快照身份，返回 (snapshot_row, promote_staged_directly)。"""
    row = db.execute('SELECT * FROM snapshot WHERE id=?', (snapshot_id,)).fetchone()
    if row is None:
        db.execute(
            'INSERT INTO snapshot(id,logical_path,contract,status) VALUES (?,?,?,?)',
            (snapshot_id, str(logical_path), contract, 'streaming'))
        db.commit()
        return db.execute('SELECT * FROM snapshot WHERE id=?', (snapshot_id,)).fetchone(), False
    if row['contract'] != contract:
        raise ValueError(
            f"snapshot_contract_conflict: {row['contract']} != {contract}")
    if row['status'] == 'committed':
        return row, False
    if row['status'] == 'staged':
        staged = db.execute(
            'SELECT sha256, length(raw) AS size FROM staging_source WHERE snapshot_id=?',
            (snapshot_id,)).fetchone()
        if staged is not None and staged['sha256'] == row['sha256'] \
                and staged['size'] == row['size']:
            return row, True
    # streaming 残留（含进程中断）或 staged 暂存损坏：整组清理后重做。
    reset_snapshot_staging(db, snapshot_id)
    db.commit()
    return db.execute('SELECT * FROM snapshot WHERE id=?', (snapshot_id,)).fetchone(), False


def _stream_attempt(db, snapshot_id, logical_path, chunk_size, mutation_hook):
    """单次打开、单份字节流同时驱动 raw 暂存、增量哈希与逐行解析。

    每块一个短事务：raw 增量 BLOB 与已解析行同批落盘，中断点之前均有持久
    暂存；正式表始终不可见半成品。Python 进程至多持有一个块与一行文本。
    返回哈希/规模/导入计数与打开时的 stat（供调用方判断路径是否被替换）。
    """
    total = os.path.getsize(logical_path)
    db.execute(
        'INSERT INTO staging_source(snapshot_id,path,sha256,raw) VALUES (?,?,?,zeroblob(?))',
        (snapshot_id, str(logical_path), '', total))
    db.commit()
    rowid = db.execute(
        'SELECT rowid FROM staging_source WHERE snapshot_id=?',
        (snapshot_id,)).fetchone()[0]
    hasher = hashlib.sha256()
    offset = 0
    imported = 0
    streamer = JsonlStreamer(chunk_size=chunk_size)
    with open(logical_path, 'rb') as fh:
        opened = os.fstat(fh.fileno())
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
            write_blob_chunk(db, 'staging_source', rowid, offset, chunk)
            offset += len(chunk)
            for line_no, line in streamer.feed(chunk):
                if mutation_hook is not None:
                    mutation_hook(line_no, logical_path)
                imported += _parse_line(db, snapshot_id, line_no, line)
            db.commit()  # raw 块与已解析行同批落盘
        for line_no, line in streamer.finish():
            if mutation_hook is not None:
                mutation_hook(line_no, logical_path)
            imported += _parse_line(db, snapshot_id, line_no, line)
        db.commit()
    if offset != total:
        raise RuntimeError('staging_size_mismatch')
    digest = hasher.hexdigest()
    db.execute('UPDATE staging_source SET sha256=? WHERE snapshot_id=?', (digest, snapshot_id))
    db.execute(
        'UPDATE snapshot SET status=?, sha256=?, size=? WHERE id=?',
        ('staged', digest, total, snapshot_id))
    db.commit()
    return {'sha256': digest, 'size': total, 'imported': imported, 'opened': opened}


def _path_replaced(logical_path, opened_stat):
    """同一逻辑路径是否在打开后被原子替换（设备/inode 变化）。"""
    try:
        current = os.stat(logical_path)
    except FileNotFoundError:
        return True
    return (current.st_dev, current.st_ino) != (opened_stat.st_dev, opened_stat.st_ino)


def _mark_committed(db, snapshot_id, source_id, digest):
    with db:
        db.execute('UPDATE snapshot SET status=?, source_id=?, sha256=? WHERE id=?',
                   ('committed', source_id, digest, snapshot_id))
        reset_snapshot_staging(db, snapshot_id)


def _promote(db, snapshot_id):
    """暂存原子晋升为正式 source/event/issue；失败整体回滚，无半份正式文件。"""
    staged = db.execute(
        'SELECT sha256 FROM staging_source WHERE snapshot_id=?',
        (snapshot_id,)).fetchone()
    with db:
        # raw 在数据库引擎内跨表复制，Python 端不经过完整 BLOB。
        sid = db.execute(
            'INSERT INTO source(path,sha256,raw) SELECT path,sha256,raw '
            'FROM staging_source WHERE snapshot_id=?', (snapshot_id,)).lastrowid
        db.execute(
            'INSERT INTO event(source_id,line_no,identity,subject,kind,time,payload,sha256) '
            'SELECT ?,line_no,identity,subject,kind,time,payload,sha256 '
            'FROM staging_event WHERE snapshot_id=? ORDER BY line_no', (sid, snapshot_id))
        db.execute(
            "INSERT INTO issue(code,detail,event_id) SELECT code,detail,NULL "
            "FROM staging_issue WHERE snapshot_id=? AND code='identity_conflict'",
            (snapshot_id,))
        # 解析错误 detail 补入 source_id；按键集分页惰性改写，内存有界且
        # 不在同一连接上嵌套使用游标。
        last_id = 0
        while True:
            page = db.execute(
                'SELECT id,detail FROM staging_issue WHERE snapshot_id=? AND code=? AND id>? '
                'ORDER BY id LIMIT 1000', (snapshot_id, 'parse_error', last_id)).fetchall()
            if not page:
                break
            for issue_id, detail in page:
                parsed = json.loads(detail)
                parsed['source_id'] = sid
                db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',
                           ('parse_error', json.dumps(parsed, ensure_ascii=False)))
            last_id = page[-1]['id']
        count = db.execute(
            'SELECT COUNT(*) AS n FROM event WHERE source_id=?', (sid,)).fetchone()['n']
        reset_snapshot_staging(db, snapshot_id)
        db.execute('UPDATE snapshot SET status=?, source_id=?, sha256=? WHERE id=?',
                   ('committed', sid, staged['sha256'], snapshot_id))
    return {'imported': count, 'duplicate_file': False,
            'sha256': staged['sha256'], 'source_id': sid, 'snapshot_id': snapshot_id}


def import_file(db_path, path, contract='freeze_on_open', snapshot_id=None,
                chunk_size=DEFAULT_CHUNK_SIZE, mutation_hook=None,
                before_promote=None):
    """低内存流式采录 JSONL 来源，并绑定明确的来源快照合同。

    contract:
      - freeze_on_open: 固定起始打开的版本读完；读取中途路径被原子替换不影响
        本次采录，正式 raw/hash/event 全部来自该原始版本。
      - verify_then_retry_latest: 读完后若发现逻辑路径已换版，废弃暂存并对
        最终稳定版本重试；反复换版超过 MAX_SNAPSHOT_ATTEMPTS 次则失败。

    同一 snapshot_id 重入按源哈希幂等；同事件身份异内容进入冲突隔离。
    before_promote(staging_summary) 在暂存完成、正式提交前触发，用于模拟
    进程中断；中断后以同一 snapshot_id 重入可从完整暂存直接恢复。
    """
    if contract not in CONTRACTS:
        raise ValueError(f'unknown_contract:{contract}')
    if not isinstance(chunk_size, int) or chunk_size <= 0:
        raise ValueError('chunk_size_must_be_positive')
    logical_path = Path(path)
    snapshot_id = snapshot_id or _identity_of_path(logical_path)
    db = connect(db_path)
    try:
        row, promote_staged = _begin_or_resume(db, snapshot_id, logical_path, contract)
        if row['status'] == 'committed':
            existing = db.execute('SELECT sha256 FROM source WHERE id=?',
                                  (row['source_id'],)).fetchone()
            if existing is not None:
                return {'imported': 0, 'duplicate_file': True,
                        'sha256': existing['sha256'],
                        'source_id': row['source_id'], 'snapshot_id': snapshot_id}
            # 快照已标记但正式行缺失（异常状态）：清理后重做。
            reset_snapshot_staging(db, snapshot_id)
            db.execute("UPDATE snapshot SET status='streaming' WHERE id=?", (snapshot_id,))
            db.commit()
            promote_staged = False

        if promote_staged:
            # 中断恢复：暂存完整且哈希自洽，直接晋升，不再重读来源路径；
            # 路径之后是否换版与本次恢复无关——合同证据由快照身份绑定。
            return _promote(db, snapshot_id)

        if contract == 'freeze_on_open':
            staged = _stream_attempt(db, snapshot_id, logical_path, chunk_size, mutation_hook)
            prior = db.execute('SELECT id FROM source WHERE sha256=?',
                               (staged['sha256'],)).fetchone()
            if prior is not None:  # 按源哈希幂等，不产生重复事件
                _mark_committed(db, snapshot_id, prior['id'], staged['sha256'])
                return {'imported': 0, 'duplicate_file': True,
                        'sha256': staged['sha256'],
                        'source_id': prior['id'], 'snapshot_id': snapshot_id}
            if before_promote is not None:
                before_promote(staged)
            return _promote(db, snapshot_id)

        # verify_then_retry_latest
        for attempt in range(1, MAX_SNAPSHOT_ATTEMPTS + 1):
            staged = _stream_attempt(db, snapshot_id, logical_path, chunk_size, mutation_hook)
            if not _path_replaced(logical_path, staged['opened']):
                prior = db.execute('SELECT id FROM source WHERE sha256=?',
                                   (staged['sha256'],)).fetchone()
                if prior is not None:  # 同快照重入按源哈希幂等
                    _mark_committed(db, snapshot_id, prior['id'], staged['sha256'])
                    return {'imported': 0, 'duplicate_file': True,
                            'sha256': staged['sha256'],
                            'source_id': prior['id'], 'snapshot_id': snapshot_id}
                if before_promote is not None:
                    before_promote(staged)
                return _promote(db, snapshot_id)
            if attempt == MAX_SNAPSHOT_ATTEMPTS:
                raise SnapshotUnstableError(
                    f'logical path kept changing after {attempt} attempts: {logical_path}')
            # 路径已换版：废弃前一暂存，对最新版本重试。
            reset_snapshot_staging(db, snapshot_id)
            db.commit()
        raise SnapshotUnstableError('unreachable')
    finally:
        db.close()
