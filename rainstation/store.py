import sqlite3
from pathlib import Path

SCHEMA="""
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS batch(id TEXT PRIMARY KEY,config_hash TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS subject(id TEXT PRIMARY KEY,batch_id TEXT NOT NULL REFERENCES batch(id));
CREATE TABLE IF NOT EXISTS source(id INTEGER PRIMARY KEY,path TEXT NOT NULL,sha256 TEXT NOT NULL UNIQUE,raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS event(id INTEGER PRIMARY KEY,source_id INTEGER NOT NULL REFERENCES source(id),line_no INTEGER NOT NULL,identity TEXT NOT NULL UNIQUE,subject TEXT NOT NULL,kind TEXT NOT NULL,time TEXT NOT NULL,payload TEXT NOT NULL,sha256 TEXT NOT NULL,UNIQUE(source_id,line_no));
CREATE TABLE IF NOT EXISTS issue(id INTEGER PRIMARY KEY,code TEXT NOT NULL,detail TEXT NOT NULL,event_id INTEGER REFERENCES event(id));
CREATE TABLE IF NOT EXISTS decision(subject TEXT PRIMARY KEY,status TEXT NOT NULL,details TEXT NOT NULL,rule_version TEXT NOT NULL,run_id INTEGER NOT NULL REFERENCES run(id));
CREATE TABLE IF NOT EXISTS evidence(subject TEXT NOT NULL REFERENCES subject(id),event_id INTEGER NOT NULL REFERENCES event(id),rule_version TEXT NOT NULL,run_id INTEGER NOT NULL REFERENCES run(id),PRIMARY KEY(subject,event_id,rule_version));
CREATE TABLE IF NOT EXISTS run(id INTEGER PRIMARY KEY,started_at TEXT NOT NULL,completed_at TEXT,status TEXT NOT NULL);
"""

# 来源快照合同：暂存与正式提交共享一个 snapshot_id。进程在流式读取、
# 暂存或晋升阶段中断后，重入同一 snapshot_id 要么幂等复用已提交结果，
# 要么清理残留暂存后重做，不会留下半份正式 source/event/issue。
STAGING_SCHEMA="""
CREATE TABLE IF NOT EXISTS snapshot(
    id TEXT PRIMARY KEY,
    logical_path TEXT NOT NULL,
    contract TEXT NOT NULL,
    status TEXT NOT NULL,
    sha256 TEXT,
    size INTEGER,
    source_id INTEGER
);
CREATE TABLE IF NOT EXISTS staging_source(
    snapshot_id TEXT PRIMARY KEY REFERENCES snapshot(id),
    path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    raw BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS staging_event(
    id INTEGER PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES snapshot(id),
    line_no INTEGER NOT NULL,
    identity TEXT NOT NULL,
    subject TEXT NOT NULL,
    kind TEXT NOT NULL,
    time TEXT NOT NULL,
    payload TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    UNIQUE(snapshot_id,line_no),
    UNIQUE(snapshot_id,identity)
);
CREATE TABLE IF NOT EXISTS staging_issue(
    id INTEGER PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES snapshot(id),
    line_no INTEGER NOT NULL,
    code TEXT NOT NULL,
    detail TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_staging_event_lookup
    ON staging_event(snapshot_id,identity);
CREATE INDEX IF NOT EXISTS idx_staging_issue_lookup
    ON staging_issue(snapshot_id,code);
"""

# 流式块大小上限。读取器在 Python 进程中至多保留一个块，行缓冲、
# 解码残段与哈希状态均为有界小对象。
DEFAULT_CHUNK_SIZE=1 << 16

# verify_then_retry_latest 在路径反复换版时的最大尝试次数。
MAX_SNAPSHOT_ATTEMPTS=3


def connect(path):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(p)
    db.row_factory=sqlite3.Row
    db.executescript(SCHEMA)
    db.executescript(STAGING_SCHEMA)
    return db


def reset_snapshot_staging(db, snapshot_id):
    """丢弃一次失败/废弃尝试留下的全部暂存，快照回到 streaming。"""
    for table in ('staging_issue','staging_event','staging_source'):
        db.execute(f'DELETE FROM {table} WHERE snapshot_id=?', (snapshot_id,))
    db.execute(
        "UPDATE snapshot SET status='streaming', sha256=NULL, size=NULL "
        'WHERE id=?', (snapshot_id,))


def write_blob_chunk(db, table, rowid, offset, chunk):
    """对已用 zeroblob 占位的 BLOB 做有界增量写入，块边界可与提交边界对齐。"""
    # 确保当前连接持有写事务后再打开 BLOB 句柄。
    db.execute(f'UPDATE {table} SET rowid=rowid WHERE rowid=?', (rowid,))
    handle=db.blobopen(table, 'raw', rowid, readonly=False)
    try:
        handle.seek(offset)
        handle.write(chunk)
    finally:
        handle.close()
