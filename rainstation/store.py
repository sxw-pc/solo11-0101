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
CREATE TABLE IF NOT EXISTS migration_request(
  id INTEGER PRIMARY KEY,
  request_hash TEXT NOT NULL,
  group_id TEXT NOT NULL,
  requested_at TEXT NOT NULL,
  effective_at TEXT NOT NULL,
  contract TEXT NOT NULL,
  status TEXT NOT NULL,
  reject_reason TEXT,
  created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_migration_request_live
  ON migration_request(request_hash) WHERE status IN ('confirmed','rejected');
CREATE TABLE IF NOT EXISTS migration_reservation(
  id INTEGER PRIMARY KEY,
  request_id INTEGER NOT NULL REFERENCES migration_request(id),
  group_id TEXT NOT NULL,
  station_id TEXT NOT NULL,
  new_site TEXT NOT NULL,
  logger_serial TEXT NOT NULL,
  logger_model TEXT NOT NULL,
  cert_sha256 TEXT NOT NULL,
  cert_file TEXT NOT NULL,
  effective_from TEXT NOT NULL,
  effective_to TEXT NOT NULL,
  valid_from TEXT NOT NULL,
  valid_to TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TEXT NOT NULL,
  cancelled_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_reservation_logger_interval
  ON migration_reservation(logger_serial,effective_from,effective_to) WHERE status='active';
CREATE UNIQUE INDEX IF NOT EXISTS idx_reservation_site_interval
  ON migration_reservation(new_site,effective_from,effective_to) WHERE status='active';
CREATE TABLE IF NOT EXISTS migration_audit(
  id INTEGER PRIMARY KEY,
  ts TEXT NOT NULL,
  action TEXT NOT NULL,
  request_id INTEGER,
  group_id TEXT NOT NULL,
  station_id TEXT,
  new_site TEXT,
  logger_serial TEXT,
  detail TEXT
);
"""
def connect(path):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(p)
    db.row_factory=sqlite3.Row
    db.executescript(SCHEMA)
    return db
