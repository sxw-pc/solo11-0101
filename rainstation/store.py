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
CREATE TABLE IF NOT EXISTS tank_log(id INTEGER PRIMARY KEY,tank_id TEXT NOT NULL,station TEXT NOT NULL,hour TEXT NOT NULL,payload TEXT NOT NULL,sha256 TEXT NOT NULL,raw_sha256 TEXT NOT NULL,raw BLOB NOT NULL,imported_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS attribute_contract(id INTEGER PRIMARY KEY,station TEXT NOT NULL,payload TEXT NOT NULL,sha256 TEXT NOT NULL,file_path TEXT NOT NULL,file_sha256 TEXT NOT NULL,declared_sha256 TEXT NOT NULL,raw BLOB NOT NULL,file_raw BLOB NOT NULL,imported_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS attribute_issuance(id INTEGER PRIMARY KEY,station TEXT NOT NULL,hour TEXT NOT NULL,title TEXT NOT NULL,holder TEXT NOT NULL,basis_hash TEXT NOT NULL,basis_json TEXT NOT NULL,volume_m3 REAL NOT NULL,carried_in_m3 REAL NOT NULL,unit_m3 REAL NOT NULL,certificates INTEGER NOT NULL,remainder_m3 REAL NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_issuance_per_hour ON attribute_issuance(station,hour) WHERE status='active';
CREATE TABLE IF NOT EXISTS attribute_certificate(id TEXT PRIMARY KEY,issuance_id INTEGER NOT NULL REFERENCES attribute_issuance(id),station TEXT NOT NULL,hour TEXT NOT NULL,title TEXT NOT NULL,holder TEXT NOT NULL,share_m3 REAL NOT NULL,seq INTEGER NOT NULL,basis_hash TEXT NOT NULL,transferred INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL,UNIQUE(issuance_id,seq));
CREATE UNIQUE INDEX IF NOT EXISTS one_active_certificate_share ON attribute_certificate(station,hour,title,seq) WHERE status='active';
CREATE TABLE IF NOT EXISTS attribute_carryover(station TEXT NOT NULL,title TEXT NOT NULL,contract_sha256 TEXT NOT NULL,remainder_m3 REAL NOT NULL,source_hour TEXT NOT NULL,source_issuance_id INTEGER NOT NULL,PRIMARY KEY(station,title,contract_sha256));
CREATE TABLE IF NOT EXISTS attribute_transfer(id INTEGER PRIMARY KEY,cert_id TEXT NOT NULL,from_holder TEXT NOT NULL,to_holder TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS attribute_recall(id INTEGER PRIMARY KEY,cert_id TEXT NOT NULL,holder TEXT NOT NULL,reason TEXT NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS attribute_pending(id INTEGER PRIMARY KEY,station TEXT NOT NULL,hour TEXT,title TEXT,code TEXT NOT NULL,detail TEXT NOT NULL,created_at TEXT NOT NULL);
"""
def connect(path):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(p)
    db.row_factory=sqlite3.Row
    db.executescript(SCHEMA)
    return db
