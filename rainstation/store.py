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
CREATE TABLE IF NOT EXISTS terminal_firmware_device(id TEXT PRIMARY KEY,cell TEXT NOT NULL,version TEXT NOT NULL,revision INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS terminal_firmware_case(id TEXT PRIMARY KEY,payload TEXT NOT NULL,digest TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS terminal_firmware_operation(id TEXT PRIMARY KEY,device TEXT NOT NULL,command TEXT NOT NULL,result TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS firmware_rollout(id TEXT PRIMARY KEY,case_id TEXT NOT NULL,contract TEXT NOT NULL,target_version TEXT NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,detail TEXT NOT NULL,half_upgraded_cells TEXT NOT NULL DEFAULT '[]');
CREATE TABLE IF NOT EXISTS firmware_intent(operation_id TEXT PRIMARY KEY,rollout_id TEXT NOT NULL REFERENCES firmware_rollout(id),device TEXT NOT NULL,command TEXT NOT NULL,version TEXT NOT NULL,expected_revision INTEGER NOT NULL,phase TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,result TEXT,UNIQUE(rollout_id,device,command));
CREATE TABLE IF NOT EXISTS firmware_rollout_device(rollout_id TEXT NOT NULL REFERENCES firmware_rollout(id),device TEXT NOT NULL,station TEXT NOT NULL,cell TEXT NOT NULL,pulse_source_sha256 TEXT NOT NULL,baseline_version TEXT NOT NULL,target_version TEXT NOT NULL,actual_version TEXT,actual_revision INTEGER,install_status TEXT,install_operation_id TEXT,install_response TEXT,reconciliation TEXT,rollback_status TEXT,rollback_operation_id TEXT,rollback_response TEXT,manual_reason TEXT,updated_at TEXT NOT NULL,PRIMARY KEY(rollout_id,device));
"""
def connect(path):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(p)
    db.row_factory=sqlite3.Row
    db.executescript(SCHEMA)
    return db
