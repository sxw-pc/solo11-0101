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
CREATE TABLE IF NOT EXISTS cleanout_container(id TEXT PRIMARY KEY,maintenance_identity TEXT NOT NULL,gross_kg TEXT NOT NULL,contaminant_kg TEXT NOT NULL,seal TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS cleanout_case(id TEXT PRIMARY KEY,payload TEXT NOT NULL,digest TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS cleanout_event(id INTEGER PRIMARY KEY,case_id TEXT NOT NULL REFERENCES cleanout_case(id),kind TEXT NOT NULL,at TEXT NOT NULL,actor TEXT NOT NULL,payload TEXT NOT NULL,dedupe_key TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS cleanout_batch(id TEXT PRIMARY KEY,case_id TEXT NOT NULL REFERENCES cleanout_case(id),parent_id TEXT REFERENCES cleanout_batch(id),container_id TEXT NOT NULL,kind TEXT NOT NULL,status TEXT NOT NULL,custody TEXT NOT NULL,gross_kg TEXT NOT NULL,contaminant_kg TEXT NOT NULL,seal TEXT NOT NULL,maintenance_identity TEXT NOT NULL,weigh_sha256 TEXT NOT NULL,recheck_id TEXT NOT NULL DEFAULT '',created_by INTEGER NOT NULL REFERENCES cleanout_event(id),created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS cleanout_contract(case_id TEXT PRIMARY KEY REFERENCES cleanout_case(id),contract_id TEXT NOT NULL,authorized_by TEXT NOT NULL,selected_at TEXT NOT NULL,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS cleanout_manifest(id TEXT PRIMARY KEY,case_id TEXT NOT NULL REFERENCES cleanout_case(id),carrier TEXT NOT NULL,carrier_license TEXT NOT NULL,license_valid_until TEXT NOT NULL,shipped_at TEXT NOT NULL,containers TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS cleanout_receipt(id TEXT PRIMARY KEY,manifest_id TEXT NOT NULL REFERENCES cleanout_manifest(id),signed_by TEXT NOT NULL,at TEXT NOT NULL,accepted_kg TEXT NOT NULL,containers TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS cleanout_review(id INTEGER PRIMARY KEY,case_id TEXT NOT NULL REFERENCES cleanout_case(id),reason TEXT NOT NULL,detail TEXT NOT NULL,at TEXT NOT NULL,dedupe_key TEXT NOT NULL UNIQUE);
"""
def connect(path):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(p)
    db.row_factory=sqlite3.Row
    db.executescript(SCHEMA)
    return db
