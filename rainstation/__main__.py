import argparse,json,tempfile
from pathlib import Path
from .ingest import freeze_config,import_file
from .rules import evaluate
from .report import export
from .store import connect
from .water_attributes import import_tank_log,import_attribute_contract,issue_attributes,transfer_certificate,recall_queue

def demo(workdir=None):
    root=Path(workdir or tempfile.mkdtemp(prefix='rainstation-'));root.mkdir(parents=True,exist_ok=True)
    db=root/'ledger.db'; base=Path(__file__).parent.parent/'fixtures'
    freeze_config(db,base/'config.json');import_file(db,base/'events.jsonl');evaluate(db)
    return export(db,root/'report.json')
def main():
    p=argparse.ArgumentParser(description='雨量站采录台 — 离线证据与业务决策')
    sub=p.add_subparsers(dest='cmd',required=True)
    d=sub.add_parser('demo');d.add_argument('--workdir')
    a=sub.add_parser('ingest');a.add_argument('--db',required=True);a.add_argument('--config',required=True);a.add_argument('events')
    e=sub.add_parser('inspect');e.add_argument('--db',required=True)
    r=sub.add_parser('report');r.add_argument('--db',required=True);r.add_argument('--out',required=True)
    i=sub.add_parser('issue');i.add_argument('--db',required=True);i.add_argument('--station',required=True);i.add_argument('--hour',required=True);i.add_argument('--title',required=True);i.add_argument('--tank-log');i.add_argument('--contract')
    t=sub.add_parser('transfer');t.add_argument('--db',required=True);t.add_argument('--cert',required=True);t.add_argument('--to',required=True)
    c=sub.add_parser('recalls');c.add_argument('--db',required=True)
    args=p.parse_args()
    if args.cmd=='demo': print(demo(args.workdir))
    elif args.cmd=='ingest': freeze_config(args.db,args.config);print(import_file(args.db,args.events))
    elif args.cmd=='inspect': print(json.dumps(evaluate(args.db),ensure_ascii=False,indent=2))
    elif args.cmd=='report': print(export(args.db,args.out))
    elif args.cmd=='issue':
        if args.tank_log: import_tank_log(args.db,args.tank_log)
        if args.contract: import_attribute_contract(args.db,args.contract)
        print(json.dumps(issue_attributes(args.db,args.station,args.hour,args.title),ensure_ascii=False,indent=2))
    elif args.cmd=='transfer': print(json.dumps(transfer_certificate(args.db,args.cert,args.to),ensure_ascii=False,indent=2))
    elif args.cmd=='recalls': print(json.dumps(recall_queue(args.db),ensure_ascii=False,indent=2))
if __name__=='__main__':main()
