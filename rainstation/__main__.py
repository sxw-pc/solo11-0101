import argparse,json,tempfile
from pathlib import Path
from .ingest import freeze_config,import_file
from .migration import cancel_transfer, list_reservations, plan_transfer
from .rules import evaluate
from .report import export
from .store import connect

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
    t=sub.add_parser('transfer',help='离线转配：求解未来站址/记录仪预约')
    t.add_argument('--db',required=True);t.add_argument('--transfer',required=True)
    t.add_argument('--cert-dir');t.add_argument('--contract',
        choices=['oldest_complete_basin','max_completed_basins'])
    c=sub.add_parser('transfer-cancel',help='取消整组迁站预约并释放设备/站址')
    c.add_argument('--db',required=True);c.add_argument('--group',required=True)
    lr=sub.add_parser('transfer-list',help='列出生效中的未来站址预约')
    lr.add_argument('--db',required=True)
    args=p.parse_args()
    if args.cmd=='demo': print(demo(args.workdir))
    elif args.cmd=='ingest': freeze_config(args.db,args.config);print(import_file(args.db,args.events))
    elif args.cmd=='inspect': print(json.dumps(evaluate(args.db),ensure_ascii=False,indent=2))
    elif args.cmd=='report': print(export(args.db,args.out))
    elif args.cmd=='transfer':
        print(json.dumps(plan_transfer(args.db,args.transfer,args.cert_dir,args.contract),
                         ensure_ascii=False,indent=2))
    elif args.cmd=='transfer-cancel':
        print(json.dumps(cancel_transfer(args.db,args.group),ensure_ascii=False,indent=2))
    elif args.cmd=='transfer-list':
        print(json.dumps(list_reservations(args.db),ensure_ascii=False,indent=2))
if __name__=='__main__':main()
