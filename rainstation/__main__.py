import argparse,json,tempfile
from pathlib import Path
from .ingest import freeze_config,import_file
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
    c=sub.add_parser('cleanout',description='清桶废液：登记、处置、签收、纠错与物料/联单报告')
    csub=c.add_subparsers(dest='cleanout_cmd',required=True)
    rg=csub.add_parser('register');rg.add_argument('--db',required=True);rg.add_argument('--case-id',required=True);rg.add_argument('--fixture',required=True)
    dp=csub.add_parser('dispose');dp.add_argument('--db',required=True);dp.add_argument('--case-id',required=True)
    dp.add_argument('--contract',required=True,choices=['closed_loop_sep','sealed_ship']);dp.add_argument('--at',required=True)
    dp.add_argument('--actor',default='ENV-TECH-1');dp.add_argument('--recheck')
    rc=csub.add_parser('receive');rc.add_argument('--db',required=True);rc.add_argument('--case-id',required=True);rc.add_argument('--receipt')
    rw=csub.add_parser('reweigh');rw.add_argument('--db',required=True);rw.add_argument('--case-id',required=True);rw.add_argument('--correction',required=True)
    rt=csub.add_parser('return');rt.add_argument('--db',required=True);rt.add_argument('--case-id',required=True)
    rt.add_argument('--container',required=True);rt.add_argument('--at',required=True);rt.add_argument('--actor',default='CAR-1')
    rp=csub.add_parser('report');rp.add_argument('--db',required=True);rp.add_argument('--case-id',required=True);rp.add_argument('--out',required=True)
    args=p.parse_args()
    if args.cmd=='demo': print(demo(args.workdir))
    elif args.cmd=='ingest': freeze_config(args.db,args.config);print(import_file(args.db,args.events))
    elif args.cmd=='inspect': print(json.dumps(evaluate(args.db),ensure_ascii=False,indent=2))
    elif args.cmd=='report': print(export(args.db,args.out))
    elif args.cmd=='cleanout':
        from .cleanout_waste import register,dispose,receive,reweigh,carrier_return,report
        if args.cleanout_cmd=='register':
            print(json.dumps(register(args.db,args.case_id,args.fixture),ensure_ascii=False))
        elif args.cleanout_cmd=='dispose':
            recheck=json.loads(Path(args.recheck).read_text(encoding='utf-8')) if args.recheck else None
            print(json.dumps(dispose(args.db,args.case_id,args.contract,args.at,args.actor,recheck),ensure_ascii=False))
        elif args.cleanout_cmd=='receive':
            receipt=json.loads(Path(args.receipt).read_text(encoding='utf-8')) if args.receipt else None
            print(json.dumps(receive(args.db,args.case_id,receipt),ensure_ascii=False))
        elif args.cleanout_cmd=='reweigh':
            correction=json.loads(Path(args.correction).read_text(encoding='utf-8'))
            print(json.dumps(reweigh(args.db,args.case_id,correction),ensure_ascii=False))
        elif args.cleanout_cmd=='return':
            print(json.dumps(carrier_return(args.db,args.case_id,args.container,args.at,args.actor),ensure_ascii=False))
        elif args.cleanout_cmd=='report':
            report(args.db,args.case_id,args.out);print(args.out)
if __name__=='__main__':main()
