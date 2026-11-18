import argparse,json,tempfile
from pathlib import Path
from .ingest import CONTRACTS, freeze_config, import_file
from .rules import evaluate
from .report import export
from .store import DEFAULT_CHUNK_SIZE, connect

def demo(workdir=None):
    root=Path(workdir or tempfile.mkdtemp(prefix='rainstation-'));root.mkdir(parents=True,exist_ok=True)
    db=root/'ledger.db'; base=Path(__file__).parent.parent/'fixtures'
    freeze_config(db,base/'config.json');import_file(db,base/'events.jsonl');evaluate(db)
    return export(db,root/'report.json')
def main():
    p=argparse.ArgumentParser(description='雨量站采录台 — 离线证据与业务决策')
    sub=p.add_subparsers(dest='cmd',required=True)
    d=sub.add_parser('demo');d.add_argument('--workdir')
    a=sub.add_parser('ingest');a.add_argument('--db',required=True);a.add_argument('--config',required=True)
    a.add_argument('--contract',choices=CONTRACTS,default='freeze_on_open',
                   help='来源快照合同：freeze_on_open（默认）或 verify_then_retry_latest')
    a.add_argument('--snapshot-id',help='显式快照身份；省略时按逻辑路径稳定派生，支持中断重入')
    a.add_argument('--chunk-size',type=int,default=DEFAULT_CHUNK_SIZE,
                   help='有界块字节数；峰值内存随该值而非文件总长增长')
    a.add_argument('events')
    e=sub.add_parser('inspect');e.add_argument('--db',required=True)
    r=sub.add_parser('report');r.add_argument('--db',required=True);r.add_argument('--out',required=True)
    args=p.parse_args()
    if args.cmd=='demo': print(demo(args.workdir))
    elif args.cmd=='ingest':
        freeze_config(args.db,args.config)
        print(import_file(args.db,args.events,contract=args.contract,
                          snapshot_id=args.snapshot_id,chunk_size=args.chunk_size))
    elif args.cmd=='inspect': print(json.dumps(evaluate(args.db),ensure_ascii=False,indent=2))
    elif args.cmd=='report': print(export(args.db,args.out))
if __name__=='__main__':main()
