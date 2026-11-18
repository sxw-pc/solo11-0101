import argparse,json,sys,tempfile
from pathlib import Path
from .ingest import freeze_config,import_file
from .rules import evaluate
from .report import export
from .store import connect
from . import reservoir_plan

def demo(workdir=None):
    root=Path(workdir or tempfile.mkdtemp(prefix='rainstation-'));root.mkdir(parents=True,exist_ok=True)
    db=root/'ledger.db'; base=Path(__file__).parent.parent/'fixtures'
    freeze_config(db,base/'config.json');import_file(db,base/'events.jsonl');evaluate(db)
    return export(db,root/'report.json')
def _emit(obj,out=None):
    text=json.dumps(obj,ensure_ascii=False,indent=2)
    if out:
        target=Path(out);target.parent.mkdir(parents=True,exist_ok=True)
        target.write_text(text,encoding='utf-8')
    print(text)
def _plan_call(func,*args):
    try: return func(*args)
    except reservoir_plan.PlanError as exc:
        print(json.dumps({'status':'error','reason':str(exc)},ensure_ascii=False),file=sys.stderr)
        raise SystemExit(2)
def main(argv=None):
    p=argparse.ArgumentParser(description='雨量站采录台 — 离线证据与业务决策')
    sub=p.add_subparsers(dest='cmd',required=True)
    d=sub.add_parser('demo');d.add_argument('--workdir')
    a=sub.add_parser('ingest');a.add_argument('--db',required=True);a.add_argument('--config',required=True);a.add_argument('events')
    e=sub.add_parser('inspect');e.add_argument('--db',required=True)
    r=sub.add_parser('report');r.add_argument('--db',required=True);r.add_argument('--out',required=True)
    pl=sub.add_parser('plan');pl.add_argument('--db',required=True);pl.add_argument('--case',required=True);pl.add_argument('--objective',required=True);pl.add_argument('--out')
    fb=sub.add_parser('feedback');fb.add_argument('--db',required=True);fb.add_argument('--file',required=True);fb.add_argument('--out')
    args=p.parse_args(argv)
    if args.cmd=='demo': print(demo(args.workdir))
    elif args.cmd=='ingest': freeze_config(args.db,args.config);print(import_file(args.db,args.events))
    elif args.cmd=='inspect': print(json.dumps(evaluate(args.db),ensure_ascii=False,indent=2))
    elif args.cmd=='report': print(export(args.db,args.out))
    elif args.cmd=='plan': _emit(_plan_call(reservoir_plan.generate_plan,args.db,args.case,args.objective),args.out)
    elif args.cmd=='feedback': _emit(_plan_call(reservoir_plan.apply_feedback,args.db,Path(args.file).read_bytes()),args.out)
if __name__=='__main__':main()
