import argparse,json,tempfile
from pathlib import Path
from .ingest import freeze_config,import_file
from .rules import evaluate
from .report import export
from . import terminal_firmware, rollout
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
    reg=sub.add_parser('firmware-register',help='登记固件升级 case（核验脉冲来源摘要）')
    reg.add_argument('--db',required=True);reg.add_argument('--case-id',required=True);reg.add_argument('--case-file',required=True)
    st=sub.add_parser('firmware-state',help='查询现场实版与 revision')
    st.add_argument('--db',required=True);st.add_argument('--device',required=True)
    pre=sub.add_parser('firmware-preflight',help='输出 case 设备集合、单元、来源摘要与现场实版')
    pre.add_argument('--db',required=True);pre.add_argument('--case-id',required=True)
    ro=sub.add_parser('firmware-rollout',help='执行/恢复跨终端灰度升级（幂等，可重跑）')
    ro.add_argument('--db',required=True);ro.add_argument('--rollout-id',required=True)
    ro.add_argument('--case-id',required=True)
    ro.add_argument('--contract',required=True,choices=rollout.CONTRACTS)
    rr=sub.add_parser('firmware-report',help='导出 JSON/Markdown 现场证据运维报告')
    rr.add_argument('--db',required=True);rr.add_argument('--rollout-id',required=True);rr.add_argument('--out',required=True)
    args=p.parse_args()
    if args.cmd=='demo': print(demo(args.workdir))
    elif args.cmd=='ingest': freeze_config(args.db,args.config);print(import_file(args.db,args.events))
    elif args.cmd=='inspect': print(json.dumps(evaluate(args.db),ensure_ascii=False,indent=2))
    elif args.cmd=='report': print(export(args.db,args.out))
    elif args.cmd=='firmware-register': print(json.dumps(terminal_firmware.register(args.db,args.case_id,args.case_file),ensure_ascii=False))
    elif args.cmd=='firmware-state': print(json.dumps(terminal_firmware.state(args.db,args.device),ensure_ascii=False))
    elif args.cmd=='firmware-preflight': print(json.dumps(rollout.preflight(args.db,args.case_id),ensure_ascii=False,indent=2))
    elif args.cmd=='firmware-rollout': print(json.dumps(rollout.run_rollout(args.db,args.rollout_id,args.case_id,args.contract),ensure_ascii=False,indent=2))
    elif args.cmd=='firmware-report': print(rollout.export_rollout_report(args.db,args.rollout_id,args.out))
if __name__=='__main__':main()
