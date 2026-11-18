import argparse, json, tempfile
from pathlib import Path
from .ingest import freeze_config, import_file
from .rules import evaluate
from .report import export, INTERNAL_AUDIT_ROLE
from .disclosure import (
    seed_prior_release, publish_request, reconcile, ledger_view, DisclosureError,
    POLICIES,
)
from .store import connect


def demo(workdir=None):
    root = Path(workdir or tempfile.mkdtemp(prefix='rainstation-'))
    root.mkdir(parents=True, exist_ok=True)
    db = root / 'ledger.db'
    base = Path(__file__).parent.parent / 'fixtures'
    freeze_config(db, base / 'config.json')
    import_file(db, base / 'events.jsonl')
    evaluate(db)
    return export(db, root / 'report.json')


def _publish_plan(db, request_file, out_dir, policy):
    """按请求文件执行外部汇总发布，返回可 JSON 序列化的结果。"""
    spec = json.loads(Path(request_file).read_text(encoding='utf-8'))
    audience = spec['audience']
    prior = spec.get('previous_release')
    if prior is not None:
        seed_prior_release(db, audience, prior)
    request = spec['new_request']
    row = publish_request(
        db, audience, request, out_dir, policy,
        allowed_expansion=spec.get('allowed_expansion', ()),
        metric_universe=spec.get('station_universe'))
    return audience, row


def main():
    p = argparse.ArgumentParser(description='雨量站采录台 — 离线证据与业务决策')
    sub = p.add_subparsers(dest='cmd', required=True)
    d = sub.add_parser('demo')
    d.add_argument('--workdir')
    a = sub.add_parser('ingest')
    a.add_argument('--db', required=True)
    a.add_argument('--config', required=True)
    a.add_argument('events')
    e = sub.add_parser('inspect')
    e.add_argument('--db', required=True)
    r = sub.add_parser('report')
    r.add_argument('--db', required=True)
    r.add_argument('--out', required=True)
    r.add_argument('--role', default=INTERNAL_AUDIT_ROLE,
                   help='完整证据报告仅 internal-auditor 可取')
    pub = sub.add_parser('publish', help='面向外部受众的安全汇总发布（与内部报告隔离）')
    pub.add_argument('--db', required=True)
    pub.add_argument('--request', required=True, help='受众请求 JSON（含历史发布与扩展池）')
    pub.add_argument('--out-dir', required=True, help='外部交付文件目录')
    pub.add_argument('--policy', required=True, choices=POLICIES)
    led = sub.add_parser('ledger', help='查看某受众的发布台账')
    led.add_argument('--db', required=True)
    led.add_argument('--audience', required=True)
    rec = sub.add_parser('reconcile', help='中断后恢复发布台账与交付文件一致性')
    rec.add_argument('--db', required=True)
    rec.add_argument('--audience', required=True)
    rec.add_argument('--out-dir', required=True)
    args = p.parse_args()
    if args.cmd == 'demo':
        print(demo(args.workdir))
    elif args.cmd == 'ingest':
        freeze_config(args.db, args.config)
        print(import_file(args.db, args.events))
    elif args.cmd == 'inspect':
        print(json.dumps(evaluate(args.db), ensure_ascii=False, indent=2))
    elif args.cmd == 'report':
        try:
            print(export(args.db, args.out, role=args.role))
        except PermissionError as exc:
            raise SystemExit(f'permission_denied: {exc}')
    elif args.cmd == 'publish':
        try:
            audience, row = _publish_plan(args.db, args.request, args.out_dir, args.policy)
        except DisclosureError as exc:
            raise SystemExit(f'publish_failed: {exc}')
        print(json.dumps({'audience': audience, 'result': row},
                         ensure_ascii=False, indent=2))
    elif args.cmd == 'ledger':
        print(json.dumps(ledger_view(args.db, args.audience), ensure_ascii=False, indent=2))
    elif args.cmd == 'reconcile':
        try:
            reconcile(args.db, args.audience, args.out_dir)
        except DisclosureError as exc:
            raise SystemExit(f'reconcile_failed: {exc}')
        print(json.dumps(ledger_view(args.db, args.audience), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
