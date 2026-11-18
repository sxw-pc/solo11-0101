import json
from pathlib import Path
from .store import connect

# 内部完整证据报告仅对持有内部审计授权的角色开放。外部设备供应商不持有
# 该角色，无法借内部通道取得 event.payload / source.path 等单站证据；
# 外部汇总只能走 rainstation.disclosure 的独立发布通道。
INTERNAL_AUDIT_ROLE = 'internal-auditor'


def export(db_path, out, role=INTERNAL_AUDIT_ROLE):
    """导出内部审计完整证据报告，需要内部审计授权。"""
    if role != INTERNAL_AUDIT_ROLE:
        raise PermissionError('internal_audit_report_requires_auditor_role')
    db = connect(db_path)
    data = {table: [dict(r) for r in db.execute(f'SELECT * FROM {table} ORDER BY 1')]
            for table in ('batch', 'subject', 'event', 'issue', 'decision',
                          'evidence', 'run', 'disclosure_ledger')}
    data['source'] = [dict(r) for r in db.execute(
        'SELECT id,path,sha256 FROM source ORDER BY id')]
    data['config_hash'] = db.execute(
        "SELECT value FROM meta WHERE key='config_hash'").fetchone()['value']
    target = Path(out)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    md = target.with_suffix('.md')
    md.write_text('# 决策报告\n\n' + '\n'.join(
        f"- {d['subject']}: {d['status']} — {d['details']}"
        for d in data['decision']) + '\n', encoding='utf-8')
    db.close()
    return target
