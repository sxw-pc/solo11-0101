"""流式来源快照合同测试。

覆盖：
1. 确定性第 1024 行替换钩子下 freeze_on_open 与 verify_then_retry_latest
   两种合同的真实入库差异（raw/SHA-256/event 证据一致，不能只换报告文字）；
2. 大文件峰值内存随块大小而非文件总长增长；
3. 分块边界上的 BOM/CRLF/空行/末尾无换行语义；
4. 坏行隔离、事件身份冲突、源行号回指；
5. 同快照重入幂等与异内容同身份冲突隔离；
6. 进程中断后的快照绑定恢复，不留半份正式文件或重复事件。
"""
import hashlib
import json
import os
import tracemalloc
from pathlib import Path

import pytest

from rainstation.ingest import (
    SnapshotUnstableError,
    freeze_config,
    import_file,
)
from rainstation.store import connect

FIXTURES = Path(__file__).parents[1] / 'fixtures'
MUTATION = json.loads((FIXTURES / 'maintenance_snapshot_mutation.json').read_text(encoding='utf-8'))
INITIAL_SHA = MUTATION['initial_sha256']
REVISED_SHA = MUTATION['revised_sha256']
HOOK_LINE = MUTATION['mutation_after_line']  # 读至第 1024 行后替换
SNAPSHOT_ID = 'snapshot:maintenance-batch-1'


def _stage_logical_path(tmp_path):
    incoming = tmp_path / 'incoming'
    incoming.mkdir()
    logical = incoming / 'maintenance_batch.jsonl'
    # 起始为增强副本真实文件的逐字节副本。
    logical.write_bytes((FIXTURES / MUTATION['initial_file']).read_bytes())
    return logical


def _replace_at_1024(revised_bytes):
    """确定性钩子：读到第 1024 行结束后，原子替换同一逻辑路径为新版。"""
    state = {'replaced': False, 'calls': 0}

    def hook(line_no, logical_path):
        state['calls'] += 1
        if not state['replaced'] and line_no >= HOOK_LINE:
            tmp = Path(str(logical_path) + '.new')
            tmp.write_bytes(revised_bytes)
            os.replace(tmp, logical_path)
            state['replaced'] = True

    hook.state = state
    return hook


def _work_order(db_path, source_id):
    con = connect(db_path)
    row = con.execute(
        "SELECT payload FROM event WHERE source_id=? AND identity='WORK:1024'",
        (source_id,)).fetchone()
    con.close()
    return json.loads(row['payload'])['work_order_ref']


def _formal_evidence(db_path):
    con = connect(db_path)
    source = con.execute('SELECT id,path,sha256,raw FROM source ORDER BY id').fetchall()
    events = con.execute(
        'SELECT source_id,line_no,identity,payload,sha256 FROM event ORDER BY id').fetchall()
    issues = con.execute('SELECT code,detail FROM issue ORDER BY id').fetchall()
    snapshots = con.execute('SELECT * FROM snapshot ORDER BY id').fetchall()
    con.close()
    return source, events, issues, snapshots


def test_freeze_on_open_pins_the_version_opened_at_start(tmp_path):
    db = tmp_path / 'stations.db'
    freeze_config(db, FIXTURES / 'config.json')
    logical = _stage_logical_path(tmp_path)
    hook = _replace_at_1024((FIXTURES / MUTATION['revised_file']).read_bytes())

    result = import_file(db, logical, contract='freeze_on_open',
                         snapshot_id=SNAPSHOT_ID, mutation_hook=hook)

    assert hook.state['replaced'] is True
    assert result['imported'] == 2048
    assert result['sha256'] == INITIAL_SHA
    initial_bytes = (FIXTURES / MUTATION['initial_file']).read_bytes()
    source, events, issues, snapshots = _formal_evidence(db)
    assert len(source) == 1
    # source.raw 逐字节等于记录 SHA-256 的版本（被替换的是磁盘路径，不是证据）。
    assert source[0]['raw'] == initial_bytes
    assert hashlib.sha256(source[0]['raw']).hexdigest() == source[0]['sha256'] == INITIAL_SHA
    assert _work_order(db, source[0]['id']) == 'WO-1024'
    assert snapshots[0]['status'] == 'committed'
    assert snapshots[0]['sha256'] == INITIAL_SHA
    assert len(events) == 2048 and issues == []
    # event 回指同一原文：行号与逐行 canonical 哈希均可核对。
    for ev in events:
        assert ev['source_id'] == source[0]['id']
        assert hashlib.sha256(ev['payload'].encode('utf-8')).hexdigest() == ev['sha256']
    line_1025 = next(e for e in events if e['line_no'] == 1025)
    assert line_1025['identity'] == 'WORK:1024'
    # 磁盘上现在确实是新版，但正式证据没有被新版冒充。
    assert hashlib.sha256(logical.read_bytes()).hexdigest() == REVISED_SHA


def test_verify_then_retry_latest_discards_staging_and_reimports_new_version(tmp_path):
    db = tmp_path / 'stations.db'
    freeze_config(db, FIXTURES / 'config.json')
    logical = _stage_logical_path(tmp_path)
    hook = _replace_at_1024((FIXTURES / MUTATION['revised_file']).read_bytes())

    result = import_file(db, logical, contract='verify_then_retry_latest',
                         snapshot_id=SNAPSHOT_ID, mutation_hook=hook)

    assert hook.state['replaced'] is True
    assert result['imported'] == 2048
    assert result['sha256'] == REVISED_SHA
    revised_bytes = (FIXTURES / MUTATION['revised_file']).read_bytes()
    source, events, issues, snapshots = _formal_evidence(db)
    assert len(source) == 1  # 废弃的旧版暂存不得晋升为正式来源
    assert source[0]['raw'] == revised_bytes
    assert hashlib.sha256(source[0]['raw']).hexdigest() == source[0]['sha256'] == REVISED_SHA
    assert _work_order(db, source[0]['id']) == 'WO-1024-REVISED'
    assert snapshots[0]['status'] == 'committed'
    assert snapshots[0]['sha256'] == REVISED_SHA
    assert len(events) == 2048 and issues == []
    for ev in events:
        assert hashlib.sha256(ev['payload'].encode('utf-8')).hexdigest() == ev['sha256']


def test_two_contracts_produce_distinct_real_evidence(tmp_path):
    """同一替换场景跑两份独立工作区，证据差异必须落到 raw/hash/event。"""
    outcomes = {}
    for contract in ('freeze_on_open', 'verify_then_retry_latest'):
        root = tmp_path / contract
        root.mkdir()
        db = root / 'stations.db'
        freeze_config(db, FIXTURES / 'config.json')
        logical = _stage_logical_path(root)
        hook = _replace_at_1024((FIXTURES / MUTATION['revised_file']).read_bytes())
        import_file(db, logical, contract=contract,
                    snapshot_id=SNAPSHOT_ID, mutation_hook=hook)
        source, events, _, _ = _formal_evidence(db)
        outcomes[contract] = {
            'sha': source[0]['sha256'],
            'raw_len': len(source[0]['raw']),
            'work_order': _work_order(db, source[0]['id']),
            'event_hashes': sorted(e['sha256'] for e in events),
        }
    assert outcomes['freeze_on_open']['sha'] == INITIAL_SHA
    assert outcomes['verify_then_retry_latest']['sha'] == REVISED_SHA
    assert outcomes['freeze_on_open']['work_order'] == 'WO-1024'
    assert outcomes['verify_then_retry_latest']['work_order'] == 'WO-1024-REVISED'
    assert outcomes['freeze_on_open']['event_hashes'] != \
        outcomes['verify_then_retry_latest']['event_hashes']


def _make_jsonl(path, rows):
    with open(path, 'wb') as fh:
        for row in rows:
            fh.write((json.dumps(row, ensure_ascii=False) + '\n').encode('utf-8'))


def _maintenance_row(i, ref=None):
    return {"kind": "maintenance", "subject": "S1", "identity": f"WORK:{i}",
            "time": "2026-01-01T08:00:00Z", "start": "2026-01-01T08:00:00Z",
            "end": "2026-01-01T08:01:00Z", "work_order_ref": ref or f"WO-{i}"}


def test_peak_memory_tracks_chunk_size_not_file_length(tmp_path):
    cfg = tmp_path / 'config.json'
    cfg.write_bytes((FIXTURES / 'config.json').read_bytes())
    small = tmp_path / 'small.jsonl'
    big = tmp_path / 'big.jsonl'
    _make_jsonl(small, [_maintenance_row(i) for i in range(2000)])
    _make_jsonl(big, [_maintenance_row(i) for i in range(24000)])
    assert os.path.getsize(big) > os.path.getsize(small) * 8

    def peak_for(src, chunk):
        db = tmp_path / f'db_{chunk}_{src.name}.db'
        freeze_config(db, cfg)
        tracemalloc.start()
        import_file(db, src, chunk_size=chunk)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        return peak

    small_4k = peak_for(small, 4096)
    big_4k = peak_for(big, 4096)
    big_64k = peak_for(big, 65536)

    # 文件扩大 8 倍以上，同块大小峰值增量必须有界（远小于文件本身）。
    assert big_4k - small_4k < os.path.getsize(small)
    assert big_4k < os.path.getsize(big) // 4
    # 同文件块加大 16 倍，峰值随块大小上升而非随文件长度变化。
    assert big_64k > big_4k
    assert big_64k - big_4k < 65536 * 4


@pytest.mark.parametrize('chunk_size', [1, 2, 3, 7, 16, 64, 4096])
def test_chunk_boundaries_bom_crlf_blank_and_no_trailing_newline(tmp_path, chunk_size):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = tmp_path / 'mixed.jsonl'
    rows = [
        '{"kind":"pulse","subject":"S1","identity":"D1:1","device":"D1","seq":1,'
        '"time":"2026-01-01T08:05:00Z","count":1}',
        '',  # 空行
        '{"kind":"heartbeat","subject":"S1","identity":"D1:2","device":"D1",'
        '"seq":2,"time":"2026-01-01T08:10:00Z"}',
        '   ',  # 纯空白行
        '{"kind":"anchor","subject":"S1","identity":"D1:3","device":"D1","seq":3,'
        '"time":"2026-01-01T08:15:00Z","offset_seconds":0}',
    ]
    # BOM + CRLF 混用 + 末尾无换行 + 含多字节字符。
    content = ('﻿' + '\r\n'.join(rows[:3]) + '\n' + rows[3] + '\r\n' + rows[4]).encode('utf-8')
    src.write_bytes(content)

    result = import_file(db, src, chunk_size=chunk_size)
    assert result['imported'] == 3
    con = connect(db)
    assert con.execute('SELECT raw FROM source').fetchone()[0] == content
    identities = [r['identity'] for r in con.execute(
        'SELECT identity FROM event ORDER BY line_no')]
    assert identities == ['D1:1', 'D1:2', 'D1:3']
    line_nos = [r['line_no'] for r in con.execute('SELECT line_no FROM event ORDER BY id')]
    # 空行仍占用物理行号，事件行号回指原文物理行。
    assert line_nos == [1, 3, 5]
    assert con.execute('SELECT COUNT(*) FROM issue').fetchone()[0] == 0
    con.close()


def test_lone_cr_is_a_line_boundary_like_splitlines(tmp_path):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = tmp_path / 'cr.jsonl'
    a = json.dumps(_maintenance_row(0))
    b = json.dumps(_maintenance_row(1))
    raw = (a + '\r' + b).encode('utf-8')  # 无 \n 的孤立 \r，且无尾换行
    src.write_bytes(raw)
    import_file(db, src, chunk_size=3)
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM event').fetchone()[0] == 2
    assert con.execute('SELECT raw FROM source').fetchone()[0] == raw
    con.close()


def test_bad_lines_are_isolated_and_line_numbers_preserved(tmp_path):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = tmp_path / 'broken.jsonl'
    good = _maintenance_row(0)
    raw = ('\n'.join([json.dumps(good), '{broken-json', '',
                      json.dumps(_maintenance_row(1))]) + '\n').encode('utf-8')
    src.write_bytes(raw)
    result = import_file(db, src, chunk_size=5)
    assert result['imported'] == 2
    con = connect(db)
    detail = json.loads(con.execute(
        "SELECT detail FROM issue WHERE code='parse_error'").fetchone()[0])
    assert detail['line_no'] == 2 and detail['raw_line'] == '{broken-json'
    assert detail['source_id'] == con.execute('SELECT id FROM source').fetchone()[0]
    assert con.execute('SELECT raw FROM source').fetchone()[0] == raw
    assert [r[0] for r in con.execute('SELECT line_no FROM event ORDER BY line_no')] == [1, 4]
    con.close()


def test_same_snapshot_reentry_is_idempotent_by_source_hash(tmp_path):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = tmp_path / 'f.jsonl'
    _make_jsonl(src, [_maintenance_row(i) for i in range(10)])

    first = import_file(db, src, snapshot_id=SNAPSHOT_ID, chunk_size=7)
    again = import_file(db, src, snapshot_id=SNAPSHOT_ID, chunk_size=4096)
    assert first['sha256'] == again['sha256']
    assert again['duplicate_file'] is True
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM source').fetchone()[0] == 1
    assert con.execute('SELECT COUNT(*) FROM event').fetchone()[0] == 10
    assert con.execute("SELECT COUNT(*) FROM snapshot WHERE status='committed'").fetchone()[0] == 1
    assert con.execute('SELECT COUNT(*) FROM staging_source').fetchone()[0] == 0
    con.close()


def test_different_content_same_identity_enters_conflict_quarantine(tmp_path):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    first_src = tmp_path / 'v1.jsonl'
    _make_jsonl(first_src, [_maintenance_row(0, 'WO-0')])
    import_file(db, first_src, snapshot_id='snap:v1')

    conflict_src = tmp_path / 'v2.jsonl'
    _make_jsonl(conflict_src, [_maintenance_row(0, 'WO-0-CHANGED')])
    import_file(db, conflict_src, snapshot_id='snap:v2')

    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM source').fetchone()[0] == 2
    # 同身份异内容：正式事件仍只有第一份，冲突进 issue，唯一约束不被破坏。
    assert con.execute('SELECT COUNT(*) FROM event').fetchone()[0] == 1
    issue = con.execute("SELECT code,detail FROM issue WHERE code='identity_conflict'").fetchone()
    assert issue['detail'] == 'WORK:0'
    con.close()


def test_interrupted_process_resumes_from_staging_without_rereading(tmp_path):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = tmp_path / 'f.jsonl'
    _make_jsonl(src, [_maintenance_row(i) for i in range(20)])
    raw_before = src.read_bytes()

    class Crash(Exception):
        pass

    def crash(_staged):
        raise Crash('simulated process kill before promotion')

    with pytest.raises(Crash):
        import_file(db, src, snapshot_id=SNAPSHOT_ID, before_promote=crash)

    # 中断后不存在半份正式文件。
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM source').fetchone()[0] == 0
    assert con.execute('SELECT COUNT(*) FROM event').fetchone()[0] == 0
    snap = con.execute('SELECT * FROM snapshot').fetchone()
    assert snap['status'] == 'staged'
    staged_raw = con.execute('SELECT raw,sha256 FROM staging_source').fetchone()
    assert staged_raw['raw'] == raw_before
    assert staged_raw['sha256'] == hashlib.sha256(raw_before).hexdigest()
    con.close()

    # 重入前把来源路径替换成异内容；恢复必须绑定原快照暂存，不重读新路径。
    _make_jsonl(src, [_maintenance_row(i, f'OTHER-{i}') for i in range(20)])
    resume = import_file(db, src, snapshot_id=SNAPSHOT_ID)
    assert resume['duplicate_file'] is False
    con = connect(db)
    source = con.execute('SELECT raw,sha256 FROM source').fetchone()
    assert source['raw'] == raw_before
    assert source['sha256'] == hashlib.sha256(raw_before).hexdigest()
    assert con.execute('SELECT COUNT(*) FROM event').fetchone()[0] == 20
    assert con.execute('SELECT COUNT(*) FROM staging_source').fetchone()[0] == 0
    assert con.execute("SELECT status FROM snapshot").fetchone()[0] == 'committed'
    con.close()

    # 再重入一次仍幂等，不产生重复事件。
    again = import_file(db, src, snapshot_id=SNAPSHOT_ID)
    assert again['duplicate_file'] is True
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM event').fetchone()[0] == 20
    con.close()


def test_interrupted_mid_stream_restarts_and_commits_one_consistent_version(tmp_path):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = tmp_path / 'f.jsonl'
    _make_jsonl(src, [_maintenance_row(i) for i in range(30)])

    calls = {'n': 0}

    def crash_after_five_lines(line_no, _path):
        calls['n'] += 1
        if line_no == 5:
            raise KeyboardInterrupt('simulated kill mid stream')

    with pytest.raises(KeyboardInterrupt):
        import_file(db, src, snapshot_id=SNAPSHOT_ID, chunk_size=4, mutation_hook=crash_after_five_lines)

    # 残留暂存对正式表不可见，且重入时被整组清理重做。
    result = import_file(db, src, snapshot_id=SNAPSHOT_ID, chunk_size=4096)
    assert result['imported'] == 30
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM source').fetchone()[0] == 1
    assert con.execute('SELECT COUNT(*) FROM event').fetchone()[0] == 30
    assert con.execute('SELECT COUNT(*) FROM issue').fetchone()[0] == 0
    raw = src.read_bytes()
    row = con.execute('SELECT raw,sha256 FROM source').fetchone()
    assert row['raw'] == raw and row['sha256'] == hashlib.sha256(raw).hexdigest()
    con.close()


def test_verify_contract_fails_when_path_keeps_changing(tmp_path):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = tmp_path / 'f.jsonl'
    _make_jsonl(src, [_maintenance_row(i) for i in range(40)])
    revised = (FIXTURES / MUTATION['revised_file']).read_bytes()

    def always_replace(line_no, logical_path):
        if line_no in (5, 15, 25):
            tmp = Path(str(logical_path) + f'.{line_no}')
            tmp.write_bytes(revised)
            os.replace(tmp, logical_path)

    with pytest.raises(SnapshotUnstableError):
        import_file(db, src, contract='verify_then_retry_latest',
                    snapshot_id=SNAPSHOT_ID, mutation_hook=always_replace)
    con = connect(db)
    # 反复不稳定时不得留下任何正式来源。
    assert con.execute('SELECT COUNT(*) FROM source').fetchone()[0] == 0
    assert con.execute('SELECT COUNT(*) FROM event').fetchone()[0] == 0
    con.close()


def test_unknown_contract_is_rejected(tmp_path):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = tmp_path / 'f.jsonl'
    _make_jsonl(src, [_maintenance_row(0)])
    with pytest.raises(ValueError, match='unknown_contract'):
        import_file(db, src, contract='nope')


def test_non_positive_chunk_size_is_rejected(tmp_path):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = tmp_path / 'f.jsonl'
    _make_jsonl(src, [_maintenance_row(0)])
    for bad in (0, -1):
        with pytest.raises(ValueError, match='chunk_size_must_be_positive'):
            import_file(db, src, chunk_size=bad)
    # 拒绝发生在任何写入之前。
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM source').fetchone()[0] == 0
    con.close()


def test_empty_file_is_recorded_with_empty_sha256(tmp_path):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = tmp_path / 'empty.jsonl'
    src.write_bytes(b'')
    result = import_file(db, src)
    assert result['imported'] == 0
    assert result['sha256'] == hashlib.sha256(b'').hexdigest()
    con = connect(db)
    row = con.execute('SELECT raw,sha256 FROM source').fetchone()
    assert bytes(row['raw']) == b''
    assert con.execute('SELECT COUNT(*) FROM event').fetchone()[0] == 0
    con.close()


def test_many_bad_lines_promote_with_bounded_pages(tmp_path):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = tmp_path / 'bad.jsonl'
    lines = ['{bad'] * 2500 + [json.dumps(_maintenance_row(0))]
    src.write_bytes(('\n'.join(lines) + '\n').encode('utf-8'))
    result = import_file(db, src, chunk_size=4096)
    assert result['imported'] == 1
    con = connect(db)
    assert con.execute(
        "SELECT COUNT(*) FROM issue WHERE code='parse_error'").fetchone()[0] == 2500
    details = [json.loads(r[0]) for r in con.execute(
        "SELECT detail FROM issue WHERE code='parse_error' ORDER BY id LIMIT 3")]
    assert [d['line_no'] for d in details] == [1, 2, 3]
    assert all(d['source_id'] == result['source_id'] for d in details)
    con.close()


def test_default_snapshot_identity_is_stable_across_process_calls(tmp_path):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = (tmp_path / 'f.jsonl').resolve()
    _make_jsonl(src, [_maintenance_row(i) for i in range(5)])
    import_file(db, src)  # 不传 snapshot_id，按逻辑路径稳定派生
    again = import_file(db, src)
    assert again['duplicate_file'] is True
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM event').fetchone()[0] == 5
    con.close()


def test_no_trailing_newline_and_multibyte_payload_round_trip(tmp_path):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = tmp_path / 'mb.jsonl'
    rows = [
        {"kind": "maintenance", "subject": "S1", "identity": "WORK:0",
         "time": "2026-01-01T08:00:00Z", "start": "2026-01-01T08:00:00Z",
         "end": "2026-01-01T08:01:00Z", "work_order_ref": "工单-甲"},
        {"kind": "maintenance", "subject": "S1", "identity": "WORK:1",
         "time": "2026-01-01T08:00:00Z", "start": "2026-01-01T08:00:00Z",
         "end": "2026-01-01T08:01:00Z", "work_order_ref": "工单-乙"},
    ]
    raw = ('﻿' + '\n'.join(json.dumps(r, ensure_ascii=False) for r in rows)).encode('utf-8')
    src.write_bytes(raw)  # 有 BOM、多字节字符、末尾无换行
    result = import_file(db, src, chunk_size=5)  # 多字节字符必被块边界切开
    assert result['imported'] == 2
    con = connect(db)
    assert con.execute('SELECT raw FROM source').fetchone()[0] == raw
    payloads = [json.loads(r['payload'])['work_order_ref']
                for r in con.execute('SELECT payload FROM event ORDER BY line_no')]
    assert payloads == ['工单-甲', '工单-乙']
    assert [r[0] for r in con.execute('SELECT line_no FROM event ORDER BY line_no')] == [1, 2]
    con.close()


def test_duplicate_identity_inside_one_snapshot_is_quarantined(tmp_path):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = tmp_path / 'dup.jsonl'
    lines = [
        json.dumps(_maintenance_row(0, 'WO-0')),
        json.dumps(_maintenance_row(1)),
        # 同 identity 但不同内容；同内容重复则按历史语义静默跳过。
        json.dumps(_maintenance_row(0, 'WO-0-CHANGED')),
        json.dumps(_maintenance_row(1)),
    ]
    src.write_bytes(('\r\n'.join(lines) + '\r\n').encode('utf-8'))
    result = import_file(db, src, chunk_size=7)
    assert result['imported'] == 2
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM event').fetchone()[0] == 2
    issue = con.execute("SELECT detail FROM issue WHERE code='identity_conflict'").fetchone()
    assert issue[0] == 'WORK:0'
    assert con.execute("SELECT COUNT(*) FROM issue WHERE code='identity_conflict'").fetchone()[0] == 1
    con.close()


def test_same_snapshot_id_rejects_contract_switch(tmp_path):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = tmp_path / 'f.jsonl'
    _make_jsonl(src, [_maintenance_row(0)])
    import_file(db, src, contract='freeze_on_open', snapshot_id=SNAPSHOT_ID)
    with pytest.raises(ValueError, match='snapshot_contract_conflict'):
        import_file(db, src, contract='verify_then_retry_latest', snapshot_id=SNAPSHOT_ID)


def test_verify_contract_succeeds_when_path_is_stable(tmp_path):
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = tmp_path / 'f.jsonl'
    _make_jsonl(src, [_maintenance_row(i) for i in range(12)])
    digest = hashlib.sha256(src.read_bytes()).hexdigest()
    result = import_file(db, src, contract='verify_then_retry_latest',
                         snapshot_id=SNAPSHOT_ID, chunk_size=11)
    assert result['imported'] == 12 and result['sha256'] == digest
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM source').fetchone()[0] == 1
    assert con.execute('SELECT COUNT(*) FROM event').fetchone()[0] == 12
    con.close()


def test_event_payload_can_point_back_to_the_same_source_bytes(tmp_path):
    """event.payload/source_id/line_no 回指同一原文：按行号从 raw 重建并核对哈希。"""
    db = tmp_path / 'db.sqlite'
    freeze_config(db, FIXTURES / 'config.json')
    src = tmp_path / 'f.jsonl'
    _make_jsonl(src, [_maintenance_row(i) for i in range(8)])
    import_file(db, src, chunk_size=3, snapshot_id=SNAPSHOT_ID)
    con = connect(db)
    source = con.execute('SELECT id,raw,sha256 FROM source').fetchone()
    raw_lines = source['raw'].decode('utf-8-sig').splitlines()
    for ev in con.execute('SELECT source_id,line_no,identity,payload,sha256 FROM event'):
        assert ev['source_id'] == source['id']
        original_line = raw_lines[ev['line_no'] - 1]
        # 行号回指的原文行规范化后必须等于入库 payload。
        canonical = json.dumps(json.loads(original_line), sort_keys=True,
                               ensure_ascii=False, separators=(',', ':'))
        assert canonical == ev['payload']
        assert hashlib.sha256(canonical.encode()).hexdigest() == ev['sha256']
    assert hashlib.sha256(source['raw']).hexdigest() == source['sha256']
    con.close()
