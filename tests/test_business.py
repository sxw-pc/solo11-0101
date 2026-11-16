import json
from pathlib import Path

from rainstation.ingest import freeze_config, import_file
from rainstation.rules import evaluate
from rainstation.store import connect


BASE = Path(__file__).parent.parent / 'fixtures'


def fixture():
    return [json.loads(line) for line in (BASE / 'events.jsonl').read_text().splitlines()]


def run_case(tmp_path, events, config=None):
    db = tmp_path / 'case.db'
    cfg = tmp_path / 'config.json'
    cfg.write_text(json.dumps(config or json.loads((BASE / 'config.json').read_text())))
    freeze_config(db, cfg)
    p = tmp_path / 'events.jsonl'
    p.write_text('\n'.join(json.dumps(e) for e in events) + '\n')
    import_file(db, p)
    result = evaluate(db)
    issues = {r['code'] for r in connect(db).execute('SELECT code FROM issue')}
    details = json.loads(connect(db).execute('SELECT details FROM decision').fetchone()['details'])
    return result, issues, details


def test_maintenance_excludes_pulses_even_if_file_order_later(tmp_path):
    events = fixture()
    events.append({'kind': 'maintenance', 'subject': 'S1', 'identity': 'M1', 'device': 'D1',
                   'time': '2026-01-01T08:30:00Z', 'start': '2026-01-01T08:15:00Z',
                   'end': '2026-01-01T08:20:00Z'})
    _, _, details = run_case(tmp_path, events)
    assert list(details['hourly_mm'].values()) == [0.6]


def test_clock_offset_moves_pulse_to_previous_hour(tmp_path):
    events = fixture()
    events[0]['offset_seconds'] = 1800
    for e in events[2:]:
        e['time'] = '2026-01-01T09:05:00Z'
    _, _, details = run_case(tmp_path, events)
    assert len(details['hourly_mm']) == 1
    assert '16:00:00+08:00' in next(iter(details['hourly_mm']))


def test_sequence_gap_marks_incomplete(tmp_path):
    events = fixture()
    events[3]['seq'] = 7
    result, issues, _ = run_case(tmp_path, events)
    assert result['S1'] == 'incomplete'
    assert 'sequence_gap' in issues


def test_spike_requires_review(tmp_path):
    events = fixture()
    events[3]['count'] = 1000
    result, issues, _ = run_case(tmp_path, events)
    assert result['S1'] == 'review'
    assert 'physical_spike' in issues


def test_missing_coefficient_is_not_trusted(tmp_path):
    config = json.loads((BASE / 'config.json').read_text())
    config['stations'][0]['coefficients'] = []
    result, issues, _ = run_case(tmp_path, fixture(), config)
    assert result['S1'] == 'review'
    assert 'coefficient_missing' in issues


def test_drifting_anchors_block_pulse_merge(tmp_path):
    events = fixture()
    events.append({'kind': 'anchor', 'subject': 'S1', 'identity': 'D1:5', 'device': 'D1', 'seq': 5,
                   'time': '2026-01-01T08:16:00Z', 'offset_seconds': 300})
    result, issues, details = run_case(tmp_path, events)
    assert result['S1'] == 'review'
    assert 'clock_drift' in issues
    assert details['hourly_mm'] == {}


def test_neighbor_disagreement_requires_review(tmp_path):
    cfg=json.loads((BASE/'config.json').read_text())
    cfg['stations'][0]['neighbor_hourly_mm']={'2026-01-01T16:00:00+08:00':50}
    cfg['stations'][0]['neighbor_tolerance_mm']=1
    result,issues,_=run_case(tmp_path,fixture(),cfg)
    assert result['S1']=='review'
    assert 'neighbor_disagreement' in issues


def test_heartbeat_gap_is_incomplete(tmp_path):
    events=fixture();events.append({'kind':'heartbeat','subject':'S1','identity':'D1:5',
        'device':'D1','seq':5,'time':'2026-01-01T11:00:00Z'})
    result,issues,_=run_case(tmp_path,events)
    assert result['S1']=='incomplete'
    assert 'heartbeat_gap' in issues


def test_unknown_device_model_is_not_accepted(tmp_path):
    events=fixture();events[2]['device']='D2';events[2]['identity']='D2:3'
    result,issues,_=run_case(tmp_path,events)
    assert result['S1']=='review'
    assert 'unknown_device_model' in issues
