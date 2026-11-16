import json
from pathlib import Path

from rainstation.ingest import freeze_config, import_file, sequence_inventory
from rainstation.rules import evaluate
from rainstation.store import connect


BASE = Path(__file__).parent.parent / 'fixtures'


def fixture():
    return [json.loads(line) for line in (BASE / 'events.jsonl').read_text().splitlines()]


def run_case(tmp_path, events, config=None, policy='conservative'):
    tmp_path.mkdir(parents=True, exist_ok=True)
    db = tmp_path / 'case.db'
    cfg = tmp_path / 'config.json'
    cfg.write_text(json.dumps(config or json.loads((BASE / 'config.json').read_text())))
    freeze_config(db, cfg)
    p = tmp_path / 'events.jsonl'
    p.write_text('\n'.join(json.dumps(e) for e in events) + '\n')
    import_file(db, p)
    result = evaluate(db, policy=policy)
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


def test_maintenance_end_is_exclusive(tmp_path):
    events=fixture()
    events.append({'kind':'maintenance','subject':'S1','identity':'M2','device':'D1',
        'time':'2026-01-01T08:30:00Z','start':'2026-01-01T08:20:00Z','end':'2026-01-01T08:25:00Z'})
    _,_,details=run_case(tmp_path,events)
    assert list(details['hourly_mm'].values())==[0.4]


def test_local_midnight_splits_hourly_buckets(tmp_path):
    events=fixture();events[2]['time']='2026-01-01T15:55:00Z';events[3]['time']='2026-01-01T16:05:00Z'
    _,_,details=run_case(tmp_path,events)
    keys=list(details['hourly_mm'])
    assert len(keys)==2
    assert keys[0].startswith('2026-01-01') and keys[1].startswith('2026-01-02')


def test_duplicate_station_contract_is_rejected(tmp_path):
    import pytest
    cfg=json.loads((BASE/'config.json').read_text());cfg['stations'].append(dict(cfg['stations'][0]))
    p=tmp_path/'bad.json';p.write_text(json.dumps(cfg))
    with pytest.raises(ValueError,match='duplicate_or_missing_subject'):
        freeze_config(tmp_path/'bad.db',p)


def test_report_contains_audit_entities(tmp_path):
    from rainstation.report import export
    run_case(tmp_path,fixture())
    data=json.loads(export(tmp_path/'case.db',tmp_path/'report.json').read_text())
    assert data['batch'] and data['subject'] and data['source'] and data['event']
    assert data['decision'] and data['evidence'] and data['run']
    assert len(data['evidence'])==len(fixture())


# ---------------------------------------------------------------------------
# Auditable hourly confidence intervals
# ---------------------------------------------------------------------------

# Local zone +08:00, so local midnight 2026-01-02T00:00+08 == 16:00 UTC.
# Coefficient T1 switches exactly at that UTC instant (0.2 -> 0.5 mm/pulse).
INTERVAL_CONFIG = {
    "stations": [{
        "id": "S1", "offset_minutes": 480,
        "device_models": {"D1": "T1"},
        "coefficients": [
            {"effective": "2025-01-01T00:00:00Z", "model": "T1", "mm_per_pulse": 0.2},
            {"effective": "2026-01-01T16:00:00Z", "model": "T1", "mm_per_pulse": 0.5}],
        "max_clock_drift_seconds": 60,
        "max_mm_per_hour": 100,
        "dispatch_threshold_mm": 2.0}]}

H23 = '2026-01-01T23:00:00+08:00'
H00 = '2026-01-02T00:00:00+08:00'


def cross_midnight_fixture(anchor_offset=30):
    # Missing seqs 5,6,7,8 -> one contiguous block of 4 unknown pulses.
    # Flanks are seq 4 (15:20Z) and seq 9 (16:40Z). Anchors drift 0 -> 30s.
    # Maintenance [15:45Z, 16:15Z) covers part of both possible hours.
    return [
        {"kind": "anchor", "subject": "S1", "identity": "D1:1", "device": "D1", "seq": 1,
         "time": "2026-01-01T15:00:00Z", "offset_seconds": 0},
        {"kind": "anchor", "subject": "S1", "identity": "D1:10", "device": "D1", "seq": 10,
         "time": "2026-01-01T17:00:00Z", "offset_seconds": anchor_offset},
        {"kind": "heartbeat", "subject": "S1", "identity": "D1:2", "device": "D1", "seq": 2,
         "time": "2026-01-01T15:05:00Z"},
        {"kind": "heartbeat", "subject": "S1", "identity": "D1:11", "device": "D1", "seq": 11,
         "time": "2026-01-01T17:05:00Z"},
        {"kind": "pulse", "subject": "S1", "identity": "D1:3", "device": "D1", "seq": 3,
         "time": "2026-01-01T15:10:00Z", "count": 1},
        {"kind": "pulse", "subject": "S1", "identity": "D1:4", "device": "D1", "seq": 4,
         "time": "2026-01-01T15:20:00Z", "count": 1},
        {"kind": "pulse", "subject": "S1", "identity": "D1:9", "device": "D1", "seq": 9,
         "time": "2026-01-01T16:40:00Z", "count": 1},
        {"kind": "maintenance", "subject": "S1", "identity": "M1", "device": "D1",
         "time": "2026-01-01T16:20:00Z", "start": "2026-01-01T15:45:00Z",
         "end": "2026-01-01T16:15:00Z"}]


def _hours(details):
    return {h['hour']: h for h in details['hours']}


def test_sequence_inventory_groups_contiguous_gap():
    events = cross_midnight_fixture()
    db_events = [{'id': i + 1, 'identity': e['identity'],
                  'payload': json.dumps(e)} for i, e in enumerate(events)]
    gaps = sequence_inventory(db_events)
    assert len(gaps) == 1
    gap = gaps[0]
    assert gap['missing_seqs'] == [5, 6, 7, 8]
    assert gap['missing_count'] == 4
    # flank identities D1:4 and D1:9
    assert db_events[gap['before_event_id'] - 1]['identity'] == 'D1:4'
    assert db_events[gap['after_event_id'] - 1]['identity'] == 'D1:9'


def test_cross_midnight_gap_bounds_and_policies_differ(tmp_path):
    events = cross_midnight_fixture()
    result_c, issues, det_c = run_case(tmp_path, events, INTERVAL_CONFIG, 'conservative')
    hours = _hours(det_c)
    assert 'sequence_gap' in issues
    # Conservative withholds both touched hours because bounds are not equal.
    assert result_c['S1'] == 'incomplete'
    for hour in (H23, H00):
        assert hours[hour]['exact'] is False
        assert hours[hour]['policies']['conservative'] == {'state': 'review', 'value': None}
    # H23: measured 0.2 + unknown [0.2, 1.0]  (cap 4 pulses * 0.2, maintenance segment cap 0)
    assert [hours[H23]['mm_lower'], hours[H23]['mm_upper']] == [0.4, 1.2]
    # H00: measured 0.5 + unknown [0.0, 2.0]  (4 pulses * new coefficient 0.5)
    assert [hours[H00]['mm_lower'], hours[H00]['mm_upper']] == [0.5, 2.5]
    # Interval policy on the same immutable inputs.
    result_i, _, det_i = run_case(tmp_path, events, INTERVAL_CONFIG, 'interval')
    hours_i = _hours(det_i)
    assert result_i['S1'] == 'incomplete'
    # H23 interval [0.4,1.2] stays under threshold 2.0 -> usable for scheduling.
    assert hours_i[H23]['policies']['interval'] == {'state': 'usable', 'range': [0.4, 1.2]}
    # H00 interval [0.5,2.5] straddles threshold -> still pending verification.
    assert hours_i[H00]['policies']['interval'] == {'state': 'pending', 'range': [0.5, 2.5]}
    # Conservative decision on the identical hour remains a withhold.
    assert hours_i[H23]['policies']['conservative']['state'] == 'review'


def test_bounds_cite_raw_event_and_source_evidence_never_midpoint(tmp_path):
    events = cross_midnight_fixture()
    _, _, details = run_case(tmp_path, events, INTERVAL_CONFIG, 'interval')
    hours = _hours(details)
    bound = hours[H00]['bounds'][0]
    assert bound['gap_id'] == 'S1:D1:seqs-5-8'
    # flank pulse events, anchor events and the maintenance event all back the bound
    con = connect(tmp_path / 'case.db')
    identities = {r['id']: r['identity'] for r in con.execute('SELECT id,identity FROM event')}
    assert identities[bound['before_event_id']] == 'D1:4'
    assert identities[bound['after_event_id']] == 'D1:9'
    anchor_identities = {identities[i] for i in bound['anchor_event_ids']}
    assert anchor_identities == {'D1:1', 'D1:10'}
    assert {identities[i] for i in bound['maintenance_event_ids']} == {'M1'}
    assert bound['source_ids'] == [1]
    # envelope reflects 60s anchor drift on both flanks
    assert bound['envelope_utc'] == {
        'earliest': '2026-01-01T15:19:00+00:00',
        'latest': '2026-01-01T16:41:00+00:00'}
    # no midpoint field exists anywhere; only bounds and measured values
    serialised = json.dumps(details)
    assert 'midpoint' not in serialised and 'mid' not in serialised
    for hour in details['hours']:
        assert hour['mm_lower'] <= hour['mm_upper']
        assert hour['measured_mm'] <= hour['mm_upper']


def test_unknown_block_is_not_double_counted_across_hours(tmp_path):
    events = cross_midnight_fixture()
    _, _, details = run_case(tmp_path, events, INTERVAL_CONFIG)
    bound = _hours(details)[H23]['bounds'][0]
    other = _hours(details)[H00]['bounds'][0]
    # the same gap block backs both touched hours (identical audit identity)
    assert other['gap_id'] == bound['gap_id']
    assert bound['missing_pulses'] == 4
    # rate caps are reported per segment; effective per-hour upper extremes
    # (min(rate_cap, block size)) are bounded by the block size
    for seg in bound['segments']:
        assert seg['max_pulses'] >= 0
    # the lower-bound allocation and every feasible allocation together never
    # use more than the 4 missing pulses; per-hour upper extremes are not
    # summed (they are mutually exclusive alternatives)
    assert bound['lower_pulse_total'] <= 4
    # reconstruct each hour's effective upper pulse count from its mm range
    h23 = _hours(details)[H23]; h00 = _hours(details)[H00]
    unknown_hi_23 = round((h23['mm_upper'] - h23['measured_mm']) / 0.2)
    unknown_hi_00 = round((h00['mm_upper'] - h00['measured_mm']) / 0.5)
    assert max(unknown_hi_23, unknown_hi_00) <= 4
    assert unknown_hi_23 + unknown_hi_00 >= 4  # the block must fit somewhere
    # maintenance segments admit zero unknown pulses
    assert all(seg['max_pulses'] == 0 for seg in bound['segments']
               if seg['inside_maintenance'])
    # gap id appears once per touched hour, never duplicated inside an hour
    for hour in details['hours']:
        assert hour['gap_ids'].count('S1:D1:seqs-5-8') == 1


def test_coefficient_switch_exactly_at_effective_instant(tmp_path):
    events = [
        {"kind": "anchor", "subject": "S1", "identity": "D1:1", "device": "D1", "seq": 1,
         "time": "2026-01-01T15:00:00Z", "offset_seconds": 0},
        {"kind": "heartbeat", "subject": "S1", "identity": "D1:2", "device": "D1", "seq": 2,
         "time": "2026-01-01T15:05:00Z"},
        # corrected time is exactly 16:00Z == local midnight == effective boundary
        {"kind": "pulse", "subject": "S1", "identity": "D1:3", "device": "D1", "seq": 3,
         "time": "2026-01-01T16:00:00Z", "count": 2}]
    _, _, details = run_case(tmp_path, events, INTERVAL_CONFIG)
    hours = _hours(details)
    assert set(hours) == {H00}
    # new coefficient 0.5 applies inclusively at the effective instant
    assert hours[H00]['measured_mm'] == 1.0
    assert hours[H00]['mm_lower'] == hours[H00]['mm_upper'] == 1.0


def test_gap_fully_inside_maintenance_contributes_zero_bound(tmp_path):
    # zero accepted drift makes the corrected gap time exact, so maintenance
    # can cover the whole uncertainty window without guessing.
    cfg = json.loads(json.dumps(INTERVAL_CONFIG))
    cfg['stations'][0]['max_clock_drift_seconds'] = 0
    events = [
        {"kind": "anchor", "subject": "S1", "identity": "D1:1", "device": "D1", "seq": 1,
         "time": "2026-01-01T12:00:00Z", "offset_seconds": 0},
        {"kind": "heartbeat", "subject": "S1", "identity": "D1:2", "device": "D1", "seq": 2,
         "time": "2026-01-01T12:01:00Z"},
        {"kind": "pulse", "subject": "S1", "identity": "D1:3", "device": "D1", "seq": 3,
         "time": "2026-01-01T13:00:00Z", "count": 2},
        {"kind": "pulse", "subject": "S1", "identity": "D1:6", "device": "D1", "seq": 6,
         "time": "2026-01-01T18:00:00Z", "count": 1},
        # zero-drift gap envelope [13:00,18:00Z) equals the half-open window;
        # every possible missing-pulse instant is maintenance time, while the
        # 18:00 flank on the exclusive end still counts as natural rainfall
        {"kind": "maintenance", "subject": "S1", "identity": "M1", "device": "D1",
         "time": "2026-01-01T15:25:00Z", "start": "2026-01-01T13:00:00Z",
         "end": "2026-01-01T18:00:00Z"}]
    result, issues, details = run_case(tmp_path, events, cfg)
    assert 'sequence_gap' in issues and 'physical_spike' not in issues
    # The unknown block collapses to exactly zero: every touched hour is exact.
    assert all(h['exact'] for h in details['hours'])
    measured = [h for h in details['hours'] if h['measured_mm']]
    assert len(measured) == 1
    hour = measured[0]
    # 18:00 local 02:00, new coefficient 0.5, one observed pulse
    assert hour['hour'].endswith('T02:00:00+08:00')
    assert hour['mm_lower'] == hour['mm_upper'] == 0.5
    # every bound over the maintenance-only gap allocates zero pulses
    for h in details['hours']:
        for b in h['bounds']:
            assert b['lower_pulse_total'] == 0 and b['upper_pulse_total'] == 0
    # exact bounds mean the conservative policy may still publish
    assert hour['policies']['conservative'] == {'state': 'published', 'value': 0.5}
    assert result['S1'] == 'incomplete'  # sequence_gap remains recorded


def test_recompute_reports_new_vs_old_bound_diff_and_keeps_history(tmp_path):
    db = tmp_path / 'case.db'
    cfg = tmp_path / 'config.json'
    cfg.write_text(json.dumps(INTERVAL_CONFIG))
    freeze_config(db, cfg)
    p = tmp_path / 'events.jsonl'
    p.write_text('\n'.join(json.dumps(e) for e in cross_midnight_fixture(30)) + '\n')
    import_file(db, p)
    evaluate(db, policy='conservative')
    con = connect(db)
    first = json.loads(con.execute('SELECT details FROM decision').fetchone()['details'])
    first_bounds = {h['hour']: (h['mm_lower'], h['mm_upper']) for h in first['hours']}
    # Re-running the same immutable ledger (different policy, same inputs)
    # must reproduce the bounds bit-for-bit.
    evaluate(db, policy='interval')
    second = json.loads(connect(db).execute('SELECT details FROM decision').fetchone()['details'])
    assert second['recompute_diff']['previous_run_id'] == 1
    assert second['recompute_diff']['changes'] == []
    assert second['recompute_diff']['added'] == [] and second['recompute_diff']['removed'] == []
    history = connect(db).execute('SELECT run_id,rule_version,status FROM decision_history').fetchall()
    assert [dict(r)['run_id'] for r in history] == [1]
    # Now alter the ledger through a genuine new source (new anchor identity).
    # The 15:30 anchor becomes the latest prior anchor for the 16:40 flank and
    # changes its corrected time, so the recomputed bounds must actually move.
    extra = {"kind": "anchor", "subject": "S1", "identity": "D1:20", "device": "D1", "seq": 20,
             "time": "2026-01-01T15:30:00Z", "offset_seconds": 0}
    p3 = tmp_path / 'events3.jsonl'
    p3.write_text(json.dumps(extra) + '\n')
    import_file(db, p3)
    evaluate(db, policy='interval')
    third = json.loads(connect(db).execute('SELECT details FROM decision').fetchone()['details'])
    third_bounds = {h['hour']: (h['mm_lower'], h['mm_upper']) for h in third['hours']}
    changes = third['recompute_diff']['changes']
    assert changes, 'a re-anchor that moves corrected times must change bounds'
    for change in changes:
        assert change['before'] == list(first_bounds[change['hour']])
        assert change['after'] == list(third_bounds[change['hour']])
        assert change['delta_lower'] == round(change['after'][0] - change['before'][0], 6)
        assert change['delta_upper'] == round(change['after'][1] - change['before'][1], 6)
    # the untouched hour is absent from the change list
    changed_hours = {c['hour'] for c in changes}
    assert H23 not in changed_hours and H00 in changed_hours
    # both prior decisions remain auditable in the history table
    runs = [dict(r)['run_id'] for r in
            connect(db).execute('SELECT run_id FROM decision_history ORDER BY run_id')]
    assert runs == [1, 2]


def test_interval_below_threshold_is_usable_above_is_pending(tmp_path):
    events = cross_midnight_fixture()
    # tighten the scenario so H00 upper bound sits exactly under threshold
    cfg = json.loads(json.dumps(INTERVAL_CONFIG))
    cfg['stations'][0]['dispatch_threshold_mm'] = 3.0
    _, _, details = run_case(tmp_path / 'a', events, cfg, 'interval')
    hours = _hours(details)
    # H00 [0.5,2.5] fully below 3.0 -> usable; H23 [0.4,1.2] usable as well
    assert hours[H00]['policies']['interval']['state'] == 'usable'
    assert hours[H23]['policies']['interval']['state'] == 'usable'
    # raise threshold strictly inside the H00 interval -> pending
    cfg['stations'][0]['dispatch_threshold_mm'] = 1.0
    _, _, details = run_case(tmp_path / 'b', events, cfg, 'interval')
    hours = _hours(details)
    assert hours[H00]['policies']['interval']['state'] == 'pending'
    assert hours[H23]['policies']['interval']['state'] == 'pending'  # [0.4,1.2] straddles 1.0
