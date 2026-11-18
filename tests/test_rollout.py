"""Cross-terminal canary rollout, lost-ack recovery and contract tests.

The field link is local/offline; reachability cuts and process crashes are
simulated through the rollout module's test hooks, never by editing the
endpoint. Every assertion below is about the *real* field version/revision
read back from the endpoint, not the planned target version.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from rainstation.ingest import freeze_config, import_file
from rainstation.rules import evaluate
from rainstation.store import connect
from rainstation.terminal_firmware import invoke, register, state
from rainstation import rollout

FIX = Path(__file__).resolve().parent.parent / 'fixtures'


def prepared(tmp_path, name='rain.db'):
    db = tmp_path / name
    freeze_config(db, FIX / 'firmware_config.json')
    assert import_file(db, FIX / 'firmware_events.jsonl')['imported'] == 9
    assert set(evaluate(db).values()) == {'trusted'}
    assert register(db, 'upgrade-1', FIX / 'firmware_case.json')['devices'] == 3
    return db


@pytest.fixture(autouse=True)
def _reset_hooks():
    rollout.set_unreachable(set())
    rollout.set_crash_hook(None)
    yield
    rollout.set_unreachable(set())
    rollout.set_crash_hook(None)


def actual_versions(report):
    return {t['device']: t['actual_version'] for t in report['terminals']}


def terminal(report, device):
    return next(t for t in report['terminals'] if t['device'] == device)


# ---------------------------------------------------------------------------
# Contract semantics on the canonical field failures
# ---------------------------------------------------------------------------

def test_fleet_atomic_rolls_back_every_upgraded_terminal(tmp_path):
    db = prepared(tmp_path)
    report = rollout.run_rollout(db, 'r-1', 'upgrade-1', 'fleet_atomic')
    assert report['status'] == 'completed'
    assert report['contract_outcome'] == 'fleet_failed_safely_restored'
    # D1 applied, D2 committed-but-ack-lost, D3 explicitly rejected.
    assert terminal(report, 'D1')['install_status'] == 'applied'
    assert terminal(report, 'D2')['install_status'] == 'applied'
    assert terminal(report, 'D3')['install_status'] == 'rejected'
    assert actual_versions(report) == {'D1': '1.0', 'D2': '1.0', 'D3': '1.0'}
    assert {state(db, d)['version'] for d in ('D1', 'D2', 'D3')} == {'1.0'}
    # The rejected terminal was never upgraded and therefore never rolled back.
    assert terminal(report, 'D3')['rollback_status'] is None


def test_cell_isolated_keeps_ridge_and_reports_valley_blocked(tmp_path):
    db = prepared(tmp_path)
    report = rollout.run_rollout(db, 'r-1', 'upgrade-1', 'cell_isolated')
    assert report['status'] == 'completed'
    assert actual_versions(report) == {'D1': '2.0', 'D2': '2.0', 'D3': '1.0'}
    assert {state(db, d)['version'] for d in ('D1', 'D2')} == {'2.0'}
    assert state(db, 'D3')['version'] == '1.0'
    assert report['cell_status'] == {'Ridge': 'on_target', 'Valley': 'baseline_or_mixed'}
    # A single rejected terminal never made its cell half-upgraded.
    assert report['half_upgraded_cells'] == []


def test_ack_lost_is_reconciled_with_same_operation_id_and_field_state(tmp_path):
    db = prepared(tmp_path)
    report = rollout.run_rollout(db, 'r-1', 'upgrade-1', 'cell_isolated')
    d2 = terminal(report, 'D2')
    methods = rollout._recon_methods(d2['reconciliation'])
    assert 'replay_operation_id' in methods
    assert 'field_state' in methods
    assert d2['install_operation_id'] == 'r-1:D2:install:1'
    assert d2['actual_version'] == '2.0' and d2['actual_revision'] == 1
    # The endpoint journal holds exactly one install operation for D2.
    rows = connect(db).execute(
        "SELECT id FROM terminal_firmware_operation WHERE device='D2' AND command='install'").fetchall()
    assert [r['id'] for r in rows] == ['r-1:D2:install:1']


def test_three_failure_modes_are_distinct_in_the_report(tmp_path):
    db = prepared(tmp_path)
    report = rollout.run_rollout(db, 'r-1', 'upgrade-1', 'fleet_atomic')
    by_device = {t['device']: t for t in report['terminals']}
    assert by_device['D1']['request_response']['status'] == 'applied'
    assert by_device['D2']['request_response']['status'] == 'applied'  # normalized after reconciliation
    assert by_device['D3']['request_response']['status'] == 'rejected'
    # ack_lost must never appear as a terminal outcome.
    assert all(t['install_status'] != 'ack_lost' for t in report['terminals'])


# ---------------------------------------------------------------------------
# Idempotency / reruns
# ---------------------------------------------------------------------------

def test_rerun_is_idempotent_and_issues_no_duplicate_operations(tmp_path):
    db = prepared(tmp_path)
    first = rollout.run_rollout(db, 'r-1', 'upgrade-1', 'fleet_atomic')
    second = rollout.run_rollout(db, 'r-1', 'upgrade-1', 'fleet_atomic')
    assert actual_versions(first) == actual_versions(second) == {
        'D1': '1.0', 'D2': '1.0', 'D3': '1.0'}
    db_rows = connect(db).execute(
        'SELECT COUNT(*) AS n FROM terminal_firmware_operation').fetchone()['n']
    third = rollout.run_rollout(db, 'r-1', 'upgrade-1', 'fleet_atomic')
    assert connect(db).execute(
        'SELECT COUNT(*) AS n FROM terminal_firmware_operation').fetchone()['n'] == db_rows
    assert actual_versions(third) == actual_versions(first)


def test_duplicate_operation_id_replays_instead_of_reinstalling(tmp_path):
    db = prepared(tmp_path)
    rollout.run_rollout(db, 'r-1', 'upgrade-1', 'cell_isolated')
    # Reusing the exact stable operation id replays the stored endpoint record.
    replay = invoke(db, 'upgrade-1', 'D1', 'install', '2.0', 'r-1:D1:install:1', 0)
    assert replay == {'status': 'replayed',
                      'record': {'status': 'applied', 'version': '2.0', 'revision': 1}}
    assert state(db, 'D1')['revision'] == 1  # no extra revision bump


# ---------------------------------------------------------------------------
# Process crash / restart recovery at every durable checkpoint
# ---------------------------------------------------------------------------

CRASH_POINTS = ('after_intent_persisted', 'after_dispatch_recorded',
                'after_ack_lost_recorded')


@pytest.mark.parametrize('contract', ['fleet_atomic', 'cell_isolated'])
@pytest.mark.parametrize('point', CRASH_POINTS)
def test_crash_restart_recovers_without_duplicate_install(tmp_path, contract, point):
    db = prepared(tmp_path)

    def hook(hook_point, ctx):
        if ctx.get('device') == 'D2' and hook_point == point:
            raise rollout.SimulatedCrash('power loss mid-run')

    rollout.set_crash_hook(hook)
    with pytest.raises(rollout.SimulatedCrash):
        rollout.run_rollout(db, 'r-1', 'upgrade-1', contract)
    rollout.set_crash_hook(None)

    recovered = rollout.run_rollout(db, 'r-1', 'upgrade-1', contract)
    d2 = terminal(recovered, 'D2')
    assert d2['install_status'] == 'applied'
    # Only one install operation id exists for D2 despite the interrupted life.
    ops = [r['id'] for r in connect(db).execute(
        "SELECT id FROM terminal_firmware_operation WHERE device='D2' AND command='install'")]
    assert ops == ['r-1:D2:install:1']
    if contract == 'fleet_atomic':
        assert actual_versions(recovered) == {'D1': '1.0', 'D2': '1.0', 'D3': '1.0'}
    else:
        assert actual_versions(recovered) == {'D1': '2.0', 'D2': '2.0', 'D3': '1.0'}


def test_crash_after_ack_lost_keeps_reconciliation_evidence(tmp_path):
    db = prepared(tmp_path)

    def hook(point, ctx):
        if ctx.get('device') == 'D2' and point == 'after_ack_lost_recorded':
            raise rollout.SimulatedCrash('reboot before reconciliation')

    rollout.set_crash_hook(hook)
    with pytest.raises(rollout.SimulatedCrash):
        rollout.run_rollout(db, 'r-1', 'upgrade-1', 'cell_isolated')
    rollout.set_crash_hook(None)
    # The change was committed before the crash: field really is on 2.0.
    assert state(db, 'D2') == {'id': 'D2', 'cell': 'Ridge', 'version': '2.0', 'revision': 1}

    recovered = rollout.run_rollout(db, 'r-1', 'upgrade-1', 'cell_isolated')
    d2 = terminal(recovered, 'D2')
    assert 'replay_operation_id' in rollout._recon_methods(d2['reconciliation'])
    assert d2['actual_version'] == '2.0'


def test_crash_during_rollback_phase_is_resumed(tmp_path):
    db = prepared(tmp_path)

    def hook(point, ctx):
        if ctx.get('device') == 'D1' and ctx.get('command') == 'rollback' \
                and point == 'after_dispatch_recorded':
            raise rollout.SimulatedCrash('handheld died during rollback')

    rollout.set_crash_hook(hook)
    with pytest.raises(rollout.SimulatedCrash):
        rollout.run_rollout(db, 'r-1', 'upgrade-1', 'fleet_atomic')
    rollout.set_crash_hook(None)
    recovered = rollout.run_rollout(db, 'r-1', 'upgrade-1', 'fleet_atomic')
    assert actual_versions(recovered) == {'D1': '1.0', 'D2': '1.0', 'D3': '1.0'}
    d1 = terminal(recovered, 'D1')
    assert d1['rollback_status'] == 'applied'
    assert [r['id'] for r in connect(db).execute(
        "SELECT id FROM terminal_firmware_operation WHERE device='D1' AND command='rollback'")] == [
        'r-1:D1:rollback:1']


def test_rollback_ack_lost_is_reconciled_with_same_operation_id(tmp_path, monkeypatch):
    db = prepared(tmp_path)
    real_invoke = rollout.tf.invoke
    calls = {'n': 0}

    def flaky(db_path, case_id, device, command, version, op_id, rev):
        if command == 'rollback' and device == 'D2':
            calls['n'] += 1
            if calls['n'] == 1:
                # The endpoint commits the rollback, then the response is lost
                # (process death before the orchestrator records the reply).
                result = real_invoke(db_path, case_id, device, command,
                                     version, op_id, rev)
                assert result['status'] == 'applied'
                raise rollout.SimulatedCrash('rollback response lost on wire')
        return real_invoke(db_path, case_id, device, command, version, op_id, rev)

    monkeypatch.setattr(rollout.tf, 'invoke', flaky)
    with pytest.raises(rollout.SimulatedCrash):
        rollout.run_rollout(db, 'r-1', 'upgrade-1', 'fleet_atomic')
    # Field already carries the committed rollback.
    assert state(db, 'D2')['version'] == '1.0'

    report = rollout.run_rollout(db, 'r-1', 'upgrade-1', 'fleet_atomic')
    d2 = terminal(report, 'D2')
    assert d2['rollback_status'] == 'applied'
    assert d2['actual_version'] == '1.0'
    assert [r['id'] for r in connect(db).execute(
        "SELECT id FROM terminal_firmware_operation WHERE device='D2' AND command='rollback'")] == [
        'r-1:D2:rollback:1']
    assert 'replay_operation_id' in rollout._recon_methods(d2['reconciliation'])


# ---------------------------------------------------------------------------
# Reachability / not-delivered
# ---------------------------------------------------------------------------

def test_unreachable_terminal_is_not_delivered_and_retried_after_link_restore(tmp_path):
    db = prepared(tmp_path)
    rollout.set_unreachable({'D1'})
    blocked = rollout.run_rollout(db, 'r-1', 'upgrade-1', 'fleet_atomic')
    assert blocked['status'] == 'recovery_pending_unreachable'
    d1 = terminal(blocked, 'D1')
    assert d1['install_status'] == 'not_delivered'
    assert d1['actual_version'] is None  # never pretend the target was reached
    assert state(db, 'D1')['version'] == '1.0'  # field untouched

    rollout.set_unreachable(set())
    resumed = rollout.run_rollout(db, 'r-1', 'upgrade-1', 'fleet_atomic')
    assert resumed['status'] == 'completed'
    assert actual_versions(resumed) == {'D1': '1.0', 'D2': '1.0', 'D3': '1.0'}
    # The retried request reused the same stable operation id.
    assert terminal(resumed, 'D1')['install_operation_id'] == 'r-1:D1:install:1'


# ---------------------------------------------------------------------------
# Rollback revision conflict: another maintainer changed the field
# ---------------------------------------------------------------------------

def test_rollback_revision_conflict_stops_override_and_flags_manual(tmp_path):
    db = prepared(tmp_path)
    # Stage 1: only D1 gets installed (its peers are offline for this run).
    rollout.set_unreachable({'D2', 'D3'})
    rollout.run_rollout(db, 'r-1', 'upgrade-1', 'fleet_atomic')
    rollout.set_unreachable(set())
    # Another maintainer works on D1 in the meantime: revision moves 1 -> 2.
    with connect(db) as c:
        c.execute(
            "UPDATE terminal_firmware_device SET version='3.0',revision=revision+1 WHERE id='D1'")
    assert state(db, 'D1')['version'] == '3.0'

    report = rollout.run_rollout(db, 'r-1', 'upgrade-1', 'fleet_atomic')
    assert report['status'] == 'manual_handling_required'
    d1 = terminal(report, 'D1')
    assert d1['rollback_status'] == 'revision_conflict'
    assert 'rollback_revision_conflict' in d1['manual_handling']
    # The orchestrator must not overwrite the other maintainer's version.
    assert state(db, 'D1')['version'] == '3.0'
    assert d1['actual_version'] == '3.0'
    # D2 was still safely restored; the fleet is not falsely declared uniform.
    assert terminal(report, 'D2')['rollback_status'] == 'applied'


# ---------------------------------------------------------------------------
# Source-summary mismatch and unknown devices
# ---------------------------------------------------------------------------

def test_pulse_source_mismatch_is_quarantined_for_manual_handling(tmp_path):
    db = prepared(tmp_path)
    # Remove D3's pulse event after registration: the event-table source summary
    # no longer matches the digest frozen in the case.
    with connect(db) as c:
        c.execute("DELETE FROM evidence WHERE subject='S3'")
        c.execute(
            "DELETE FROM event WHERE subject='S3' AND kind='pulse'")
    report = rollout.run_rollout(db, 'r-1', 'upgrade-1', 'cell_isolated')
    d3 = terminal(report, 'D3')
    assert d3['install_status'] == 'drift_manual'
    assert 'pulse_source_mismatch' in d3['manual_handling']
    assert report['status'] == 'manual_handling_required'
    # No install request was ever sent to the quarantined terminal.
    assert connect(db).execute(
        "SELECT COUNT(*) AS n FROM terminal_firmware_operation WHERE device='D3'").fetchone()['n'] == 0
    assert state(db, 'D3')['version'] == '1.0'


def test_unknown_device_is_skipped_and_flagged(tmp_path):
    db = prepared(tmp_path)
    with connect(db) as c:
        c.execute("DELETE FROM terminal_firmware_device WHERE id='D3'")
    report = rollout.run_rollout(db, 'r-1', 'upgrade-1', 'cell_isolated')
    d3 = terminal(report, 'D3')
    assert d3['install_status'] == 'drift_manual'
    assert 'unknown_device' in d3['manual_handling']


# ---------------------------------------------------------------------------
# Half-upgraded cell must never be declared complete
# ---------------------------------------------------------------------------

def _write_case(tmp_path, devices):
    case = {'target_version': '2.0', 'devices': devices}
    path = tmp_path / 'case.json'
    path.write_text(json.dumps(case))
    return path


def test_half_upgraded_cell_is_rolled_back_and_not_declared_complete(tmp_path):
    db = prepared(tmp_path)
    digest = json.loads((FIX / 'firmware_case.json').read_text())['devices'][0]['pulse_source_sha256']
    # Two devices in Ridge: D1 healthy, D2 forced to reject so the cell is split.
    case_path = _write_case(tmp_path, [
        {'id': 'D1', 'station': 'S1', 'cell': 'Ridge', 'current_version': '1.0',
         'pulse_source_sha256': digest},
        {'id': 'D2', 'station': 'S2', 'cell': 'Ridge', 'current_version': '1.0',
         'pulse_source_sha256': digest, 'failure': 'reject'},
    ])
    register(db, 'upgrade-2', case_path)
    report = rollout.run_rollout(db, 'r-half', 'upgrade-2', 'cell_isolated')
    assert report['status'] == 'half_upgraded_cell_recovered'
    assert report['half_upgraded_cells'] == ['Ridge']
    assert report['cell_status']['Ridge'] == 'half_upgraded_not_complete'
    # The one upgraded peer was compensated back to baseline; nothing claims 2.0.
    assert actual_versions(report) == {'D1': '1.0', 'D2': '1.0'}
    assert terminal(report, 'D1')['rollback_status'] == 'applied'


# ---------------------------------------------------------------------------
# Preflight and evidence report
# ---------------------------------------------------------------------------

def test_preflight_reports_source_summary_cell_and_real_field_version(tmp_path):
    db = prepared(tmp_path)
    pre = rollout.preflight(db, 'upgrade-1')
    assert pre['target_version'] == '2.0'
    by_id = {d['device']: d for d in pre['devices']}
    assert by_id['D2']['cell'] == 'Ridge'
    assert by_id['D2']['observed_pulse_source_sha256'] == by_id['D2']['pulse_source_sha256']
    assert by_id['D2']['registered']['version'] == '1.0'


def test_json_and_markdown_reports_carry_per_terminal_evidence(tmp_path):
    db = prepared(tmp_path)
    rollout.run_rollout(db, 'r-1', 'upgrade-1', 'fleet_atomic')
    out = tmp_path / 'evidence' / 'rollout.json'
    path = rollout.export_rollout_report(db, 'r-1', out)
    data = json.loads(path.read_text())
    assert {t['device'] for t in data['terminals']} == {'D1', 'D2', 'D3'}
    required = {'station', 'pulse_source_sha256', 'cell', 'baseline_version',
                'target_version', 'actual_version', 'actual_revision',
                'request_response', 'reconciliation', 'rollback_status',
                'manual_handling'}
    assert required <= set(data['terminals'][0])
    md = path.with_suffix('.md').read_text()
    assert 'D2' in md and '复核' in md
    # The report must quote the real field version, not the target.
    d2 = next(t for t in data['terminals'] if t['device'] == 'D2')
    assert d2['actual_version'] == '1.0' and d2['target_version'] == '2.0'


def test_bad_contract_is_rejected(tmp_path):
    db = prepared(tmp_path)
    with pytest.raises(ValueError, match='bad_rollout_contract'):
        rollout.run_rollout(db, 'r-1', 'upgrade-1', 'no_such_contract')


def test_cli_end_to_end_writes_json_and_markdown(tmp_path, monkeypatch):
    import subprocess, sys
    db = prepared(tmp_path)
    out = tmp_path / 'cli-report.json'
    env = dict(os.environ)
    repo = Path(__file__).resolve().parent.parent
    env['PYTHONPATH'] = str(repo)
    base = [sys.executable, '-m', 'rainstation']
    common = ['--db', str(db)]

    def run(*args):
        return subprocess.run(base + list(args), cwd=tmp_path, env=env,
                              capture_output=True, text=True, check=True)
    run('firmware-rollout', *common, '--rollout-id', 'cli-1',
        '--case-id', 'upgrade-1', '--contract', 'cell_isolated')
    run('firmware-report', *common, '--rollout-id', 'cli-1', '--out', str(out))
    data = json.loads(out.read_text())
    assert data['contract'] == 'cell_isolated'
    assert {t['device']: t['actual_version'] for t in data['terminals']} == {
        'D1': '2.0', 'D2': '2.0', 'D3': '1.0'}
    assert out.with_suffix('.md').exists()
