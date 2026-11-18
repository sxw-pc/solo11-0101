"""Cross-terminal canary firmware rollout and offline recovery orchestration.

The handheld endpoint in :mod:`rainstation.terminal_firmware` executes one
command at a time.  This layer adds the missing fleet coordination:

* every request is preceded by a durable *intent* carrying a stable
  ``operation_id``, the expected revision and a recovery phase;
* the three failure modes are kept distinct — not delivered, explicitly
  rejected, and committed-but-acknowledgement-lost;
* a lost acknowledgement is reconciled (same operation id replay plus field
  state query) before any new install or compensating rollback;
* crashes at any point are recoverable because the journal in
  ``firmware_intent`` / ``firmware_rollout_device`` is the only source of
  truth and re-running a rollout is idempotent;
* rollback revision drift caused by another maintainer stops the override and
  flags the terminal for manual handling instead of blindly overwriting it.

Only the Python standard library is used and no network access is performed.
"""

import json
from datetime import datetime, timezone

from . import terminal_firmware as tf
from .store import connect

CONTRACTS = ('fleet_atomic', 'cell_isolated')

# Recovery phases written to the intent journal *before* each network step.
INSTALL_INTENDED = 'install_intended'
INSTALL_DISPATCHED = 'install_dispatched'
INSTALL_ACK_LOST = 'install_ack_lost'
INSTALL_RECONCILED = 'install_reconciled'
ROLLBACK_INTENDED = 'rollback_intended'
ROLLBACK_DISPATCHED = 'rollback_dispatched'
ROLLBACK_ACK_LOST = 'rollback_ack_lost'
ROLLBACK_RECONCILED = 'rollback_reconciled'


def _now():
    return datetime.now(timezone.utc).isoformat()


def _op_id(rollout_id, device, command, seq):
    """Stable, deterministic operation id so a rerun never issues a new one."""
    return f'{rollout_id}:{device}:{command}:{seq}'


class SimulatedCrash(RuntimeError):
    """Raised by the test crash hook to mimic a process death on site."""


# Reachability gate. The field link is local/offline; tests use this to cut a
# terminal. Only devices present in the set are considered unreachable.
_unreachable = set()

# Crash hook used by the recovery tests: ``hook(point, context)`` may raise
# :class:`SimulatedCrash` at any durable checkpoint. Production code leaves it
# as None.
_crash_hook = None


def set_unreachable(devices):
    """Restrict which terminals simulate a broken local link (test support)."""
    _unreachable.clear()
    _unreachable.update(devices)


def set_crash_hook(hook):
    """Install (or clear with ``None``) the process-crash test hook."""
    global _crash_hook
    _crash_hook = hook


def _crash(point, context):
    if _crash_hook is not None:
        _crash_hook(point, context)


def _reachable(device):
    return device not in _unreachable


def preflight(db_path, case_id):
    """Return, for every device in the frozen case, the pulse source summary,
    the registered cell/baseline and whether the local endpoint still knows
    the terminal. Nothing here is mutated."""
    db = connect(db_path)
    try:
        case = db.execute(
            'SELECT payload FROM terminal_firmware_case WHERE id=?', (case_id,)
        ).fetchone()
        if not case:
            raise ValueError('unknown_firmware_case')
        doc = json.loads(case['payload'])
        items = []
        for spec in doc['devices']:
            source = db.execute(
                '''SELECT s.sha256 FROM event e JOIN source s ON s.id=e.source_id
                   WHERE e.subject=? AND json_extract(e.payload,'$.device')=?
                     AND e.kind='pulse' ORDER BY e.id LIMIT 1''',
                (spec['station'], spec['id']),
            ).fetchone()
            device_row = db.execute(
                'SELECT id,cell,version,revision FROM terminal_firmware_device WHERE id=?',
                (spec['id'],),
            ).fetchone()
            items.append({
                'device': spec['id'],
                'station': spec['station'],
                'cell': spec['cell'],
                'pulse_source_sha256': spec['pulse_source_sha256'],
                'observed_pulse_source_sha256': source['sha256'] if source else None,
                'case_baseline_version': spec['current_version'],
                'registered': dict(device_row) if device_row else None,
            })
        return {'case_id': case_id, 'target_version': doc['target_version'], 'devices': items}
    finally:
        db.close()


def _upsert_intent(db, rollout_id, device, command, version, expected_revision, phase):
    """Persist (or advance) a durable intent before touching the endpoint.

    On recovery the journal may already sit in an in-flight phase
    (dispatched / ack-lost): that evidence must not be overwritten with the
    initial intended phase, otherwise the caller could not tell that a request
    had already left the handheld and would skip reconciliation.
    """
    operation_id = _op_id(rollout_id, device, command, 1)
    in_flight = (
        INSTALL_DISPATCHED, INSTALL_ACK_LOST,
        ROLLBACK_DISPATCHED, ROLLBACK_ACK_LOST,
    )
    row = db.execute(
        'SELECT phase,attempts FROM firmware_intent WHERE operation_id=?', (operation_id,)
    ).fetchone()
    ts = _now()
    if row is None:
        db.execute(
            '''INSERT INTO firmware_intent(operation_id,rollout_id,device,command,
                   version,expected_revision,phase,attempts,created_at,updated_at,result)
               VALUES (?,?,?,?,?,?,?,1,?,?,NULL)''',
            (operation_id, rollout_id, device, command, version,
             expected_revision, phase, ts, ts),
        )
    elif row['phase'] in in_flight:
        # Keep the in-flight recovery phase; only refresh bookkeeping.
        db.execute(
            'UPDATE firmware_intent SET expected_revision=?,updated_at=?,attempts=attempts+1 WHERE operation_id=?',
            (expected_revision, ts, operation_id),
        )
    else:
        db.execute(
            'UPDATE firmware_intent SET phase=?,expected_revision=?,updated_at=?,attempts=attempts+1 WHERE operation_id=?',
            (phase, expected_revision, ts, operation_id),
        )
    return operation_id


def _drive(db_path, rollout_id, case_id, device, command, version, expected_revision):
    """Drive one durable intent to completion.

    Returns ``(status, response, reconciliation)`` where ``status`` is one of
    ``applied``, ``rejected``, ``revision_conflict`` or ``not_delivered`` and
    ``response`` is the verbatim endpoint/state evidence. A
    commit-then-ack-lost endpoint is always reconciled here, via the stable
    operation id and the field state query, before the function returns.
    """
    dispatch_phase = INSTALL_DISPATCHED if command == 'install' else ROLLBACK_DISPATCHED
    intended_phase = ROLLBACK_INTENDED if command == 'rollback' else INSTALL_INTENDED
    ack_lost_phase = INSTALL_ACK_LOST if command == 'install' else ROLLBACK_ACK_LOST
    reconciled_phase = INSTALL_RECONCILED if command == 'install' else ROLLBACK_RECONCILED
    in_flight_phases = (
        INSTALL_DISPATCHED, INSTALL_ACK_LOST,
        ROLLBACK_DISPATCHED, ROLLBACK_ACK_LOST,
    )

    db = connect(db_path)
    try:
        with db:
            operation_id = _upsert_intent(db, rollout_id, device, command,
                                          version, expected_revision, intended_phase)
        # Reconciliation of a request whose acknowledgement was lost on an
        # earlier process life, or whose process died while the request was in
        # flight: replay the same operation id first, never send a new one.
        prior = db.execute(
            'SELECT phase FROM firmware_intent WHERE operation_id=?',
            (operation_id,),
        ).fetchone()
        need_reconcile = prior['phase'] in in_flight_phases
    finally:
        db.close()

    _crash('after_intent_persisted', {'device': device, 'command': command,
                                      'operation_id': operation_id})

    reconciliation = []
    response = None
    if not _reachable(device):
        # The request cannot leave the handheld: nothing was delivered and the
        # field revision/version are untouched. The recovery phase already on
        # the journal is preserved (only the timestamp moves) so a later retry
        # still knows whether the request had ever been dispatched.
        db = connect(db_path)
        try:
            with db:
                db.execute(
                    'UPDATE firmware_intent SET updated_at=? WHERE operation_id=?',
                    (_now(), operation_id),
                )
        finally:
            db.close()
        return ('not_delivered',
                {'status': 'not_delivered', 'reason': 'terminal_unreachable'},
                reconciliation)

    if need_reconcile:
        replay = tf.invoke(db_path, case_id, device, command,
                           version, operation_id, expected_revision)
        reconciliation.append({'method': 'replay_operation_id', 'response': replay})
        if replay['status'] == 'revision_conflict':
            # Stale expected revision: consult the field before deciding.
            field = tf.state(db_path, device)
            reconciliation.append({'method': 'field_state', 'response': field})
            response = replay
            status = 'revision_conflict'
        else:
            # Covers both the normal replay envelope and a synchronous result
            # the endpoint returned on a journal that had lost its reply.
            response = replay['record'] if replay['status'] == 'replayed' else replay
            status = response['status']
    else:
        db = connect(db_path)
        try:
            with db:
                db.execute(
                    'UPDATE firmware_intent SET phase=?,updated_at=? WHERE operation_id=?',
                    (dispatch_phase, _now(), operation_id),
                )
        finally:
            db.close()
        _crash('after_dispatch_recorded', {'device': device, 'command': command,
                                           'operation_id': operation_id})
        try:
            raw = tf.invoke(db_path, case_id, device, command,
                            version, operation_id, expected_revision)
        except SimulatedCrash:
            # The process died (or the link dropped) after the request was
            # dispatched. The endpoint may already have committed, so persist
            # the in-flight phase and let the next process life reconcile via
            # the same operation id instead of issuing a second request.
            db = connect(db_path)
            try:
                with db:
                    db.execute(
                        'UPDATE firmware_intent SET phase=?,updated_at=? WHERE operation_id=?',
                        (dispatch_phase, _now(), operation_id),
                    )
            finally:
                db.close()
            raise
        if raw is None:
            raise RuntimeError('endpoint_returned_no_response')
        if raw['status'] == 'ack_lost':
            # The endpoint committed the change but the reply never came back.
            # Mark the loss durably, then reconcile before any further action.
            db = connect(db_path)
            try:
                with db:
                    db.execute(
                        'UPDATE firmware_intent SET phase=?,updated_at=? WHERE operation_id=?',
                        (ack_lost_phase, _now(), operation_id),
                    )
            finally:
                db.close()
            _crash('after_ack_lost_recorded', {'device': device, 'command': command,
                                               'operation_id': operation_id})
            replay = tf.invoke(db_path, case_id, device, command,
                               version, operation_id, expected_revision)
            reconciliation.append({'method': 'replay_operation_id', 'response': replay})
            field = tf.state(db_path, device)
            reconciliation.append({'method': 'field_state', 'response': field})
            if replay['status'] == 'replayed':
                response = replay['record']
                status = replay['record']['status']
            else:  # revision drift between commit and reconciliation
                response = replay
                status = replay['status']
        else:
            response = raw
            status = raw['status']
    # Final normalization: ack_lost is a transport symptom, never a terminal
    # outcome. The durable operation table holds the real committed result.
    if isinstance(response, dict) and response.get('status') == 'ack_lost':
        db = connect(db_path)
        try:
            stored = db.execute(
                'SELECT result FROM terminal_firmware_operation WHERE id=?',
                (operation_id,),
            ).fetchone()
        finally:
            db.close()
        if stored is not None:
            response = json.loads(stored['result'])
            status = response['status']
            if not any(step.get('method') == 'field_state' for step in reconciliation):
                reconciliation.append(
                    {'method': 'field_state', 'response': tf.state(db_path, device)})

    db = connect(db_path)
    try:
        with db:
            db.execute(
                'UPDATE firmware_intent SET phase=?,result=?,updated_at=? WHERE operation_id=?',
                (reconciled_phase,
                 json.dumps(response, sort_keys=True, ensure_ascii=False),
                 _now(), operation_id),
            )
    finally:
        db.close()
    return status, response, reconciliation


def run_rollout(db_path, rollout_id, case_id, contract):
    """Idempotently execute (or resume) one canary rollout under ``contract``.

    Re-running with the same ``rollout_id`` never installs twice, never skips a
    terminal that already committed, and never silently drops an unknown
    terminal. The returned structure, also persisted, drives the JSON report.
    """
    if contract not in CONTRACTS:
        raise ValueError('bad_rollout_contract')

    db = connect(db_path)
    try:
        existing = db.execute(
            'SELECT status FROM firmware_rollout WHERE id=?', (rollout_id,)
        ).fetchone()
    finally:
        db.close()
    _FINAL_STATES = ('completed', 'manual_handling_required',
                     'half_upgraded_cell_recovered')
    if existing and existing['status'] in _FINAL_STATES:
        # A finished rollout is immutable; rerun only re-reads real field state
        # and refreshes the report instead of issuing new commands.
        return _refresh_report(db_path, rollout_id)

    pre = preflight(db_path, case_id)
    target = pre['target_version']

    db = connect(db_path)
    try:
        with db:
            ts = _now()
            db.execute(
                '''INSERT OR IGNORE INTO firmware_rollout(id,case_id,contract,
                       target_version,status,created_at,updated_at,detail)
                   VALUES (?,?,?,?,?,?,?,?)''',
                (rollout_id, case_id, contract, target, 'running', ts, ts, '{}'),
            )
            db.execute(
                'UPDATE firmware_rollout SET updated_at=? WHERE id=?', (ts, rollout_id)
            )
            for item in pre['devices']:
                db.execute(
                    '''INSERT OR IGNORE INTO firmware_rollout_device(rollout_id,device,
                           station,cell,pulse_source_sha256,baseline_version,target_version,
                           actual_version,actual_revision,install_status,install_operation_id,
                           install_response,reconciliation,rollback_status,rollback_operation_id,
                           rollback_response,manual_reason,updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (rollout_id, item['device'], item['station'], item['cell'],
                     item['pulse_source_sha256'], item['case_baseline_version'], target,
                     None, None, 'pending', None, None, None, None, None, None, None, ts),
                )
    finally:
        db.close()

    # ---- install sweep -----------------------------------------------------
    for item in pre['devices']:
        device = item['device']
        db = connect(db_path)
        try:
            rd = db.execute(
                'SELECT * FROM firmware_rollout_device WHERE rollout_id=? AND device=?',
                (rollout_id, device),
            ).fetchone()
            intent = db.execute(
                'SELECT phase,operation_id FROM firmware_intent WHERE rollout_id=? AND device=? AND command=?',
                (rollout_id, device, 'install'),
            ).fetchone()
        finally:
            db.close()
        # Settled outcomes from a previous process life. ``not_delivered`` is
        # deliberately absent: nothing reached the terminal, so a later run
        # after the link is restored must retry with the same operation id.
        if rd['install_status'] in ('applied', 'rejected', 'revision_conflict',
                                    'already_target', 'drift_manual'):
            continue

        manual = []
        if item['observed_pulse_source_sha256'] != item['pulse_source_sha256']:
            manual.append('pulse_source_mismatch')
        if item['registered'] is None:
            manual.append('unknown_device')
        if manual:
            _set_device(db_path, rollout_id, device,
                        install_status='drift_manual',
                        manual_reason=';'.join(manual))
            continue

        expected_revision = item['registered']['revision']
        current_version = item['registered']['version']
        in_flight = intent is not None and intent['phase'] in (
            INSTALL_DISPATCHED, INSTALL_ACK_LOST)
        if current_version == target and not in_flight:
            # Another maintainer (or a prior life) already put this terminal on
            # the target and this rollout has no in-flight request of its own;
            # record the real field version, do not reinstall.
            field = tf.state(db_path, device) if _reachable(device) else item['registered']
            _set_device(db_path, rollout_id, device,
                        actual_version=field['version'], actual_revision=field['revision'],
                        install_status='already_target',
                        install_response=json.dumps(
                            {'status': 'already_target', 'field': field},
                            sort_keys=True, ensure_ascii=False))
            continue

        status, response, reconciliation = _drive(
            db_path, rollout_id, case_id, device, 'install', target, expected_revision)
        # Merge rather than overwrite: a reconciliation performed inside a
        # resumed _drive (replay after process restart) must remain auditable.
        prior_recon = json.loads(rd['reconciliation']) if rd['reconciliation'] else []
        updates = {
            'install_status': status,
            'install_operation_id': _op_id(rollout_id, device, 'install', 1),
            'install_response': json.dumps(response, sort_keys=True, ensure_ascii=False),
            'reconciliation': json.dumps(prior_recon + reconciliation,
                                         sort_keys=True, ensure_ascii=False),
        }
        if status == 'applied':
            field = tf.state(db_path, device)
            updates['actual_version'] = field['version']
            updates['actual_revision'] = field['revision']
        elif status == 'revision_conflict':
            field = tf.state(db_path, device)
            updates['actual_version'] = field['version']
            updates['actual_revision'] = field['revision']
            updates['manual_reason'] = 'install_revision_conflict'
        _set_device(db_path, rollout_id, device, **updates)

    # ---- contract evaluation ----------------------------------------------
    # Recomputed on every (re)run so a device that was undelivered in an earlier
    # life and later reaches the endpoint can change the compensation picture.
    plan = _compensation_plan(db_path, rollout_id, contract)
    db = connect(db_path)
    try:
        with db:
            # Cells that were ever left half-upgraded are kept durably, unioned
            # across runs, even after a compensating rollback restores uniform
            # baseline versions, so the report never announces them as cleanly
            # upgraded.
            previous = json.loads(
                db.execute('SELECT half_upgraded_cells FROM firmware_rollout WHERE id=?',
                           (rollout_id,)).fetchone()['half_upgraded_cells'] or '[]')
            merged_cells = sorted(set(previous) | set(plan['half_upgraded_cells']))
            db.execute(
                'UPDATE firmware_rollout SET half_upgraded_cells=?,updated_at=? WHERE id=?',
                (json.dumps(merged_cells, sort_keys=True), _now(), rollout_id),
            )
    finally:
        db.close()

    # ---- compensating rollback sweep --------------------------------------
    for device in plan['rollback']:
        db = connect(db_path)
        try:
            rd = db.execute(
                'SELECT * FROM firmware_rollout_device WHERE rollout_id=? AND device=?',
                (rollout_id, device),
            ).fetchone()
        finally:
            db.close()
        # revision_conflict means another maintainer changed the terminal: the
        # override is permanently stopped for this rollout and flagged manual.
        # not_delivered stays retriable and is deliberately not skipped.
        if rd['rollback_status'] in ('applied', 'revision_conflict'):
            continue
        baseline = rd['baseline_version']
        # The rollback compares against the revision this rollout produced; the
        # request's target version is the case baseline, never the live field
        # version. A different field revision means another maintainer acted.
        expected_revision = rd['actual_revision']
        status, response, reconciliation = _drive(
            db_path, rollout_id, case_id, device, 'rollback', baseline, expected_revision)
        prior_recon = json.loads(rd['reconciliation']) if rd['reconciliation'] else []
        updates = {
            'rollback_status': status,
            'rollback_operation_id': _op_id(rollout_id, device, 'rollback', 1),
            'rollback_response': json.dumps(response, sort_keys=True, ensure_ascii=False),
            'reconciliation': json.dumps(
                prior_recon + [{'rollback': reconciliation}],
                sort_keys=True, ensure_ascii=False),
        }
        if status == 'applied':
            field = tf.state(db_path, device)
            updates['actual_version'] = field['version']
            updates['actual_revision'] = field['revision']
            # A delivered rollback clears an earlier not-delivered marker.
            if rd['manual_reason'] == 'rollback_not_delivered':
                updates['manual_reason'] = None
        elif status == 'revision_conflict':
            field = tf.state(db_path, device)
            updates['actual_version'] = field['version']
            updates['actual_revision'] = field['revision']
            updates['manual_reason'] = (
                (rd['manual_reason'] + ';' if rd['manual_reason'] else '')
                + 'rollback_revision_conflict')
        elif status == 'not_delivered':
            updates['manual_reason'] = (
                (rd['manual_reason'] + ';' if rd['manual_reason'] else '')
                + 'rollback_not_delivered')
        _set_device(db_path, rollout_id, device, **updates)

    return _refresh_report(db_path, rollout_id)


def _set_device(db_path, rollout_id, device, **changes):
    if not changes:
        return
    changes['updated_at'] = _now()
    cols = ', '.join(f'{k}=?' for k in changes)
    values = list(changes.values()) + [rollout_id, device]
    db = connect(db_path)
    try:
        with db:
            db.execute(
                f'UPDATE firmware_rollout_device SET {cols} '
                'WHERE rollout_id=? AND device=?',
                values,
            )
    finally:
        db.close()


def _compensation_plan(db_path, rollout_id, contract):
    """Decide which upgraded terminals must be rolled back under the contract.

    Only terminals this rollout actually installed (``applied``) become
    rollback candidates; rejected, undelivered and manual terminals are never
    "upgraded" and therefore never need compensation.
    """
    db = connect(db_path)
    try:
        rows = [dict(r) for r in db.execute(
            'SELECT device,cell,install_status FROM firmware_rollout_device WHERE rollout_id=?',
            (rollout_id,))]
    finally:
        db.close()
    installed = {r['device'] for r in rows if r['install_status'] == 'applied'}
    # Only definitive failures block the contract. A terminal that was never
    # reached keeps status not_delivered and stays retriable on the next run;
    # it must not trigger compensation for peers that were merely waiting.
    blocked = {r['device'] for r in rows
               if r['install_status'] in ('rejected', 'revision_conflict', 'drift_manual')}
    undelivered = {r['device'] for r in rows if r['install_status'] == 'not_delivered'}
    rollback = []
    half_upgraded_cells = []
    if contract == 'fleet_atomic':
        if blocked:
            rollback = sorted(installed)
    else:  # cell_isolated
        cells = {}
        status_by_device = {r['device']: r['install_status'] for r in rows}
        for r in rows:
            cells.setdefault(r['cell'], set()).add(r['device'])
        for cell, members in sorted(cells.items()):
            cell_installed = {m for m in members
                              if status_by_device[m] == 'applied'}
            cell_blocked = members & blocked
            if cell_blocked and cell_installed:
                # Some peers upgraded and at least one could not: the cell is
                # genuinely half-upgraded and the upgraded peers must roll back.
                half_upgraded_cells.append(cell)
                rollback.extend(sorted(cell_installed))
            # A cell whose every device was blocked (e.g. a single rejected
            # terminal) never reached a half-upgraded state: nothing to roll
            # back and nothing to announce as completed either.
    return {'rollback': rollback,
            'half_upgraded_cells': sorted(set(half_upgraded_cells)),
            'undelivered': sorted(undelivered)}


def _refresh_report(db_path, rollout_id):
    """Re-read the *real* field versions/revisions and finalize the rollout.

    Planned/target versions are never reported as the achieved version: every
    ``actual_*`` value comes from a field state query (or the durable evidence
    recorded when the terminal was reachable).
    """
    db = connect(db_path)
    try:
        rollout = db.execute(
            'SELECT * FROM firmware_rollout WHERE id=?', (rollout_id,)
        ).fetchone()
        if not rollout:
            raise ValueError('unknown_rollout')
        rows = [dict(r) for r in db.execute(
            'SELECT * FROM firmware_rollout_device WHERE rollout_id=? ORDER BY device',
            (rollout_id,))]
    finally:
        db.close()

    terminals = []
    manual_devices = []
    cell_views = {}
    for rd in rows:
        # Trust the live endpoint when reachable; otherwise keep the last
        # evidence the endpoint itself returned. A quarantined/unknown terminal
        # has no endpoint row at all, so keep whatever durable evidence exists.
        field = None
        if _reachable(rd['device']):
            try:
                field = tf.state(db_path, rd['device'])
            except ValueError:
                field = None
        if field is not None:
            actual_version = field['version']
            actual_revision = field['revision']
            _set_device(db_path, rollout_id, rd['device'],
                        actual_version=actual_version,
                        actual_revision=actual_revision)
        else:
            actual_version = rd['actual_version']
            actual_revision = rd['actual_revision']
        if rd['manual_reason']:
            manual_devices.append({'device': rd['device'], 'reason': rd['manual_reason']})
        cell_views.setdefault(rd['cell'], []).append(rd['device'])
        terminals.append({
            'device': rd['device'],
            'station': rd['station'],
            'cell': rd['cell'],
            'pulse_source_sha256': rd['pulse_source_sha256'],
            'baseline_version': rd['baseline_version'],
            'target_version': rd['target_version'],
            'actual_version': actual_version,
            'actual_revision': actual_revision,
            'install_operation_id': rd['install_operation_id'],
            'request_response': json.loads(rd['install_response']) if rd['install_response'] else None,
            'reconciliation': json.loads(rd['reconciliation']) if rd['reconciliation'] else [],
            'rollback_operation_id': rd['rollback_operation_id'],
            'rollback_response': json.loads(rd['rollback_response']) if rd['rollback_response'] else None,
            'rollback_status': rd['rollback_status'],
            'install_status': rd['install_status'],
            'manual_handling': rd['manual_reason'],
        })

    contract = rollout['contract']
    target = rollout['target_version']
    ever_half_cells = json.loads(rollout['half_upgraded_cells'] or '[]')
    cell_status = {}
    half_cells = []
    for cell, members in sorted(cell_views.items()):
        versions = {next(t['actual_version'] for t in terminals if t['device'] == m)
                    for m in members}
        on_target = {m for m in members
                     if next(t for t in terminals if t['device'] == m)['actual_version'] == target}
        if contract == 'cell_isolated' and (on_target and on_target != set(members)
                                            or cell in ever_half_cells):
            # A half-upgraded cell must never be declared complete — neither
            # while it is mixed nor after compensation restored the baseline.
            cell_status[cell] = 'half_upgraded_not_complete'
            half_cells.append(cell)
        elif versions == {target}:
            cell_status[cell] = 'on_target'
        else:
            cell_status[cell] = 'baseline_or_mixed'
    half_cells = sorted(set(half_cells))

    if contract == 'fleet_atomic':
        all_on_target = all(t['actual_version'] == target for t in terminals)
        contract_outcome = 'fleet_uniform_target' if all_on_target else 'fleet_failed_safely_restored'
    else:
        contract_outcome = 'cell_isolated_resolved'

    if manual_devices:
        status = 'manual_handling_required'
    elif any(t['install_status'] == 'not_delivered'
             or t['rollback_status'] == 'not_delivered' for t in terminals):
        # At least one terminal never received its request; the rollout is not
        # finished and must be resumed when the local link is back.
        status = 'recovery_pending_unreachable'
    elif half_cells:
        # The cell's partial upgrade was compensated, but the cell itself was
        # not completed; surface that instead of a plain "completed".
        status = 'half_upgraded_cell_recovered'
    else:
        status = 'completed'

    report = {
        'rollout_id': rollout_id,
        'case_id': rollout['case_id'],
        'contract': contract,
        'target_version': target,
        'status': status,
        'contract_outcome': contract_outcome,
        'half_upgraded_cells': half_cells,
        'cell_status': cell_status,
        'manual_handling': manual_devices,
        'terminals': terminals,
        'generated_at': _now(),
    }
    db = connect(db_path)
    try:
        with db:
            db.execute(
                'UPDATE firmware_rollout SET status=?,updated_at=?,detail=? WHERE id=?',
                (status, _now(),
                 json.dumps(report, sort_keys=True, ensure_ascii=False), rollout_id),
            )
    finally:
        db.close()
    return report


def export_rollout_report(db_path, rollout_id, out_path):
    """Write the auditable JSON field-evidence report and a Markdown summary."""
    from pathlib import Path
    report = _refresh_report(db_path, rollout_id)
    target = Path(out_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    lines = [
        f"# 固件灰度升级运维报告 — {report['rollout_id']} ({report['contract']})",
        '',
        f"状态: {report['status']}；合约结论: {report['contract_outcome']}；目标版本: {report['target_version']}",
        '',
        '| 终端 | 站点 | 单元 | 脉冲来源摘要 | 旧版 | 目标版 | 实版 | revision | 请求应答 | 复核 | 回退 | 人工处理 |',
        '| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |',
    ]
    for t in report['terminals']:
        digest = (t['pulse_source_sha256'] or '')[:12]
        recon = _recon_methods(t['reconciliation'])
        lines.append(
            f"| {t['device']} | {t['station']} | {t['cell']} | {digest} | "
            f"{t['baseline_version']} | {t['target_version']} | {t['actual_version']} | "
            f"{t['actual_revision']} | {t['install_status']} | {recon or '—'} | "
            f"{t['rollback_status'] or '—'} | {t['manual_handling'] or '—'} |"
        )
    if report['manual_handling']:
        lines += ['', '## 待人工处理', '']
        lines += [f"- {m['device']}: {m['reason']}" for m in report['manual_handling']]
    target.with_suffix('.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    return target


def _recon_methods(reconciliation):
    methods = []
    for entry in reconciliation:
        if isinstance(entry, dict) and 'method' in entry:
            methods.append(entry['method'])
        elif isinstance(entry, dict) and 'rollback' in entry:
            for sub in entry['rollback']:
                methods.extend(_recon_methods([sub]))
    return ';'.join(methods)
