from pathlib import Path

import pytest

from rainstation.ingest import freeze_config, import_file
from rainstation.rules import evaluate
from rainstation.terminal_firmware import invoke, register, state


FIX = Path(__file__).resolve().parent.parent / 'fixtures'


def prepared(tmp_path):
    db = tmp_path / 'rain.db'
    freeze_config(db, FIX / 'firmware_config.json')
    assert import_file(db, FIX / 'firmware_events.jsonl')['imported'] == 9
    assert set(evaluate(db).values()) == {'trusted'}
    assert register(db, 'upgrade-1', FIX / 'firmware_case.json')['devices'] == 3
    return db


def test_firmware_endpoint_failure_and_recovery_primitives(tmp_path):
    db = prepared(tmp_path)
    assert invoke(db, 'upgrade-1', 'D1', 'install', '2.0', 'op-1', 0)['status'] == 'applied'
    assert invoke(db, 'upgrade-1', 'D2', 'install', '2.0', 'op-2', 0)['status'] == 'ack_lost'
    assert state(db, 'D2')['version'] == '2.0'
    assert invoke(db, 'upgrade-1', 'D2', 'install', '2.0', 'op-2', 0)['status'] == 'replayed'
    assert invoke(db, 'upgrade-1', 'D3', 'install', '2.0', 'op-3', 0)['status'] == 'rejected'
    assert invoke(db, 'upgrade-1', 'D1', 'rollback', '1.0', 'op-4', 1)['status'] == 'applied'
    assert invoke(db, 'upgrade-1', 'D2', 'rollback', '1.0', 'op-5', 1)['status'] == 'applied'
    assert [state(db, x)['version'] for x in ('D1', 'D2', 'D3')] == ['1.0', '1.0', '1.0']


def test_firmware_requires_real_pulse_source(tmp_path):
    db = tmp_path / 'rain.db'
    freeze_config(db, FIX / 'firmware_config.json')
    with pytest.raises(ValueError, match='firmware_pulse_source_missing'):
        register(db, 'upgrade-1', FIX / 'firmware_case.json')
