#!/usr/bin/env python3
"""Durable operation recovery in a fresh exec, without graceful DB teardown."""
import argparse
import json
import os
import subprocess
import tempfile
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--binary', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
root = Path(tempfile.mkdtemp(prefix='ncp-operations-process-', dir='/tmp'))
checks = [subprocess.check_output([str(a.binary), mode, str(root)], text=True, timeout=30).strip()
          for mode in ('--crash-accept', '--recover')]
rollback = []
for phase in range(3):
    directory = Path(tempfile.mkdtemp(prefix='ncp-checkpoint-rollback-', dir='/tmp'))
    written = subprocess.check_output([str(a.binary), '--crash-progress', str(directory), str(phase)],
                                      text=True, timeout=30)
    boundaries = [line.split() for line in written.splitlines() if line.startswith('WAL ')]
    assert boundaries
    for _, name, size in boundaries:
        wal = Path(name).resolve()
        assert wal.is_relative_to(directory.resolve()) and wal.suffix == '.log'
        assert wal.stat().st_size > int(size)
        os.truncate(wal, int(size))
    checks.append(subprocess.check_output([str(a.binary), '--recover-progress', str(directory), str(phase)],
                                         text=True, timeout=30).strip())
    rollback.append({'phase': phase, 'root': str(directory)})
result = {'checks': checks, 'root': str(root),
          'wal_tail_rollback': rollback,
          'scope': 'durable operation state only; no claim of live FENCE publication or physical file completion'}
a.output.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result), flush=True)
