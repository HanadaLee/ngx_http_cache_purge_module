#!/usr/bin/env python3
"""Verify application index recovery using fresh processes and abrupt exits."""
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
root = Path(tempfile.mkdtemp(prefix='cache-purge-index-process-', dir='/tmp'))
checks = []
for mode in ('--crash-prepare', '--crash-commit', '--recover'):
    checks.append(subprocess.check_output([str(a.binary), mode, str(root)], text=True, timeout=30).strip())
publication = Path(tempfile.mkdtemp(prefix='cache-purge-publication-process-', dir='/tmp'))
for mode in ('--crash-unpublished', '--recover-unpublished'):
    checks.append(subprocess.check_output([str(a.binary), mode, str(publication)], text=True, timeout=30).strip())
native = []
for truncate in (False, True):
    directory = Path(tempfile.mkdtemp(prefix='cache-purge-native-wal-', dir='/tmp'))
    written = subprocess.check_output([str(a.binary), '--crash-native-deferred', str(directory)],
                                      text=True, timeout=30)
    boundaries = [line.split() for line in written.splitlines() if line.startswith('WAL ')]
    assert boundaries
    for _, name, size in boundaries:
        wal = Path(name).resolve()
        assert wal.is_relative_to(directory.resolve()) and wal.suffix == '.log'
        assert wal.stat().st_size > int(size)
        if truncate:
            os.truncate(wal, int(size))
    mode = '--recover-native-lost' if truncate else '--recover-native-deferred'
    checks.append(subprocess.check_output([str(a.binary), mode, str(directory)],
                                         text=True, timeout=30).strip())
    native.append({'root': str(directory), 'truncated': truncate})
result = {'checks': checks, 'database': str(root),
          'publication_database': str(publication),
          'native_deferred': native,
          'scope': 'application index WAL recovery; no filesystem or nginx cache-hook simulation'}
a.output.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result), flush=True)
