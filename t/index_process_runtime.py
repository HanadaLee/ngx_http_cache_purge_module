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
catalog = Path(tempfile.mkdtemp(prefix='cache-purge-catalog-upgrade-', dir='/tmp'))
checks.append(subprocess.check_output([str(a.binary), '--catalog-legacy', str(catalog)], text=True, timeout=30).strip())
cut = subprocess.run([str(a.binary), '--catalog-cut', str(catalog)], timeout=30)
assert cut.returncode == 86, cut.returncode
checks.append(subprocess.check_output([str(a.binary), '--catalog-finish', str(catalog)], text=True, timeout=30).strip())
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
collection = []
for truncate in (False, True):
    directory = Path(tempfile.mkdtemp(prefix='cache-purge-collection-wal-', dir='/tmp'))
    written = subprocess.check_output([str(a.binary), '--crash-collect-deferred', str(directory)],
                                      text=True, timeout=30)
    boundaries = [line.split() for line in written.splitlines() if line.startswith('WAL ')]
    assert boundaries
    for _, name, size in boundaries:
        wal = Path(name).resolve()
        assert wal.is_relative_to(directory.resolve()) and wal.suffix == '.log'
        assert wal.stat().st_size > int(size)
        if truncate:
            os.truncate(wal, int(size))
    mode = '--recover-collect-lost' if truncate else '--recover-collect-deferred'
    checks.append(subprocess.check_output([str(a.binary), mode, str(directory)],
                                         text=True, timeout=30).strip())
    collection.append({'root': str(directory), 'truncated': truncate})
retention = []
for barrier in ('none', 'retire', 'validate'):
    for truncate in (False, True):
        directory = Path(tempfile.mkdtemp(prefix='cache-purge-retention-wal-', dir='/tmp'))
        written = subprocess.check_output([str(a.binary), '--crash-retention', str(directory), barrier],
                                          text=True, timeout=30)
        boundaries = [line.split() for line in written.splitlines() if line.startswith('WAL ')]
        assert boundaries
        for _, name, size in boundaries:
            wal = Path(name).resolve()
            assert wal.is_relative_to(directory.resolve()) and wal.suffix == '.log'
            assert wal.stat().st_size > int(size)
            if truncate:
                os.truncate(wal, int(size))
        mode = '--recover-retention-lost' if truncate else '--recover-retention'
        checks.append(subprocess.check_output([str(a.binary), mode, str(directory), barrier],
                                             text=True, timeout=30).strip())
        retention.append({'root': str(directory), 'barrier': barrier, 'truncated': truncate})
result = {'checks': checks, 'database': str(root),
          'catalog_upgrade_database': str(catalog),
          'publication_database': str(publication),
          'native_deferred': native,
          'collection_deferred': collection,
          'retention_deferred': retention,
          'scope': 'application index WAL recovery; no filesystem or nginx cache-hook simulation'}
a.output.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result), flush=True)
