#!/usr/bin/env python3
"""Verify application index recovery using fresh processes and abrupt exits."""
import argparse
import json
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
result = {'checks': checks, 'database': str(root),
          'scope': 'application index WAL recovery; no filesystem or nginx cache-hook simulation'}
a.output.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result), flush=True)
