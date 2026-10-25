#!/usr/bin/env python3
"""Durable operation recovery in a fresh exec, without graceful DB teardown."""
import argparse
import json
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
result = {'checks': checks, 'root': str(root),
          'scope': 'durable operation state only; no claim of live FENCE publication or physical file completion'}
a.output.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result), flush=True)
