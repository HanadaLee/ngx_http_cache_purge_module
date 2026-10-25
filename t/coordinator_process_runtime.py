#!/usr/bin/env python3
"""Recover a durable operation that was never published to shared memory."""
import argparse
import json
import subprocess
import tempfile
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--binary', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
root = Path(tempfile.mkdtemp(prefix='ncp-coordinator-process-', dir='/tmp'))
checks = [subprocess.check_output([str(a.binary), mode, str(root)], text=True, timeout=30).strip()
          for mode in ('--crash-accept', '--recover')]
result = {'checks': checks, 'root': str(root),
          'scope': 'durable PREPARING control recovery; native cache readers/writers and physical completion are not attested'}
a.output.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result), flush=True)
