#!/usr/bin/env python3
"""Exercise ownership and acknowledged durability across separate execs."""
import argparse
import json
import subprocess
import tempfile
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--binary', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
root = Path(tempfile.mkdtemp(prefix='cache-purge-store-process-', dir='/tmp'))
regular = subprocess.check_output([str(a.binary)], text=True).strip()
owner = subprocess.Popen([str(a.binary), '--hold', str(root)], stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
try:
    assert owner.stdout.readline().strip() == 'READY'
    subprocess.run([str(a.binary), '--locked', str(root)], check=True)
finally:
    stdout, stderr = owner.communicate('\n', timeout=10)
assert owner.returncode == 0, stderr
subprocess.run([str(a.binary), '--crash-write', str(root)], check=True)
subprocess.run([str(a.binary), '--recover', str(root)], check=True)
result = {'functional': regular, 'separate_exec_owner_lock': 'passed',
          'sync_cross_cf_batch_after_abrupt_exit': 'passed', 'database': str(root),
          'scope': 'storage API only; does not prove filesystem/index transactions or nginx integration'}
a.output.parent.mkdir(parents=True, exist_ok=True)
a.output.write_text(json.dumps(result, indent=2))
print('PASS', json.dumps(result), flush=True)
