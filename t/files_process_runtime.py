"""Recover the disk marker + index boundary using independent exec processes."""
import argparse
import json
import subprocess
import tempfile
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--binary', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
checks = []
roots = []
for cut in ('prepared', 'rename', 'index', 'marker', 'missing'):
    root = Path(tempfile.mkdtemp(prefix='cache-purge-file-publication-', dir='/tmp'))
    roots.append(str(root))
    mode = '--crash-' + ('index' if cut == 'missing' else cut)
    checks.append(subprocess.check_output([str(a.binary), mode, str(root)], text=True, timeout=30).strip())
    if cut == 'missing':
        (root / 'cache/file').unlink()
    unrelated = root / 'cache/unrelated'
    unrelated.write_bytes(b'untouched')
    for _ in range(2):
        checks.append(subprocess.check_output([str(a.binary), '--recover', str(root)], text=True, timeout=30).strip())
    assert unrelated.read_bytes() == b'untouched'
a.output.write_text(json.dumps({'checks': checks, 'directories': roots,
    'scope': 'Real PREPARE/FINISH, xattrs, rename and durable index publication recovery; not nginx integration or power-loss testing'}, indent=2) + '\n')
print('PASS file/index publication crash cuts and repeated recovery', flush=True)
