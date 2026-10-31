#!/usr/bin/env python3
"""Load the delivered modules and retain only deterministic package evidence."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--nginx', type=Path, required=True)
parser.add_argument('--modules', type=Path, required=True)
parser.add_argument('--root', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
details = args.root.with_suffix('.json')
subprocess.run([sys.executable, str(Path(__file__).with_name('native_propagation_runtime.py')),
                '--binary', str(args.nginx), '--modules', str(args.modules),
                '--root', str(args.root), '--output', str(details)], check=True)
report = json.loads(details.read_text())
assert report['status'] == 'passed' and report['checks'] and len(report['modules']) == 2
args.output.write_text(json.dumps({'status': 'passed', 'checks': report['checks'],
                                 'modules': report['modules']}, indent=2) + '\n')
print('PASS', len(report['checks']), 'packaged module checks', flush=True)
