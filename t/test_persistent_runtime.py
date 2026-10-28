"""Ensure orchestration failures cannot become successful verification reports."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
TOOL = '''#!/usr/bin/env python3
import os, pathlib, sys
args = sys.argv[1:]
stage = 'backend' if pathlib.Path(sys.argv[0]).name == 'ctest' else 'build' if '--build' in args else 'configure'
if os.environ.get('FAKE_FAILURE') == stage: sys.exit(23)
if stage == 'backend':
    content = '<testsuite tests="1" failures="0"><testcase name="fixture"/></testsuite>'
    if os.environ.get('FAKE_EMPTY'): content = '<testsuite tests="0" failures="0"/>'
    pathlib.Path(args[args.index('--output-junit') + 1]).write_text(content)
'''


class VerificationRunner(unittest.TestCase):
    def invoke(self, failure='', empty=False, stale=False):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            tools = root / 'bin'
            tools.mkdir()
            for command in ('cmake', 'ctest'):
                path = tools / command
                path.write_text(TOOL)
                path.chmod(0o755)
            reports = root / 'reports'
            if stale:
                reports.mkdir()
                (reports / 'sentinel').write_text('previous result')
            env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ['PATH'],
                       FAKE_FAILURE=failure, FAKE_EMPTY='1' if empty else '')
            result = subprocess.run([sys.executable, str(ROOT / 't/persistent_runtime.py'),
                '--backend-only', '--reports', str(reports), '--build', str(root / 'build')],
                env=env, capture_output=True, text=True)
            state = reports / 'run.json'
            saved = json.loads(state.read_text()) if state.exists() else None
            if saved is not None:
                self.assertEqual((reports / 'failure.xml').exists(), saved['status'] == 'failed')
            sentinel = (reports / 'sentinel').read_text() if stale else None
            return result, saved, sentinel

    def test_each_command_failure_stops_the_run(self):
        order = ['configure', 'build', 'backend']
        for stage in order:
            with self.subTest(stage=stage):
                result, saved, _ = self.invoke(failure=stage)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(saved['status'], 'failed')
                self.assertEqual([x['name'] for x in saved['stages']], order[:order.index(stage) + 1])
                self.assertEqual(saved['stages'][-1]['returncode'], 23)

    def test_empty_backend_report_is_not_success(self):
        result, saved, _ = self.invoke(empty=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(saved['status'], 'failed')
        self.assertIn('empty', saved['error'])

    def test_stale_reports_are_preserved_and_rejected(self):
        result, saved, sentinel = self.invoke(stale=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIsNone(saved)
        self.assertEqual(sentinel, 'previous result')

    def test_success_requires_all_stages_and_a_nonempty_report(self):
        result, saved, _ = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(saved['status'], 'passed')
        self.assertEqual(len(saved['stages']), 3)


if __name__ == '__main__':
    unittest.main()
