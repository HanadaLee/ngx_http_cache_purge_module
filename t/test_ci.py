"""Check workflow selection and failure handling without remote writes."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[1]


class WorkflowContract(unittest.TestCase):
    def test_source_tags_run_checks_without_publishing_packages(self):
        workflow = (ROOT / '.github/workflows/build-test.yml').read_text()
        self.assertIn("tags: ['5.*']", workflow)
        self.assertIn("startsWith(github.ref, 'refs/tags/')", workflow)
        self.assertNotIn('release-modules:', workflow)
        self.assertNotIn('create-release:', workflow)
        self.assertNotIn('contents: write', workflow)
        self.assertNotIn('gh release', workflow)

    def test_both_cores_have_static_dynamic_and_sanitized_checks(self):
        for full in ('false', 'true'):
            with self.subTest(full=full), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                output = root / 'outputs'
                env = dict(os.environ, CI_FULL=full, CI_BASE='',
                           GITHUB_OUTPUT=str(output),
                           GITHUB_STEP_SUMMARY=str(root / 'summary'))
                subprocess.run(['perl', str(ROOT / 't/ci_matrix.pl')],
                               env=env, check=True, capture_output=True)
                values = dict(line.split('=', 1) for line in output.read_text().splitlines())
                matrix = json.loads(values['integration'])['include']
                self.assertEqual(values['run'], 'true')
                for core in ('nginx', 'nginx-ext'):
                    entries = [entry for entry in matrix if entry['core'] == core
                               and entry['nginx'] == '1.31.8']
                    expected = {(False, False, 'gcc'), (True, False, 'gcc'),
                                (False, True, 'gcc')}
                    if full == 'true':
                        expected.add((True, False, 'clang'))
                    self.assertEqual({(e['dynamic'] == 'true', e['sanitize'] == 'true',
                                       e['cc']) for e in entries}, expected)


class ValgrindGate(unittest.TestCase):
    def test_selected_valgrind_cannot_be_skipped(self):
        workflow = (ROOT / '.github/workflows/build-test.yml').read_text()
        gate = workflow.split("python3 - <<'PYTHON'\n", 1)[1].split('          PYTHON', 1)[0]
        gate = textwrap.dedent(gate)
        for event, full, run, memory, expected in (
            ('pull_request', False, True, 'success', 0),
            ('pull_request', False, True, 'skipped', 1),
            ('push', False, True, 'skipped', 0),
            ('push', True, True, 'skipped', 1),
            ('pull_request', False, False, 'skipped', 0),
            ('pull_request', False, True, 'failure', 1),
        ):
            with self.subTest(event=event, full=full, run=run, memory=memory):
                jobs = {
                    'prepare': {'result': 'success', 'outputs': {
                        'run': str(run).lower(), 'full': str(full).lower()}},
                    'memory-leak-test': {'result': memory},
                    'test-matrix': {'result': 'success' if run else 'skipped'},
                }
                with tempfile.NamedTemporaryFile() as summary:
                    env = dict(os.environ, RESULTS=json.dumps(jobs), EVENT_NAME=event,
                               GITHUB_STEP_SUMMARY=summary.name)
                    result = subprocess.run(['python3', '-c', gate], env=env,
                                            capture_output=True, text=True)
                self.assertEqual(result.returncode, expected, result.stderr)
