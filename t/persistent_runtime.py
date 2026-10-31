#!/usr/bin/env python3
"""Build and verify the persistent backend and real nginx module interfaces."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--nginx', type=Path, default=os.getenv('NGINX_SOURCE'))
    parser.add_argument('--nginx-ref', default='HEAD')
    parser.add_argument('--key', type=Path, default=os.getenv('CACHE_KEY_SOURCE'))
    parser.add_argument('--expr', type=Path)
    parser.add_argument('--proxy-filter', type=Path)
    parser.add_argument('--extension', type=int, choices=(0, 1), default=0)
    parser.add_argument('--build', type=Path)
    parser.add_argument('--reports', type=Path, required=True)
    parser.add_argument('--jobs', type=int, default=2)
    parser.add_argument('--dynamic', action='store_true',
                        default=os.getenv('TEST_DYNAMIC', '').lower() in ('1', 'true'))
    parser.add_argument('--sanitize', action='store_true',
                        default=os.getenv('TEST_SANITIZE', '').lower() in ('1', 'true'))
    parser.add_argument('--backend-only', action='store_true')
    args = parser.parse_args()
    if args.jobs < 1 or args.jobs > 32:
        parser.error('--jobs must be between 1 and 32')
    if not args.backend_only:
        if not args.nginx:
            parser.error('HTTP checks require an --nginx source tree')
        if os.geteuid() == 0:
            parser.error('run HTTP checks as an ordinary user, including inside test containers')
    repo = Path(__file__).resolve().parents[1]
    reports = args.reports.resolve()
    if reports.exists() and any(reports.iterdir()):
        parser.error('report directory must be empty; do not reuse stale results')
    reports.mkdir(parents=True, exist_ok=True)
    build = args.build.resolve() if args.build else Path(tempfile.mkdtemp(prefix='cache-purge-build-'))
    result = {'build': str(build), 'backend_only': args.backend_only,
              'dynamic': args.dynamic, 'sanitize': args.sanitize, 'stages': [],
              'status': 'running'}

    def save():
        (reports / 'run.json').write_text(json.dumps(result, indent=2) + '\n')

    def run(name, command):
        print('RUN', name, flush=True)
        started = time.monotonic()
        stage = {'name': name, 'command': [str(x) for x in command]}
        result['stages'].append(stage)
        save()
        with (reports / (name + '.log')).open('w') as log:
            process = subprocess.run(stage['command'], stdout=log, stderr=subprocess.STDOUT)
        stage.update(returncode=process.returncode, seconds=time.monotonic() - started)
        save()
        if process.returncode:
            print((reports / (name + '.log')).read_text()[-12000:], file=sys.stderr)
            raise RuntimeError(name + ' failed; see ' + str(reports / (name + '.log')))
        print('PASS', name, round(stage['seconds'], 2), 'seconds', flush=True)

    try:
        run('configure', ['cmake', '-S', repo, '-B', build,
            '-DCMAKE_BUILD_TYPE=RelWithDebInfo', '-DCACHE_PURGE_STORE_TESTS=ON',
            '-DCACHE_PURGE_STORE_SANITIZE=' + ('ON' if args.sanitize else 'OFF')])
        run('build', ['cmake', '--build', build, '--parallel', args.jobs])
        run('backend', ['ctest', '--test-dir', build, '--output-on-failure',
            '--output-junit', reports / 'backend.xml', '--parallel', args.jobs,
            '--no-tests=error'])
        backend = ET.parse(reports / 'backend.xml').getroot()
        if not backend.findall('testcase') or any(
                case.find(kind) is not None for case in backend.findall('testcase')
                for kind in ('failure', 'error', 'skipped')):
            raise RuntimeError('backend report is empty or contains unsuccessful tests')
        if not args.backend_only:
            def suite(name, script, arguments):
                output = reports / (name + '.json')
                run(name, [sys.executable, repo / 't' / script,
                           *arguments, '--output', output])
                report = json.loads(output.read_text())
                checks = report.get('checks', [])
                if report.get('status') != 'passed' or not checks:
                    raise RuntimeError(name + ' returned no successful checks')
                junit = ET.Element('testsuite', name=name, tests=str(len(checks)), failures='0')
                for check in checks:
                    ET.SubElement(junit, 'testcase', classname=name, name=check)
                ET.ElementTree(junit).write(reports / (name + '.xml'), encoding='utf-8', xml_declaration=True)
                return report

            flags = ['--sanitize'] if args.sanitize else []
            public_args = ['--nginx', args.nginx.resolve(), '--nginx-ref', args.nginx_ref,
                           '--backend', build, '--extension', args.extension,
                           '--public-module', '--no-thread-pool', '--slice', '--protocols', '--ssl', *flags]
            for option, path in (('--key', args.key), ('--expr', args.expr), ('--proxy-filter', args.proxy_filter)):
                if path:
                    public_args += [option, path.resolve()]
            if args.dynamic:
                public_args += ['--dynamic']
            public = suite('public', 'native_cache_runtime.py', public_args)
            nginx = Path(public['root']) / 'nginx'
            # This standalone primitive harness does not import expr headers.
            if not args.expr:
                suite('native', 'native_files_runtime.py', [
                    '--nginx', nginx, '--backend', build, '--state', *flags])
            propagation = ['--binary', nginx / 'objs/nginx']
            if args.dynamic:
                propagation += ['--modules', nginx / 'objs']
            if args.proxy_filter and args.extension:
                propagation += ['--framework']
            if args.expr:
                propagation += ['--expr']
            suite('propagation', 'native_propagation_runtime.py', propagation)
            suite('propagation-streaming', 'native_propagation_runtime.py',
                  [*propagation, '--tls', '--streaming', '--custom-headers'])
        result['status'] = 'passed'
    except Exception as error:
        result['status'] = 'failed'
        result['error'] = str(error)
        suite = ET.Element('testsuite', name='verification', tests='1', failures='1')
        case = ET.SubElement(suite, 'testcase', classname='verification', name='complete run')
        ET.SubElement(case, 'failure', message=str(error)).text = str(error)
        ET.ElementTree(suite).write(reports / 'failure.xml', encoding='utf-8', xml_declaration=True)
        raise
    finally:
        save()
    print('PASS persistent module checks:', reports, flush=True)


if __name__ == '__main__':
    main()
