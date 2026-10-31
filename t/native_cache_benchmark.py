#!/usr/bin/env python3
"""Compare one nginx binary with purge disabled, idle, and deleting in background."""
import argparse
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import tempfile
import threading
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--binary', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--seconds', type=int, default=5)
parser.add_argument('--rounds', type=int, default=3)
parser.add_argument('--bulk', type=int, default=8192)
parser.add_argument('--connections', type=int, default=16)
parser.add_argument('--workers', type=int, default=2)
parser.add_argument('--server-cores', type=int, default=2)
parser.add_argument('--fills', type=int, default=0)
parser.add_argument('--fill-rate', type=int, default=500)
parser.add_argument('--profile', action='store_true', help='Sample worker main threads with perf; timing is diagnostic only')
parser.add_argument('--owner-control', action='store_true', help='Also measure configured resources with the business location disabled')
parser.add_argument('--cgroup', type=Path, help='Preconfigured cgroup v1 memory directory; only nginx joins it')
args = parser.parse_args()
assert args.seconds > 0 and args.rounds >= 0 and args.bulk > 0
root = Path(tempfile.mkdtemp(prefix='cache-simple-benchmark-'))
calls = hot_calls = 0


class Origin(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global calls, hot_calls
        calls += 1
        hot_calls += self.path == '/hot'
        body = b'cached body\n' * 128
        self.send_response(200)
        self.send_header('Cache-Control', 'max-age=3600')
        self.send_header('Cache-Tag', 'benchmark')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *unused):
        pass


origin = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Origin)
threading.Thread(target=origin.serve_forever, daemon=True).start()
cpus = sorted(os.sched_getaffinity(0))
assert args.workers > 0 and 0 < args.server_cores <= len(cpus)
server_cpus = cpus[:args.server_cores]
client_cpus = cpus[args.server_cores:] or cpus
(root / 'stats.lua').write_text('''done = function(summary, latency, requests)
    io.write(string.format('RESULT {"requests":%d,"duration_us":%d,"p50_us":%d,"p95_us":%d,"p99_us":%d,"connect_errors":%d,"read_errors":%d,"write_errors":%d,"status_errors":%d,"timeouts":%d}\\n',
        summary.requests, summary.duration, latency:percentile(50),
        latency:percentile(95), latency:percentile(99), summary.errors.connect,
        summary.errors.read, summary.errors.write, summary.errors.status,
        summary.errors.timeout))
end
''')
report = {'root': str(root), 'binary': str(args.binary),
          'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
          'server_cpus': server_cpus, 'client_cpus': client_cpus,
          'workers': args.workers,
          'connections': args.connections, 'seconds': args.seconds,
          'fills': args.fills, 'fill_rate': args.fill_rate,
          'owner_control': args.owner_control,
          'cgroup': str(args.cgroup) if args.cgroup else None,
          'bulk_objects': args.bulk, 'samples': [],
          'scope': 'Local closed-loop wrk comparison; an optional preconfigured memory cgroup contains only nginx and its children.'}


def setup_child():
    os.sched_setaffinity(0, server_cpus)
    if args.cgroup:
        descriptor = os.open(args.cgroup / 'cgroup.procs', os.O_WRONLY)
        os.write(descriptor, str(os.getpid()).encode())
        os.close(descriptor)


def resources(pid):
    pending, seen = [pid], set()
    pss = rss = ticks = 0
    policies = {}
    policy_ticks = {}
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        directory = Path('/proc') / str(current)
        try:
            pending.extend(map(int, (directory / 'task' / str(current) / 'children').read_text().split()))
            stat = (directory / 'stat').read_text().rsplit(')', 1)[1].split()
            ticks += int(stat[11]) + int(stat[12])
            memory = dict(re.findall(r'^(Pss|Rss):\s+(\d+)',
                                     (directory / 'smaps_rollup').read_text(), re.M))
            pss += int(memory['Pss'])
            rss += int(memory['Rss'])
            for task in (directory / 'task').iterdir():
                policy = str(os.sched_getscheduler(int(task.name)))
                policies[policy] = policies.get(policy, 0) + 1
                stat = (task / 'stat').read_text().rsplit(')', 1)[1].split()
                policy_ticks[policy] = policy_ticks.get(policy, 0) + int(stat[11]) + int(stat[12])
        except FileNotFoundError:
            pass
    result = {'pss_kib': pss, 'rss_kib': rss, 'cpu_ticks': ticks,
              'processes': len(seen), 'scheduler_policies': policies,
              'scheduler_cpu_ticks': policy_ticks}
    if args.cgroup:
        result['cgroup'] = {name: (args.cgroup / name).read_text().strip()
                           for name in ('memory.limit_in_bytes', 'memory.usage_in_bytes',
                                        'memory.max_usage_in_bytes', 'memory.failcnt', 'memory.oom_control')}
    return result


try:
    variants = [(False, False), (True, False), (True, True)] if args.owner_control else [(False, False), (True, True)]
    variants = variants * args.rounds + [(True, True)]
    for number, (enabled, active) in enumerate(variants):
        background = number == len(variants) - 1
        mode = 'background-delete' if background else 'idle' if active else 'owner-only' if enabled else 'disabled'
        run = root / str(number)
        (run / 'cache').mkdir(parents=True)
        (run / 'logs').mkdir()
        with socket.socket() as available:
            available.bind(('127.0.0.1', 0))
            port = available.getsockname()[1]
        settings = f'''cache_purge_store {run}/store;
            cache_purge_state_zone purge 1m;
            cache_purge_key_url http://bench.test$uri;
            cache_purge_tag $upstream_http_cache_tag;
            location /management/ {{ cache_purge_api; }}
            location = /control {{
                proxy_cache_purge on;
                cache_purge_key_url $http_x_target;
            }}''' if enabled else ''
        config = f'''daemon off;
master_process on;
worker_processes {args.workers};
pid {run}/nginx.pid;
error_log {run}/error.log notice;
events {{ worker_connections 1024; }}
http {{
    access_log off;
    proxy_cache_path {run}/cache levels=1:2 keys_zone=cache:4m;
    server {{
        listen 127.0.0.1:{port};
        {settings}
        location / {{
            proxy_pass http://127.0.0.1:{origin.server_port};
            proxy_cache cache;
            proxy_cache_key $uri;
            {'proxy_cache_purge on;' if active else ''}
            add_header X-Cache $upstream_cache_status always;
        }}
    }}
}}
'''
        # Store and shared-zone directives belong to http, not server.
        if enabled:
            head, locations = settings.split('location /management/', 1)
            config = config.replace(settings, 'location /management/' + locations)
            config = config.replace('    access_log off;', '    access_log off;\n' + head)
        (run / 'nginx.conf').write_text(config)
        with (run / 'stdout.log').open('w') as output:
            process = subprocess.Popen([str(args.binary), '-p', str(run), '-c', str(run / 'nginx.conf')],
                stdout=output, stderr=subprocess.STDOUT,
                preexec_fn=setup_child)
        load = None
        profiler = None
        filler = None
        stop_fill = threading.Event()
        connection = http.client.HTTPConnection('127.0.0.1', port, timeout=15)

        def request(path, method='GET', headers=None):
            connection.request(method, path, headers={'Host': 'bench.test', **(headers or {})})
            response = connection.getresponse()
            data = response.read()
            return response.status, response.getheader('X-Cache'), data

        try:
            deadline = time.monotonic() + 10
            while True:
                assert process.poll() is None, (run / 'stdout.log').read_text()
                try:
                    if enabled:
                        status, _, body = request('/management/capabilities')
                        ready = status == 200 and json.loads(body)['ready']
                    else:
                        ready = request('/hot')[0] == 200
                    if ready:
                        break
                except OSError:
                    connection.close()
                assert time.monotonic() < deadline, 'nginx startup timed out'
                time.sleep(.02)
            for _ in range(10):
                if request('/hot')[1] == 'HIT':
                    break
            assert request('/hot')[1] == 'HIT'
            if background:
                for i in range(args.bulk):
                    assert request(f'/bulk/{i:08d}')[0] == 200
                    if i % 64 == 63:
                        # A queued status read drains preceding fill messages.
                        assert request('/management/operations/barrier')[0] == 404
                assert request('/management/operations/barrier')[0] == 404
                time.sleep(1.05)
            for mode in (('background-delete', 'retained-rule') if background else (mode,)):
                deleting = mode == 'background-delete'
                if mode == 'retained-rule':
                    deadline = time.monotonic() + 120
                    while True:
                        status, _, body = request('/management/operations/' + operation)
                        assert status == 200, (status, body)
                        cleanup = json.loads(body)['cleanup']
                        if cleanup['state'] == 'done':
                            assert cleanup['affected'] == args.bulk, cleanup
                            break
                        assert time.monotonic() < deadline, cleanup
                        time.sleep(.1)
                before = resources(process.pid)
                origin_before = hot_calls
                fill_stats = {'completed': 0}
                if args.fills and not background:
                    mode += '-fill'

                    def fill():
                        client = http.client.HTTPConnection('127.0.0.1', port, timeout=3)
                        started = time.monotonic()
                        try:
                            for i in range(args.fills):
                                if stop_fill.is_set():
                                    break
                                if args.fill_rate:
                                    stop_fill.wait(max(0, started + i / args.fill_rate - time.monotonic()))
                                client.request('GET', f'/fill/{i:08d}', headers={'Host': 'bench.test'})
                                response = client.getresponse()
                                response.read()
                                assert response.status == 200, response.status
                                fill_stats['completed'] += 1
                        except Exception as error:
                            fill_stats['error'] = repr(error)
                        finally:
                            fill_stats['seconds'] = time.monotonic() - started
                            client.close()

                    filler = threading.Thread(target=fill, daemon=True)
                    filler.start()
                command = ['taskset', '-c', ','.join(map(str, client_cpus)), 'wrk', '-t1',
                           '-c' + str(args.connections), '-d' + str(args.seconds) + 's',
                           '--latency', '-s', str(root / 'stats.lua'),
                           '-H', 'Host: bench.test', f'http://127.0.0.1:{port}/hot']
                if args.profile:
                    children = Path(f'/proc/{process.pid}/task/{process.pid}/children').read_text().split()
                    workers = [pid for pid in children
                               if Path(f'/proc/{pid}/cmdline').read_bytes().startswith(b'nginx: worker process')]
                    assert len(workers) == args.workers, workers
                    profiler = subprocess.Popen(['perf', 'record', '-e', 'cpu-clock:u',
                        '-F', '999', '-t', ','.join(workers), '-o', str(run / (mode + '.perf'))],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                load = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                progress = []
                admission_ms = None
                if deleting:
                    submitted = time.monotonic()
                    status, _, body = request('/control', 'PURGE', {
                        'X-Target': 'http://bench.test/bulk/', 'X-Cache-Purge-Type': 'prefix'})
                    assert status == 202, (status, body)
                    admission_ms = (time.monotonic() - submitted) * 1000
                    operation = json.loads(body)['operation_id']
                    started = time.monotonic()
                    while load.poll() is None:
                        status, _, body = request('/management/operations/' + operation)
                        assert status == 200, (status, body)
                        progress.append({'seconds': time.monotonic() - started,
                                         **json.loads(body)['cleanup']})
                        time.sleep(.1)
                stdout, _ = load.communicate(timeout=args.seconds + 10)
                if profiler is not None:
                    profiler.send_signal(signal.SIGINT)
                    assert profiler.wait(5) in (0, -signal.SIGINT)
                    assert (run / (mode + '.perf')).stat().st_size > 4096
                    profiler = None
                (run / (mode + '.log')).write_text(stdout)
                assert load.returncode == 0, stdout
                sample = json.loads(next(line[7:] for line in stdout.splitlines() if line.startswith('RESULT ')))
                assert not any(sample[field] for field in ('connect_errors', 'read_errors', 'write_errors', 'status_errors', 'timeouts')), sample
                assert hot_calls == origin_before and request('/hot')[1] == 'HIT'
                sample.update(mode=mode, run=str(run), resources_before=before,
                              resources_after=resources(process.pid), progress=progress,
                              fill=fill_stats, admission_ms=admission_ms)
                sample['requests_per_second'] = sample['requests'] * 1e6 / sample['duration_us']
                report['samples'].append(sample)
                print(json.dumps({k: sample[k] for k in ('mode', 'requests_per_second', 'p50_us', 'p99_us')}), flush=True)
                if filler is not None:
                    filler.join(max(15, args.fills / max(args.fill_rate, 1) * 2))
                    assert not filler.is_alive() and 'error' not in fill_stats, fill_stats
                    assert fill_stats['completed'] == args.fills, fill_stats
                    filler = None
                    time.sleep(1.05)
                    fill_stats['files'] = sum(path.is_file() for path in (run / 'cache').rglob('*')) - 1
                    assert fill_stats['files'] == args.fills, fill_stats
                    if active:
                        status, _, body = request('/control', 'PURGE', {
                            'X-Target': 'http://bench.test/fill/', 'X-Cache-Purge-Type': 'prefix'})
                        assert status == 202, (status, body)
                        identifier = json.loads(body)['operation_id']
                        deadline = time.monotonic() + max(30, args.fills / 50)
                        while True:
                            status, _, body = request('/management/operations/' + identifier)
                            assert status == 200, (status, body)
                            cleanup = json.loads(body)['cleanup']
                            if cleanup['state'] == 'done':
                                fill_stats['indexed_files'] = cleanup['affected']
                                assert cleanup['affected'] == args.fills, fill_stats
                                break
                            assert time.monotonic() < deadline, cleanup
                            time.sleep(.1)
        finally:
            stop_fill.set()
            if filler is not None:
                filler.join(5)
            if load is not None and load.poll() is None:
                load.terminate()
                load.communicate(timeout=5)
            if profiler is not None:
                profiler.send_signal(signal.SIGINT)
                profiler.wait(5)
            connection.close()
            process.send_signal(signal.SIGQUIT)
            assert process.wait(15) == 0
        errors = (run / 'error.log').read_text()
        assert not any(marker in errors for marker in ('[alert]', '[crit]', '[emerg]', 'index queue is full')), errors
    report['status'] = 'passed'
except BaseException as error:
    report['status'] = 'failed'
    report['error'] = repr(error)
    raise
finally:
    origin.shutdown()
    origin.server_close()
    args.output.write_text(json.dumps(report, indent=2) + '\n')
