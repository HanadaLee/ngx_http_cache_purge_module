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
import sys
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
parser.add_argument('--server-cores', type=int, default=2, help='Number of distinct CPU cores reserved for nginx')
parser.add_argument('--fills', type=int, default=0)
parser.add_argument('--fill-rate', type=int, default=500)
parser.add_argument('--request-delay', type=int, default=0, help='wrk delay in milliseconds between requests on each connection')
parser.add_argument('--evict', action='store_true', help='Limit the native cache to 1 MiB during fill samples to exercise its cache manager')
parser.add_argument('--seed-store', type=Path, help='Copy an offline, closed RocksDB store into each enabled run before nginx starts')
parser.add_argument('--url-origin', default='http://bench.test', help='Scheme and host used by the configured purge URL identity')
parser.add_argument('--io-rate', help='Override the store background I/O rate, for example 16m or 0')
parser.add_argument('--profile', nargs='?', const='worker', choices=('worker', 'all'), help='Sample worker main threads or all nginx threads with perf; timing is diagnostic only')
parser.add_argument('--owner-control', action='store_true', help='Also measure configured resources with the business location disabled')
parser.add_argument('--fill-only-control', action='store_true', help='Also index fills while disabling purge only for the measured hot URL')
parser.add_argument('--hold-cleanup', action='store_true', help='Keep the first target unreadable to measure a rule without bulk file processing')
parser.add_argument('--rules', type=int, default=1, help='Number of simultaneous prefix tasks')
parser.add_argument('--distinct-prefixes', action='store_true', help='Use a separate target prefix and blocked file for each task')
parser.add_argument('--hot-path', default='/hot')
parser.add_argument('--cgroup', type=Path, help='Preconfigured cgroup v1 memory directory; only nginx joins it')
parser.add_argument('--cache-io', action='store_true', help='Enable sendfile and cache native file descriptors')
args = parser.parse_args()
cache_io = '''sendfile on;
    open_file_cache max=10000 inactive=20s;
    open_file_cache_valid 10s;
    open_file_cache_min_uses 2;
    open_file_cache_errors on;''' if args.cache_io else ''
assert args.seconds > 0 and args.rounds >= 0 and args.bulk > 0
assert args.request_delay >= 0
assert 0 < args.rules <= 256 and args.hot_path.startswith('/hot')
assert not args.distinct_prefixes or args.bulk >= args.rules
assert not args.evict or args.fills > 512
assert re.fullmatch(r'https?://[a-zA-Z0-9.:-]+', args.url_origin)
assert args.io_rate is None or re.fullmatch(r'(?:0|[1-9][0-9]*[kKmMgG]?)', args.io_rate)
store_options = '' if args.io_rate is None else ' io_rate=' + args.io_rate
root = Path(tempfile.mkdtemp(prefix='cache-simple-benchmark-'))
calls = hot_calls = 0


class Origin(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global calls, hot_calls
        calls += 1
        hot_calls += self.path == args.hot_path
        body = b'cached body\n' * 128
        self.send_response(200)
        self.send_header('Cache-Control', 'max-age=3600')
        self.send_header('Cache-Tag', 'benchmark')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *unused):
        pass


def cpu_siblings(cpu):
    path = Path(f'/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list')
    siblings = set()
    for part in path.read_text().strip().split(','):
        bounds = list(map(int, part.split('-')))
        siblings.update(range(bounds[0], bounds[-1] + 1))
    return siblings


cpus = sorted(os.sched_getaffinity(0))
cores, seen = [], set()
topology = {cpu: sorted(cpu_siblings(cpu)) for cpu in cpus}
for cpu in cpus:
    if cpu not in seen:
        group = sorted(set(topology[cpu]).intersection(cpus))
        assert cpu in group
        cores.append(group)
        seen.update(group)
assert args.workers > 0 and 0 < args.server_cores < len(cores), \
    'A separate physical core is required for the load generator'
server_cpus = [group[0] for group in cores[:args.server_cores]]
server_siblings = [cpu for group in cores[:args.server_cores] for cpu in group]
client_cpus = [cores[args.server_cores][0]]
controller_cpus = [cpu for group in cores[args.server_cores + 1:] for cpu in group] or client_cpus
assert not set(server_siblings).intersection(client_cpus + controller_cpus)
os.sched_setaffinity(0, controller_cpus)
origin = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Origin)
threading.Thread(target=origin.serve_forever, daemon=True).start()
(root / 'stats.lua').write_text('''done = function(summary, latency, requests)
    io.write(string.format('RESULT {"requests":%d,"duration_us":%d,"p50_us":%d,"p95_us":%d,"p99_us":%d,"connect_errors":%d,"read_errors":%d,"write_errors":%d,"status_errors":%d,"timeouts":%d}\\n',
        summary.requests, summary.duration, latency:percentile(50),
        latency:percentile(95), latency:percentile(99), summary.errors.connect,
        summary.errors.read, summary.errors.write, summary.errors.status,
        summary.errors.timeout))
end
''')
if args.request_delay:
    with (root / 'stats.lua').open('a') as script:
        script.write(f'\ndelay = function() return {args.request_delay} end\n')
report = {'root': str(root), 'binary': str(args.binary),
          'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
          'script_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
          'server_cpus': server_cpus, 'client_cpus': client_cpus,
          'controller_cpus': controller_cpus,
          'server_siblings': server_siblings, 'cpu_topology': topology,
          'origin_and_controller_pinned': True,
          'workers': args.workers,
          'connections': args.connections, 'seconds': args.seconds,
          'request_delay_ms': args.request_delay,
          'fills': args.fills, 'fill_rate': args.fill_rate,
          'native_eviction': args.evict,
          'cache_io': args.cache_io,
          'seed_store': str(args.seed_store) if args.seed_store else None,
          'seed_store_synced_in_cgroup': bool(args.seed_store and args.cgroup),
          'url_origin': args.url_origin,
          'io_rate': args.io_rate,
          'owner_control': args.owner_control,
          'fill_only_control': args.fill_only_control,
          'hold_cleanup': args.hold_cleanup,
          'admission_before_load': args.hold_cleanup,
          'distinct_prefixes': args.distinct_prefixes,
          'rules': args.rules, 'hot_path': args.hot_path,
          'profile': args.profile,
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
    user_ticks = system_ticks = minor_faults = 0
    threads = {}
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
            user_ticks += int(stat[11])
            system_ticks += int(stat[12])
            minor_faults += int(stat[7])
            memory = dict(re.findall(r'^(Pss|Rss):\s+(\d+)',
                                     (directory / 'smaps_rollup').read_text(), re.M))
            pss += int(memory['Pss'])
            rss += int(memory['Rss'])
            for task in (directory / 'task').iterdir():
                affinity = sorted(os.sched_getaffinity(int(task.name)))
                assert set(affinity).issubset(server_cpus), (task, affinity)
                policy = str(os.sched_getscheduler(int(task.name)))
                policies[policy] = policies.get(policy, 0) + 1
                stat = (task / 'stat').read_text().rsplit(')', 1)[1].split()
                policy_ticks[policy] = policy_ticks.get(policy, 0) + int(stat[11]) + int(stat[12])
                status = (task / 'status').read_text()
                threads[task.name] = dict(name=(task / 'comm').read_text().strip(),
                    affinity=affinity,
                    policy=int(policy), nice=int(stat[16]), user_ticks=int(stat[11]),
                    system_ticks=int(stat[12]), minor_faults=int(stat[7]),
                    **{key: int(value) for key, value in re.findall(
                        r'^(voluntary_ctxt_switches|nonvoluntary_ctxt_switches):\s+(\d+)', status, re.M)})
        except FileNotFoundError:
            pass
    result = {'pss_kib': pss, 'rss_kib': rss, 'cpu_ticks': ticks,
              'processes': len(seen), 'scheduler_policies': policies,
              'scheduler_cpu_ticks': policy_ticks, 'user_ticks': user_ticks,
              'system_ticks': system_ticks, 'minor_faults': minor_faults, 'threads': threads}
    if args.cgroup:
        result['cgroup'] = {name: (args.cgroup / name).read_text().strip()
                           for name in ('memory.limit_in_bytes', 'memory.usage_in_bytes',
                                        'memory.max_usage_in_bytes', 'memory.failcnt', 'memory.oom_control')}
    return result


try:
    variants = [(False, False, False)]
    if args.owner_control:
        variants.append((True, False, False))
    if args.fill_only_control:
        variants.append((True, True, True))
    variants.append((True, True, False))
    variants = variants * args.rounds + [(True, True, False)]
    for number, (enabled, active, fill_only) in enumerate(variants):
        background = number == len(variants) - 1
        mode = 'background-delete' if background else 'fill-only' if fill_only else 'idle' if active else 'owner-only' if enabled else 'disabled'
        run = root / str(number)
        (run / 'cache').mkdir(parents=True)
        (run / 'logs').mkdir()
        if enabled and args.seed_store:
            subprocess.run([sys.executable, '-c',
                            '''import os,shutil,sys
from pathlib import Path
target = Path(sys.argv[2])
shutil.copytree(sys.argv[1], target)
for path in target.rglob('*'):
    if path.is_file():
        with path.open('rb') as handle:
            os.fsync(handle.fileno())
descriptor = os.open(target, os.O_RDONLY | os.O_DIRECTORY)
os.fsync(descriptor)
os.close(descriptor)
''',
                            str(args.seed_store), str(run / 'store')],
                           preexec_fn=setup_child, check=True)
        with socket.socket() as available:
            available.bind(('127.0.0.1', 0))
            port = available.getsockname()[1]
        settings = f'''cache_purge_store {run}/store{store_options};
            cache_purge_state_zone purge 1m;
            cache_purge_key_url {args.url_origin}$uri;
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
    {cache_io}
    proxy_cache_path {run}/cache levels=1:2 keys_zone=cache:4m {'max_size=1m' if args.evict and not background else ''};
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
        if args.fill_only_control:
            hot_location = f'''location = {args.hot_path} {{
                proxy_pass http://127.0.0.1:{origin.server_port};
                proxy_cache cache;
                proxy_cache_key $uri;
                {'proxy_cache_purge on;' if active and not fill_only else 'proxy_cache_purge off;'}
                add_header X-Cache $upstream_cache_status always;
            }}
            '''
            config = config.replace('location / {', hot_location + 'location / {')
        (run / 'nginx.conf').write_text(config)
        with (run / 'stdout.log').open('w') as output:
            process = subprocess.Popen([str(args.binary), '-p', str(run), '-c', str(run / 'nginx.conf')],
                stdout=output, stderr=subprocess.STDOUT,
                preexec_fn=setup_child)
        load = None
        profiler = None
        filler = None
        held = []
        stop_fill = threading.Event()
        connection = http.client.HTTPConnection('127.0.0.1', port, timeout=15)

        def request(path, method='GET', headers=None):
            connection.request(method, path, headers={'Host': 'bench.test', **(headers or {})})
            response = connection.getresponse()
            data = response.read()
            return response.status, response.getheader('X-Cache'), data

        def bulk_path(i):
            group = f'{i % args.rules:04d}/' if args.distinct_prefixes else ''
            return f'/bulk/{group}{i:08d}'

        def submit():
            submitted = time.monotonic()
            operations = []
            for i in range(args.rules):
                group = f'{i:04d}/' if args.distinct_prefixes else ''
                status, _, body = request('/control', 'PURGE', {
                    'X-Target': args.url_origin + '/bulk/' + group,
                    'X-Cache-Purge-Type': 'prefix'})
                assert status == 202, (status, body)
                operations.append(json.loads(body)['operation_id'])
            return operations, (time.monotonic() - submitted) * 1000

        try:
            deadline = time.monotonic() + 10
            while True:
                assert process.poll() is None, (run / 'stdout.log').read_text()
                try:
                    if enabled:
                        status, _, body = request('/management/capabilities')
                        ready = status == 200 and json.loads(body)['ready']
                    else:
                        ready = request(args.hot_path)[0] == 200
                    if ready:
                        break
                except OSError:
                    connection.close()
                assert time.monotonic() < deadline, 'nginx startup timed out'
                time.sleep(.02)
            for _ in range(10):
                if request(args.hot_path)[1] == 'HIT':
                    break
            assert request(args.hot_path)[1] == 'HIT'
            if background:
                for i in range(args.bulk):
                    assert request(bulk_path(i))[0] == 200
                    if i % 64 == 63:
                        # A queued status read drains preceding fill messages.
                        assert request('/management/operations/barrier')[0] == 404
                assert request('/management/operations/barrier')[0] == 404
                time.sleep(1.05)
            modes = ('held-rule', 'background-delete', 'retained-rule') if args.hold_cleanup else ('background-delete', 'retained-rule')
            for mode in (modes if background else (mode,)):
                deleting = mode in ('held-rule', 'background-delete')
                submitting = background and mode == modes[0]
                admission_ms = None
                if mode == 'held-rule':
                    for i in range(args.rules if args.distinct_prefixes else 1):
                        digest = hashlib.md5(bulk_path(i).encode()).hexdigest()
                        path = next((run / 'cache').rglob(digest))
                        held.append((path, path.stat().st_mode))
                        path.chmod(0)
                    operations, admission_ms = submit()
                    submitting = False
                    operation = operations[-1]
                elif mode == 'background-delete':
                    for path, permissions in held:
                        path.chmod(permissions)
                    held = []
                if mode == 'retained-rule':
                    deadline = time.monotonic() + 120
                    while True:
                        cleanups = []
                        for operation in operations:
                            status, _, body = request('/management/operations/' + operation)
                            assert status == 200, (status, body)
                            cleanups.append(json.loads(body)['cleanup'])
                        if all(item['state'] == 'done' for item in cleanups):
                            assert sum(item['affected'] for item in cleanups) == args.bulk, cleanups
                            break
                        assert time.monotonic() < deadline, cleanups
                        time.sleep(.1)
                before = resources(process.pid)
                cleanup_before = None
                if enabled:
                    status, _, body = request('/management/capabilities')
                    assert status == 200
                    cleanup_before = json.loads(body).get('index_cleanup')
                origin_before = hot_calls
                fill_stats = {'completed': 0}
                if args.fills and not background:
                    mode += '-fill'
                    if active:
                        status, _, body = request('/management/capabilities')
                        assert status == 200
                        fill_stats['cleanup_before'] = json.loads(body).get(
                            'index_cleanup', {}).get('retained', 0)

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
                command = ['wrk', '-t1',
                           '-c' + str(args.connections), '-d' + str(args.seconds) + 's',
                           '--latency', '-s', str(root / 'stats.lua'),
                           '-H', 'Host: bench.test', f'http://127.0.0.1:{port}{args.hot_path}']
                if args.profile:
                    children = Path(f'/proc/{process.pid}/task/{process.pid}/children').read_text().split()
                    workers = [pid for pid in children
                               if Path(f'/proc/{pid}/cmdline').read_bytes().startswith(b'nginx: worker process')]
                    assert len(workers) == args.workers, workers
                    targets = children + [str(process.pid)] if args.profile == 'all' else workers
                    (run / (mode + '.tasks.json')).write_text(json.dumps({
                        pid: {'command': Path(f'/proc/{pid}/cmdline').read_bytes().replace(b'\0', b' ').decode().strip(),
                              'threads': {task.name: (task / 'comm').read_text().strip()
                                          for task in Path(f'/proc/{pid}/task').iterdir()}}
                        for pid in targets}, indent=2) + '\n')
                    profiler = subprocess.Popen(['perf', 'record', '-e', 'cpu-clock:u',
                        '-F', '999', '-p' if args.profile == 'all' else '-t',
                        ','.join(targets), '-o', str(run / (mode + '.perf'))],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                load = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                        preexec_fn=lambda: os.sched_setaffinity(0, client_cpus))
                load_affinity = sorted(os.sched_getaffinity(load.pid))
                assert load_affinity == client_cpus, load_affinity
                progress = []
                if submitting:
                    operations, admission_ms = submit()
                    operation = operations[-1]
                if deleting:
                    started = time.monotonic()
                    while load.poll() is None:
                        status, _, body = request('/management/operations/' + operation)
                        assert status == 200, (status, body)
                        progress.append({'seconds': time.monotonic() - started,
                                         **json.loads(body)['cleanup']})
                        time.sleep(.1)
                    if mode == 'held-rule':
                        assert progress and all(item['affected'] == 0 and item['state'] == 'pending' for item in progress), progress
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
                assert hot_calls == origin_before and request(args.hot_path)[1] == 'HIT'
                sample.update(mode=mode, run=str(run), resources_before=before,
                              resources_after=resources(process.pid), progress=progress,
                              fill=fill_stats, admission_ms=admission_ms, load_affinity=load_affinity)
                if enabled:
                    status, _, body = request('/management/capabilities')
                    assert status == 200
                    sample['index_cleanup_before'] = cleanup_before
                    sample['index_cleanup_after'] = json.loads(body).get('index_cleanup')
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
                    if args.evict:
                        assert 0 < fill_stats['files'] < args.fills, fill_stats
                    else:
                        assert fill_stats['files'] == args.fills, fill_stats
                    if active:
                        status, _, body = request('/control', 'PURGE', {
                            'X-Target': args.url_origin + '/fill/', 'X-Cache-Purge-Type': 'prefix'})
                        assert status == 202, (status, body)
                        identifier = json.loads(body)['operation_id']
                        deadline = time.monotonic() + max(30, args.fills / 50)
                        while True:
                            status, _, body = request('/management/operations/' + identifier)
                            assert status == 200, (status, body)
                            cleanup = json.loads(body)['cleanup']
                            if cleanup['state'] == 'done':
                                fill_stats['indexed_files'] = cleanup['affected']
                                fill_stats['indexed_records'] = cleanup['visited']
                                status, _, body = request('/management/capabilities')
                                assert status == 200
                                retired = json.loads(body).get('index_cleanup', {}).get('retained', 0)
                                fill_stats['swept_records'] = retired - fill_stats['cleanup_before']
                                assert fill_stats['swept_records'] >= 0, fill_stats
                                assert cleanup['visited'] + fill_stats['swept_records'] == args.fills, fill_stats
                                if args.evict:
                                    assert 0 < cleanup['affected'] < args.fills, fill_stats
                                else:
                                    assert fill_stats['swept_records'] == 0, fill_stats
                                    assert cleanup['affected'] == args.fills, fill_stats
                                break
                            assert time.monotonic() < deadline, cleanup
                            time.sleep(.1)
        finally:
            for path, permissions in held:
                if path.exists():
                    path.chmod(permissions)
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
