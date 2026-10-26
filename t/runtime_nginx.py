#!/usr/bin/env python3
"""Build the real addon; exercise managed lifecycle and a separate test guard."""
import argparse
import fcntl
import grp
import hashlib
import http.server
import http.client
import json
import os
import pwd
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--nginx', type=Path, required=True)
p.add_argument('--nginx-ref', default='HEAD')
p.add_argument('--agent-patches', type=Path, help='Existing OpenResty privileged-agent patches; no local nginx-ext patch')
p.add_argument('--libraries', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--build-root', type=Path)
p.add_argument('--dynamic', action='store_true')
p.add_argument('--shared-rocksdb', action='store_true', help='Use the system shared engine when its static archive is not PIC')
p.add_argument('--sanitize', action='store_true')
p.add_argument('--runtime-only', action='store_true', help='Reuse the verified build for a separate runtime UID test')
p.add_argument('--worker-user', help='Worker user for tests started by root')
a = p.parse_args()
source = Path(__file__).resolve().parents[1]
root = a.build_root or Path(tempfile.mkdtemp(prefix='ncp-addon-', dir='/tmp'))
root.mkdir(exist_ok=True)
root.chmod(0o755)
validation = Path(tempfile.mkdtemp(prefix='validation-', dir=root))
validation.chmod(0o755)
if a.sanitize:
    # Native worker exit retains three ngx_event_process_init arrays. Keep
    # leak detection for the executor and IPC; suppress only that known origin.
    suppressions = validation / 'native-event-arrays.lsan'
    suppressions.write_text('leak:ngx_event_process_init\n')
    os.environ['LSAN_OPTIONS'] = 'suppressions=' + str(suppressions)
addon, probe, build = root / 'addon', root / 'probe', root / 'nginx'
for path in (addon, probe, build):
    path.mkdir(exist_ok=True)
for files, destination in [([source / 'config', *source.glob('*.c'), *source.glob('*.h')], addon),
                           (list((source / 't/runtime_probe').iterdir()), probe)]:
    for path in files:
        contents = path.read_bytes().replace(b'\r\n', b'\n')
        if a.runtime_only:
            assert (destination / path.name).read_bytes() == contents, ('stale addon', path)
        else:
            (destination / path.name).write_bytes(contents)
(addon / 'rocksdb').mkdir(exist_ok=True)
for path in (source / 'rocksdb').iterdir():
    if path.suffix in ('.c', '.h'):
        contents = path.read_bytes().replace(b'\r\n', b'\n')
        target = addon / 'rocksdb' / path.name
        if a.runtime_only:
            assert target.read_bytes() == contents, ('stale provider', path)
        else:
            target.write_bytes(contents)
git = ['git', '-c', 'safe.directory=' + str(a.nginx), '-C', str(a.nginx)]
revision = subprocess.check_output(git + ['rev-parse', a.nginx_ref], text=True).strip()
patches = {}
if a.agent_patches:
    for suffix in ('', '_thread_pool', '_connections'):
        patch = a.agent_patches / f'nginx-1.31.7-privileged_agent_process{suffix}.patch'
        patches[patch.name] = hashlib.sha256(patch.read_bytes()).hexdigest()
if not (build / 'src').exists():
    with (root / 'source.tar').open('wb') as f:
        subprocess.run(git + ['archive', revision], stdout=f, check=True)
    subprocess.run(['tar', '-xf', str(root / 'source.tar'), '-C', str(build)], check=True)
    for name in patches:
        applied = subprocess.run(['patch', '--batch', '--fuzz=2', '-p1'], cwd=build,
                                 input=(a.agent_patches / name).read_bytes().replace(b'\r\n', b'\n'),
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        with (root / 'patches.log').open('ab') as log:
            log.write(name.encode() + b'\n' + applied.stdout)
        assert applied.returncode == 0, (name, applied.stdout.decode())

env = os.environ.copy()
env['CACHE_PURGE_ROCKSDB_LIB_DIR'] = str(a.libraries)
engine = '-lrocksdb' if a.shared_rocksdb else '/usr/lib/x86_64-linux-gnu/librocksdb.a'
env['CACHE_PURGE_ROCKSDB_LIBS'] = engine + ' -lcrypto -lsnappy -lz -lbz2 -llz4 -lzstd -lstdc++ -lm -pthread'
env['CACHE_PURGE_PROBE_HEADERS'] = str(addon)
print('BUILD', root, flush=True)
manifest = {'nginx_revision': revision, 'openresty_patches': patches, 'dynamic': a.dynamic,
            'shared_rocksdb': a.shared_rocksdb,
            'sanitizer': a.sanitize,
            'archives': {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                         for path in a.libraries.glob('libngx_http_cache_*.a')}}
if a.runtime_only:
    assert json.loads((root / 'build.json').read_text()) == manifest, 'stale build manifest'
else:
  with (root / 'build.log').open('w') as f:
    configure = 'auto/configure' if (build / 'auto/configure').exists() else 'configure'
    module_option = '--add-dynamic-module=' if a.dynamic else '--add-module='
    args = ['sh', configure, '--prefix=' + str(root / 'run'), '--without-http_rewrite_module',
            '--without-http_gzip_module', module_option + str(addon), module_option + str(probe)]
    if not a.sanitize:
        args += ['--with-cc-opt=-O2']
    if a.sanitize:
        args += ['--with-cc-opt=-O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined -fno-sanitize=nonnull-attribute -fno-pie',
                 '--with-ld-opt=-fsanitize=address,undefined -no-pie']
    subprocess.run(args,
                   cwd=build, env=env, stdout=f, stderr=subprocess.STDOUT, check=True)
    subprocess.run(['make', '-j2'], cwd=build, stdout=f, stderr=subprocess.STDOUT, check=True)
    if a.dynamic:
        subprocess.run(['make', 'modules', '-j2'], cwd=build, stdout=f, stderr=subprocess.STDOUT, check=True)
  (root / 'build.json').write_text(json.dumps(manifest, indent=2) + '\n')
binary = build / 'objs/nginx'
checks = []
checks.append('worker lifecycle needs no new core capability or privileged agent')
origin_calls = {}
hold_gates = {}


class Origin(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        origin_calls[self.path] = origin_calls.get(self.path, 0) + 1
        if self.path in hold_gates:
            self.send_response(200)
            self.send_header('Content-Length', '2')
            self.end_headers()
            self.wfile.write(b'A')
            self.wfile.flush()
            hold_gates[self.path].wait(15)
            self.wfile.write(b'B')
            return
        body = f'origin:{self.path}:{origin_calls[self.path]}'.encode()
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'max-age=3600')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


origin = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Origin)
threading.Thread(target=origin.serve_forever, daemon=True).start()
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def text(value):
    raw = value.encode()
    return struct.pack('>I', len(raw)) + raw


def ipc(path, op, data=b''):
    request = struct.pack('>4sHHQII', b'NCP1', 1, op, 1, len(data), 0) + data
    if os.geteuid() == 0 and a.worker_user and pwd.getpwnam(a.worker_user).pw_uid != 0:
        script = """import socket,sys
s=socket.socket(socket.AF_UNIX);s.settimeout(3);s.connect(sys.argv[1]);s.sendall(sys.stdin.buffer.read())
def read(n):
 b=b''
 while len(b)<n:
  part=s.recv(n-len(b));assert part;b+=part
 return b
h=read(24);sys.stdout.buffer.write(h+read(int.from_bytes(h[16:20],'big')))
"""
        response = subprocess.check_output(['runuser', '-u', a.worker_user, '--',
            'python3', '-c', script, str(path / 'index/component.sock')], input=request, stderr=subprocess.PIPE)
    else:
        with socket.socket(socket.AF_UNIX) as client:
            client.settimeout(3)
            client.connect(str(path / 'index/component.sock'))
            client.sendall(request)
            def read(n):
                result = b''
                while n:
                    chunk = client.recv(n)
                    assert chunk, 'unexpected IPC EOF'
                    result += chunk
                    n -= len(chunk)
                return result
            header = read(24)
            response = header + read(struct.unpack('>I', header[16:20])[0])
    _, _, actual, _, size, code = struct.unpack('>4sHHQII', response[:24])
    result = response[24:]
    assert actual == op and code == 0 and len(result) == size, (code, result)
    return result


def until(predicate, timeout=10):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            value = predicate()
            if value:
                return value
        except (OSError, AssertionError, subprocess.CalledProcessError):
            pass
        time.sleep(.01)
    raise AssertionError('runtime deadline')


runtime_cases = []

class Run:
    def __init__(self, enabled=True, workers=2, rpc_socket=None):
        runtime_cases.append(self)
        self.path = Path(tempfile.mkdtemp(prefix='case-', dir=root))
        self.path.chmod(0o755)
        (self.path / 'logs').mkdir(exist_ok=True)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            self.port = sock.getsockname()[1]
        user = ''
        if os.geteuid() == 0:
            account = pwd.getpwnam(a.worker_user or 'root')
            user = f'user {account.pw_name} {grp.getgrgid(account.pw_gid).gr_name};'
        loads = ''
        if a.dynamic:
            loads = (f'load_module {build}/objs/ngx_http_cache_rocksdb_module.so;\n'
                     f'load_module {build}/objs/ngx_http_cache_purge_module.so;\n'
                     f'load_module {build}/objs/ngx_http_cache_purge_runtime_probe_module.so;\n')
        rpc_locations = '' if rpc_socket is None else f'''
client_body_buffer_size 1m;
location /rpc/ {{ cache_purge_test_rpc runtime; }}
location /rpc-files/ {{ client_body_in_file_only on; cache_purge_test_rpc runtime; }}
location /rpc-files-fake/ {{ client_body_in_file_only on; cache_purge_test_rpc {rpc_socket}; }}
location /rpc-fake/ {{ cache_purge_test_rpc {rpc_socket}; }}
location /rpc-wrong/ {{ cache_purge_test_rpc {rpc_socket}; cache_purge_test_rpc_wrong_uid on; }}
location /rpc-missing/ {{ cache_purge_test_rpc {self.path}/missing.sock; }}
'''
        extra_paths = '' if rpc_socket is None else f'''
fastcgi_cache_path {self.path}/cache-fcgi keys_zone=f:1m;
uwsgi_cache_path {self.path}/cache-uwsgi keys_zone=u:1m;
scgi_cache_path {self.path}/cache-scgi keys_zone=s:1m;
'''
        self.config = loads + user + f'''
daemon off; worker_processes {workers}; pid {self.path}/pid;
error_log {self.path}/error.log notice;
events {{ worker_connections 128; }}
http {{ access_log off;
cache_purge_store {self.path}/index cache=8m write_buffer=2m;
cache_purge_state_zone control 4m;
proxy_cache_path {self.path}/cache-a keys_zone=a:1m;
proxy_cache_path {self.path}/cache-b keys_zone=b:1m;
{extra_paths}
server {{ listen 127.0.0.1:{self.port};
{rpc_locations}
location /hold/ {{ proxy_pass http://127.0.0.1:{origin.server_port}; proxy_buffering off; }}
location /cache/a {{ cache_purge_test_reader on; proxy_cache a; proxy_cache_key $uri;
proxy_pass http://127.0.0.1:{origin.server_port}; add_header X-Cache $upstream_cache_status always; }}
location /cache/b {{ cache_purge_test_reader on; proxy_cache b; proxy_cache_key $uri;
proxy_pass http://127.0.0.1:{origin.server_port}; add_header X-Cache $upstream_cache_status always; }}
location /other {{ cache_purge_test_reader on; proxy_cache a; proxy_cache_key $uri;
proxy_pass http://127.0.0.1:{origin.server_port}; add_header X-Cache $upstream_cache_status always; }}
}} }}'''
        if not enabled:
            self.config = self.config.replace(f'cache_purge_store {self.path}/index cache=8m write_buffer=2m;', '')
            self.config = self.config.replace('cache_purge_state_zone control 4m;', '')
            self.config = self.config.replace('cache_purge_test_reader on;', 'cache_purge_test_reader off;')
        self.conf = self.path / 'nginx.conf'
        self.conf.write_text(self.config)
        self.command = [str(binary), '-p', str(self.path) + '/', '-c', str(self.conf)]
        tested = subprocess.run(self.command + ['-t'], capture_output=True)
        assert tested.returncode == 0, tested.stderr.decode()
        assert not (self.path / 'index/CURRENT').exists()
        checks.append('real addon config test initializes no database or DB threads')

    def invalid(self, content, expected):
        self.conf.write_text(content)
        result = subprocess.run(self.command + ['-t'], capture_output=True)
        assert result.returncode and expected in result.stderr.decode(), result.stderr
        self.conf.write_text(self.config)

    def start(self, wait=True):
        self.err = (self.path / 'stderr.log').open('a')
        self.process = subprocess.Popen(self.command, stderr=self.err)
        if not wait:
            return None
        try:
            return self.ready()
        except BaseException:
            self.stop(signal.SIGTERM)
            raise

    def ready(self, old=0):
        def read():
            pid = struct.unpack('>9Q', ipc(self.path, 1))[0]
            state, _ = struct.unpack('>IQ', ipc(self.path, 16))
            return pid if pid != old and state == 6 else 0
        return until(read)

    def get(self, path):
        try:
            response = opener.open(f'http://127.0.0.1:{self.port}' + path, timeout=3)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return response.status, response.headers.get('X-Cache'), response.read()

    def stop(self, signo=signal.SIGQUIT):
        self.process.send_signal(signo)
        try:
            assert self.process.wait(timeout=10) == 0
        finally:
            self.err.close()

def process_memory(master):
    pids = [master] + list(map(int, Path(f'/proc/{master}/task/{master}/children').read_text().split()))
    members = []
    for pid in pids:
        fields = {}
        for line in Path(f'/proc/{pid}/smaps_rollup').read_text().splitlines():
            words = line.split()
            if len(words) >= 2 and words[0] in ('Rss:', 'Pss:', 'Private_Clean:', 'Private_Dirty:'):
                fields[words[0][:-1]] = int(words[1])
        members.append({'pid': pid, 'title': Path(f'/proc/{pid}/cmdline').read_bytes().replace(b'\0', b' ').decode().strip(),
                        'rss_kib': fields['Rss'], 'pss_kib': fields['Pss'],
                        'private_kib': fields['Private_Clean'] + fields['Private_Dirty']})
    return {'pss_kib': sum(m['pss_kib'] for m in members), 'members': members}

def warm(case, path):
    assert case.get(path)[:2] == (200, 'MISS')
    zone = 'cache-b' if path == '/cache/b' else 'cache-a'
    until(lambda: (case.path / zone / hashlib.md5(path.encode()).hexdigest()).is_file())
    assert case.get(path)[:2] == (200, 'HIT')

memory = {}
baseline = Run(enabled=False)
baseline.start(wait=False)
try:
    until(lambda: baseline.get('/other')[0] == 200)
    for path in ('/cache/a', '/cache/b'):
        warm(baseline, path)
    memory['baseline'] = process_memory(baseline.process.pid)
    assert all('privileged agent process' not in m['title'] and 'cache purge process' not in m['title']
               for m in memory['baseline']['members'])
finally:
    baseline.stop()
origin_calls.clear()

case = Run()
case.invalid(case.config.replace('write_buffer=2m', 'write_buffer=8m'), 'write_buffer <= cache/2')
case.invalid(case.config.replace('cache_purge_state_zone control 4m;', ''), 'must be configured together')
case.invalid(case.config.replace(str(case.path) + '/index', str(case.path) + '/cache-a/index'), 'outside cache path')
alias = case.path / 'database-alias'
alias.symlink_to(case.path / 'cache-a', target_is_directory=True)
case.invalid(case.config.replace(str(case.path) + '/index', str(alias)), 'outside cache path')
case.invalid('master_process off;\n' + case.config, 'master_process on')
checks.append('resource budgets, paired zone, cache-path/symlink overlap and single-process misuse rejected')
if os.geteuid() == 0 and a.worker_user and pwd.getpwnam(a.worker_user).pw_uid != 0:
    foreign = case.path / 'foreign-index'
    foreign.mkdir(mode=0o711)
    worker_uid = pwd.getpwnam(a.worker_user).pw_uid
    os.chown(foreign, 0, -1)
    case.invalid(case.config.replace(str(case.path / 'index'), str(foreign)), 'owned by the worker UID')
    assert foreign.stat().st_uid == 0 and not (foreign / 'CURRENT').exists()
    checks.append('a foreign-owned DB directory is rejected without changing its ownership or opening a DB')
owner = case.start()
try:
    expected = b'worker process'
    assert expected in Path(f'/proc/{owner}/cmdline').read_bytes()
    credentials = dict(line.split(':', 1) for line in Path(f'/proc/{owner}/status').read_text().splitlines())
    expected_uid = pwd.getpwnam(a.worker_user or 'root').pw_uid if os.geteuid() == 0 else os.geteuid()
    expected_gid = pwd.getpwnam(a.worker_user or 'root').pw_gid if os.geteuid() == 0 else os.getegid()
    assert list(map(int, credentials['Uid'].split())) == [expected_uid] * 4
    assert list(map(int, credentials['Gid'].split())) == [expected_gid] * 4
    assert all(int(credentials[key], 16) == 0 for key in ('CapInh', 'CapPrm', 'CapEff', 'CapAmb'))
    assert int(credentials['PPid']) == case.process.pid
    assert any(f'] {owner}#' in line and 'probe HTTP init_process called' in line
                   for line in (case.path / 'error.log').read_text().splitlines())
    checks.append('one ordinary HTTP worker owns the executor; other workers have no DB threads')

    children = list(map(int, Path(f'/proc/{case.process.pid}/task/{case.process.pid}/children').read_text().split()))
    for pid in children + [case.process.pid]:
        if pid != owner:
            assert len(list(Path(f'/proc/{pid}/task').iterdir())) == 1
    checks.append('real addon opens DB/control publisher only in ' + expected.decode())
    for path in ('/cache/a', '/cache/b', '/other'):
        warm(case, path)
    memory['component_empty'] = process_memory(case.process.pid)
    if os.geteuid() == 0 and a.worker_user:
        worker_uid = pwd.getpwnam(a.worker_user).pw_uid
        socket_path = case.path / 'index/component.sock'
        assert socket_path.stat().st_uid == worker_uid and socket_path.stat().st_mode & 0o777 == 0o600
        directory_mode = (case.path / 'index').stat().st_mode & 0o777
        assert (case.path / 'index').stat().st_uid == worker_uid and directory_mode == 0o700
        probe_code = '''import socket,struct,sys
s=socket.socket(socket.AF_UNIX);s.settimeout(2);s.connect(sys.argv[1]);print('connected',flush=True)
s.sendall(struct.pack('>4sHHQII',b'NCP1',1,1,1,0,0))
def read(n):
 b=b''
 while len(b)<n:
  p=s.recv(n-len(b));assert p;b+=p
 return b
h=read(24);assert struct.unpack('>I',h[20:])[0]==0
print(struct.unpack('>Q',read(struct.unpack('>I',h[16:20])[0])[:8])[0])
'''
        def peer(user):
            return subprocess.run(['runuser', '-u', user, '--', 'python3', '-c', probe_code, str(socket_path)],
                                  capture_output=True, text=True)
        allowed = peer(a.worker_user)
        assert allowed.returncode == 0 and str(owner) in allowed.stdout, (allowed.stdout, allowed.stderr)
        # Isolate credential checking from the Unix path permission check.
        (case.path / 'index').chmod(0o711)
        socket_path.chmod(0o666)
        try:
            denied = peer('daemon')
            assert denied.returncode != 0 and 'connected' in denied.stdout, (denied.stdout, denied.stderr)
        finally:
            socket_path.chmod(0o600)
            (case.path / 'index').chmod(directory_mode)
        checks.append('worker executor owns DB/socket as worker UID and rejects another connected UID with SO_PEERCRED')
    payload = text('native-resource-op') + b'\x01' + struct.pack('>Q', 1000) + b'\x02' + text('/cache/') + text('') + text('probe')
    receipt = ipc(case.path, 14, payload)
    assert receipt[len(payload) + 40] == 1
    assert struct.unpack('>I', receipt[len(payload) + 41:len(payload) + 45])[0] == 300000
    case.ready()
    memory['component_with_control'] = process_memory(case.process.pid)
    for path in ('/cache/a', '/cache/b'):
        assert case.get(path)[0] == 503 and origin_calls[path] == 1
    assert case.get('/other')[:2] == (200, 'HIT')
    checks.append('test precontent consumer blocks matching HITs across two zones; unrelated HIT continues')
    checks.append('accepted record stays PREPARING with default 300-second policy')
    for iteration in range(3):
        if iteration == 0:
            case.config = case.config.replace('cache_purge_state_zone control 4m;',
                                              'cache_purge_state_zone control 4m; cache_purge_propagation_timeout 123s;')
            case.conf.write_text(case.config)
        os.kill(case.process.pid, signal.SIGHUP)
        owner = case.ready(owner)
        if os.geteuid() == 0:
            assert (case.path / 'index').stat().st_uid == (pwd.getpwnam(a.worker_user).pw_uid if a.worker_user else 0)
        assert ipc(case.path, 15, text('native-resource-op')) == receipt
        assert case.get('/cache/a')[0] == 503
    checks.append('three HUP reloads preserve real addon SHM and durable operation')
    # New worker channel descriptors must not accumulate in the long-lived helper.
    before_fds = len(list(Path(f'/proc/{owner}/fd').iterdir()))
    for _ in range(3):
        children = list(map(int, Path(f'/proc/{case.process.pid}/task/{case.process.pid}/children').read_text().split()))
        worker = next(pid for pid in children if pid != owner and b'worker process' in Path(f'/proc/{pid}/cmdline').read_bytes())
        os.kill(worker, signal.SIGKILL)
        def worker_replaced():
            current = list(map(int, Path(f'/proc/{case.process.pid}/task/{case.process.pid}/children').read_text().split()))
            return worker not in current and sum(b'worker process' in Path(f'/proc/{pid}/cmdline').read_bytes() for pid in current) == 2
        until(worker_replaced)
    assert case.ready() == owner
    until(lambda: len(list(Path(f'/proc/{owner}/fd').iterdir())) <= before_fds)
    checks.append('non-owner worker respawns preserve owner identity without descriptor growth')
    os.kill(case.process.pid, signal.SIGUSR1)
    until(lambda: 'reopening logs' in (case.path / 'error.log').read_text())
    assert case.ready() == owner
    checks.append('master USR1 reopens worker logs without restarting DB')

    custom = text('custom-window') + b'\x01' + struct.pack('>Q', 1000) + b'\x02' + text('/never/') + text('') + text('probe')
    custom_receipt = ipc(case.path, 14, custom)
    assert struct.unpack('>I', custom_receipt[len(custom) + 41:len(custom) + 45])[0] == 123000
    assert ipc(case.path, 15, text('native-resource-op')) == receipt
    case.ready()
    checks.append('configured 123-second policy applies to new operations without rewriting existing windows')
    os.kill(owner, signal.SIGKILL)
    owner = case.ready(owner)
    assert case.get('/cache/b')[0] == 503
    checks.append('owner SIGKILL respawn rebuilds shared controls before opening gate')
    case.conf.write_text(case.config.replace('control 4m', 'control 8m'))
    os.kill(case.process.pid, signal.SIGHUP)
    until(lambda: 'requires a full restart' in (case.path / 'error.log').read_text())
    assert case.ready() == owner
    case.conf.write_text(case.config)
    checks.append('incompatible SHM resize reload is rejected while old owner stays available')
finally:
    case.stop()
assert not (case.path / 'index/component.sock').exists()
checks.append('owner exit closes real component and removes rendezvous socket')
case.start()
try:
    assert ipc(case.path, 15, text('native-resource-op')) == receipt
    assert case.get('/cache/a')[0] == 503
    checks.append('full nginx restart reconstructs barriers from durable operation ledger')
finally:
    case.stop()
draining = Run(workers=1)
old_owner = draining.start()
held = '/hold/' + str(draining.port)
hold_gates[held] = threading.Event()
connection = http.client.HTTPConnection('127.0.0.1', draining.port, timeout=10)
try:
    connection.request('GET', held)
    response = connection.getresponse()
    assert response.status == 200 and response.read(1) == b'A'
    os.kill(draining.process.pid, signal.SIGHUP)
    replacement = draining.ready(old_owner)
    assert replacement != old_owner and Path(f'/proc/{old_owner}').exists()
    assert b'shutting down' in Path(f'/proc/{old_owner}/cmdline').read_bytes()
    hold_gates[held].set()
    assert response.read() == b'B'
    connection.close()
    until(lambda: not Path(f'/proc/{old_owner}').exists())
    checks.append('owner releases DB during graceful drain without waiting for or interrupting a long-lived HTTP response')
finally:
    hold_gates[held].set()
    connection.close()
    draining.stop()
standby = Run()
with (standby.path / 'index/index.lock').open('w') as lease:
    if os.geteuid() == 0 and a.worker_user:
        os.chown(lease.name, pwd.getpwnam(a.worker_user).pw_uid, -1)
    fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
    standby.start(wait=False)
    def waiting():
        pids = Path(f'/proc/{standby.process.pid}/task/{standby.process.pid}/children').read_text().split()
        return any(b'worker process' in Path(f'/proc/{pid}/cmdline').read_bytes()
                   and len(list(Path(f'/proc/{pid}/task').iterdir())) == 2 for pid in pids)
    try:
        until(waiting)
        assert not (standby.path / 'index/CURRENT').exists()
    finally:
        start = time.monotonic()
        standby.stop(signal.SIGTERM)
        assert time.monotonic() - start < 3
    checks.append('owner executor lock wait is cancelled and joined during TERM')
if a.agent_patches:
    coexist = Run()
    coexist.conf.write_text(coexist.config.replace('http { access_log off;',
        'http { access_log off; cache_purge_test_enable_agent on;'))
    coexist_owner = coexist.start()
    try:
        def agent_pid():
            children = Path(f'/proc/{coexist.process.pid}/task/{coexist.process.pid}/children').read_text().split()
            return next((int(pid) for pid in children if b'privileged agent process' in Path(f'/proc/{pid}/cmdline').read_bytes()), 0)
        agent = until(agent_pid)
        assert agent != coexist_owner and len(list(Path(f'/proc/{agent}/task').iterdir())) == 1
        assert b'worker process' in Path(f'/proc/{coexist_owner}/cmdline').read_bytes()
        checks.append('independently enabled privileged agent coexists without opening DB or running its executor')
    finally:
        coexist.stop()
daemon = Run()
daemon.conf.write_text(daemon.config.replace('daemon off;', 'daemon on;'))
subprocess.run(daemon.command, check=True, capture_output=True)
master = until(lambda: int((daemon.path / 'pid').read_text()))
try:
    daemon_owner = daemon.ready()
    fields = dict(line.split(':', 1) for line in Path(f'/proc/{daemon_owner}/status').read_text().splitlines())
    assert int(fields['PPid']) == master
    os.kill(master, signal.SIGHUP)
    daemon_owner = daemon.ready(daemon_owner)
    checks.append('daemonized master directly owns the worker and replaces its executor on reload')
    os.kill(master, signal.SIGUSR2)
    def upgraded():
        pid = int((daemon.path / 'pid').read_text())
        return pid if pid != master else 0
    successor = until(upgraded)
    assert int((daemon.path / 'pid.oldbin').read_text()) == master
    def standby_successor():
        children = Path(f'/proc/{successor}/task/{successor}/children').read_text().split()
        for pid in children:
            if b'worker process' in Path(f'/proc/{pid}/cmdline').read_bytes():
                return int(pid) if len(list(Path(f'/proc/{pid}/task').iterdir())) == 2 else 0
        return 0
    until(standby_successor)
    assert daemon.ready() == daemon_owner
    os.kill(master, signal.SIGWINCH)
    daemon_owner = daemon.ready(daemon_owner)
    fields = dict(line.split(':', 1) for line in Path(f'/proc/{daemon_owner}/status').read_text().splitlines())
    assert int(fields['PPid']) == successor
    checks.append('binary upgrade starts a standby worker executor and transfers DB ownership when old workers drain')
    os.kill(successor, signal.SIGQUIT)
    until(lambda: int((daemon.path / 'pid').read_text()) == master)
    daemon.ready(daemon_owner)
    checks.append('binary upgrade rollback restores the old master worker executor through native restart')

finally:
    os.kill(master, signal.SIGQUIT)
    until(lambda: not (daemon.path / 'pid').exists())
    until(lambda: not (daemon.path / 'index/component.sock').exists())
sys.dont_write_bytecode = True
from client_cases import run_client_cases
client_result = run_client_cases(Run, root, checks)
for instance in runtime_cases:
    for name in ('error.log', 'stderr.log'):
        path = instance.path / name
        if path.exists():
            log = path.read_text()
            assert not any(marker in log for marker in ('AddressSanitizer', 'LeakSanitizer', 'runtime error:')), path
origin.shutdown()
checks = list(dict.fromkeys(checks))
result = {'root': str(root), 'nginx_revision': revision, 'dynamic': a.dynamic,
          'sanitizer': a.sanitize,
          'sanitizer_exclusions': ['nonnull-attribute', 'leak:ngx_event_process_init'] if a.sanitize else [],
          'owner_backend': 'worker-zero',
          'openresty_patches': patches, 'worker_user': a.worker_user,
          'rocksdb_linkage': 'shared' if a.shared_rocksdb else 'static', 'checks': checks,
          'process_group_memory': memory,
          'empty_component_pss_increment_kib': memory['component_empty']['pss_kib'] - memory['baseline']['pss_kib'],
          'library_sha256': {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                             for path in a.libraries.glob('libngx_http_cache_*.a')},
          'addon_sha256': {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in addon.iterdir() if path.is_file()},
          'probe_sha256': {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in probe.iterdir() if path.is_file()},
          'runtime_paths': [str(instance.path) for instance in runtime_cases],
          'worker_client': client_result,
          'scope': 'actual addon resource lifecycle and worker IPC; separate test consumers; no native file/304/index transaction or final purge API'}
a.output.write_text(json.dumps(result, indent=2) + '\n')
print('PASS', len(checks), 'worker owner checks;', 'results:', a.output, flush=True)
