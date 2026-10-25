#!/usr/bin/env python3
"""Build the real addon; exercise managed lifecycle and a separate test guard."""
import argparse
import hashlib
import http.server
import json
import os
import signal
import socket
import struct
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--nginx', type=Path, required=True)
p.add_argument('--libraries', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--build-root', type=Path)
p.add_argument('--dynamic', action='store_true')
p.add_argument('--shared-rocksdb', action='store_true', help='Use the system shared engine when its static archive is not PIC')
a = p.parse_args()
source = Path(__file__).resolve().parents[1]
root = a.build_root or Path(tempfile.mkdtemp(prefix='ncp-addon-', dir='/tmp'))
root.mkdir(exist_ok=True)
addon, probe, build = root / 'addon', root / 'probe', root / 'nginx'
for path in (addon, probe, build):
    path.mkdir(exist_ok=True)
for path in [source / 'config', *source.glob('*.c'), *source.glob('*.h')]:
    (addon / path.name).write_bytes(path.read_bytes().replace(b'\r\n', b'\n'))
for path in (source / 't/runtime_probe').iterdir():
    (probe / path.name).write_bytes(path.read_bytes().replace(b'\r\n', b'\n'))
revision = subprocess.check_output(['git', '-C', str(a.nginx), 'rev-parse', 'HEAD'], text=True).strip()
if not (build / 'src').exists():
    with (root / 'source.tar').open('wb') as f:
        subprocess.run(['git', '-C', str(a.nginx), 'archive', revision], stdout=f, check=True)
    subprocess.run(['tar', '-xf', str(root / 'source.tar'), '-C', str(build)], check=True)
env = os.environ.copy()
env['CACHE_PURGE_ROCKSDB_LIB_DIR'] = str(a.libraries)
engine = '-lrocksdb' if a.shared_rocksdb else '/usr/lib/x86_64-linux-gnu/librocksdb.a'
env['CACHE_PURGE_ROCKSDB_LIBS'] = engine + ' -lcrypto -lsnappy -lz -lbz2 -llz4 -lzstd -lstdc++ -lm -pthread'
env['CACHE_PURGE_PROBE_HEADERS'] = str(addon)
print('BUILD', root, flush=True)
with (root / 'build.log').open('w') as f:
    configure = 'auto/configure' if (build / 'auto/configure').exists() else 'configure'
    module_option = '--add-dynamic-module=' if a.dynamic else '--add-module='
    subprocess.run(['sh', configure, '--prefix=' + str(root / 'run'), '--without-http_rewrite_module',
                    '--without-http_gzip_module', module_option + str(addon), module_option + str(probe)],
                   cwd=build, env=env, stdout=f, stderr=subprocess.STDOUT, check=True)
    subprocess.run(['make', '-j2'], cwd=build, stdout=f, stderr=subprocess.STDOUT, check=True)
    if a.dynamic:
        subprocess.run(['make', 'modules', '-j2'], cwd=build, stdout=f, stderr=subprocess.STDOUT, check=True)
binary = build / 'objs/nginx'
checks = []
negative = subprocess.run(['cc', '-c', '-DNGX_RESTY_EXT=0',
    *[item for path in ('objs', 'src/core', 'src/event', 'src/event/modules', 'src/os/unix', 'src/http', 'src/http/modules')
      for item in ('-I', str(build / path))], '-I', str(addon), str(addon / 'ngx_cache_purge_runtime.c'),
    '-o', str(root / 'without-exit-hook.o')], capture_output=True)
assert negative.returncode and b'requires the cache manager exit capability' in negative.stderr
checks.append('build without the required manager-exit capability is rejected')
origin_calls = {}


class Origin(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        origin_calls[self.path] = origin_calls.get(self.path, 0) + 1
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
    with socket.socket(socket.AF_UNIX) as client:
        client.settimeout(3)
        client.connect(str(path / 'index/component.sock'))
        client.sendall(struct.pack('>4sHHQII', b'NCP1', 1, op, 1, len(data), 0) + data)

        def read(n):
            result = b''
            while n:
                chunk = client.recv(n)
                assert chunk, 'unexpected IPC EOF'
                result += chunk
                n -= len(chunk)
            return result

        _, _, actual, _, size, code = struct.unpack('>4sHHQII', read(24))
        result = read(size)
        assert actual == op and code == 0, (code, result)
        return result


def until(predicate, timeout=10):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            value = predicate()
            if value:
                return value
        except (OSError, AssertionError):
            pass
        time.sleep(.01)
    raise AssertionError('runtime deadline')


class Run:
    def __init__(self):
        self.path = Path(tempfile.mkdtemp(prefix='case-', dir=root))
        (self.path / 'logs').mkdir(exist_ok=True)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            self.port = sock.getsockname()[1]
        user = 'user root;' if os.geteuid() == 0 else ''
        loads = ''
        if a.dynamic:
            loads = (f'load_module {build}/objs/ngx_http_cache_purge_module.so;\n'
                     f'load_module {build}/objs/ngx_http_cache_purge_runtime_probe_module.so;\n')
        self.config = loads + user + f'''
daemon off; worker_processes 2; pid {self.path}/pid;
error_log {self.path}/error.log notice;
events {{ worker_connections 128; }}
http {{ access_log off;
cache_purge_store {self.path}/index cache=8m write_buffer=2m;
cache_purge_state_zone control 4m;
proxy_cache_path {self.path}/cache-a keys_zone=a:1m;
proxy_cache_path {self.path}/cache-b keys_zone=b:1m;
server {{ listen 127.0.0.1:{self.port};
location /cache/a {{ cache_purge_test_reader on; proxy_cache a; proxy_cache_key $uri;
proxy_pass http://127.0.0.1:{origin.server_port}; add_header X-Cache $upstream_cache_status always; }}
location /cache/b {{ cache_purge_test_reader on; proxy_cache b; proxy_cache_key $uri;
proxy_pass http://127.0.0.1:{origin.server_port}; add_header X-Cache $upstream_cache_status always; }}
location /other {{ cache_purge_test_reader on; proxy_cache a; proxy_cache_key $uri;
proxy_pass http://127.0.0.1:{origin.server_port}; add_header X-Cache $upstream_cache_status always; }}
}} }}'''
        self.conf = self.path / 'nginx.conf'
        self.conf.write_text(self.config)
        self.command = [str(binary), '-p', str(self.path) + '/', '-c', str(self.conf)]
        subprocess.run(self.command + ['-t'], capture_output=True, check=True)
        assert not (self.path / 'index/CURRENT').exists()
        checks.append('real addon config test initializes no database or DB threads')

    def invalid(self, content, expected):
        self.conf.write_text(content)
        result = subprocess.run(self.command + ['-t'], capture_output=True)
        assert result.returncode and expected in result.stderr.decode(), result.stderr
        self.conf.write_text(self.config)

    def start(self):
        self.err = (self.path / 'stderr.log').open('a')
        self.process = subprocess.Popen(self.command, stderr=self.err)
        return self.ready()

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

    def stop(self):
        self.process.send_signal(signal.SIGQUIT)
        try:
            assert self.process.wait(timeout=10) == 0
        finally:
            self.err.close()


case = Run()
case.invalid(case.config.replace('write_buffer=2m', 'write_buffer=8m'), 'write_buffer <= cache/2')
case.invalid(case.config.replace('cache_purge_state_zone control 4m;', ''), 'must be configured together')
case.invalid(case.config.replace(str(case.path) + '/index', str(case.path) + '/cache-a/index'), 'outside cache path')
alias = case.path / 'database-alias'
alias.symlink_to(case.path / 'cache-a', target_is_directory=True)
case.invalid(case.config.replace(str(case.path) + '/index', str(alias)), 'outside cache path')
case.invalid('master_process off;\n' + case.config, 'master_process on')
checks.append('resource budgets, paired zone, cache-path/symlink overlap and single-process misuse rejected')
owner = case.start()
try:
    assert b'cache manager process' in Path(f'/proc/{owner}/cmdline').read_bytes()
    children = list(map(int, Path(f'/proc/{case.process.pid}/task/{case.process.pid}/children').read_text().split()))
    for pid in children + [case.process.pid]:
        if pid != owner:
            assert len(list(Path(f'/proc/{pid}/task').iterdir())) == 1
    checks.append('real addon opens DB/control publisher only in cache_manager')
    for path in ('/cache/a', '/cache/b', '/other'):
        assert case.get(path)[:2] == (200, 'MISS')
        assert case.get(path)[:2] == (200, 'HIT')
    payload = text('native-resource-op') + b'\x01' + struct.pack('>Q', 1000) + b'\x02' + text('/cache/') + text('') + text('probe')
    receipt = ipc(case.path, 14, payload)
    assert receipt[len(payload) + 40] == 1
    assert struct.unpack('>I', receipt[len(payload) + 41:len(payload) + 45])[0] == 300000
    case.ready()
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
        assert ipc(case.path, 15, text('native-resource-op')) == receipt
        assert case.get('/cache/a')[0] == 503
    checks.append('three HUP reloads preserve real addon SHM and durable operation')
    custom = text('custom-window') + b'\x01' + struct.pack('>Q', 1000) + b'\x02' + text('/never/') + text('') + text('probe')
    custom_receipt = ipc(case.path, 14, custom)
    assert struct.unpack('>I', custom_receipt[len(custom) + 41:len(custom) + 45])[0] == 123000
    assert ipc(case.path, 15, text('native-resource-op')) == receipt
    case.ready()
    checks.append('configured 123-second policy applies to new operations without rewriting existing windows')
    os.kill(owner, signal.SIGKILL)
    owner = case.ready(owner)
    assert case.get('/cache/b')[0] == 503
    checks.append('manager SIGKILL respawn rebuilds shared controls before opening gate')
    case.conf.write_text(case.config.replace('control 4m', 'control 8m'))
    os.kill(case.process.pid, signal.SIGHUP)
    until(lambda: 'requires a full restart' in (case.path / 'error.log').read_text())
    assert case.ready() == owner
    case.conf.write_text(case.config)
    checks.append('incompatible SHM resize reload is rejected while old owner stays available')
finally:
    case.stop()
assert not (case.path / 'index/component.sock').exists()
checks.append('manager exit hook closes real component and removes rendezvous socket')
case.start()
try:
    assert ipc(case.path, 15, text('native-resource-op')) == receipt
    assert case.get('/cache/a')[0] == 503
    checks.append('full nginx restart reconstructs barriers from durable operation ledger')
finally:
    case.stop()
origin.shutdown()
result = {'root': str(root), 'nginx_revision': revision, 'dynamic': a.dynamic,
          'rocksdb_linkage': 'shared' if a.shared_rocksdb else 'static', 'checks': checks,
          'library_sha256': {name: hashlib.sha256((a.libraries / f'libngx_cache_purge_{name}.a').read_bytes()).hexdigest()
                             for name in ('component', 'coordinator', 'control', 'operations', 'index', 'store')},
          'scope': 'actual addon resource lifecycle; separate test precontent consumer; no native file/304/write hooks or final purge API'}
a.output.write_text(json.dumps(result, indent=2) + '\n')
print('PASS', json.dumps(result), flush=True)
