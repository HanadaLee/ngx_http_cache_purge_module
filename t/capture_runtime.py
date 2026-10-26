#!/usr/bin/env python3
"""Real upstream cache fills through the key module, worker IPC and RocksDB."""
import argparse
import concurrent.futures
import hashlib
import http.client
import http.server
import io
import json
import os
from pathlib import Path
import signal
import socket
import socketserver
import struct
import subprocess
import tarfile
import tempfile
import threading
import time

p = argparse.ArgumentParser()
p.add_argument('--nginx', type=Path, required=True)
p.add_argument('--nginx-ref', default='HEAD')
p.add_argument('--core-working-tree', action='store_true', help='Apply the inspected core source delta to the archived ref')
p.add_argument('--key', type=Path, required=True)
p.add_argument('--libraries', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--dynamic', action='store_true')
p.add_argument('--sanitize', action='store_true')
p.add_argument('--expr', type=Path)
p.add_argument('--workers', type=int, default=2)
p.add_argument('--capacity', action='store_true')
a = p.parse_args()
repo = Path(__file__).resolve().parents[1]
root = Path(tempfile.mkdtemp(prefix='ncp-capture-'))
build, addon, key = root / 'nginx', root / 'addon', root / 'key'
for directory in (build, addon, key): directory.mkdir()
for source, destination in [(repo, addon), (a.key, key)]:
    for path in [source / 'config', *source.glob('*.c'), *source.glob('*.h')]:
        (destination / path.name).write_bytes(path.read_bytes().replace(b'\r\n', b'\n'))
revision = subprocess.check_output(['git', '-C', str(a.nginx), 'rev-parse', a.nginx_ref], text=True).strip()
archive = subprocess.check_output(['git', '-c', 'core.autocrlf=false', '-C', str(a.nginx), 'archive', revision])
subprocess.run(['tar', '-xf', '-', '-C', str(build)], input=archive, check=True)
source_patch = b''
if a.core_working_tree:
    source_patch = subprocess.check_output(['git', '-c', 'core.autocrlf=true', '-c', 'core.safecrlf=false',
                                           '-C', str(a.nginx), 'diff', a.nginx_ref, '--', 'src'])
    subprocess.run(['git', 'apply', '-'], cwd=build, input=source_patch, check=True)
env = os.environ.copy()
env['CACHE_PURGE_ROCKSDB_LIB_DIR'] = str(a.libraries)
env['CACHE_PURGE_ROCKSDB_LIBS'] = ('-lrocksdb' if a.dynamic else '/usr/lib/x86_64-linux-gnu/librocksdb.a') + ' -lcrypto -lsnappy -lz -lbz2 -llz4 -lzstd -lstdc++ -lm -pthread'
option = '--add-dynamic-module=' if a.dynamic else '--add-module='
configure = 'auto/configure' if (build / 'auto/configure').exists() else 'configure'
command = ['sh', configure, '--prefix=' + str(root / 'default'), '--with-debug', '--with-threads',
           '--with-http_slice_module']
if a.expr: command.append('--add-module=' + str(a.expr))
command += [option + str(key), option + str(addon)]
if a.sanitize:
    command += ['--with-cc-opt=-O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined -fno-sanitize=nonnull-attribute -fno-pie',
                '--with-ld-opt=-fsanitize=address,undefined -no-pie']
print('BUILD', root, flush=True)
with (root / 'build.log').open('w') as log:
    subprocess.run(command, cwd=build, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    subprocess.run(['make', '-j2'], cwd=build, stdout=log, stderr=subprocess.STDOUT, check=True)
with tarfile.open(fileobj=io.BytesIO(archive)) as source:
    sources = [m for m in source if m.isfile() and m.name.startswith('src/')]
    for member in sources:
        expected = (a.nginx / member.name).read_bytes().replace(b'\r\n', b'\n') if a.core_working_tree else source.extractfile(member).read()
        assert (build / member.name).read_bytes() == expected

calls, policies = [], {}
capacity_gate = threading.Event()
slice_body = b'0123456789' * 300
class Origin(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_): pass
    def do_GET(self):
        path = self.path.split('?', 1)[0]
        calls.append(path)
        if path == '/capacity': capacity_gate.wait(20)
        body = path.encode() if not path.startswith('/slice') else slice_body
        if path == '/vary': body += self.headers.get('Accept-Language', '').encode()
        if path == '/large': body *= 40000
        status, content_range = 200, None
        if path.startswith('/slice'):
            start, end = map(int, self.headers['Range'][6:].split('-')); end = min(end, len(body) - 1)
            content_range = f'bytes {start}-{end}/{len(body)}'; body = body[start:end + 1]; status = 206
        self.send_response(status)
        self.send_header('Cache-Control', 'max-age=3600')
        self.send_header('Cache-Tag', policies.get(path, 'news, red, news'))
        if path == '/vary': self.send_header('Vary', 'Accept-Language')
        if content_range: self.send_header('Content-Range', content_range)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        try: self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError): pass

http.server.ThreadingHTTPServer.request_queue_size = 256
origin = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Origin)
threading.Thread(target=origin.serve_forever, daemon=True).start()

class Gateway(socketserver.BaseRequestHandler):
    def read(self, size):
        data = b''
        while len(data) < size:
            part = self.request.recv(size - len(data))
            if not part: raise EOFError('gateway request ended early')
            data += part
        return data
    def handle(self):
        self.request.settimeout(5)
        name, ident = self.server.protocol, 1
        if name == 'fastcgi':
            while True:
                version, kind, ident, size, padding, _ = struct.unpack('!BBHHBB', self.read(8))
                assert version == 1
                self.read(size + padding)
                if kind == 5 and size == 0: break
        elif name == 'uwsgi':
            _, size, _ = struct.unpack('<BHB', self.read(4)); self.read(size)
        else:
            digits = b''
            while (c := self.read(1)) != b':':
                digits += c; assert len(digits) <= 10
            self.read(int(digits)); assert self.read(1) == b','
        body = (name + ':body').encode()
        response = (b'Status: 200 OK\r\nContent-Type: text/plain\r\nCache-Control: max-age=3600\r\nCache-Tag: '
                    + name.encode() + b'\r\nContent-Length: ' + str(len(body)).encode() + b'\r\n\r\n' + body)
        if name == 'fastcgi':
            def frame(kind, value): return struct.pack('!BBHHBB', 1, kind, ident, len(value), 0, 0) + value
            response = frame(6, response) + frame(6, b'') + frame(3, b'\0' * 8)
        self.request.sendall(response)

gateways = {}
for name in ('fastcgi', 'scgi', 'uwsgi'):
    server = socketserver.ThreadingTCPServer(('127.0.0.1', 0), Gateway)
    server.daemon_threads = True; server.protocol = name; gateways[name] = server
    threading.Thread(target=server.serve_forever, daemon=True).start()
with socket.socket() as s: s.bind(('127.0.0.1', 0)); port = s.getsockname()[1]
run = root / 'run'; run.mkdir(); (run / 'logs').mkdir()
for directory in ('cache-a', 'cache-b', 'index'): (run / directory).mkdir()
(run / 'index').chmod(0o700)
gateway_paths, gateway_locations = [], []
for name, server in gateways.items():
    (run / ('cache-' + name)).mkdir()
    gateway_paths.append(f'{name}_cache_path {run}/cache-{name} levels=1:2 keys_zone={name}:1m;')
    gateway_locations.append(f'''location /{name}/ {{
      {name}_cache {name}; {name}_cache_key $cache_key_full; {name}_cache_valid 200 1h;
      {name}_param REQUEST_METHOD GET; {name}_param REQUEST_URI $uri;
      cache_purge_key $cache_key_base; cache_purge_url $cache_key_url;
      {name}_pass 127.0.0.1:{server.server_address[1]};
    }}''')
loads = ''.join(f'load_module {build}/objs/{name}.so;\n' for name in
                ('ngx_http_cache_key_module', 'ngx_http_cache_purge_module')) if a.dynamic else ''
extension = any(line.split() == ['#define', 'NGX_RESTY_EXT', '1']
                for line in (build / 'objs/ngx_auto_config.h').read_text().splitlines())
slice_config = 'slice on; slice_size 1024;' if extension else 'slice 1024;'
config = loads + f'''daemon off; master_process on; worker_processes {a.workers};
pid {run}/pid; error_log {run}/error.log debug;
thread_pool default threads=2;
events {{ worker_connections 256; }}
http {{ access_log off; client_body_temp_path {run}/body; proxy_temp_path {run}/temp;
 cache_purge_store {run}/index cache=8m write_buffer=2m;
 cache_purge_state_zone capture 4m;
 proxy_cache_path {run}/cache-a levels=1:2 keys_zone=a:1m;
 proxy_cache_path {run}/cache-b levels=1:2 keys_zone=b:1m;
 {' '.join(gateway_paths)}
 map $arg_z $zone {{ default a; b b; }}
 cache_key_namespace tenant; cache_key_uri $uri; cache_key_misc $arg_m; cache_key_range $slice_range;
 server {{ listen 127.0.0.1:{port};
  proxy_cache $zone; proxy_cache_key $cache_key_full;
  proxy_cache_valid 200 206 1h; proxy_cache_bypass $arg_bypass;
  proxy_buffers 8 4k; proxy_buffer_size 4k;
  cache_purge_tag $upstream_http_cache_tag;
  cache_purge_tag_namespace $cache_key_namespace;
  add_header X-Status $upstream_cache_status always;
  {' '.join(gateway_locations)}
  location / {{ cache_purge_key $cache_key_base; cache_purge_url $cache_key_url;
                proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /large {{ cache_purge_key $cache_key_base; cache_purge_url $cache_key_url;
                     aio threads; aio_write on; proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /slice {{ cache_purge_key $cache_key_base; cache_purge_url $cache_key_url;
                    {slice_config} proxy_set_header Range $slice_range;
                    proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /raw {{ proxy_cache_key "$uri|$arg_m"; cache_purge_key $uri;
                  cache_purge_tag_namespace rawtenant;
                  proxy_pass http://127.0.0.1:{origin.server_port}; }}
 }}
}}
'''
(run / 'nginx.conf').write_text(config)
if a.sanitize:
    suppression = run / 'leaks.supp'; suppression.write_text('leak:ngx_event_process_init\n')
    env['LSAN_OPTIONS'] = 'suppressions=' + str(suppression)
processes = []
def start():
    log = (run / f'stdout-{len(processes)}.log').open('w')
    process = subprocess.Popen([str(build / 'objs/nginx'), '-p', str(run) + '/', '-c', str(run / 'nginx.conf')],
                               env=env, stdout=log, stderr=subprocess.STDOUT)
    processes.append((process, log))
    return process
def stop(process):
    if process.poll() is None:
        process.send_signal(signal.SIGQUIT)
        try: assert process.wait(timeout=12) == 0
        except subprocess.TimeoutExpired:
            process.kill(); process.wait(); raise
def until(fn, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if processes and processes[-1][0].poll() is not None:
            raise AssertionError((run / f'stdout-{len(processes) - 1}.log').read_text())
        value = fn()
        if value: return value
        time.sleep(.01)
    raise AssertionError('condition timeout')
def get(path, headers=None):
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=25)
    conn.request('GET', path, headers={'Host': 'cache.test', **(headers or {})})
    response = conn.getresponse(); result = response.status, response.getheader('X-Status'), response.read()
    conn.close(); return result
def string(value):
    if isinstance(value, str): value = value.encode()
    return struct.pack('>I', len(value)) + value
class Decode:
    def __init__(self, value): self.data = value; self.at = 0
    def take(self, n):
        assert n <= len(self.data) - self.at
        result = self.data[self.at:self.at+n]; self.at += n; return result
    def number(self, n): return int.from_bytes(self.take(n), 'big')
    def string(self): return self.take(self.number(4))
    def end(self): assert self.at == len(self.data)
def rpc(op, payload=b''):
    with socket.socket(socket.AF_UNIX) as s:
        s.settimeout(8); s.connect(str(run / 'index/component.sock'))
        s.sendall(struct.pack('>4sHHQII', b'NCP1', 1, op, 1, len(payload), 0) + payload)
        def read(n):
            value = b''
            while len(value) < n:
                part = s.recv(n - len(value)); assert part; value += part
            return value
        magic, version, operation, ident, length, result = struct.unpack('>4sHHQII', read(24))
        body = read(length)
        assert (magic, version, operation, ident, result) == (b'NCP1', 1, op, 1, 0), (result, body)
        return body
def query(value, kind=1, scope=''):
    d = Decode(rpc(8, bytes([kind]) + string(value) + string(scope) + string('') + bytes([32])))
    count, done = d.number(1), d.number(1); d.string()
    found = [(d.take(16), d.number(8), d.number(1)) for _ in range(count)]
    d.end(); assert done
    return found
def records(path, count):
    found = until(lambda: (v if len(v := query('http://cache.test' + path)) == count
                           and all(item[2] == 2 for item in v) else None))
    return [record(item[0]) for item in found]
def record(ident):
    d = Decode(rpc(2, ident)); state, revision = d.number(1), d.number(8); assert d.take(16) == ident
    names = ('key', 'url', 'host', 'namespace', 'root', 'zone', 'path', 'range', 'vary')
    result = {name: d.string() for name in names}
    result.update(id=ident, state=state, revision=revision, metadata=d.string())
    result['tags'] = [d.string() for _ in range(d.number(1))]
    result['flags'] = d.take(2); result['components'] = [d.string() for _ in range(6)]; d.end()
    return result
def pending():
    return rpc(9, string('') + bytes([32]))[0]
def ready_file(item):
    path = Path(os.fsdecode(item['root'])) / os.fsdecode(item['path'])
    marker = os.getxattr(path, 'user.ngx_cache_purge.id')
    assert len(marker) == 40 and marker[:8] == b'NCF1\0\0\0\2' and marker[8:24] == item['id'], (path, marker.hex(), item['id'].hex())
    assert item['state'] == 2 and item['metadata'].startswith(b'NFP1')
    return path

checks = []
process = start()
try:
    until(lambda: (run / 'index/component.sock').exists())
    assert get('/asset') == (200, 'MISS', b'/asset')
    first = records('/asset', 1)[0]; ready_file(first)
    assert first['key'] == b'tenant\x1fhttp\x1fcache.test\x1f/asset\x1f'
    assert first['namespace'] == b'tenant' and set(first['tags']) == {b'news', b'red'}
    assert get('/asset')[1] == 'HIT' and calls.count('/asset') == 1
    checks.append('real upstream fill is durably indexed with the native key snapshot, scoped tags and READY marker')
    assert get('/asset?z=b')[1] == 'MISS'
    both = records('/asset', 2)
    assert {x['zone'] for x in both} == {b'a', b'b'}
    assert {x['key'] for x in both} == {first['key']}
    checks.append('one URL and key retain physical instances in two cache zones')
    assert get('/asset?m=alt')[1] == 'MISS'
    assert len({x['key'] for x in records('/asset', 3)}) == 2
    checks.append('URL lookup covers different native key misc values')
    assert get('/vary', {'Accept-Language': 'en'})[2] == b'/varyen'; records('/vary', 1)
    assert get('/vary', {'Accept-Language': 'de'})[2] == b'/varyde'
    varied = records('/vary', 2)
    assert len({x['path'] for x in varied}) == 2 and all(x['vary'] == b'Accept-Language' for x in varied)
    checks.append('native Vary secondary files are published under the same base key')
    assert get('/slice')[2] == slice_body
    sliced = records('/slice', 3)
    assert len({x['key'] for x in sliced}) == 1 and len({x['range'] for x in sliced}) == 3
    assert get('/slice')[2] == slice_body and calls.count('/slice') == 3
    checks.append('slice subrequests publish all ranges under one base key')
    assert get('/large')[2] == b'/large' * 40000
    ready_file(records('/large', 1)[0])
    checks.append('threaded native temporary writes finish before durable publication')
    assert get('/tags')[1] == 'MISS'; old = records('/tags', 1)[0]
    policies['/tags'] = 'blue'
    assert get('/tags?bypass=1')[1] == 'BYPASS'
    until(lambda: (v := records('/tags', 1)[0])['id'] != old['id'])
    new = records('/tags', 1)[0]; ready_file(new)
    assert new['tags'] == [b'blue'] and record(old['id'])['state'] == 3
    assert new['id'] in [x[0] for x in query('blue', kind=5, scope='tenant')]
    assert not query('blue', kind=5, scope='another-tenant')
    checks.append('BYPASS replacement captures before origin, retires the prior instance and updates scoped TAG membership')
    assert get('/raw?m=native')[1] == 'MISS'
    raw = until(lambda: (v if (v := query('/raw|native')) and all(item[2] == 2 for item in v) else None))
    item = record(raw[0][0]); ready_file(item)
    assert item['key'] == b'/raw' and item['namespace'] == b'rawtenant'
    checks.append('ordinary expressions work without a companion snapshot; default URL follows the actual native key bytes')
    policies['/tag-limit'] = ','.join('tag' + str(i) for i in range(65))
    assert get('/tag-limit')[1] == 'MISS'
    time.sleep(.1)
    assert not query('http://cache.test/tag-limit') and get('/tag-limit')[1] == 'MISS'
    policies['/tag-limit'] = ''
    assert get('/tag-limit')[1] == 'MISS'; assert records('/tag-limit', 1)[0]['tags'] == []
    checks.append('excess TAG metadata skips caching; an empty TAG set can subsequently publish')
    for name in gateways:
        path = '/' + name + '/fill'
        assert get(path) == (200, 'MISS', (name + ':body').encode())
        item = records(path, 1)[0]; ready_file(item)
        assert item['zone'] == name.encode() and item['tags'] == [name.encode()]
        assert get(path)[1] == 'HIT'
        checks.append(name + ': actual native file, key snapshot and scoped TAG are durably indexed')
    if a.capacity:
        count = a.workers * 64 + 8
        with concurrent.futures.ThreadPoolExecutor(count) as pool:
            responses = [pool.submit(get, '/capacity?m=' + str(i)) for i in range(count)]
            try:
                until(lambda: calls.count('/capacity') == count, timeout=15)
                assert get('/asset')[1] == 'HIT'
            finally: capacity_gate.set()
            values = [future.result() for future in responses]
        assert all(value[0] == 200 for value in values)
        assert all(value[1] == 'MISS' for value in values)
        def recovered():
            value = get('/after-capacity')
            assert value[0] == 200 and value[1] in ('MISS', 'HIT')
            matches = query('http://cache.test/after-capacity')
            return matches if matches and all(item[2] == 2 for item in matches) else None
        until(recovered, timeout=10)
        ready_file(records('/after-capacity', 1)[0])
        assert get('/after-capacity')[1] == 'HIT'
        checks.append('publication pressure preserves ordinary HITs and response bodies; subsequent fills recover')
    until(lambda: pending() == 0)
    before = {x[0] for x in query('http://cache.test/asset')}
    stop(process); process = start()
    until(lambda: (run / 'index/component.sock').exists())
    assert {x[0] for x in query('http://cache.test/asset')} == before
    assert get('/asset')[1] == 'HIT'
    checks.append('full nginx restart reuses persisted file relations without rebuilding the index')
finally:
    capacity_gate.set()
    for process, log in processes: stop(process); log.close()
    origin.shutdown(); origin.server_close()
    for server in gateways.values(): server.shutdown(); server.server_close()
logs = (run / 'error.log').read_text() + ''.join(p.read_text() for p in run.glob('stdout-*.log'))
assert not any(marker in logs for marker in ('[alert]', 'AddressSanitizer', 'LeakSanitizer', 'runtime error:', 'exited on signal'))
result = {'root': str(root), 'run': str(run), 'nginx_revision': revision,
          'core_source_files_unchanged': len(sources) if not a.core_working_tree else 0,
          'core_working_tree_files_match': len(sources) if a.core_working_tree else 0,
          'core_source_patch_sha256': hashlib.sha256(source_patch).hexdigest(),
          'dynamic': a.dynamic, 'sanitizer': a.sanitize, 'expr': bool(a.expr), 'extension': extension,
          'workers': a.workers, 'checks': checks,
          'sources': {group: {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.iterdir()}
                      for group, directory in [('addon', addon), ('key', key)]},
          'library_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in a.libraries.glob('libngx_cache_purge_*.a')},
          'scope': 'Real key provider and upstream PREPARE/rename/FINISH over bounded worker IPC; no completed purge API or FENCE activation'}
a.output.write_text(json.dumps(result, indent=2) + '\n')
print('PASS', len(checks), 'native index capture checks')
