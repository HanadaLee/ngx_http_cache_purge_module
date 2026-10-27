#!/usr/bin/env python3
"""Real upstream cache fills through the key module, worker IPC and RocksDB."""
import argparse
import array
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
(addon / 'rocksdb').mkdir()
for path in (repo / 'rocksdb').iterdir():
    if path.suffix in ('.c', '.h'):
        (addon / 'rocksdb' / path.name).write_bytes(path.read_bytes().replace(b'\r\n', b'\n'))
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
validation_tags = {'/revalidate-replace': 'new-tag', '/revalidate-clear': ''}
conditional_paths = set()
conditional_calls = []
conditional_policies = {}
validation_waiting, validation_release = threading.Event(), threading.Event()
late_waiting, late_release = threading.Event(), threading.Event()
retry_waiting, retry_release = threading.Event(), threading.Event()
stream_waiting, stream_release = threading.Event(), threading.Event()
fresh_bodies = {}
redirects = {'/redirect-cache': '/redirect-old', '/redirect-tag': '/redirect-old',
             '/redirect-unknown': '/redirect-old', '/redirect-named': '@redirect_named',
             '/redirect-stale': '/redirect-old', '/redirect-revalidated': '/redirect-old'}
negative_outage = True
origin_failures = set()
capacity_gate = threading.Event()
slice_body = b'0123456789' * 300
class Origin(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_): pass
    def do_GET(self):
        path = self.path.split('?', 1)[0]
        calls.append(path)
        if path == '/negative-cache' and negative_outage:
            self.connection.shutdown(socket.SHUT_RDWR)
            return
        if path == '/negative-timeout': time.sleep(.25)
        if path in origin_failures:
            self.send_response(500); self.send_header('Content-Length', '0'); self.end_headers(); return
        if path == '/no-store-stream':
            self.send_response(200); self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Length', '4096'); self.end_headers()
            self.wfile.write(b'x'); self.wfile.flush(); stream_waiting.set()
            assert stream_release.wait(15)
            self.wfile.write(b'x' * 4095); return
        if (path.startswith('/revalidate-') or path in conditional_paths) and self.headers.get('If-None-Match') == '"capture-v1"':
            conditional_calls.append(path)
            if path == '/revalidate-race':
                validation_waiting.set()
                assert validation_release.wait(15)
            self.send_response(304); self.send_header('Cache-Control', conditional_policies.get(path, 'max-age=60'))
            if path in validation_tags: self.send_header('Cache-Tag', validation_tags[path])
            self.end_headers(); return
        if path == '/redirect-revalidated' and self.headers.get('If-None-Match') == '"capture-v1"':
            self.send_response(304); self.send_header('Cache-Control', 'max-age=60'); self.end_headers(); return
        if path == '/capacity': capacity_gate.wait(20)
        body = path.encode() if not path.startswith('/slice') else slice_body
        body = fresh_bodies.get(path, body)
        if path == '/revalidate-race' and calls.count(path) >= 3:
            retry_waiting.set()
            assert retry_release.wait(15)
        if path == '/late-fill' and calls.count(path) == 1:
            body = b'old-late-entity'
            late_waiting.set()
            assert late_release.wait(15)
        if path == '/vary': body += self.headers.get('Accept-Language', '').encode()
        if path == '/large': body *= 40000
        status, content_range = (500 if path.startswith('/error-') else 200), None
        if path in ('/error-disk-502', '/error-disk-504'): status = int(path[-3:])
        if path.startswith('/slice'):
            start, end = map(int, self.headers['Range'][6:].split('-')); end = min(end, len(body) - 1)
            content_range = f'bytes {start}-{end}/{len(body)}'; body = body[start:end + 1]; status = 206
        self.send_response(status)
        self.send_header('Cache-Control', 'no-store' if path == '/error-no-store' else 'max-age=' + ('1' if path.startswith('/revalidate-') or path in ('/redirect-stale', '/redirect-revalidated') else '3600'))
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('ETag', '"capture-v1"')
        if path in redirects:
            self.send_header('X-Accel-Redirect', redirects[path])
            self.send_header('X-Cached-Only', 'hidden-on-redirect')
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
                ('ngx_http_cache_key_module', 'ngx_http_cache_rocksdb_module', 'ngx_http_cache_purge_module')) if a.dynamic else ''
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
  location /alias/ {{ cache_key_uri /alias/shared;
                     proxy_cache_revalidate on;
                     cache_purge_key $cache_key_base;
                     cache_purge_url "$scheme://$host$uri";
                     proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /invalidate/ {{ cache_purge_key $cache_key_base; cache_purge_url $cache_key_url;
                          proxy_cache_revalidate on;
                          proxy_cache_use_stale error timeout http_500 updating;
                          proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /revalidate- {{ cache_purge_key $cache_key_base; cache_purge_url $cache_key_url;
                          proxy_cache_revalidate on; proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /revalidate-expression {{ cache_purge_key $cache_key_base; cache_purge_url $cache_key_url;
                          cache_purge_tag "$upstream_http_cache_tag, expression";
                          proxy_cache_revalidate on; proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location = /negative-cache {{
      cache_purge_key $cache_key_base; cache_purge_url $cache_key_url;
      proxy_cache_valid 502 1h; error_page 502 = /negative-error;
      proxy_pass http://127.0.0.1:{origin.server_port};
  }}
  location = /negative-error {{ return 200 "negative-old"; }}
  location = /negative-timeout {{
      cache_purge_key $cache_key_base; cache_purge_url $cache_key_url;
      proxy_cache_valid 504 1h; error_page 504 = /negative-error;
      proxy_read_timeout 50ms;
      proxy_pass http://127.0.0.1:{origin.server_port};
  }}
  location = /error-no-store {{
      cache_purge_key $cache_key_base; cache_purge_url $cache_key_url;
      proxy_intercept_errors on; error_page 404 = /negative-error;
      proxy_cache_valid 500 1h;
      proxy_pass http://127.0.0.1:{origin.server_port};
  }}
  location = /error-memory {{
      cache_purge_key $cache_key_base; cache_purge_url $cache_key_url;
      proxy_intercept_errors on; error_page 500 = /negative-error;
      proxy_cache_valid 500 1h;
      proxy_pass http://127.0.0.1:{origin.server_port};
  }}
  location = /error-disk {{
      cache_purge_key $cache_key_base; cache_purge_url $cache_key_url;
      proxy_cache_valid 500 1h;
      proxy_pass http://127.0.0.1:{origin.server_port};
  }}
  location = /error-disk-unmatched {{
      cache_purge_key $cache_key_base; cache_purge_url $cache_key_url;
      proxy_intercept_errors on; error_page 404 = /negative-error;
      proxy_cache_valid 500 1h;
      proxy_pass http://127.0.0.1:{origin.server_port};
  }}
  location /error-disk- {{
      cache_purge_key $cache_key_base; cache_purge_url $cache_key_url;
      proxy_cache_valid 500 502 504 1h;
      proxy_pass http://127.0.0.1:{origin.server_port};
  }}
  location ~ ^/redirect-(cache|tag|unknown|named|stale|revalidated)$ {{
      cache_purge_key $cache_key_base; cache_purge_url $cache_key_url;
      proxy_ignore_headers X-Accel-Redirect;
      proxy_cache_revalidate on;
      proxy_cache_use_stale error timeout http_500 updating;
      proxy_pass http://127.0.0.1:{origin.server_port};
  }}
  location = /redirect-old {{ return 200 "redirect-old"; }}
  location = /redirect-new {{ return 200 "redirect-new"; }}
  location @redirect_named {{ return 200 "redirect-named"; }}
 }}
}}
'''
if extension and a.expr:
    error_when = f'''location /error-when/ {{
      cache_purge_key $cache_key_base; cache_purge_url $cache_key_url;
      proxy_intercept_errors on;
      expr intercept_error = $arg_intercept 1;
      when intercept_error {{ error_page 500 = /negative-error; }}
      proxy_cache_valid 500 1h;
      proxy_pass http://127.0.0.1:{origin.server_port};
    }}
    '''
    config = config.replace('location = /negative-error', error_when + 'location = /negative-error')
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
def get(path, headers=None, method='GET', details=False):
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=25)
    conn.request(method, path, headers={'Host': 'cache.test', **(headers or {})})
    response = conn.getresponse(); result = response.status, (dict(response.getheaders()) if details else response.getheader('X-Status')), response.read()
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
def rpc(op, payload=b'', fds=(), receive_fd=False):
    with socket.socket(socket.AF_UNIX) as s:
        s.settimeout(8); s.connect(str(run / 'index/component.sock'))
        packet = struct.pack('>4sHHQII', b'NCP1', 1, op, 1, len(payload), 0) + payload
        if fds:
            assert s.sendmsg([packet[:1]], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', fds))]) == 1
            s.sendall(packet[1:])
        else:
            s.sendall(packet)
        def read(n):
            value = b''
            while len(value) < n:
                part = s.recv(n - len(value)); assert part; value += part
            return value
        received = array.array('i')
        try:
            first = b''
            if receive_fd:
                first, control, flags, _ = s.recvmsg(1, socket.CMSG_SPACE(8), socket.MSG_CMSG_CLOEXEC)
                for level, kind, data in control:
                    assert (level, kind) == (socket.SOL_SOCKET, socket.SCM_RIGHTS)
                    received.frombytes(data)
                assert len(received) == 1 and not flags & socket.MSG_CTRUNC
            magic, version, operation, ident, length, result = struct.unpack('>4sHHQII', first + read(24 - len(first)))
            body = read(length)
            assert (magic, version, operation, ident, result) == (b'NCP1', 1, op, 1, 0), (result, body)
            return (body, received.pop()) if receive_fd else body
        finally:
            for fd in received: os.close(fd)

def physical_remove(item, outcome=1):
    path = Path(os.fsdecode(item['root'])) / os.fsdecode(item['path'])
    _, lease = rpc(17, string(str(path)), receive_fd=True)
    try:
        removed = rpc(24, item['id'] + struct.pack('>Q', item['revision']), (lease,))
        assert len(removed) == 29 and removed[:4] == struct.pack('>I', outcome)
        assert removed[4:20] == item['id'] and removed[-1] == 3
    finally:
        os.close(lease)
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

def fence(name, kind, value, scope='', mode=2):
    previous = struct.unpack('>IQ', rpc(16))[1]
    payload = (string(name) + bytes([mode]) + struct.pack('>Q', int(time.time() * 1000))
               + bytes([kind]) + string(value) + string(scope) + string('read-test'))
    receipt = rpc(14, payload)
    assert receipt[len(payload) + 40] == 1
    until(lambda: (state := struct.unpack('>IQ', rpc(16)))[0] == 6 and state[1] != previous)
    return int.from_bytes(receipt[len(payload):len(payload) + 8], 'big')

def writers(sequence):
    try:
        value = rpc(23, struct.pack('>Q', sequence))
    except AssertionError as error:
        # Concurrent registration updates return BUSY, never a partial count.
        if error.args and isinstance(error.args[0], tuple) and error.args[0][0] == 6:
            return None
        raise
    assert len(value) == 4
    return int.from_bytes(value, 'big')
def ready_file(item):
    path = Path(os.fsdecode(item['root'])) / os.fsdecode(item['path'])
    marker = os.getxattr(path, 'user.ngx_cache_purge.id')
    assert len(marker) == 40 and marker[:8] == b'NCF1\0\0\0\2' and marker[8:24] == item['id'], (path, marker.hex(), item['id'].hex())
    assert item['state'] == 2 and item['metadata'].startswith(b'NFP1')
    return path

def watermark(item):
    metadata = item['metadata']
    if metadata.startswith(b'NFR1'):
        assert int.from_bytes(metadata[60:64], 'big') == len(metadata) - 64
        metadata = metadata[64:]
    assert metadata.startswith(b'NFP1')
    at = 44 + int.from_bytes(metadata[40:44], 'big')
    size = int.from_bytes(metadata[at:at + 4], 'big')
    value = metadata[at + 4:]
    if value.startswith(b'NFW2'):
        assert size == len(value) and int.from_bytes(value[28:32], 'big') == len(value) - 32
        value = value[32:]
        size = len(value)
    assert size == 28 and len(value) == size and value[:4] == b'NFW1'
    return value[4:20], int.from_bytes(value[20:], 'big')

checks = []
process = start()
try:
    until(lambda: (run / 'index/component.sock').exists())
    until(lambda: struct.unpack('>IQ', rpc(16))[0] == 6)
    assert get('/asset') == (200, 'MISS', b'/asset')
    first = records('/asset', 1)[0]; ready_file(first)
    ledger, sequence = watermark(first)
    assert ledger != bytes(16) and sequence == 0
    assert first['key'] == b'tenant\x1fhttp\x1fcache.test\x1f/asset\x1f'
    assert first['namespace'] == b'tenant' and set(first['tags']) == {b'news', b'red'}
    assert get('/asset')[1] == 'HIT' and calls.count('/asset') == 1
    checks.append('real upstream fill is durably indexed with the native key snapshot, scoped tags and READY marker')
    assert get('/asset', method='HEAD') == (200, 'HIT', b'')
    assert get('/asset', {'Range': 'bytes=1-3'}) == (206, 'HIT', b'ass')
    assert get('/asset', {'If-None-Match': '"capture-v1"'}) == (304, 'HIT', b'')
    checks.append('deferred descriptor inspection preserves native HEAD, Range and client conditional output filters')
    assert get('/remove-native')[1] == 'MISS'
    assert get('/remove-native?z=b')[1] == 'MISS'
    removed_instances = records('/remove-native', 2)
    removed_paths = [ready_file(item) for item in removed_instances]
    for item in removed_instances: physical_remove(item)
    assert all(not path.exists() for path in removed_paths)
    assert query('http://cache.test/remove-native') == []
    fresh_bodies['/remove-native'] = b'new-after-removal'
    assert get('/remove-native') == (200, 'MISS', b'new-after-removal')
    assert get('/remove-native?z=b') == (200, 'MISS', b'new-after-removal')
    replacements = records('/remove-native', 2)
    for item in removed_instances: physical_remove(record(item['id']), outcome=3)
    assert {item['id'] for item in records('/remove-native', 2)} == {item['id'] for item in replacements}
    assert get('/remove-native') == (200, 'HIT', b'new-after-removal')
    assert get('/remove-native?z=b') == (200, 'HIT', b'new-after-removal')
    checks.append('physical removal of two indexed native zone instances refills fresh entities and old-instance retries preserve their replacements')
    assert get('/background-native')[1] == 'MISS'
    assert get('/background-native?z=b')[1] == 'MISS'
    background_files = records('/background-native', 2)
    background_paths = [ready_file(item) for item in background_files]
    fence('background-native', 1, 'http://cache.test/background-native')
    stop(process)
    # Activation is an offline fixture, not a public effective HTTP receipt.
    subprocess.run([str(a.libraries / 'cache_purge_files_test'),
                    '--activate-background', str(run / 'index'), 'background-native'],
                   check=True, timeout=15, stdout=subprocess.DEVNULL)
    process = start()
    until(lambda: (run / 'index/component.sock').exists())
    until(lambda: struct.unpack('>IQ', rpc(16))[0] == 6)
    until(lambda: all(not path.exists() for path in background_paths))
    def background_done():
        d = Decode(rpc(15, string('background-native')))
        d.string(); d.number(1); d.number(8); d.number(1)
        d.string(); d.string(); d.string()
        for _ in range(5): d.number(8)
        stage = d.number(1); d.number(4); d.number(1); d.string()
        done = d.number(1); d.number(8); d.number(8)
        pending, failed = d.number(8), d.number(8)
        d.number(8); d.string(); d.end()
        return stage == 3 and done and not pending and not failed
    until(background_done)
    assert query('http://cache.test/background-native') == []
    fresh_bodies['/background-native'] = b'new-after-background'
    assert get('/background-native') == (200, 'BYPASS', b'new-after-background')
    assert get('/background-native?z=b') == (200, 'BYPASS', b'new-after-background')
    assert {item['id'] for item in records('/background-native', 2)}.isdisjoint(
        {item['id'] for item in background_files})
    checks.append('persisted active Delete automatically processes both native zones after restart without per-file IPC, then retains its FENCE while node cleanup is pending')
    assert get('/alias/one') == (200, 'MISS', b'/alias/one')
    alias = records('/alias/one', 1)[0]; ready_file(alias)
    assert get('/alias/two') == (200, 'HIT', b'/alias/one')
    assert get('/alias/three') == (200, 'HIT', b'/alias/one')
    assert records('/alias/two', 1)[0]['id'] == alias['id'] == records('/alias/three', 1)[0]['id']
    assert calls.count('/alias/two') == calls.count('/alias/three') == 0
    checks.append('native HITs register distinct observed URL aliases for one key without issuing extra origin requests')
    fence('read-discovered-alias', 1, 'http://cache.test/alias/two')
    fresh_bodies['/alias/one'] = b'fresh-alias-entity'
    assert get('/alias/one') == (200, 'BYPASS', b'fresh-alias-entity')
    old_alias = alias['id']
    alias = until(lambda: (item if (item := records('/alias/one', 1)[0])['id'] != old_alias else None))
    ready_file(alias)
    assert records('/alias/two', 1)[0]['id'] == alias['id']
    assert get('/alias/three') == (200, 'HIT', b'fresh-alias-entity')
    checks.append('a URL FENCE on a persisted third-party alias invalidates another request URL while a post-push entity can HIT under that FENCE')
    fence('read-discovered-alias-prefix', 3, 'http://cache.test/alias/tw')
    fresh_bodies['/alias/one'] = b'prefix-fresh-alias'
    assert get('/alias/one') == (200, 'BYPASS', b'prefix-fresh-alias')
    old_alias = alias['id']
    alias = until(lambda: (item if (item := records('/alias/one', 1)[0])['id'] != old_alias else None))
    ready_file(alias)
    assert get('/alias/three') == (200, 'HIT', b'prefix-fresh-alias')
    checks.append('bytewise URL prefix FENCEs cover persisted aliases even when neither the current nor the original stored URL matches')
    conditional_paths.add('/alias/one')
    original_alias = alias
    fence('invalidate-discovered-alias', 1, 'http://cache.test/alias/two', mode=1)
    assert get('/alias/one') == (200, 'REVALIDATED', b'prefix-fresh-alias')
    alias = records('/alias/one', 1)[0]
    assert alias['id'] == original_alias['id'] and alias['revision'] == original_alias['revision'] + 2
    assert watermark(alias) == watermark(original_alias)
    assert get('/alias/three') == (200, 'HIT', b'prefix-fresh-alias')
    checks.append('Invalidate on another persisted URL alias supports conditional 304 without replacing the shared entity')
    conditional_paths.add('/invalidate/304')
    policies['/invalidate/304'] = 'invalidate-304'
    assert get('/invalidate/304') == (200, 'MISS', b'/invalidate/304')
    invalidated = records('/invalidate/304', 1)[0]
    target = ready_file(invalidated); old_inode = target.stat().st_ino
    old_full = watermark(invalidated)
    fence('force-invalidate-304', 5, 'invalidate-304', 'tenant', mode=1)
    result = get('/invalidate/304')
    assert result == (200, 'REVALIDATED', b'/invalidate/304'), result
    refreshed = records('/invalidate/304', 1)[0]
    assert refreshed['id'] == invalidated['id'] and refreshed['revision'] == invalidated['revision'] + 2
    assert target.stat().st_ino == old_inode and watermark(refreshed) == old_full
    assert conditional_calls.count('/invalidate/304') == 1
    assert get('/invalidate/304') == (200, 'HIT', b'/invalidate/304')
    checks.append('TAG Invalidate forces conditional origin validation; durable 304 retains the entity and full-origin proof, then permits HIT under the live FENCE')
    fence('delete-after-invalidate-304', 5, 'invalidate-304', 'tenant', mode=2)
    fresh_bodies['/invalidate/304'] = b'new-full-entity'
    assert get('/invalidate/304') == (200, 'BYPASS', b'new-full-entity')
    until(lambda: records('/invalidate/304', 1)[0]['id'] != invalidated['id'])
    assert conditional_calls.count('/invalidate/304') == 1
    assert get('/invalidate/304') == (200, 'HIT', b'new-full-entity')
    checks.append('Delete after successful Invalidate cannot reuse the conditional proof and requires a new full entity')
    policies['/invalidate/200'] = 'invalidate-200'
    assert get('/invalidate/200')[1] == 'MISS'
    old = records('/invalidate/200', 1)[0]
    fence('force-invalidate-200', 5, 'invalidate-200', 'tenant', mode=1)
    fresh_bodies['/invalidate/200'] = b'replaced-by-200'
    assert get('/invalidate/200') == (200, 'EXPIRED', b'replaced-by-200')
    until(lambda: records('/invalidate/200', 1)[0]['id'] != old['id'])
    assert get('/invalidate/200') == (200, 'HIT', b'replaced-by-200')
    checks.append('Invalidate accepts a successful full 200 replacement and then reuses the new indexed entity')
    policies['/invalidate/failure'] = 'invalidate-failure'
    assert get('/invalidate/failure')[1] == 'MISS'
    failed_original = records('/invalidate/failure', 1)[0]
    fence('force-invalidate-failure', 5, 'invalidate-failure', 'tenant', mode=1)
    origin_failures.add('/invalidate/failure')
    result = get('/invalidate/failure')
    assert result == ((500, 'EXPIRED', b'') if extension else (200, 'STALE', b'/invalidate/failure')), result
    assert records('/invalidate/failure', 1)[0]['revision'] == failed_original['revision']
    origin_failures.remove('/invalidate/failure')
    checks.append('NGX_RESTY_EXT Invalidate suppresses configured failure stale while plain nginx retains the agreed stale downgrade without validating the old entity')
    conditional_paths.add('/invalidate/policy')
    conditional_policies['/invalidate/policy'] = 'max-age=1, must-revalidate'
    policies['/invalidate/policy'] = 'invalidate-policy'
    assert get('/invalidate/policy')[1] == 'MISS'; records('/invalidate/policy', 1)
    fence('force-invalidate-policy', 5, 'invalidate-policy', 'tenant', mode=1)
    assert get('/invalidate/policy') == (200, 'REVALIDATED', b'/invalidate/policy')
    origin_failures.add('/invalidate/policy')
    time.sleep(2.1)
    result = get('/invalidate/policy')
    assert result == ((500, 'EXPIRED', b'') if extension else (200, 'STALE', b'/invalidate/policy')), result
    origin_failures.remove('/invalidate/policy')
    checks.append('a successful 304 persists a newly received origin must-revalidate policy while clearing only the purge reason in the extension build')
    assert get('/unmarked')[1] == 'MISS'
    unmarked = records('/unmarked', 1)[0]
    os.removexattr(ready_file(unmarked), 'user.ngx_cache_purge.id')
    assert get('/unmarked') == (200, 'BYPASS', b'/unmarked')
    until(lambda: records('/unmarked', 1)[0]['id'] != unmarked['id'])
    assert get('/unmarked')[1] == 'HIT' and calls.count('/unmarked') == 2
    assert record(unmarked['id'])['state'] == 3
    checks.append('a native HIT with no publication marker refills and replaces its old index instance before later reuse')
    assert get('/read-wait')[1] == 'MISS'
    waiting_file = ready_file(records('/read-wait', 1)[0])
    waiting_marker = os.getxattr(waiting_file, 'user.ngx_cache_purge.id')
    os.setxattr(waiting_file, 'user.ngx_cache_purge.id', waiting_marker[:7] + b'\1' + waiting_marker[8:])
    release = threading.Timer(.15, os.setxattr, (waiting_file, 'user.ngx_cache_purge.id', waiting_marker))
    before_wait = time.monotonic(); release.start()
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=5) as held:
            held.sendall(b'GET /read-wait HTTP/1.1\r\nHost: cache.test\r\nConnection: close\r\n\r\n')
            held.settimeout(.05)
            try:
                early = held.recv(1)
                raise AssertionError(('unverified response bytes escaped', early))
            except socket.timeout: pass
            held.settimeout(5)
            response = http.client.HTTPResponse(held); response.begin()
            assert response.status == 200 and response.getheader('X-Status') == 'HIT'
            assert response.read() == b'/read-wait'
        assert time.monotonic() - before_wait >= .1
    finally: release.join()
    assert calls.count('/read-wait') == 1
    checks.append('PREPARED-marker reads wait asynchronously until READY without exposing bytes or fetching a new entity')
    os.setxattr(waiting_file, 'user.ngx_cache_purge.id', waiting_marker[:7] + b'\1' + waiting_marker[8:])
    with socket.create_connection(('127.0.0.1', port), timeout=5) as abandoned:
        abandoned.sendall(b'GET /read-wait HTTP/1.1\r\nHost: cache.test\r\nConnection: close\r\n\r\n')
        time.sleep(.04)
    os.setxattr(waiting_file, 'user.ngx_cache_purge.id', waiting_marker)
    assert get('/read-wait')[1] == 'HIT'
    checks.append('client abandonment during descriptor inspection drains without losing later HITs')
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
    assert get('/vary', {'Accept-Language': 'en'})[1] == 'HIT'
    assert get('/vary', {'Accept-Language': 'de'})[1] == 'HIT'
    checks.append('native Vary secondary files are published under the same base key')
    german = next(item for item in varied if ready_file(item).read_bytes().endswith(b'/varyde'))
    os.removexattr(ready_file(german), 'user.ngx_cache_purge.id')
    assert get('/vary', {'Accept-Language': 'en'})[1] == 'HIT'
    assert get('/vary', {'Accept-Language': 'de'}) == (200, 'BYPASS', b'/varyde')
    until(lambda: german['id'] not in {item['id'] for item in records('/vary', 2)})
    assert get('/vary', {'Accept-Language': 'de'})[1] == 'HIT'
    checks.append('read admission inspects the selected Vary file and refills only the unmarked variant')
    assert get('/slice')[2] == slice_body
    sliced = records('/slice', 3)
    assert len({x['key'] for x in sliced}) == 1 and len({x['range'] for x in sliced}) == 3
    assert get('/slice')[2] == slice_body and calls.count('/slice') == 3
    checks.append('slice subrequests publish all ranges under one base key')
    middle = next(item for item in sliced if item['range'] == b'bytes=1024-2047')
    os.removexattr(ready_file(middle), 'user.ngx_cache_purge.id')
    assert get('/slice')[2] == slice_body
    until(lambda: middle['id'] not in {item['id'] for item in records('/slice', 3)})
    assert get('/slice')[2] == slice_body and calls.count('/slice') == 4
    checks.append('an unmarked slice refills independently without mixing output or refetching verified slices')
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
        os.removexattr(ready_file(item), 'user.ngx_cache_purge.id')
        assert get(path) == (200, 'BYPASS', (name + ':body').encode())
        until(lambda: records(path, 1)[0]['id'] != item['id'])
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
        until(lambda: pending() == 0)
        def recovered():
            value = get('/after-capacity')
            assert value[0] == 200 and value[2] == b'/after-capacity', value
            assert value[1] in ('MISS', 'HIT', 'BYPASS'), value
            matches = query('http://cache.test/after-capacity')
            return matches if matches and all(item[2] == 2 for item in matches) and pending() == 0 else None
        until(recovered, timeout=10)
        ready_file(records('/after-capacity', 1)[0])
        assert get('/after-capacity')[1] == 'HIT'
        checks.append('publication pressure preserves ordinary HITs and response bodies; subsequent fills recover')
    until(lambda: pending() == 0)
    before = {x[0] for x in query('http://cache.test/asset')}
    stop(process); process = start()
    until(lambda: (run / 'index/component.sock').exists())
    until(lambda: struct.unpack('>IQ', rpc(16))[0] == 6)
    assert {x[0] for x in query('http://cache.test/asset')} == before
    assert get('/asset')[1] == 'HIT'
    assert {watermark(record(ident))[0] for ident in before} == {ledger}
    checks.append('full nginx restart reuses persisted file relations without rebuilding the index')
    assert get('/alias/three') == (200, 'HIT', b'prefix-fresh-alias')
    assert records('/alias/two', 1)[0]['id'] == alias['id']
    assert calls.count('/alias/three') == 0
    checks.append('HIT-discovered aliases and their satisfied FENCE coverage survive a full nginx restart without index rebuilding')
    validation_files = {}
    policies['/revalidate-race'] = 'race-old'
    assert get('/revalidate-race') == (200, 'MISS', b'/revalidate-race')
    race_original = records('/revalidate-race', 1)[0]
    for path in ('/revalidate-retain', '/revalidate-replace', '/revalidate-clear', '/revalidate-expression'):
        assert get(path) == (200, 'MISS', path.encode())
        item = records(path, 1)[0]
        target = ready_file(item)
        validation_files[path] = item, target, target.stat().st_ino, target.read_bytes()
    time.sleep(2.1)
    for path, (original, target, inode, contents) in validation_files.items():
        result = get(path)
        assert result == (200, 'REVALIDATED', path.encode()), (path, result)
        item = records(path, 1)[0]
        assert item['id'] == original['id'] and item['revision'] == original['revision'] + 2
        assert item['metadata'].startswith(b'NFR1') and target.stat().st_ino == inode
        header_size = contents.index(b'\nKEY:')
        assert target.read_bytes()[header_size:] == contents[header_size:]
        expected = ({b'new-tag'} if path.endswith('-replace') else set() if path.endswith('-clear')
                    else {b'expression'} if path.endswith('-expression') else set(original['tags']))
        assert set(item['tags']) == expected, (path, item['tags'])
        assert get(path) == (200, 'HIT', path.encode()) and calls.count(path) == 2
    checks.append('304 validation preserves the file/entity, durably refreshes TTL and handles absent, replaced, empty and expression TAGs')
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        request = pool.submit(get, '/revalidate-race')
        assert validation_waiting.wait(5)
        race_sequence = fence('invalidate-during-304', 5, 'race-old', 'tenant', mode=1)
        until(lambda: (count := writers(race_sequence)) is not None and count > 0)
        policies['/revalidate-race'] = 'race-new'
        validation_tags['/revalidate-race'] = 'race-new'
        fresh_bodies['/revalidate-race'] = b'fresh-entity'
        validation_release.set()
        assert retry_waiting.wait(5)
        until(lambda: (count := writers(race_sequence)) is not None and count > 0)
        retry_release.set()
        assert request.result(timeout=15) == (200, 'BYPASS', b'fresh-entity')
    until(lambda: records('/revalidate-race', 1)[0]['id'] != race_original['id'])
    assert get('/revalidate-race') == (200, 'HIT', b'fresh-entity')
    assert calls.count('/revalidate-race') == 3
    until(lambda: writers(race_sequence) == 0)
    checks.append('a TAG FENCE accepted while origin 304 is pending forces a fresh entity even when the 304 replaces that TAG')
    policies['/late-fill'] = 'late-tag'
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        request = pool.submit(get, '/late-fill')
        assert late_waiting.wait(5)
        late_sequence = fence('late-fill-rule', 5, 'late-tag', 'tenant')
        until(lambda: (count := writers(late_sequence)) is not None and count > 0)
        publication = struct.unpack('>IQ', rpc(16))[1]
        process.send_signal(signal.SIGHUP)
        def handed_off():
            try:
                state, current = struct.unpack('>IQ', rpc(16))
                return state == 6 and current != publication
            except (OSError, EOFError):
                return False
        until(handed_off)
        until(lambda: (count := writers(late_sequence)) is not None and count > 0)
        fresh_bodies['/late-fill'] = b'new-late-entity'
        late_release.set()
        assert request.result(timeout=15) == (200, 'MISS', b'old-late-entity')
    native_key = b'tenant\x1fhttp\x1fcache.test\x1f/late-fill\x1f\x1f'
    digest = hashlib.md5(native_key).hexdigest()
    target = run / 'cache-a' / digest[-1] / digest[-3:-1] / digest
    until(target.exists)
    marker = os.getxattr(target, 'user.ngx_cache_purge.id')
    late = until(lambda: (item if (item := record(marker[8:24]))['state'] == 3 else None))
    assert os.getxattr(target, 'user.ngx_cache_purge.id')[7] == 1
    assert query('http://cache.test/late-fill') == []
    until(lambda: writers(late_sequence) == 0)
    assert get('/late-fill') == (200, 'BYPASS', b'new-late-entity')
    fresh = records('/late-fill', 1)[0]; ready_file(fresh)
    assert watermark(fresh)[0] == ledger and watermark(fresh)[1] > watermark(late)[1]
    assert get('/late-fill') == (200, 'HIT', b'new-late-entity') and calls.count('/late-fill') == 2
    checks.append('a pre-push late origin fill is retired, while a post-push entity carries durable coverage and reuses HIT under the same TAG FENCE')
    checks.append('worker reload and executor handoff retain the old worker origin registration until its file transaction finishes')
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        request = pool.submit(get, '/no-store-stream')
        assert stream_waiting.wait(5)
        cutoff = fence('noncacheable-stream-cutoff', 1, 'http://cache.test/never-cached')
        until(lambda: writers(cutoff) == 0)
        assert not request.done()
        stream_release.set()
        assert request.result(timeout=10) == (200, 'MISS', b'x' * 4096)
    assert query('http://cache.test/no-store-stream') == []
    checks.append('origin registrations persist through stale-to-fresh retry and release after publication or a confirmed noncacheable response')
    assert get('/fenced-url')[1] == 'MISS'; records('/fenced-url', 1)
    assert get('/fenced-url?z=b')[1] == 'MISS'; records('/fenced-url', 2)
    assert get('/fenced-url')[1] == 'HIT'
    fence('read-url', 1, 'http://cache.test/fenced-url')
    assert get('/fenced-url') == (200, 'BYPASS', b'/fenced-url')
    assert get('/fenced-url?z=b') == (200, 'BYPASS', b'/fenced-url')
    assert get('/asset')[1] == 'HIT'
    checks.append('native read admission checks the published URL FENCE across both zones while unrelated cached files remain HITs')
    assert get('/fenced-prefix/a')[1] == 'MISS'; records('/fenced-prefix/a', 1)
    fence('read-prefix', 3, 'http://cache.test/fenced-prefix/')
    assert get('/fenced-prefix/a')[1] == 'BYPASS'
    checks.append('native read admission applies bytewise prefix FENCEs to cached files')
    assert get('/fenced-key')[1] == 'MISS'
    fenced_key = records('/fenced-key', 1)[0]['key']
    fence('read-key', 0, fenced_key)
    assert get('/fenced-key')[1] == 'BYPASS'
    checks.append('native read admission applies exact base-key FENCEs')
    policies['/fenced-tag'] = 'only-this-tag'
    assert get('/fenced-tag')[1] == 'MISS'; records('/fenced-tag', 1)
    fence('read-wrong-tag', 5, 'only-this-tag', 'other-tenant')
    assert get('/fenced-tag')[1] == 'HIT'
    fence('read-tag', 5, 'only-this-tag', 'tenant')
    assert get('/fenced-tag')[1] == 'BYPASS'
    until(lambda: watermark(records('/fenced-tag', 1)[0])[1] > 0)
    assert get('/fenced-tag')[1] == 'HIT' and calls.count('/fenced-tag') == 2
    assert get('/asset')[1] == 'HIT'
    checks.append('native TAG admission uses persisted entity tags with namespace isolation')
    policies['/revalidate-covered'] = 'covered-tag'
    assert get('/revalidate-covered')[1] == 'MISS'
    original = records('/revalidate-covered', 1)[0]
    fence('covered-tag-rule', 5, 'covered-tag', 'tenant')
    assert get('/revalidate-covered')[1] == 'BYPASS'
    current = until(lambda: (item if (item := records('/revalidate-covered', 1)[0])['id'] != original['id'] else None))
    covered = watermark(current)
    time.sleep(2.1)
    assert get('/revalidate-covered') == (200, 'REVALIDATED', b'/revalidate-covered')
    assert watermark(records('/revalidate-covered', 1)[0]) == covered
    assert get('/revalidate-covered')[1] == 'HIT' and calls.count('/revalidate-covered') == 3
    checks.append('ordinary 304 validation preserves prior durable coverage while its already-satisfied TAG FENCE remains active')
    assert get('/negative-cache') == (200, 'MISS', b'negative-old')
    assert get('/negative-cache') == (200, 'MISS', b'negative-old')
    assert calls.count('/negative-cache') == 2
    assert query('http://cache.test/negative-cache') == []
    negative_outage = False
    fence('read-negative', 1, 'http://cache.test/negative-cache')
    negative_result = get('/negative-cache')
    assert negative_result == (200, 'BYPASS', b'/negative-cache'), negative_result
    assert calls.count('/negative-cache') == 3
    assert get('/negative-timeout') == (200, 'MISS', b'negative-old')
    assert get('/negative-timeout') == (200, 'MISS', b'negative-old')
    assert calls.count('/negative-timeout') == 2 and query('http://cache.test/negative-timeout') == []
    checks.append('transport errors do not create memory-only cache entries, and URL FENCE still protects subsequent fills')
    assert get('/error-memory') == (200, 'MISS', b'negative-old')
    assert get('/error-memory') == (200, 'MISS', b'negative-old')
    assert calls.count('/error-memory') == 2 and query('http://cache.test/error-memory') == []
    checks.append('intercepted upstream errors never publish memory-only nodes even with cacheable response headers')
    for uri, status in (('/error-disk', 500), ('/error-disk-unmatched', 500),
                        ('/error-disk-502', 502), ('/error-disk-504', 504)):
        assert get(uri) == (status, 'MISS', uri.encode())
        ready_file(records(uri, 1)[0])
        assert get(uri) == (status, 'HIT', uri.encode()) and calls.count(uri) == 1
    assert get('/error-no-store') == (500, 'MISS', b'/error-no-store')
    assert get('/error-no-store') == (500, 'MISS', b'/error-no-store')
    assert query('http://cache.test/error-no-store') == []
    if extension and a.expr:
        assert get('/error-when/off?intercept=0') == (500, 'MISS', b'/error-when/off')
        ready_file(records('/error-when/off', 1)[0])
        assert get('/error-when/off?intercept=0') == (500, 'HIT', b'/error-when/off')
        assert get('/error-when/on?intercept=1') == (200, 'MISS', b'negative-old')
        assert get('/error-when/on?intercept=1') == (200, 'MISS', b'negative-old')
        assert calls.count('/error-when/on') == 2 and query('http://cache.test/error-when/on') == []
    checks.append('ordinary disk-backed errors remain indexed and reusable, including an unmatched error_page status')
    error_cutoff = fence('error-writers-drained', 1, 'http://cache.test/never-error-request')
    until(lambda: writers(error_cutoff) == 0)
    assert get('/redirect-cache') == (200, 'MISS', b'/redirect-cache')
    ready_file(records('/redirect-cache', 1)[0])
    policies['/redirect-tag'] = 'redirect-tag'
    for uri in ('/redirect-tag', '/redirect-unknown', '/redirect-named', '/redirect-stale', '/redirect-revalidated'):
        assert get(uri) == (200, 'MISS', uri.encode())
        ready_file(records(uri, 1)[0])
    stop(process)
    (run / 'nginx.conf').write_text(config.replace('proxy_ignore_headers X-Accel-Redirect;', ''))
    process = start()
    until(lambda: (run / 'index/component.sock').exists())
    until(lambda: struct.unpack('>IQ', rpc(16))[0] == 6)
    redirected = get('/redirect-cache', details=True)
    assert redirected[0] == 200 and redirected[2] == b'redirect-old', redirected
    assert redirected[1]['X-Status'] == 'HIT' and redirected[1]['Cache-Control'] == 'max-age=3600'
    assert 'X-Cached-Only' not in redirected[1] and 'ETag' not in redirected[1], redirected
    assert calls.count('/redirect-cache') == 1
    assert get('/redirect-named') == (200, 'HIT', b'redirect-named')
    assert get('/redirect-named', method='HEAD') == (200, 'HIT', b'')
    assert calls.count('/redirect-named') == 1
    checks.append('verified cached redirects preserve named locations, HEAD and the native redirect-header allowlist')
    origin_failures.add('/redirect-stale')
    redirects['/redirect-revalidated'] = '/redirect-new'
    time.sleep(2.1)
    assert get('/redirect-stale') == (200, 'STALE', b'redirect-old')
    assert calls.count('/redirect-stale') == 2
    checks.append('cached redirect admission safely closes a failed origin attempt before resuming native stale redirect behavior')
    result = get('/redirect-revalidated')
    assert result == (200, 'REVALIDATED', b'redirect-old'), result
    assert get('/redirect-revalidated') == (200, 'HIT', b'redirect-old')
    assert calls.count('/redirect-revalidated') == 2
    checks.append('durable 304 validation reuses the verified cached redirect and later serves HIT without an extra origin fetch')
    unknown = records('/redirect-unknown', 1)[0]
    os.removexattr(ready_file(unknown), 'user.ngx_cache_purge.id')
    redirects['/redirect-unknown'] = '/redirect-new'
    assert get('/redirect-unknown') == (200, 'BYPASS', b'redirect-new')
    assert calls.count('/redirect-unknown') == 2
    checks.append('an unmarked cached redirect cannot select its old target before descriptor inspection')
    redirects['/redirect-tag'] = '/redirect-new'
    fence('read-redirect-tag', 5, 'redirect-tag', 'tenant')
    assert get('/redirect-tag') == (200, 'BYPASS', b'redirect-new')
    assert calls.count('/redirect-tag') == 2
    checks.append('scoped TAG FENCE matches persisted metadata before a cached redirect executes')
    redirects['/redirect-cache'] = '/redirect-new'
    fence('read-redirect', 1, 'http://cache.test/redirect-cache')
    redirect_result = get('/redirect-cache')
    assert redirect_result == (200, 'BYPASS', b'redirect-new'), redirect_result
    assert calls.count('/redirect-cache') == 2
    checks.append('URL FENCE bypasses a cached X-Accel-Redirect before it selects the old target')
    fence('read-host', 4, 'cache.test')
    assert get('/asset')[1] == 'BYPASS'
    checks.append('native read admission applies host FENCEs; receipts still remain PREPARING pending complete protocol coverage')
finally:
    capacity_gate.set()
    validation_release.set()
    late_release.set()
    retry_release.set()
    stream_release.set()
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
          'sources': {group: {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.rglob('*') if p.is_file()}
                      for group, directory in [('addon', addon), ('key', key)]},
          'library_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in a.libraries.glob('libngx_http_cache_*.a')},
          'scope': 'Real key provider and upstream PREPARE/rename/FINISH over bounded worker IPC; no completed purge API or FENCE activation'}
a.output.write_text(json.dumps(result, indent=2) + '\n')
print('PASS', len(checks), 'native index capture checks')
