#!/usr/bin/env python3
"""Real native-cache read/admission tests; this is not the purge consumer."""
import argparse
import concurrent.futures
import hashlib
import http.server
import http.client
import json
import os
import signal
import socket
import socketserver
import struct
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--nginx', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--build-root', type=Path)
p.add_argument('--dynamic', action='store_true')
p.add_argument('--expr', type=Path)
p.add_argument('--sanitize', action='store_true')
a = p.parse_args()
root = a.build_root or Path(tempfile.mkdtemp(prefix='ncp-cache-adapter-'))
root.mkdir(exist_ok=True)
build, probe = root / 'nginx', root / 'probe'
build.mkdir(exist_ok=True)
probe.mkdir(exist_ok=True)
source = Path(__file__).resolve().parent / 'cache_adapter_probe'
addon = source.parents[1]
addon_files = [addon / 'config', *sorted(addon.glob('*.c')), *sorted(addon.glob('*.h'))]
addon_hashes = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in addon_files}
for f in source.iterdir():
    (probe / f.name).write_bytes(f.read_bytes().replace(b'\r\n', b'\n'))
revision = subprocess.check_output(['git', '-C', str(a.nginx), 'rev-parse', 'HEAD'], text=True).strip()
with (root / 'source.tar').open('wb') as f:
    subprocess.run(['git', '-C', str(a.nginx), 'archive', revision], stdout=f, check=True)
subprocess.run(['tar', '-xf', str(root / 'source.tar'), '-C', str(build)], check=True)
patch = subprocess.check_output(['git', '-c', 'core.autocrlf=true', '-c', 'core.safecrlf=false',
                                '-C', str(a.nginx), 'diff', 'HEAD', '--', 'src'])
if patch:
    subprocess.run(['git', 'apply', '-'], input=patch, cwd=build, check=True)
configure = 'auto/configure' if (build / 'auto/configure').exists() else 'configure'
args = ['sh', configure, '--prefix=' + str(root / 'default'), '--with-http_ssl_module',
        '--with-http_slice_module', '--with-threads', '--with-debug']
args.append('--add-module=' + str(source.parents[1]))
if a.expr:
    args.append('--add-module=' + str(a.expr))
args.append(('--add-dynamic-module=' if a.dynamic else '--add-module=') + str(probe))
if a.sanitize:
    # Native nginx uses memcpy(NULL, ..., 0) in string/cache-header paths.
    # Keep ASan and other UBSan checks, excluding only this known annotation.
    args.extend(['--with-cc-opt=-O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined -fno-sanitize=nonnull-attribute -fno-pie',
                 '--with-ld-opt=-fsanitize=address,undefined -no-pie'])
print('BUILD', root, flush=True)
with (root / 'build.log').open('w') as log:
    subprocess.run(args, cwd=build, stdout=log, stderr=subprocess.STDOUT, check=True)
    subprocess.run(['make', '-j2'], cwd=build, stdout=log, stderr=subprocess.STDOUT, check=True)

states, calls = {}, []
lock = threading.Lock()
body = b'0123456789' * 300
class Origin(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        path = urllib.parse.urlsplit(self.path).path
        with lock:
            current = dict(states.get(path, {}))
            calls.append((path, self.headers.get('If-None-Match'), self.headers.get('Range')))
        if current.get('gate'):
            current['gate'].wait(3)
        if current.get('delay'):
            time.sleep(current['delay'])
        if current.get('close'):
            self.connection.close()
            return
        version = current.get('version', 1)
        language = self.headers.get('Accept-Language', '') if path == '/vary' else ''
        entity = current.get('body', body if path.startswith('/slice') else f'{path}:v{version}:{language}'.encode())
        etag = '"' + hashlib.sha256(entity).hexdigest() + '"'
        status = current.get('status', 200)
        if status == 200 and self.headers.get('If-None-Match') == etag:
            status = 304
        content_range = None
        if status == 200 and path.startswith('/slice') and self.headers.get('Range'):
            start, end = map(int, self.headers['Range'][6:].split('-'))
            end = min(end, len(entity) - 1)
            content_range = f'bytes {start}-{end}/{len(entity)}'
            entity = entity[start:end + 1]
            status = 206
        self.send_response(status)
        self.send_header('ETag', etag)
        self.send_header('Cache-Control', current.get('policy',
            'max-age=3600, stale-while-revalidate=3600, stale-if-error=3600'))
        if path == '/vary':
            self.send_header('Vary', 'Accept-Language')
        if content_range:
            self.send_header('Content-Range', content_range)
        if current.get('chunked'):
            self.send_header('Transfer-Encoding', 'chunked')
        elif not current.get('eof'):
            self.send_header('Content-Length', str(len(entity)))
        self.end_headers()
        if status != 304:
            try:
                if current.get('chunked'):
                    for chunk in (entity[:3], entity[3:]):
                        if chunk:
                            self.wfile.write(f'{len(chunk):x}\r\n'.encode() + chunk + b'\r\n')
                    self.wfile.write(b'0\r\n\r\n')
                else:
                    self.wfile.write(entity)
            except (BrokenPipeError, ConnectionResetError):
                pass

origin = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Origin)
threading.Thread(target=origin.serve_forever, daemon=True).start()

class ProtocolOrigin(socketserver.BaseRequestHandler):
    def read_exact(self, length):
        value = b''
        while len(value) < length:
            part = self.request.recv(length - len(value))
            if not part:
                raise EOFError('incomplete gateway request')
            value += part
        return value

    def handle(self):
        self.request.settimeout(5)
        protocol = self.server.protocol
        request_id = 1
        if protocol == 'fastcgi':
            while True:
                version, kind, request_id, size, padding, _ = struct.unpack('!BBHHBB', self.read_exact(8))
                assert version == 1
                self.read_exact(size + padding)
                if kind == 5 and size == 0:
                    break
        elif protocol == 'uwsgi':
            _, size, _ = struct.unpack('<BHB', self.read_exact(4))
            self.read_exact(size)
        else:
            digits = b''
            while True:
                char = self.read_exact(1)
                if char == b':':
                    break
                digits += char
                assert len(digits) <= 10
            self.read_exact(int(digits))
            assert self.read_exact(1) == b','
        entity = (protocol + ':body').encode()
        response = (b'Status: 200 OK\r\nContent-Type: text/plain\r\n'
                    b'Cache-Control: max-age=3600\r\nContent-Length: '
                    + str(len(entity)).encode() + b'\r\n\r\n' + entity)
        if protocol == 'fastcgi':
            def record(kind, value):
                return struct.pack('!BBHHBB', 1, kind, request_id, len(value), 0, 0) + value
            response = record(6, response) + record(6, b'') + record(3, b'\0' * 8)
        try:
            self.request.sendall(response)
        except (BrokenPipeError, ConnectionResetError):
            pass

protocol_servers = {}
protocol_locations = []
for name in ('fastcgi', 'uwsgi', 'scgi'):
    server = socketserver.ThreadingTCPServer(('127.0.0.1', 0), ProtocolOrigin)
    server.daemon_threads = True
    server.protocol = name
    protocol_servers[name] = server
    threading.Thread(target=server.serve_forever, daemon=True).start()
    protocol_locations.append(f'''
    location /{name}/ {{
      {name}_cache {name}_cache; {name}_cache_key $uri; {name}_cache_valid 200 1h;
      {name}_cache_lock on; {name}_read_timeout 250ms;
      {name}_param REQUEST_METHOD GET; {name}_param REQUEST_URI $uri;
      {name}_pass 127.0.0.1:{server.server_address[1]};
    }}''')
run = root / ('run-' + str(time.time_ns()))
run.mkdir()
(run / 'logs').mkdir()
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
user = 'user root;' if os.geteuid() == 0 else ''
load = f'load_module {build}/objs/ngx_http_cache_adapter_probe_module.so;' if a.dynamic else ''
conf = run / 'nginx.conf'
conf.write_text(load + user + f'''
daemon off; worker_processes 2; pid {run}/pid;
error_log {run}/error.log debug;
events {{ worker_connections 128; }}
http {{
  access_log off; cache_adapter_probe on;
  proxy_cache_path {run}/cache-a keys_zone=a:1m;
  proxy_cache_path {run}/cache-b keys_zone=b:1m;
  {''.join(f'{name}_cache_path {run}/cache-{name} keys_zone={name}_cache:1m;' for name in protocol_servers)}
  map $http_x_zone $zone {{ default a; b b; }}
  map $arg_offline $origin_port {{ default {origin.server_port}; 1 1; }}
  server {{
    listen 127.0.0.1:{port}; server_name probe.test;
    proxy_cache $zone; proxy_cache_key $uri; proxy_cache_revalidate on;
    proxy_cache_lock on; proxy_cache_lock_timeout 2s;
    proxy_cache_background_update on;
    proxy_cache_use_stale error timeout http_500 http_502 http_504 updating;
    proxy_next_upstream off; proxy_read_timeout 250ms;
    proxy_cache_valid 200 206 1h; proxy_cache_valid 502 60s;
    proxy_cache_bypass $arg_bypass;
    add_header X-Cache $upstream_cache_status always;
    add_header X-Probe $cache_adapter_probe_state always;
    location / {{ proxy_pass http://127.0.0.1:{origin.server_port}; }}
    location /native {{ cache_adapter_probe off; proxy_pass http://127.0.0.1:{origin.server_port}; }}
    location /aio {{ aio threads; aio_write on; proxy_pass http://127.0.0.1:{origin.server_port}; }}
    {''.join(protocol_locations)}
    location /slice {{
      slice on; slice_size 1024; proxy_cache_key "$uri$slice_range";
      proxy_set_header Range $slice_range;
      proxy_pass http://127.0.0.1:{origin.server_port};
    }}
    location /negative {{ proxy_pass http://127.0.0.1:$origin_port; }}
    location /redirect {{ error_page 503 = /fallback;
      proxy_pass http://127.0.0.1:{origin.server_port}; }}
    location = /fallback {{ return 200 "fallback"; }}
  }}
}}
''')
binary = build / 'objs/nginx'
command = [str(binary), '-p', str(run) + '/', '-c', str(conf)]
tested = subprocess.run(command + ['-t'], capture_output=True)
assert tested.returncode == 0, tested.stderr.decode()
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
checks = []

def get(path, headers=None):
    request = urllib.request.Request(f'http://127.0.0.1:{port}' + path, headers=headers or {})
    try:
        response = opener.open(request, timeout=5)
    except urllib.error.HTTPError as e:
        response = e
    with response:
        return response.status, response.headers.get('X-Cache'), response.read(), response.headers.get('X-Probe')

def count(path):
    with lock:
        return sum(item[0] == path for item in calls)

def until(fn, timeout=6):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if fn():
            return
        time.sleep(.01)
    raise AssertionError('deadline')

def warm(path, headers=None):
    first, second = get(path, headers), get(path, headers)
    assert first[0] == second[0] == 200 and second[1] == 'HIT', (first, second)
    return second

def cache_file(path):
    return run / 'cache-a' / hashlib.md5(path.encode()).hexdigest()

def phase_seen(path, phase, result=0, complete=False):
    label = 'complete' if complete else 'phase'
    return f'probe store {label} {phase} {result} {path} ' in (run / 'error.log').read_text()

stderr = (run / 'stderr.log').open('w')
process = subprocess.Popen(command, stderr=stderr)
try:
    def ready():
        try:
            return get('/ready')[0] == 200
        except OSError:
            return False
    until(ready)
    value = warm('/basic')
    assert value[3] == 'a:1:0:0:0', value
    warm('/basic', {'X-Zone': 'b'})
    assert get('/basic', {'X-Zone': 'b'})[3] == 'b:1:0:0:0'
    checks.append('native key and actual zone bind once to each cache context')

    before = count('/basic')
    value = get('/basic?delay=40')
    assert value[1] == 'HIT' and value[2] == b'/basic:v1:' and count('/basic') == before, value
    checks.append('async metadata lookup resumes parsed file exactly once')
    value = get('/basic?bypass=1')
    assert value[1] == 'BYPASS' and value[3] == 'a:0:0:0:0', value
    checks.append('BYPASS captures identity without invoking file lookup')

    warm('/aio')
    assert get('/aio?delay=25')[1] == 'HIT'
    checks.append('native thread AIO and adapter async continuation compose')

    value = get('/basic?mode=expire&delay=20')
    assert value[0] == 200 and value[1] == 'REVALIDATED' and value[3] == 'a:1:0:1:0', value
    states['/basic'] = {'version': 2}
    value = get('/basic?mode=expire')
    assert value[1] == 'EXPIRED' and value[2] == b'/basic:v2:', value
    assert get('/basic')[3] == 'a:1:0:0:0'
    checks.append('Invalidate allows 304 reuse and new 200 without persisting synthetic origin policy')

    for name, origin_state, expected in [('status', {'status': 500}, 500),
                                         ('timeout', {'delay': .5}, 504),
                                         ('connection', {'close': True}, 502)]:
        path = '/failure-' + name
        warm(path)
        states[path] = origin_state
        value = get(path + '?mode=expire&delay=10')
        assert value[0] == expected and value[1] == 'EXPIRED', value
    checks.append('HTTP errors, read timeout and connection failure cannot serve invalidated stale entities')

    warm('/updating')
    gate = threading.Event()
    states['/updating'] = {'gate': gate}
    initial = count('/updating')
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        f1 = pool.submit(get, '/updating?mode=expire')
        until(lambda: count('/updating') == initial + 1)
        f2 = pool.submit(get, '/updating?mode=expire&delay=5')
        until(lambda: count('/updating') == initial + 2)
        assert not f1.done() and not f2.done()
        gate.set()
        assert f1.result()[1] == f2.result()[1] == 'REVALIDATED'
    checks.append('UPDATING and background-update branches cannot reuse invalidated stale entities')

    warm('/deny304')
    filename = run / 'cache-a' / hashlib.md5(b'/deny304').hexdigest()
    old = filename.read_bytes()
    states['/deny304'] = {'policy': 'max-age=7200'}
    value = get('/deny304?mode=deny304')
    assert value[0] == 503 and filename.read_bytes() == old, value
    assert get('/deny304')[1] == 'HIT'
    checks.append('rejected 304 reuse does not update on-disk freshness or corrupt native policy')
    assert get('/basic?mode=send_error')[0] == 503
    assert get('/basic?mode=lookup_error&delay=15')[0] == 503
    assert get('/basic?mode=bad_return&delay=15')[0] == 500
    value = get('/basic?mode=miss&delay=10')
    assert value[0] == 200 and value[1] == 'MISS', value
    checks.append('lookup denial, send denial, invalid result and cache-entry rejection propagate safely')

    assert get('/negative?offline=1')[0] == 502
    value = get('/negative')
    assert value[0] == 502 and value[1] == 'HIT' and count('/negative') == 0, value
    value = get('/negative?mode=miss&delay=20')
    assert value[0] == 200 and value[1] == 'MISS' and count('/negative') == 1, value
    checks.append('cached in-memory upstream failures also pass async lookup and can be bypassed')

    warm('/vary', {'Accept-Language': 'en'})
    warm('/vary', {'Accept-Language': 'fr'})
    value = get('/vary?delay=20', {'Accept-Language': 'fr'})
    assert value[2] == b'/vary:v1:fr' and value[3] == 'a:1:1:0:0', value
    checks.append('Vary reopening checks only the final selected file')
    assert warm('/slice')[2] == body
    assert get('/slice?delay=10')[2] == body
    checks.append('slice subrequests retain separate adapter contexts during async reads')

    states['/lock'] = {'delay': .12}
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        first = pool.submit(get, '/lock')
        until(lambda: count('/lock') == 1)
        second = pool.submit(get, '/lock?delay=10')
        assert first.result()[0] == second.result()[0] == 200
    assert count('/lock') == 1
    checks.append('cache-lock wait resumes into one metadata lookup without another origin fill')

    warm('/redirect')
    assert get('/redirect?mode=send_error')[2] == b'fallback'
    warm('/abort')
    for _ in range(12):
        with socket.create_connection(('127.0.0.1', port)) as client:
            client.sendall(b'GET /abort?delay=100 HTTP/1.1\r\nHost: probe.test\r\n\r\n')
            time.sleep(.005)
            client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
    time.sleep(.15)
    assert get('/abort')[1] == 'HIT'
    checks.append('internal redirect and client abort leave no adapter timer use-after-free or cache lock leak')

    value = warm('/native')
    assert value[3] is None
    checks.append('unmanaged locations preserve native behavior with no adapter context')

    value = get('/store-pending?prepare=300&finish=300')
    assert value[0] == 200
    until(lambda: phase_seen('/store-pending', 1))
    assert not cache_file('/store-pending').exists()
    until(lambda: phase_seen('/store-pending', 2, complete=True))
    assert cache_file('/store-pending').is_file()
    assert get('/store-pending')[1] == 'HIT' and count('/store-pending') == 1
    checks.append('async prepare precedes rename; finish outlives origin FIN and its read timeout')

    states['/store-eof'] = {'eof': True}
    assert get('/store-eof?prepare=25&finish=25')[0] == 200
    until(lambda: phase_seen('/store-eof', 2, complete=True))
    assert get('/store-eof')[1] == 'HIT'
    checks.append('EOF-delimited complete bodies use the same async store lifecycle')

    states['/store-empty'] = {'body': b''}
    assert get('/store-empty?prepare=25&finish=25')[2] == b''
    until(lambda: phase_seen('/store-empty', 2, complete=True))
    assert get('/store-empty')[1:3] == ('HIT', b'')
    checks.append('empty responses retain their cache header and complete both store phases')

    states['/store-chunked'] = {'chunked': True}
    assert get('/store-chunked?prepare=25&finish=25')[2] == b'/store-chunked:v1:'
    until(lambda: phase_seen('/store-chunked', 2, complete=True))
    assert get('/store-chunked')[1:3] == ('HIT', b'/store-chunked:v1:')
    checks.append('chunked responses finish after the decoded body and store valid cache files')

    client = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
    try:
        client.request('GET', '/store-keepalive?prepare=100&finish=100')
        response = client.getresponse()
        assert response.read() == b'/store-keepalive:v1:'
        connection = client.sock
        assert connection is not None
        client.request('GET', '/store-keepalive')
        response = client.getresponse()
        assert response.read() == b'/store-keepalive:v1:'
        assert response.getheader('X-Cache') == 'HIT' and client.sock is connection
    finally:
        client.close()
    assert count('/store-keepalive') == 1
    checks.append('HTTP/1 keepalive reuses the same connection after asynchronous store completion')

    for protocol in protocol_servers:
        path = '/' + protocol + '/fill'
        assert get(path + '?prepare=300&finish=25')[2] == (protocol + ':body').encode()
        until(lambda: phase_seen(path, 2, complete=True))
        assert get(path)[1:3] == ('HIT', (protocol + ':body').encode())
        checks.append(protocol + ': native complete-body store survives a wait longer than its read timeout')

    assert get('/slice-delayed?prepare=30&finish=30')[2] == body
    until(lambda: (run / 'error.log').read_text().count('probe store complete 2 0 /slice-delayed ') == 3)
    assert get('/slice-delayed')[2] == body and count('/slice-delayed') == 3
    checks.append('all three slice subrequests finish asynchronous stores without sharing fill state')

    large = body * 128
    states['/aio/large'] = {'body': large}
    assert get('/aio/large?prepare=25&finish=25')[2] == large
    until(lambda: phase_seen('/aio/large', 2, complete=True))
    value = get('/aio/large?delay=10')
    assert value[1] == 'HIT' and value[2] == large, value[:2]
    checks.append('native threaded temp-file writes and cache response reads preserve a large body across async store')

    assert get('/store-rejected?mode=store_reject&prepare=25&finish=25')[0] == 200
    until(lambda: phase_seen('/store-rejected', 2, -5))
    until(lambda: phase_seen('/store-rejected', 2, complete=True))
    assert not cache_file('/store-rejected').exists()
    warm('/store-rejected')
    checks.append('prepare rejection skips rename, releases the node and permits a later fill')

    assert get('/store-cancel-prepare?mode=store_cancel_prepare&prepare=30&finish=30')[0] == 200
    until(lambda: phase_seen('/store-cancel-prepare', 2, -6))
    until(lambda: phase_seen('/store-cancel-prepare', 2, complete=True))
    assert not cache_file('/store-cancel-prepare').exists()
    warm('/store-cancel-prepare')
    assert get('/store-cancel-finish?mode=store_cancel_finish&prepare=20&finish=30')[0] == 200
    until(lambda: phase_seen('/store-cancel-finish', 2, complete=True))
    assert get('/store-cancel-finish')[1] == 'HIT'
    checks.append('termination before rename aborts; termination after rename retains completion and releases pins')

    assert get('/store-finish-failed?mode=store_finish_error&finish=30')[0] == 200
    until(lambda: phase_seen('/store-finish-failed', 2, -1, complete=True))
    assert cache_file('/store-finish-failed').exists()
    checks.append('finish failure preserves the published file for consumer reconciliation without double free')

    assert get('/store-rename-failed?prepare=200&finish=15')[0] == 200
    until(lambda: phase_seen('/store-rename-failed', 1))
    cache_file('/store-rename-failed').mkdir()
    until(lambda: phase_seen('/store-rename-failed', 2, -1))
    until(lambda: phase_seen('/store-rename-failed', 2, complete=True))
    cache_file('/store-rename-failed').rmdir()
    warm('/store-rename-failed')
    checks.append('native rename failure reaches finish and releases the cache node for retry')

    assert get('/store-reload?prepare=150&finish=200')[0] == 200
    until(lambda: phase_seen('/store-reload', 1))
    process.send_signal(signal.SIGHUP)
    until(lambda: phase_seen('/store-reload', 2, complete=True))
    assert get('/store-reload')[1] == 'HIT'
    checks.append('reload retains the old worker until both store phases and native node cleanup complete')

    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        pending = pool.submit(get, '/basic?delay=150')
        time.sleep(.03)
        process.send_signal(signal.SIGHUP)
        assert pending.result()[0] == 200
    until(lambda: get('/basic')[1] == 'HIT')
    checks.append('reload drains pending metadata callbacks while replacement workers serve existing files')
finally:
    process.send_signal(signal.SIGQUIT)
    try:
        code = process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        raise
    stderr.close()
    origin.shutdown()
    origin.server_close()
    for server in protocol_servers.values():
        server.shutdown()
        server.server_close()
assert code == 0, code
errors = (run / 'error.log').read_text() + (run / 'stderr.log').read_text()
assert 'http upstream thread:' in errors, 'aio_write did not execute a native upstream thread completion'
assert addon_hashes == {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in addon_files}
for marker in ('[alert]', 'AddressSanitizer', 'runtime error:', 'exited on signal'):
    assert marker not in errors, (marker, run)
result = {'root': str(root), 'run': str(run), 'revision': revision,
          'source_patch_sha256': hashlib.sha256(patch).hexdigest(),
          'addon_source_sha256': addon_hashes,
          'fixture_source_sha256': hashlib.sha256((probe / 'module.c').read_bytes()).hexdigest(),
          'native_aio_write_completion_observed': True,
          'dynamic': a.dynamic, 'expr': bool(a.expr), 'sanitizer': a.sanitize,
          'sanitizer_exclusions': ['nonnull-attribute'] if a.sanitize else [],
          'checks': checks,
          'scope': 'Native read/admission and two-phase store fixture; no production purge consumer, durable DB/file transaction, 304 update adapter or deletion'}
a.output.write_text(json.dumps(result, indent=2) + '\n')
print('PASS', len(checks), 'native cache adapter scenarios', flush=True)
