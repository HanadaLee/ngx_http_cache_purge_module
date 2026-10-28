"""Validate existing header/finalizer interfaces on an untouched nginx core."""
import argparse
import hashlib
import http.client
import http.server
import io
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tarfile
import tempfile
import threading
import time

p = argparse.ArgumentParser()
p.add_argument('--output', type=Path, required=True)
p.add_argument('--nginx', type=Path, required=True)
p.add_argument('--nginx-ref', default='HEAD')
p.add_argument('--libraries', type=Path, required=True)
p.add_argument('--dynamic', action='store_true')
p.add_argument('--sanitize', action='store_true')

a = p.parse_args()
repo = Path(__file__).resolve().parents[1]
root = Path(tempfile.mkdtemp(prefix='cache-native-fill-'))
build = root / 'nginx'
core = str(a.nginx)
git = ['git', '-c', 'safe.directory=' + core, '-C', core]
revision = subprocess.check_output(git + ['rev-parse', a.nginx_ref], text=True).strip()
archive = subprocess.check_output(git + ['-c', 'core.autocrlf=false', 'archive', revision])
addon, probe = root / 'addon', root / 'probe'
for directory in (addon, probe): directory.mkdir()
for source, target in [(repo / 'config', addon / 'config'),
                       *[(path, addon / path.name) for path in repo.glob('*.c')],
                       *[(path, addon / path.name) for path in repo.glob('*.h')],
                       *[(path, probe / path.name) for path in (repo / 't/native_fill_probe').iterdir()]]:
    target.write_bytes(source.read_bytes().replace(b'\r\n', b'\n'))
(addon / 'rocksdb').mkdir()
for source in (repo / 'rocksdb').iterdir():
    if source.suffix in ('.c', '.h'):
        (addon / 'rocksdb' / source.name).write_bytes(source.read_bytes().replace(b'\r\n', b'\n'))
env = os.environ.copy()
env['CACHE_PURGE_PROBE_HEADERS'] = str(addon)
env['CACHE_PURGE_ROCKSDB_LIB_DIR'] = str(a.libraries)
env['CACHE_PURGE_ROCKSDB_LIBS'] = '-lrocksdb -lcrypto -lstdc++ -lm -pthread'
build.mkdir()
subprocess.run(['tar', '-xf', '-', '-C', str(build)], input=archive, check=True)
configure = 'auto/configure' if (build / 'auto/configure').exists() else 'configure'
args = ['sh', configure, '--prefix=' + str(root / 'default'), '--with-threads',
        '--with-http_slice_module', '--with-debug']
option = '--add-dynamic-module=' if a.dynamic else '--add-module='
args += [option + str(addon), option + str(probe)]
if a.sanitize:
    args += ['--with-cc-opt=-O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined -fno-sanitize=nonnull-attribute -fno-pie',
             '--with-ld-opt=-fsanitize=address,undefined -no-pie']
with (root / 'build.log').open('w') as log:
    subprocess.run(args, cwd=build, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    if a.sanitize:
        # Native x86 MD5 deliberately uses unaligned word loads. Preserve
        # alignment instrumentation for every module and other core object.
        makefile = build / 'objs/Makefile'
        text = makefile.read_text()
        begin = text.index('objs/src/core/ngx_md5.o:')
        end = text.index('\n\n', begin)
        rule = text[begin:end]
        assert '$(CC) -c $(CFLAGS)' in rule
        rule = rule.replace('$(CC) -c $(CFLAGS)',
                            '$(CC) -c $(CFLAGS) -fno-sanitize=alignment')
        makefile.write_text(text[:begin] + rule + text[end:])
    subprocess.run(['make', '-j2'], cwd=build, stdout=log, stderr=subprocess.STDOUT, check=True)
with tarfile.open(fileobj=io.BytesIO(archive)) as source:
    members = [m for m in source if m.isfile() and m.name.startswith('src/')]
    for member in members:
        assert (build / member.name).read_bytes() == source.extractfile(member).read(), member.name
print('BUILD', root, flush=True)

calls = []
large = b'0123456789abcdef' * 60000
slice_body = b'0123456789' * 300
class Origin(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_): pass
    def do_GET(self):
        path = self.path.split('?', 1)[0]
        calls.append(path)
        body = large if path.startswith('/thread') else slice_body if path.startswith('/slice') else path.encode()
        if path == '/vary': body += self.headers.get('Accept-Language', '').encode()
        status, content_range = 200, None
        if path.startswith('/slice'):
            start, end = map(int, self.headers['Range'][6:].split('-'))
            end = min(end, len(body) - 1)
            content_range = f'bytes {start}-{end}/{len(body)}'
            body = body[start:end + 1]; status = 206
        self.send_response(status)
        self.send_header('Cache-Control', 'max-age=3600')
        if path == '/vary': self.send_header('Vary', 'Accept-Language')
        if content_range: self.send_header('Content-Range', content_range)
        if path == '/chunk': self.send_header('Transfer-Encoding', 'chunked')
        elif path != '/eof': self.send_header('Content-Length', str(len(body) + (10 if path == '/truncated' else 0)))
        self.end_headers()
        try:
            if path == '/chunk': self.wfile.write(f'{len(body):x}\r\n'.encode() + body + b'\r\n0\r\n\r\n')
            else: self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError): pass

origin = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Origin)
threading.Thread(target=origin.serve_forever, daemon=True).start()
with socket.socket() as s: s.bind(('127.0.0.1', 0)); port = s.getsockname()[1]
run = root / ('run-' + str(time.time_ns())); run.mkdir()
(run / 'cache').mkdir(); (run / 'logs').mkdir()
modules = ''.join('load_module ' + str(build / 'objs' / name) + ';\n' for name in
                  ('ngx_http_cache_rocksdb_module.so', 'ngx_http_cache_purge_module.so',
                   'ngx_http_native_fill_probe_module.so')) if a.dynamic else ''
extension = any(line.split() == ['#define', 'NGX_RESTY_EXT', '1']
                for line in (build / 'objs/ngx_auto_config.h').read_text().splitlines())
slice_config = 'slice on; slice_size 1024;' if extension else 'slice 1024;'
config = modules + f'''daemon off; master_process on; worker_processes 2;
pid {run}/pid; error_log {run}/error.log debug;
thread_pool default threads=2;
events {{ worker_connections 128; }}
http {{ access_log off; client_body_temp_path {run}/body; proxy_temp_path {run}/temp;
 proxy_cache_path {run}/cache levels=1:2 keys_zone=c:1m;
 server {{ listen 127.0.0.1:{port};
  proxy_cache c; proxy_cache_key "$uri$slice_range"; proxy_cache_valid 200 206 1h;
  proxy_cache_bypass $arg_bypass; proxy_cache_revalidate on; proxy_buffering on;
  proxy_read_timeout 100ms; proxy_buffers 8 4k; proxy_buffer_size 4k;
  add_header X-Status $upstream_cache_status always;
  location / {{ proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /thread {{ aio threads; aio_write on; proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /slice {{ {slice_config} proxy_set_header Range $slice_range;
                    proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /nocache {{ proxy_no_cache 1; proxy_pass http://127.0.0.1:{origin.server_port}; }}
 }}
}}
'''
(run / 'nginx.conf').write_text(config)
if a.sanitize:
    suppressions = run / 'leaks.supp'; suppressions.write_text('leak:ngx_event_process_init\n')
    env['LSAN_OPTIONS'] = 'suppressions=' + str(suppressions)
log = (run / 'stdout.log').open('w')
process = subprocess.Popen([str(build / 'objs/nginx'), '-p', str(run) + '/', '-c', str(run / 'nginx.conf')],
                           stdout=log, stderr=subprocess.STDOUT, env=env)
checks = []
def until(fn, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = fn()
        if value: return value
        time.sleep(.01)
    raise AssertionError('wait expired')
def request(path, headers=None, method='GET', connection=None):
    c = connection or http.client.HTTPConnection('127.0.0.1', port, timeout=5)
    c.request(method, path, headers=headers or {})
    r = c.getresponse()
    try: body = r.read()
    except http.client.IncompleteRead as e: body = e.partial
    result = r.status, r.getheader('X-Status'), body
    if connection is None: c.close()
    return result
def finishes(path):
    return (run / 'error.log').read_text().count('native finalize FINISH ' + path + ' ')
def warm(path, body=None, headers=None):
    before = finishes(path)
    first = request(path, headers)
    until(lambda: finishes(path) > before)
    second = request(path, headers)
    assert first[0] == 200 and second[1] == 'HIT' and second[2] == first[2], (path, first, second)
    if body is not None: assert second[2] == body
    return second
try:
    until(lambda: (run / 'pid').exists())
    warm('/basic'); checks.append('complete Content-Length body publishes after native finalize')
    warm('/eof'); checks.append('complete EOF body publishes')
    warm('/chunk'); checks.append('complete chunked body publishes')
    warm('/thread', large)
    assert 'thread write' in (run / 'error.log').read_text()
    checks.append('native aio_write finishes before asynchronous publication')
    assert request('/truncated')[1] == 'MISS'
    time.sleep(.15)
    assert request('/truncated')[1] == 'MISS' and finishes('/truncated') == 0
    checks.append('truncated body never publishes')
    assert request('/nocache')[1] == 'MISS'; time.sleep(.15)
    assert request('/nocache')[1] == 'MISS' and finishes('/nocache') == 0
    checks.append('late no_cache predicate still suppresses publication and releases the native node')
    assert request('/reject?reject=1')[1] == 'MISS'
    until(lambda: finishes('/reject') > 0)
    warm('/reject'); checks.append('failed preparation abandons temp file and permits a new fill')
    assert request('/bypass?bypass=1')[1] == 'BYPASS'
    until(lambda: finishes('/bypass') > 0)
    assert request('/bypass')[1] == 'HIT'
    checks.append('BYPASS creates and publishes the native node after header interception')
    warm('/vary', headers={'Accept-Language': 'en'})
    warm('/vary', headers={'Accept-Language': 'de'})
    assert request('/vary', {'Accept-Language': 'en'})[2] == b'/varyen'
    checks.append('secondary Vary instances preserve native identity selection')
    assert request('/head', method='HEAD')[:2] == (200, 'MISS')
    until(lambda: finishes('/head') > 0)
    assert request('/head')[1] == 'HIT'
    checks.append('HEAD conversion completes cached GET without downstream body')
    assert request('/slice')[2] == slice_body
    until(lambda: finishes('/slice') == 3)
    assert request('/slice')[2] == slice_body and calls.count('/slice') == 3
    checks.append('main and two slice subrequests retain independent finalization references')
    c = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
    assert request('/keep', connection=c)[1] == 'MISS'
    assert request('/basic', connection=c)[1] == 'HIT'
    c.close()
    until(lambda: finishes('/keep') > 0)
    checks.append('keepalive request waits for retained asynchronous publication before reuse')
    assert request('/reload')[1] == 'MISS'
    process.send_signal(signal.SIGHUP)
    until(lambda: finishes('/reload') > 0)
    assert request('/reload')[1] == 'HIT'
    checks.append('graceful reload drains the outstanding finalize timer')
finally:
    if process.poll() is None:
        process.send_signal(signal.SIGQUIT)
        try: process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill(); process.wait(); raise
    log.close(); origin.shutdown()
logs = (run / 'error.log').read_text() + (run / 'stdout.log').read_text()
assert not any(s in logs for s in ('AddressSanitizer', 'runtime error:', 'LeakSanitizer', '[alert]')), logs[-5000:]
result = {'root': str(root), 'run': str(run), 'native_revision': revision, 'native_source_files_unchanged': len(members),
          'checks': checks, 'sanitizer': a.sanitize, 'production_fill_helper': True, 'dynamic': a.dynamic,
          'addon_sha256': {str(path.relative_to(addon)): hashlib.sha256(path.read_bytes()).hexdigest()
                           for path in addon.rglob('*') if path.is_file()},
          'library_sha256': {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                             for path in a.libraries.glob('libngx_http_cache_*.a')},
          'sanitizer_exclusions': ['nonnull-attribute', 'alignment for native ngx_md5.o only',
                                   'leak:ngx_event_process_init'] if a.sanitize else [],
          'probe_sha256': {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in probe.iterdir()},
          'scope': 'Native header/finalize fill ownership only; timer test consumer, no durable index or complete purge consumer'}
a.output.write_text(json.dumps(result, indent=2) + '\n')
print('PASS', len(checks))
