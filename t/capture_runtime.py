#!/usr/bin/env python3
"""Real upstream cache fills through the key module, worker IPC and RocksDB."""
import argparse
import array
import base64
import calendar
import concurrent.futures
import contextlib
import hashlib
import http.client
import http.server
import io
import json
import os
import re
import shlex
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
p.add_argument('--key-last', action='store_true', help='Configure/load the independent key producer after purge')
p.add_argument('--sanitize', action='store_true')
p.add_argument('--expr', type=Path)
p.add_argument('--workers', type=int, default=2)
p.add_argument('--capacity', action='store_true')
p.add_argument('--reuse-build', type=Path, help='Reuse matching compiled sources after test-only edits')
a = p.parse_args()
repo = Path(__file__).resolve().parents[1]
root = a.reuse_build or Path(tempfile.mkdtemp(prefix='ncp-capture-'))
build, addon, key = root / 'nginx', root / 'addon', root / 'key'
for directory in (build, addon, key): directory.mkdir(exist_ok=bool(a.reuse_build))
for source, destination in [(repo, addon), (a.key, key)]:
    for path in [source / 'config', *source.glob('*.c'), *source.glob('*.h')]:
        data = path.read_bytes().replace(b'\r\n', b'\n')
        if a.reuse_build:
            assert (destination / path.name).read_bytes() == data, ('stale source', path)
        else:
            (destination / path.name).write_bytes(data)
(addon / 'rocksdb').mkdir(exist_ok=bool(a.reuse_build))
for path in (repo / 'rocksdb').iterdir():
    if path.suffix in ('.c', '.h'):
        data = path.read_bytes().replace(b'\r\n', b'\n')
        if a.reuse_build:
            assert (addon / 'rocksdb' / path.name).read_bytes() == data, ('stale provider', path)
        else:
            (addon / 'rocksdb' / path.name).write_bytes(data)
git = ['git', '-c', 'safe.directory=' + str(a.nginx), '-C', str(a.nginx)]
revision = subprocess.check_output(git + ['rev-parse', a.nginx_ref], text=True).strip()
archive = subprocess.check_output(git + ['-c', 'core.autocrlf=false', 'archive', revision])
if not a.reuse_build:
    subprocess.run(['tar', '-xf', '-', '-C', str(build)], input=archive, check=True)
source_patch = b''
if a.core_working_tree:
    source_patch = subprocess.check_output(['git', '-c', 'core.autocrlf=true', '-c', 'core.safecrlf=false',
                                           '-C', str(a.nginx), 'diff', a.nginx_ref, '--', 'src'])
    if not a.reuse_build:
        subprocess.run(['git', 'apply', '-'], cwd=build, input=source_patch, check=True)
env = os.environ.copy()
env['CACHE_PURGE_ROCKSDB_LIB_DIR'] = str(a.libraries)
engine = '-lrocksdb' if a.dynamic else str(Path(subprocess.check_output(
    ['pkg-config', '--variable=libdir', 'rocksdb'], text=True).strip()) / 'librocksdb.a')
env['CACHE_PURGE_ROCKSDB_LIBS'] = engine + ' -lcrypto -lsnappy -lz -lbz2 -llz4 -lzstd -lstdc++ -lm -pthread'
option = '--add-dynamic-module=' if a.dynamic else '--add-module='
configure = 'auto/configure' if (build / 'auto/configure').exists() else 'configure'
command = ['sh', configure, '--prefix=' + str(root / 'default'), '--with-debug', '--with-threads',
           '--with-http_slice_module']
if a.expr: command.append('--add-module=' + str(a.expr))
command += [option + str(path) for path in ((addon, key) if a.key_last else (key, addon))]
if a.sanitize:
    command += ['--with-cc-opt=-O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined -fno-sanitize=nonnull-attribute -fno-pie',
                '--with-ld-opt=-fsanitize=address,undefined -no-pie']
print('BUILD', root, flush=True)
with (root / 'build.log').open('a' if a.reuse_build else 'w') as log:
    if a.reuse_build:
        version = subprocess.check_output([str(build / 'objs/nginx'), '-V'], stderr=subprocess.STDOUT, text=True)
        assert shlex.split(version.split('configure arguments: ', 1)[1]) == command[2:]
        for library in a.libraries.glob('libngx_http_cache_*.a'):
            assert library.stat().st_mtime <= (build / 'objs/nginx').stat().st_mtime, ('newer library', library)
    else:
        subprocess.run(command, cwd=build, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    if a.sanitize and not a.reuse_build:
        # Native x86 MD5 intentionally reads unaligned words. Do not disable
        # alignment checks for request code, the backend, or other core files.
        makefile = build / 'objs/Makefile'
        text = makefile.read_text()
        begin = text.index('objs/src/core/ngx_md5.o:')
        end = text.index('\n\n', begin)
        rule = text[begin:end]
        if '-fno-sanitize=alignment' not in rule:
            assert '$(CC) -c $(CFLAGS)' in rule
            rule = rule.replace('$(CC) -c $(CFLAGS)',
                                '$(CC) -c $(CFLAGS) -fno-sanitize=alignment')
            makefile.write_text(text[:begin] + rule + text[end:])
            (build / 'objs/src/core/ngx_md5.o').unlink(missing_ok=True)
    if not a.reuse_build:
        subprocess.run(['make', '-j2'], cwd=build, stdout=log, stderr=subprocess.STDOUT, check=True)
with tarfile.open(fileobj=io.BytesIO(archive)) as source:
    sources = [m for m in source if m.isfile() and m.name.startswith('src/')]
    for member in sources:
        expected = (a.nginx / member.name).read_bytes().replace(b'\r\n', b'\n') if a.core_working_tree else source.extractfile(member).read()
        assert (build / member.name).read_bytes() == expected

calls, policies = [], {}
propagation_calls = []
propagation_methods = []
validation_tags = {'/revalidate-replace': 'new-tag', '/revalidate-clear': ''}
conditional_paths = set()
conditional_calls = []
conditional_policies = {}
validation_waiting, validation_release = threading.Event(), threading.Event()
invalidate_waiting, invalidate_release = threading.Event(), threading.Event()
late_waiting, late_release = threading.Event(), threading.Event()
finish_waiting, finish_release = threading.Event(), threading.Event()
send_waiting, send_release = threading.Event(), threading.Event()
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
        if path == '/propagation/send-inflight' and calls.count(path) == 1:
            send_waiting.set()
            assert send_release.wait(15)
        if path.startswith('/propagation/'):
            propagation_calls.append((path, {k.lower(): v for k, v in self.headers.items()}))
            propagation_methods.append((path, self.command))
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
            if path == '/invalidate/race':
                invalidate_waiting.set()
                assert invalidate_release.wait(15)
            self.send_response(304); self.send_header('Cache-Control', conditional_policies.get(path, 'max-age=60'))
            if path in validation_tags: self.send_header('Cache-Tag', validation_tags[path])
            self.end_headers(); return
        if path == '/redirect-revalidated' and self.headers.get('If-None-Match') == '"capture-v1"':
            self.send_response(304); self.send_header('Cache-Control', 'max-age=60'); self.end_headers(); return
        if path == '/capacity': capacity_gate.wait(20)
        is_slice = path.startswith(('/slice', '/propagation/slice'))
        body = path.encode() if not is_slice else slice_body
        body = fresh_bodies.get(path, body)
        if path == '/revalidate-race' and calls.count(path) >= 3:
            retry_waiting.set()
            assert retry_release.wait(15)
        if path == '/late-fill' and calls.count(path) == 1:
            body = b'old-late-entity'
            late_waiting.set()
            assert late_release.wait(15)
        if path == '/alias/finish-wait' and calls.count(path) == 1:
            body = b'finish-retry-entity'
            finish_waiting.set()
            assert finish_release.wait(15)
        varying = path in ('/vary', '/revalidate-vary', '/propagation/vary', '/propagation/uncached-seed')
        if varying: body += self.headers.get('Accept-Language', '').encode()
        if path == '/large': body *= 40000
        status, content_range = (500 if path.startswith('/error-') else 200), None
        if path in ('/error-disk-502', '/error-disk-504'): status = int(path[-3:])
        if is_slice:
            start, end = map(int, self.headers['Range'][6:].split('-')); end = min(end, len(body) - 1)
            content_range = f'bytes {start}-{end}/{len(body)}'; body = body[start:end + 1]; status = 206
        self.send_response(status)
        self.send_header('Cache-Control', 'no-store' if path == '/error-no-store' else 'max-age=' + ('1' if path.startswith('/revalidate-') or path in ('/redirect-stale', '/redirect-revalidated') else '3600'))
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('ETag', '"capture-v1"')
        if path in redirects:
            self.send_header('X-Accel-Redirect', redirects[path])
            self.send_header('X-Cached-Only', 'hidden-on-redirect')
        tag_identity = (path, self.headers.get('Accept-Language', '') if varying else self.headers.get('Range', ''))
        self.send_header('Cache-Tag', policies.get(tag_identity, policies.get(path, 'news, red, news')))
        if varying: self.send_header('Vary', 'Accept-Language')
        if content_range: self.send_header('Content-Range', content_range)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        try: self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError): pass

    do_POST = do_GET

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
run = root / (f'run-{time.time_ns()}' if a.reuse_build else 'run'); run.mkdir(); (run / 'logs').mkdir()
for directory in ('cache-a', 'cache-b', 'index'): (run / directory).mkdir()
(run / 'index').chmod(0o700)
gateway_paths, gateway_locations = [], []
for name, server in gateways.items():
    (run / ('cache-' + name)).mkdir()
    gateway_paths.append(f'{name}_cache_path {run}/cache-{name} levels=1:2 keys_zone={name}:1m;')
    gateway_locations.append(f'''location /{name}/ {{
      {name}_cache {name}; {name}_cache_key $cache_key_full; {name}_cache_valid 200 1h;
      {name}_param REQUEST_METHOD GET; {name}_param REQUEST_URI $uri;

      {name}_pass 127.0.0.1:{server.server_address[1]};
    }}''')
    gateway_locations.append(f'''location /{name}-off/ {{
      {name}_cache {name}; {name}_cache_key $cache_key_full; {name}_cache_valid 200 1h;
      {name}_cache_purge off; cache_purge_key_url $arg_missing;
      {name}_param REQUEST_METHOD GET; {name}_param REQUEST_URI $uri;
      {name}_pass 127.0.0.1:{server.server_address[1]};
    }}''')
    switches = ' '.join(f'{protocol}_cache_purge {"on" if protocol == name else "off"};'
                        for protocol in ('proxy', 'fastcgi', 'scgi', 'uwsgi'))
    gateway_locations.append(f'''location /push-{name}/ {{
      {switches} proxy_cache off;
      cache_purge_key_url $http_x_target;
    }}''')
modules = ['ngx_http_cache_rocksdb_module', 'ngx_http_cache_purge_module']
modules.insert(len(modules) if a.key_last else 0, 'ngx_http_cache_key_module')
loads = ''.join(f'load_module {build}/objs/{name}.so;\n' for name in modules) if a.dynamic else ''
extension = any(line.split() == ['#define', 'NGX_RESTY_EXT', '1']
                for line in (build / 'objs/ngx_auto_config.h').read_text().splitlines())
slice_config = 'slice on; slice_size 1024;' if extension else 'slice 1024;'
propagation_config = '''cache_key_namespace prop;


  cache_purge_propagation on;
  proxy_set_header Host $host;
  proxy_set_header X-Cache-Purge-Time $cache_purge_propagation_time;
  proxy_set_header X-Cache-Purge-Mode $cache_purge_propagation_mode;
  proxy_set_header X-Cache-Purge-Fence $cache_purge_propagation_fence;
  proxy_set_header X-Cache-Purge-Namespace "";'''
propagation_off_config = propagation_config.replace('cache_purge_propagation on;',
                                                    'cache_purge_propagation off;')
propagation_manual_config = propagation_config.replace('cache_key_namespace prop;', '')

dns_holds = {}
class DNS(socketserver.BaseRequestHandler):
    def handle(self):
        packet, sock = self.request
        at = 12
        names = []
        while packet[at]:
            length = packet[at]
            names.append(packet[at+1:at+1+length].decode())
            at += 1 + length
        at += 1
        question = packet[12:at+4]
        name = '.'.join(names)
        hold = dns_holds.get(name)
        if hold:
            hold[0].set()
            if not hold[1].wait(10):
                return
        if packet[at:at+2] != b'\x00\x01':
            return
        answer = (packet[:2] + b'\x81\x80\x00\x01\x00\x01\x00\x00\x00\x00'
                  + question + b'\xc0\x0c\x00\x01\x00\x01'
                  + struct.pack('>IH', 1, 4) + socket.inet_aton('127.0.0.1'))
        sock.sendto(answer, self.client_address)
dns_server = socketserver.ThreadingUDPServer(('127.0.0.1', 0), DNS)
dns_server.daemon_threads = True
threading.Thread(target=dns_server.serve_forever, daemon=True).start()

config = loads + f'''daemon off; master_process on; worker_processes {a.workers};
pid {run}/pid; error_log {run}/error.log debug;
thread_pool default threads=2;
thread_pool proof threads=1 max_queue=64;
events {{ worker_connections 256; }}
http {{ access_log off; client_body_temp_path {run}/body; proxy_temp_path {run}/temp;
 cache_purge_store {run}/index cache=8m write_buffer=2m;
 cache_purge_state_zone capture 4m;
 cache_purge_read_pool proof;
 proxy_cache_path {run}/cache-a levels=1:2 keys_zone=a:1m;
 proxy_cache_path {run}/cache-b levels=1:2 keys_zone=b:1m;
 {' '.join(gateway_paths)}
 resolver 127.0.0.1:{dns_server.server_address[1]} ipv6=off;
 resolver_timeout 5s;
 map $arg_gate $send_host {{ default $arg_gate; "" 127.0.0.1; }}
 map $arg_z $zone {{ default a; b b; off off; }}
 map $request_method $inline_misc {{ default $arg_m; PURGE $http_x_misc; }}
 cache_key_namespace tenant; cache_key_uri $uri; cache_key_misc $arg_m; cache_key_range $slice_range;
 cache_purge_key_url $cache_key_url;
 cache_purge_key_misc $cache_key_misc;
 cache_purge_key_namespace $cache_key_namespace;
 proxy_cache_purge on; fastcgi_cache_purge on; scgi_cache_purge on; uwsgi_cache_purge on;
 server {{ listen 127.0.0.1:{port};
  proxy_cache $zone; proxy_cache_key $cache_key_full;
  proxy_cache_valid 200 206 1h; proxy_cache_bypass $arg_bypass;
  proxy_buffers 8 4k; proxy_buffer_size 4k;
  cache_purge_tag $upstream_http_cache_tag;

  add_header X-Status $upstream_cache_status always;
  {' '.join(gateway_locations)}
  location /__purge/v2/ {{ cache_purge_api; proxy_cache off;
      allow 127.0.0.1; deny all; }}
  location /__custom/v2/ {{ cache_purge_api; proxy_cache off;
      allow 127.0.0.1; deny all;
      cache_purge_tag_namespace_header X-Test-Purge-Scope; }}
  location /__denied/v2/ {{ cache_purge_api; proxy_cache off; deny all; }}
  location /__open/v2/ {{ cache_purge_api; proxy_cache off; }}
  location /push/ {{ proxy_cache off;
      cache_purge_key_url $http_x_target;
      cache_purge_key_misc $http_x_misc;
      cache_purge_key_namespace $http_x_namespace; }}
  location /push-custom/ {{ proxy_cache off;
      cache_purge_key_url $http_x_target;
      cache_purge_key_misc $http_x_misc;
      cache_purge_key_namespace $http_x_namespace;
      cache_purge_type_header X-Test-Type;
      cache_purge_action_header X-Test-Action;
      cache_purge_tags_header X-Test-Tags;
      cache_purge_operation_id_header X-Test-Id;
      cache_purge_issued_at_header X-Test-Issued; }}
  location /push-denied/ {{ deny all; proxy_cache off; }}
  location /push-off/ {{ proxy_cache_purge off; fastcgi_cache_purge off;
      scgi_cache_purge off; uwsgi_cache_purge off;
      proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /inline/ {{ cache_purge_key_misc $inline_misc;
      proxy_cache_revalidate on;
      proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /propagation/ {{ {propagation_config}
      proxy_cache_revalidate on; proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /propagation/send-race {{ {propagation_config}
      proxy_pass http://$send_host:{origin.server_port}; }}
  location /propagation/uncached-vary {{ {propagation_config}
      proxy_cache off; cache_key_uri /propagation/uncached-seed;
      proxy_pass http://127.0.0.1:{origin.server_port}/propagation/uncached-seed; }}
  location /propagation/uncached-slice {{ {propagation_config}
      proxy_cache off; cache_key_uri /propagation/slice-uncached-seed;
      {slice_config} proxy_set_header Range $slice_range;
      proxy_pass http://127.0.0.1:{origin.server_port}/propagation/slice-uncached-seed; }}
  location /propagation/uncached-origin {{ {propagation_off_config}
      proxy_cache off; proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /propagation/uncached-manual {{ {propagation_manual_config}
      cache_key_namespace $arg_unused_namespace;
      cache_purge_key_url "$scheme://$host$uri";
      cache_purge_key_misc ""; cache_purge_key_namespace manual;
      proxy_cache off; proxy_cache_key "manual\x1fhttp\x1f$host\x1f$uri\x1f\x1f";
      proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /propagation/uncached-ignore {{ {propagation_config}
      cache_purge_ignore_propagation on;
      proxy_cache off; proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /propagation/slice {{ {propagation_config}
      {slice_config} proxy_set_header Range $slice_range;
      proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /propagation/groups/ {{ {propagation_config}

      proxy_cache_revalidate on;
      proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location / {{
                proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /empty-namespace {{ cache_key_namespace ""; proxy_cache_revalidate on;
      proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /large {{
                     aio threads; aio_write on; proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /slice {{
                    {slice_config} proxy_set_header Range $slice_range;
                    proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /raw {{ proxy_cache_key "native-format:$host|$uri|$arg_m";
                  cache_purge_key_url "$scheme://$host$uri";
                  cache_purge_key_misc $arg_m; cache_purge_key_namespace rawtenant;

                  proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /index-off {{ proxy_cache_purge off;
      cache_purge_key_url $arg_missing; cache_purge_key_namespace $arg_bad;
      proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /alias/finish- {{ cache_key_uri /finish/shared;
                     proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /alias/ {{ cache_key_uri /alias/shared;
                     proxy_cache_revalidate on;


                     proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /host-shared/ {{ cache_key_host $arg_cache_host;

                           proxy_cache_revalidate on;
                           proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /invalidate/ {{
                          proxy_cache_revalidate on;
                          proxy_cache_use_stale error timeout http_500 updating;
                          proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /revalidate- {{
                          proxy_cache_revalidate on; proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location /revalidate-expression {{
                          cache_purge_tag "$upstream_http_cache_tag, expression";
                          proxy_cache_revalidate on; proxy_pass http://127.0.0.1:{origin.server_port}; }}
  location = /negative-cache {{

      proxy_cache_valid 502 1h; error_page 502 = /negative-error;
      proxy_pass http://127.0.0.1:{origin.server_port};
  }}
  location = /negative-error {{ return 200 "negative-old"; }}
  location = /negative-timeout {{

      proxy_cache_valid 504 1h; error_page 504 = /negative-error;
      proxy_read_timeout 50ms;
      proxy_pass http://127.0.0.1:{origin.server_port};
  }}
  location = /error-no-store {{

      proxy_intercept_errors on; error_page 404 = /negative-error;
      proxy_cache_valid 500 1h;
      proxy_pass http://127.0.0.1:{origin.server_port};
  }}
  location = /error-memory {{

      proxy_intercept_errors on; error_page 500 = /negative-error;
      proxy_cache_valid 500 1h;
      proxy_pass http://127.0.0.1:{origin.server_port};
  }}
  location = /error-disk {{

      proxy_cache_valid 500 1h;
      proxy_pass http://127.0.0.1:{origin.server_port};
  }}
  location = /error-disk-unmatched {{

      proxy_intercept_errors on; error_page 404 = /negative-error;
      proxy_cache_valid 500 1h;
      proxy_pass http://127.0.0.1:{origin.server_port};
  }}
  location /error-disk- {{

      proxy_cache_valid 500 502 504 1h;
      proxy_pass http://127.0.0.1:{origin.server_port};
  }}
  location ~ ^/redirect-(cache|tag|unknown|named|stale|revalidated)$ {{

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

      proxy_intercept_errors on;
      expr intercept_error = $arg_intercept 1;
      when intercept_error {{ error_page 500 = /negative-error; }}
      proxy_cache_valid 500 1h;
      proxy_pass http://127.0.0.1:{origin.server_port};
    }}
    location /tag-when/ {{
      proxy_cache_revalidate on;
      expr tag_policy = $arg_policy 1;
      when tag_policy {{ cache_purge_tag "$upstream_http_cache_tag, conditional"; }}
      proxy_pass http://127.0.0.1:{origin.server_port};
    }}
    location /identity-when/ {{
      cache_purge_tag identity-when;
      expr identity_policy = $arg_managed 1;
      when identity_policy {{
        proxy_cache_purge on;
        cache_purge_key_url "http://cache.test$uri";
        cache_purge_key_misc $arg_misc;
        cache_purge_key_namespace "";
      }}
      proxy_cache_purge off;
      cache_purge_key_url $arg_missing;
      proxy_cache_key "$uri|$arg_misc";
      proxy_pass http://127.0.0.1:{origin.server_port};
    }}
    location /push-when/ {{
      fastcgi_cache_purge off; scgi_cache_purge off; uwsgi_cache_purge off;
      expr allow_push = $arg_allow 1;
      when allow_push {{ proxy_cache_purge on; }}
      proxy_cache_purge off;
      cache_purge_key_url $http_x_target;
      proxy_cache off; proxy_pass http://127.0.0.1:{origin.server_port};
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

def api(path, payload=None, method=None, headers=None, raw=None):
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=15)
    body = raw if raw is not None else (json.dumps(payload, ensure_ascii=False).encode() if payload is not None else None)
    supplied = {'Host': 'cache.test', **(headers or {})}
    if body is not None: supplied.setdefault('Content-Type', 'application/json')
    conn.request(method or ('POST' if body is not None else 'GET'), path, body, supplied)
    response = conn.getresponse(); data = response.read()
    result = response.status, dict(response.getheaders()), json.loads(data) if data else None
    conn.close()
    return result

def api_input(name, selector, mode='delete'):
    return {'version': 'ngx-http-cache-purge/v2', 'operation_id': name, 'mode': mode,
            'selector': selector, 'event': {'issuer': 'scm', 'stream_id': 'test-stream',
            'sequence': '1042', 'issued_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}}
def tag_group(*values):
    tags = sorted({value.encode() for value in values})
    return bytes([len(tags)]) + b''.join(struct.pack('>H', len(tag)) + tag for tag in tags)

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
def rpc(op, payload=b'', fds=(), receive_fd=False, codes=(0,)):
    with socket.socket(socket.AF_UNIX) as s:
        s.settimeout(8); s.connect(str(run / 'index/component.sock'))
        packet = struct.pack('>4sHHQII', b'NCP1', 2, op, 1, len(payload), 0) + payload
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
                assert not flags & socket.MSG_CTRUNC
            magic, version, operation, ident, length, result = struct.unpack('>4sHHQII', first + read(24 - len(first)))
            body = read(length)
            assert (magic, version, operation, ident) == (b'NCP1', 2, op, 1)
            assert result in codes, (result, body)
            if receive_fd: assert len(received) == (1 if result == 0 else 0)
            if result != 0: return None
            return (body, received.pop()) if receive_fd else body
        finally:
            for fd in received: os.close(fd)

def physical_remove(item, outcome=1, collected=False):
    path = Path(os.fsdecode(item['root'])) / os.fsdecode(item['path'])
    _, lease = until(lambda: rpc(17, string(str(path)), receive_fd=True,
                                 codes=(0, 6)))
    try:
        removed = rpc(24, item['id'] + struct.pack('>Q', item['revision']), (lease,),
                      codes=(0, 1) if collected else (0,))
        if removed is None: return
        assert len(removed) == 29 and removed[:4] == struct.pack('>I', outcome)
        assert removed[4:20] == item['id'] and removed[-1] == 3
    finally:
        os.close(lease)
def query(value, kind=1, scope=''):
    found, cursor = [], b''
    while True:
        d = Decode(rpc(8, bytes([kind]) + string(value) + string(scope) + string(cursor) + bytes([32])))
        count, done = d.number(1), d.number(1)
        next_cursor = d.string()
        found.extend((d.take(16), d.number(8), d.number(1)) for _ in range(count))
        d.end()
        if done: return found
        assert next_cursor and next_cursor != cursor and len(found) < 1000
        cursor = next_cursor
def checked_marker(path):
    marker = os.getxattr(path, 'user.ngx_cache_purge.id')
    assert len(marker) >= 40 and marker[4:7] == b'\0\0\0'
    if marker[:4] == b'NCF1':
        assert len(marker) == 40 and marker[7] in (1, 2, 3, 4)
    else:
        assert marker[:4] == b'NCF2' and len(marker) >= 152 and marker[7] == 2
        count = int.from_bytes(marker[76:80], 'big')
        assert count <= 64 and len(marker) == 152 + 16 * count
    return marker


def records(path, count, host='cache.test'):
    # Fixture setup requires published files. query()/record() remain available
    # for tests that intentionally observe an intermediate index transaction.
    def published():
        found = query('http://' + host + path)
        if len(found) != count or any(item[2] != 2 for item in found): return None
        result = [record(item[0]) for item in found]
        for item in result:
            target = Path(os.fsdecode(item['root'])) / os.fsdecode(item['path'])
            try:
                marker = checked_marker(target)
            except OSError as error:
                if error.errno in (2, 61): return None  # ENOENT / ENODATA
                raise
            if marker[7] != 2 or marker[8:24] != item['id']: return None
        return result
    return until(published)

def identity_key(namespace, url, misc):
    return hashlib.sha256(b'NCI1' + string(namespace) + string(url) + string(misc)).hexdigest().encode()

def record(ident, optional=False):
    raw = rpc(2, ident, codes=(0, 1) if optional else (0,))
    if raw is None: return None
    d = Decode(raw); state, revision = d.number(1), d.number(8); assert d.take(16) == ident
    names = ('key', 'url', 'host', 'namespace', 'root', 'zone', 'path', 'range', 'vary')
    result = {name: d.string() for name in names}
    result.update(id=ident, state=state, revision=revision, metadata=d.string(), misc=d.string())
    result['tags'] = [d.string() for _ in range(d.number(1))]
    result['flags'] = d.take(2); result['components'] = [d.string() for _ in range(6)]; d.end()
    return result

def retired(ident):
    item = record(ident, optional=True)
    return item is None or item['state'] == 3

def deleted_response(path, body, headers=None):
    reply = get(path, headers)
    # An effective Delete can reach the request gate or remove the old file
    # first. Both paths must fetch the expected entity, never reuse stale data.
    assert reply[0] == 200 and reply[1] in ('BYPASS', 'MISS') and reply[2] == body, reply
    return reply
def pending():
    return rpc(9, string('') + bytes([32]))[0]

def fence(name, kind, value, scope='', mode=2, submit=False, activate=True):
    previous = struct.unpack('>IQ', rpc(16))[1]
    payload = (string(name) + bytes([mode]) + struct.pack('>Q', int(time.time() * 1000))
               + bytes([kind]) + string(value) + string(scope) + string('read-test'))
    # Acceptance is idempotent. Keep its input frozen while publication is busy.
    receipt = until(lambda: rpc(27 if submit else 14, payload, codes=(0, 6)))
    assert receipt[len(payload) + 40] == 1
    until(lambda: (state := struct.unpack('>IQ', rpc(16)))[0] == 6 and state[1] != previous)
    if activate and not submit:
        # Ordinary request checks use effective rules with a fixed propagation
        # deadline. Only recovery fixtures deliberately leave them PREPARING.
        previous = struct.unpack('>IQ', rpc(16))[1]
        active = until(lambda: rpc(26, string(name), codes=(0, 6)))
        assert active[len(payload) + 40] >= 2
        until(lambda: (state := struct.unpack('>IQ', rpc(16)))[0] == 6 and state[1] != previous)
    if submit:
        until(lambda: rpc(15, string(name))[len(payload) + 40] >= 2
              and struct.unpack('>IQ16sQQQQI', rpc(28))[0] == 6)
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
    def published():
        marker = checked_marker(path)
        assert marker[8:24] == item['id'], (path, marker.hex(), item['id'].hex())
        return marker[7] == 2
    # COMMITTED retains its intent while the WAL group awaits durability.
    # An admitted file requires the separate READY publication as well.
    until(published)
    metadata = item['metadata']
    if metadata.startswith(b'NFR1'):
        assert int.from_bytes(metadata[60:64], 'big') == len(metadata) - 64
        metadata = metadata[64:]
    assert item['state'] == 2 and metadata.startswith(b'NFP1')
    marker = checked_marker(path)
    assert marker[:4] == b'NCF2'
    identity = b'NCI1' + b''.join(string(value) for value in
        (item['namespace'], item['url'], item['misc']))
    assert marker[120:152] == hashlib.sha256(identity).digest()
    assert marker[152:] == b''.join(hashlib.sha256(tag).digest()[:16]
                                   for tag in item['tags'])
    return path

def origin_metadata(item):
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
    assert size in (28, 64) and len(value) == size and value[:4] == b'NFW1'
    if size == 64: assert value[28:32] == b'NFK1'
    return value

def watermark(item):
    value = origin_metadata(item)
    return value[4:20], int.from_bytes(value[20:28], 'big')

@contextlib.contextmanager
def prepared_replacement(item):
    target = ready_file(item)
    address = b''.join(string(item[name]) for name in ('root', 'zone', 'path'))
    acquired, lease = rpc(20, address, receive_fd=True)
    temporary = None
    fd = -1
    try:
        d = Decode(acquired)
        assert d.string() == item['root'] and d.take(16) == item['id']
        d.take(16); d.number(8); d.end()
        fd, name = tempfile.mkstemp(dir=run / 'temp')
        temporary = Path(name)
        # Copy native bytes, not the old instance's publication marker.
        temporary.write_bytes(target.read_bytes())
        ident = os.urandom(16)
        fields = ('key', 'url', 'host', 'namespace', 'root', 'zone', 'path', 'range', 'vary')
        payload = (ident + b''.join(string(item[name]) for name in fields)
                   + string(origin_metadata(item)) + string(item['misc'])
                   + bytes([len(item['tags'])])
                   + b''.join(map(string, item['tags'])) + item['flags']
                   + b''.join(map(string, item['components'])) + item['id']
                   + string(str(temporary)))
        assert rpc(18, payload, (lease, fd)) == ident + struct.pack('>QB', 1, 1)
        temporary.replace(target)

        def finish():
            result = rpc(19, ident + struct.pack('>Q', 1), (lease,))
            assert result == ident + struct.pack('>QB', 2, 2), result
            ready_file(record(ident))

        yield finish
    finally:
        if fd >= 0: os.close(fd)
        if temporary is not None: temporary.unlink(missing_ok=True)
        os.close(lease)


def missing_index_read():
    assert get('/lost-index')[1] == 'MISS'
    original = records('/lost-index', 1)[0]
    path = ready_file(original)
    marker = os.getxattr(path, 'user.ngx_cache_purge.id')
    # A legacy publisher could persist READY before its lost WAL suffix.
    # NCF2 publication now waits for its index batch, so use the legacy marker
    # to model this recoverable upgrade case rather than forge a new proof.
    missing = os.urandom(16)
    assert record(missing, optional=True) is None
    os.setxattr(path, 'user.ngx_cache_purge.id', b'NCF1\0\0\0\2' + missing + marker[24:40])
    fresh_bodies['/lost-index'] = b'refilled-after-lost-index'
    assert get('/lost-index') == (200, 'BYPASS', b'refilled-after-lost-index')
    replacement = until(lambda: (item if (item := records('/lost-index', 1)[0])['id']
                                != original['id'] else None))
    ready_file(replacement)
    assert get('/lost-index') == (200, 'HIT', b'refilled-after-lost-index')
    assert calls.count('/lost-index') == 2
    checks.append('a READY native file with a missing index ID refills instead of serving the old body or requiring a cache scan')


def finish_busy_retries():
    suffix = '?m=finish-retry'
    assert get('/alias/finish-0' + suffix) == (200, 'MISS', b'/alias/finish-0')
    original = records('/finish/shared', 1)[0]
    before = (run / 'error.log').read_text().count('publication stage 2 failed:')
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        held = pool.submit(get, '/alias/finish-wait' + suffix + '&bypass=1')
        try:
            assert finish_waiting.wait(5)
            fence('finish-busy-unrelated', 1, 'http://cache.test/unrelated-finish')
        finally:
            finish_release.set()
        assert held.result(timeout=10) == (200, 'BYPASS', b'finish-retry-entity')
    replacement = until(lambda: (item if (item := records('/finish/shared', 1)[0])['id']
                                 != original['id'] else None))
    ready_file(replacement)
    assert get('/alias/finish-wait' + suffix) == (200, 'HIT', b'finish-retry-entity')
    assert calls.count('/alias/finish-wait') == 1
    assert (run / 'error.log').read_text().count('publication stage 2 failed:') == before
    checks.append('an unrelated push during a real fill does not drop its publication lease or refetch the entity')


def variant_hashes(item, language):
    value = origin_metadata(item)
    assert len(value) == 64
    contents = ready_file(item).read_bytes()
    native_key = contents.split(b'\nKEY: ', 1)[1].split(b'\n', 1)[0]
    main = hashlib.md5(native_key).digest()
    variant = hashlib.md5(main + b'accept-language:' + language.encode() + b'\r\n').digest()
    assert value[32:48] == main and value[48:64] == variant
    assert item['path'].split(b'/')[-1] in (main.hex().encode(), variant.hex().encode())
    return main, variant

def inline_purge_cases():
    def purge(path, headers=None):
        status, response_headers, body = get(path, headers, method='PURGE', details=True)
        return status, response_headers, json.loads(body)

    def accepted(path, headers=None):
        status, response_headers, receipt = purge(path, headers)
        assert status == 202 and receipt['effective'] and receipt['durable'] and receipt['ready'], (status, receipt)
        assert response_headers['Cache-Control'] == 'no-store'
        assert receipt['status_url'] is None and len(receipt['request_digest']) == 64
        return receipt

    def status(receipt):
        return api('/__purge/v2/operations/' + receipt['operation_id'])[2]

    uri = '/inline/url'
    for suffix in ('', '?m=other', '?z=b'):
        assert get(uri + suffix)[1] == 'MISS'
    old = records(uri, 3)
    for item in old: ready_file(item)
    before = calls.count(uri)
    pushed = accepted(uri)
    assert pushed['selector'] == {'type': 'url', 'value': 'http://cache.test' + uri}
    assert pushed['mode'] == 'delete' and calls.count(uri) == before
    fresh_bodies[uri] = b'inline-url-fresh'
    for suffix in ('', '?m=other', '?z=b'):
        deleted_response(uri + suffix, b'inline-url-fresh')
    checks.append('PURGE on the business URL defaults to durable URL Delete across misc variants and zones without reaching upstream')
    for protocol in gateways:
        uri = '/' + protocol + '/inline'
        assert get(uri)[1] == 'MISS'
        ready_file(records(uri, 1)[0])
        accepted('/push-' + protocol + '/control', {'X-Target': 'http://cache.test' + uri})
        deleted_response(uri, (protocol + ':body').encode())
    checks.append('each non-proxy protocol switch independently activates the PURGE handler on a channel without an upstream or cache zone')
    if extension and a.expr:
        assert get('/push-when/disabled', method='PURGE')[0] == 501
        accepted('/push-when/enabled?allow=1', {'X-Target': 'http://cache.test/inline/conditional'})
        checks.append('expr when controls PURGE activation after access checks and preserves the disabled location handler')

    uri = '/inline/key'
    for suffix in ('', '?m=one', '?m=two', '?m=one&z=b'):
        assert get(uri + suffix)[1] == 'MISS'
    for item in records(uri, 4): ready_file(item)
    headers = {'X-Cache-Purge-Type': 'KEY', 'X-Misc': 'one',
               'X-Cache-Purge-Operation-Id': 'inline-key-one',
               'X-Cache-Purge-Issued-At': str(int(time.time() * 1000))}
    pushed = accepted(uri, headers)
    assert pushed['selector'] == {'type': 'key'}
    assert accepted(uri, headers)['sequence'] == pushed['sequence']
    assert purge(uri, {**headers, 'X-Misc': 'two'})[0] == 409
    fresh_bodies[uri] = b'inline-key-fresh'
    deleted_response(uri + '?m=one', b'inline-key-fresh')
    deleted_response(uri + '?m=one&z=b', b'inline-key-fresh')
    assert get(uri + '?m=two') == (200, 'HIT', uri.encode())
    assert get(uri) == (200, 'HIT', uri.encode())
    accepted(uri, {'X-Cache-Purge-Type': 'key'})
    deleted_response(uri, b'inline-key-fresh')
    assert get(uri + '?m=two') == (200, 'HIT', uri.encode())
    checks.append('PURGE key uses configured URL and header-supplied misc, keeps empty misc exact, and supports stable retries with conflict detection')

    target = 'http://cache.test' + uri
    accepted('/push/arbitrary-route', {'Host': 'purge.test', 'X-Target': target,
             'X-Cache-Purge-Type': 'key', 'X-Misc': 'two', 'X-Namespace': 'other'})
    assert get(uri + '?m=two') == (200, 'HIT', uri.encode())
    custom = {'Host': 'purge.test', 'X-Target': target, 'X-Misc': 'two', 'X-Namespace': 'tenant',
              'X-Test-Type': 'key', 'X-Test-Action': 'Delete', 'X-Test-Id': 'inline-custom',
              'X-Test-Issued': str(int(time.time() * 1000)), 'X-Cache-Purge-Type': 'host'}
    pushed = accepted('/push-custom/unrelated-path', custom)
    assert pushed['selector'] == {'type': 'key'} and pushed['operation_id'] == 'inline-custom'
    assert accepted('/push-custom/unrelated-path', custom)['sequence'] == pushed['sequence']
    deleted_response(uri + '?m=two', b'inline-key-fresh')
    checks.append('dedicated PURGE channels use configured targets and namespace isolation; custom control headers override their default names')

    tagged = ('/inline/tag-a', '/inline/tag-b')
    for path, tag in zip(tagged, ('inline-red', 'inline-blue')):
        policies[path] = tag
        conditional_paths.add(path)
        assert get(path)[1] == 'MISS'
        ready_file(records(path, 1)[0])
    policies['/raw/inline-tag'] = 'inline-red'
    assert get('/raw/inline-tag')[1] == 'MISS'
    ready_file(records('/raw/inline-tag', 1)[0])
    pushed = accepted('/push/no-target-url', {'X-Cache-Purge-Type': 'tag',
             'X-Cache-Purge-Action': 'Invalidate', 'X-Cache-Purge-Tags': 'inline-red, inline-blue, inline-red',
             'X-Namespace': 'tenant'})
    assert pushed['selector']['values'] == ['inline-blue', 'inline-red']
    assert pushed['tag_namespace'] == 'tenant'
    for path in tagged:
        assert get(path) == (200, 'REVALIDATED', path.encode())
        assert get(path)[1] == 'HIT'
    assert get('/raw/inline-tag')[1] == 'HIT'
    accepted('/push-custom/tag', {'X-Test-Type': 'tag', 'X-Test-Tags': 'inline-red',
             'X-Test-Action': 'delete', 'X-Namespace': 'rawtenant'})
    deleted_response('/raw/inline-tag', b'/raw/inline-tag')
    checks.append('namespace TAG PURGE evaluates only namespace, canonicalizes groups, supports Invalidate/304 and honors renamed TAG headers')

    for path in ('/empty-namespace-inline', '/inline/named-tag'):
        policies[path] = 'inline-empty'
        assert get(path)[1] == 'MISS'
        ready_file(records(path, 1)[0])
    pushed = accepted('/push/empty', {'X-Cache-Purge-Type': 'tag', 'X-Cache-Purge-Tags': 'inline-empty'})
    assert pushed['tag_namespace'] == ''
    deleted_response('/empty-namespace-inline', b'/empty-namespace-inline')
    assert get('/inline/named-tag')[1] == 'HIT'
    checks.append('an absent namespace expression value remains the exact empty TAG scope')

    uri = '/inline/host-tag'
    policies[uri] = 'inline-host-tag'
    for host in ('inline-host.test', 'other-inline.test'):
        assert get(uri, {'Host': host})[1] == 'MISS'
        ready_file(records(uri, 1, host=host)[0])
    accepted('/push/host-tag', {'Host': 'purge.test', 'X-Target': 'http://inline-host.test/unused',
             'X-Cache-Purge-Type': 'host-tag', 'X-Cache-Purge-Tags': 'inline-host-tag'})
    deleted_response(uri, uri.encode(), {'Host': 'inline-host.test'})
    assert get(uri, {'Host': 'other-inline.test'})[1] == 'HIT'
    accepted('/push/host', {'Host': 'purge.test', 'X-Target': 'http://other-inline.test/unused', 'X-Cache-Purge-Type': 'host'})
    deleted_response(uri, uri.encode(), {'Host': 'other-inline.test'})
    checks.append('host and host-scoped TAG PURGE derive the target host from configuration, not the management connection Host')

    for path in ('/inline/bulk/a', '/inline/bulk/b'):
        assert get(path)[1] == 'MISS'
        ready_file(records(path, 1)[0])
    item = records('/inline/bulk/a', 1)[0]
    target = ready_file(item)
    address = b''.join(string(item[name]) for name in ('root', 'zone', 'path'))
    _, lease = rpc(20, address, receive_fd=True)
    try:
        pushed = accepted('/push/prefix', {'X-Target': 'http://cache.test/inline/bulk/', 'X-Cache-Purge-Type': 'prefix'})
        assert status(pushed)['cleanup']['state'] != 'done' and target.exists()
        assert get('/asset')[1] == 'HIT'
    finally:
        os.close(lease)
    until(lambda: status(pushed)['cleanup']['state'] == 'done')
    assert not target.exists()
    checks.append('prefix PURGE returns effective durable acceptance while a file lease is held; cleanup completes after the lease is released')

    previous = api('/__purge/v2/capabilities')[2]['last_sequence']
    invalid = ({'X-Cache-Purge-Type': 'regex'}, {'X-Cache-Purge-Type': ''},
               {'X-Cache-Purge-Action': 'refresh'}, {'X-Cache-Purge-Operation-Id': 'missing-time'},
               {'X-Cache-Purge-Issued-At': '1'}, {'X-Cache-Purge-Type': 'tag'},
               {'X-Cache-Purge-Tags': 'unselected'}, {'X-Cache-Purge-Type': 'key', 'X-Namespace': 'bad,name'})
    for headers in invalid:
        status_code, _, rejected = purge('/push/invalid', {'X-Target': 'http://cache.test/inline/invalid', **headers})
        assert status_code == 400 and rejected['outcome'] == 'not_submitted', rejected
    assert purge('/push/missing-target')[0] == 400
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=10)
    conn.putrequest('PURGE', '/inline/duplicate', skip_host=True)
    conn.putheader('Host', 'cache.test')
    conn.putheader('X-Cache-Purge-Type', 'url'); conn.putheader('X-Cache-Purge-Type', 'host')
    conn.endheaders(); reply = conn.getresponse()
    assert reply.status == 400 and json.loads(reply.read())['outcome'] == 'not_submitted'
    conn.close()
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=10)
    conn.request('PURGE', '/inline/body', body='x', headers={'Host': 'cache.test'})
    reply = conn.getresponse(); assert reply.status == 400
    reply.read(); conn.close()
    assert get('/push-denied/blocked', method='PURGE')[0] == 403
    assert get('/push-off/native', method='PURGE')[0] == 501
    assert api('/__purge/v2/capabilities')[2]['last_sequence'] == previous
    checks.append('PURGE preserves access controls and disabled handlers and rejects malformed, duplicate, ambiguous or body-bearing requests before acceptance')


checks = []
obsolete_directives = (
    'cache_purge_key $cache_key_base;', 'cache_purge_url $cache_key_url;',
    'cache_purge_tag_namespace $cache_key_namespace;',
    'cache_purge_namespace old 1m /tmp/old-state;',
    'proxy_cache_refresh old $uri;',
    'cache_purge_refresh_profile /old/ /;',
    'cache_purge_refresh_timeout 1s;',
    'cache_purge_response_type json;', 'cache_purge_thread_pool default;',
    'cache_purge_background_queue on;', 'cache_purge_queue_size 10;',
    'cache_purge_batch_size 10;', 'cache_purge_throttle_ms 10ms;',
    'cache_purge_legacy_status off;', 'cache_purge_vary_aware on;',
    'cache_purge_queue_status /tmp/old-cache;',
)
for directive in obsolete_directives:
    candidate_path = run / 'obsolete.conf'
    candidate_path.write_text(config.replace('http {', 'http {\n' + directive, 1))
    tested = subprocess.run([str(build / 'objs/nginx'), '-t', '-p', str(run) + '/', '-c', str(candidate_path)],
                            env=env, capture_output=True, text=True, timeout=15)
    assert tested.returncode != 0 and ('unknown directive "' + directive.split()[0] + '"') in tested.stderr, tested.stderr
checks.append('removed generation, REFRESH and SHM queue directives fail configuration explicitly')
rogue = f'''  location = /rogue-cache {{
    proxy_pass http://127.0.0.1:{origin.server_port};
  }}
'''
mixed = config.replace('  location / {', rogue + '  location / {', 1)
for label, candidate in (
        ('inherited', mixed),
        ('disabled', mixed.replace('location = /rogue-cache {', 'location = /rogue-cache { proxy_cache off;'))):
    candidate_path = run / (label + '.conf'); candidate_path.write_text(candidate)
    tested = subprocess.run([str(build / 'objs/nginx'), '-t', '-p', str(run) + '/', '-c', str(candidate_path)],
                            env=env, capture_output=True, text=True, timeout=15)
    assert tested.returncode == 0, (label, tested.stderr)
checks.append('protocol switches and independent identity expressions inherit into cached locations')
for label, candidate, message in (
        ('missing-read-pool', config.replace('cache_purge_read_pool proof;', ''),
         'require cache_purge_read_pool'),
        ('unknown-read-pool', config.replace('cache_purge_read_pool proof;', 'cache_purge_read_pool missing;'),
         'unknown thread pool'),
        ('missing-url', config.replace('cache_purge_key_url $cache_key_url;', ''),
         'requires cache_purge_key_url'),
        ('empty-url', config.replace('cache_purge_key_url $cache_key_url;', 'cache_purge_key_url "";'),
         'nonempty absolute'),
        ('relative-url', config.replace('cache_purge_key_url $cache_key_url;', 'cache_purge_key_url /relative;'),
         'nonempty absolute'),
        ('invalid-namespace', config.replace('cache_purge_key_namespace $cache_key_namespace;', 'cache_purge_key_namespace "bad,name";'),
         'printable ASCII namespace'),
        ('invalid-switch', config.replace('proxy_cache_purge on;', 'proxy_cache_purge maybe;', 1),
         'invalid value')):
    candidate_path = run / (label + '.conf'); candidate_path.write_text(candidate)
    tested = subprocess.run([str(build / 'objs/nginx'), '-t', '-p', str(run) + '/', '-c', str(candidate_path)],
                            env=env, capture_output=True, text=True, timeout=15)
    assert tested.returncode != 0 and message in tested.stderr, (label, tested.stderr)
checks.append('enabled locations require an explicit URL and reject invalid static URL, namespace and switch configuration')
legacy_config = re.sub(r'cache_purge_(?:store|state_zone)\s+[^;]+;', '', config)
legacy_config = legacy_config.replace('cache_purge_api;', 'return 503;')
legacy_config = re.sub(r'(proxy|fastcgi|scgi|uwsgi)_cache_purge on;', r'\1_cache_purge off;', legacy_config)
(run / 'nginx.conf').write_text(legacy_config)
process = start()
try:
    def listening():
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=.1):
                return True
        except OSError:
            return False
    until(listening)
    assert get('/negative-cache') == (200, 'MISS', b'negative-old')
    assert get('/negative-cache') == (200, 'HIT', b'negative-old')
    assert calls.count('/negative-cache') == 1
    (run / 'nginx.conf').write_text(config)
    process.send_signal(signal.SIGHUP)
    until(lambda: 'enabling/disabling cache purge indexing requires a full restart' in (run / 'error.log').read_text())
    assert get('/negative-cache') == (200, 'HIT', b'negative-old')
    assert calls.count('/negative-cache') == 1
    stop(process)
    calls.clear()
    process = start()
    until(lambda: (run / 'index/component.sock').exists())
    until(lambda: struct.unpack('>IQ', rpc(16))[0] == 6)
    checks.append('enabling persistent indexing by reload is rejected while legacy negative nodes exist; a full restart enters the indexed mode')
    assert get('/asset') == (200, 'MISS', b'/asset')
    first = records('/asset', 1)[0]; ready_file(first)
    ledger, sequence = watermark(first)
    assert ledger != bytes(16) and sequence == 0
    assert first['key'] == identity_key('tenant', 'http://cache.test/asset', '')
    assert first['namespace'] == b'tenant' and set(first['tags']) == {b'news', b'red'}
    assert len(origin_metadata(first)) == 28
    assert get('/asset')[1] == 'HIT' and calls.count('/asset') == 1
    checks.append('real upstream fill is WAL-indexed with the native key snapshot, scoped tags and READY marker')
    assert get('/index-off?bad=bad,namespace')[1] == 'MISS'
    until(lambda: get('/index-off?bad=bad,namespace')[1] == 'HIT')
    initial_requests = int.from_bytes(rpc(1)[40:48], 'big')
    for _ in range(3):
        assert get('/index-off?bad=bad,namespace')[1] == 'HIT'
    final_requests = int.from_bytes(rpc(1)[40:48], 'big')
    assert final_requests - initial_requests == 1
    assert query('http://cache.test/index-off') == []
    checks.append('an off protocol skips identity evaluation, indexing and all request IPC while retaining native HITs')
    for protocol in gateways:
        uri = '/' + protocol + '-off/item'
        assert get(uri)[1] == 'MISS'
        until(lambda: get(uri)[1] == 'HIT')
        assert query('http://cache.test' + uri) == []
    checks.append('FastCGI, SCGI and uwsgi switches independently disable capture even when other protocol switches are on')
    if extension and a.expr:
        uri = '/identity-when/item'
        assert get(uri + '?misc=alpha')[1] == 'MISS'
        until(lambda: get(uri + '?misc=alpha')[1] == 'HIT')
        assert query('http://cache.test' + uri) == []
        assert get(uri + '?managed=1&misc=alpha')[1] in ('MISS', 'BYPASS')
        item = records(uri, 1)[0]
        assert item['namespace'] == b'' and item['misc'] == b'alpha'
        assert item['key'] == identity_key('', 'http://cache.test' + uri, 'alpha')
        ready_file(item)
        assert get(uri + '?managed=1&misc=alpha')[1] == 'HIT'
        assert get(uri + '?misc=alpha')[1] == 'HIT'
        checks.append('expr when enables capture and selects URL, misc and an empty namespace without evaluating inactive candidates')
    misc_proofs = []
    for misc in ('one', 'two'):
        assert get('/misc-proof?m=' + misc)[1] == 'MISS'
        items = records('/misc-proof', len(misc_proofs) + 1)
        item = next(item for item in items if item['misc'] == misc.encode())
        target = ready_file(item)
        expected = b'NCI1' + string('tenant') + string('http://cache.test/misc-proof') + string(misc)
        proof = checked_marker(target)[120:152]
        assert proof == hashlib.sha256(expected).digest()
        misc_proofs.append(proof)
        assert get('/misc-proof?m=' + misc)[1] == 'HIT'
    assert misc_proofs[0] != misc_proofs[1]
    checks.append('two configured misc values under one URL produce independently verified distinct file proofs')
    inline_purge_cases()
    conditional_paths.add('/empty-namespace')
    assert get('/empty-namespace') == (200, 'MISS', b'/empty-namespace')
    empty_namespace = records('/empty-namespace', 1)[0]
    assert empty_namespace['namespace'] == b''
    ready_file(empty_namespace)
    assert get('/empty-namespace') == (200, 'HIT', b'/empty-namespace')
    checks.append('an empty namespace remains empty in the index and file proof and reuses HIT')
    assert query('news', kind=5, scope='') == [(empty_namespace['id'], 2, 2)]
    fence('empty-namespace-tag', 7, tag_group('news', 'empty-absent'),
          scope='', mode=1)
    empty_reply = get('/empty-namespace')
    assert empty_reply == (200, 'REVALIDATED', b'/empty-namespace'), empty_reply
    assert get('/asset') == (200, 'HIT', b'/asset') and calls.count('/asset') == 1
    checks.append('TAG lookup and invalidation isolate the empty namespace from named namespaces')
    assert get('/asset', method='HEAD') == (200, 'HIT', b'')
    assert get('/asset', {'Range': 'bytes=1-3'}) == (206, 'HIT', b'ass')
    assert get('/asset', {'If-None-Match': '"capture-v1"'}) == (304, 'HIT', b'')
    checks.append('deferred descriptor inspection preserves native HEAD, Range and client conditional output filters')
    assert get('/head-fill', method='HEAD') == (200, 'MISS', b'')
    head_file = records('/head-fill', 1)[0]
    assert get('/head-fill') == (200, 'HIT', b'/head-fill')
    fresh_bodies['/head-fill'] = b'head-bypass-replacement'
    assert get('/head-fill?bypass=1', method='HEAD') == (200, 'BYPASS', b'')
    until(lambda: records('/head-fill', 1)[0]['id'] != head_file['id'])
    head_reply = get('/head-fill')
    assert head_reply == (200, 'HIT', b'head-bypass-replacement'), head_reply
    assert calls.count('/head-fill') == 2
    checks.append('cache-entry waits preserve HEAD-to-GET conversion on MISS and BYPASS and publish the replacement entity')
    finish_busy_retries()
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
    for item in removed_instances:
        previous = record(item['id'], optional=True)
        if previous is not None: physical_remove(previous, outcome=3, collected=True)
    assert {item['id'] for item in records('/remove-native', 2)} == {item['id'] for item in replacements}
    assert get('/remove-native') == (200, 'HIT', b'new-after-removal')
    assert get('/remove-native?z=b') == (200, 'HIT', b'new-after-removal')
    checks.append('physical removal of two indexed native zone instances refills fresh entities and old-instance retries preserve their replacements')
    materialized_uri = '/invalidate/materialized/304'
    materialized_failure = '/invalidate/materialized/failure'
    conditional_paths.add(materialized_uri)
    materialized_targets = [materialized_uri, materialized_uri + '?z=b', materialized_failure]
    for target in materialized_targets: assert get(target)[1] == 'MISS'
    materialized = records(materialized_uri, 2) + records(materialized_failure, 1)
    materialized_contents = {str(ready_file(item)): ready_file(item).read_bytes() for item in materialized}
    fence('background-invalidate', 3, 'http://cache.test/invalidate/materialized/', mode=1, activate=False)
    stop(process)
    subprocess.run([str(a.libraries / 'cache_purge_files_test'),
                    '--activate-background', str(run / 'index'), 'background-invalidate'],
                   check=True, timeout=15, stdout=subprocess.DEVNULL)
    process = start()
    until(lambda: (run / 'index/component.sock').exists())
    until(lambda: struct.unpack('>IQ', rpc(16))[0] == 6)
    def materialization_done(name='background-invalidate', expected_visits=3, expected_stage=4):
        d = Decode(rpc(15, string(name)))
        d.string(); d.number(1); d.number(8); d.number(1)
        d.string(); d.string(); d.string()
        for _ in range(5): d.number(8)
        stage = d.number(1); d.number(4); d.number(1); d.string()
        done = d.number(1); d.number(8); visits = d.number(8)
        pending, failed = d.number(8), d.number(8)
        d.number(8); d.string(); d.end()
        return stage == expected_stage and done and visits == expected_visits and not pending and not failed
    until(materialization_done)
    assert all(Path(path).read_bytes() == contents for path, contents in materialized_contents.items())
    stop(process)
    process = start()
    until(lambda: (run / 'index/component.sock').exists())
    until(lambda: struct.unpack('>IQ', rpc(16))[0] == 6)
    assert all(Path(path).read_bytes() == contents for path, contents in materialized_contents.items())
    assert get('/asset')[1] == 'HIT'
    for target in materialized_targets[:2]:
        assert get(target) == (200, 'REVALIDATED', materialized_uri.encode())
        assert get(target) == (200, 'HIT', materialized_uri.encode())
    assert {item['id'] for item in records(materialized_uri, 2)} == {item['id'] for item in materialized[:2]}
    origin_failures.add(materialized_failure)
    result = get(materialized_failure)
    assert result == ((500, 'EXPIRED', b'') if extension else (200, 'STALE', materialized_failure.encode())), result
    origin_failures.remove(materialized_failure)
    conditional_paths.add(materialized_failure)
    assert get(materialized_failure) == (200, 'REVALIDATED', materialized_failure.encode())
    assert get(materialized_failure) == (200, 'HIT', materialized_failure.encode())
    checks.append('background Invalidate automatically retires its FENCE after persisting three requirements across two zones; after restart, unchanged fresh native headers still require validation')
    checks.append('persistent per-file Invalidate keeps strict failure handling and plain stale downgrade after all resident rules are removed')
    host_cases = []
    for number, (kind, mode) in enumerate(((4, 1), (4, 2), (6, 1), (6, 2))):
        uri = '/host-shared/object-' + str(number)
        canonical = f'canonical-{number}.test'
        request_host = f'request-{number}.test'
        suffix = '?cache_host=' + canonical
        tag = 'host-tag-' + str(number)
        policies[uri] = tag
        for query_suffix in (suffix, suffix + '&z=b'):
            assert get(uri + query_suffix, {'Host': request_host}) == (200, 'MISS', uri.encode())
        files = records(uri, 2, canonical)
        assert {item['host'] for item in files} == {canonical.encode()}
        original_ids = {item['id'] for item in files}
        assert get(uri + suffix, {'Host': 'another-request.test'}) == (200, 'HIT', uri.encode())
        assert not query(request_host, kind=4)
        assert not query(tag, kind=6, scope=request_host)
        assert {item[0] for item in query(canonical, kind=4)} == original_ids
        assert {item[0] for item in query(tag, kind=6, scope=canonical)} == original_ids
        conditional_paths.add(uri)
        fence('canonical-host-' + str(number), kind, canonical if kind == 4 else tag,
              scope='' if kind == 4 else canonical, mode=mode)
        body = uri.encode() if mode == 1 else ('fresh-host-' + str(number)).encode()
        if mode == 2: fresh_bodies[uri] = body
        for query_suffix in (suffix, suffix + '&z=b'):
            if mode == 1:
                assert get(uri + query_suffix, {'Host': request_host}) == (200, 'REVALIDATED', body)
            else:
                deleted_response(uri + query_suffix, body, {'Host': request_host})
        current_ids = {item['id'] for item in records(uri, 2, canonical)}
        assert current_ids == original_ids if mode == 1 else current_ids.isdisjoint(original_ids)
        assert get(uri + suffix, {'Host': request_host}) == (200, 'HIT', body)
        host_cases.append((uri, canonical, request_host, body))
        checks.append(f'canonical host identity: kind={kind}, mode={mode} covers both zones without observing request Host aliases')
    assert get('/invalidate/background-native')[1] == 'MISS'
    assert get('/invalidate/background-native?z=b')[1] == 'MISS'
    for item in records('/invalidate/background-native', 2): ready_file(item)
    conditional_paths.add('/invalidate/background-native')
    fence('before-background-delete', 1, 'http://cache.test/invalidate/background-native', mode=1)
    assert (reply := get('/invalidate/background-native')) == (200, 'REVALIDATED', b'/invalidate/background-native'), reply
    assert (reply := get('/invalidate/background-native?z=b')) == (200, 'REVALIDATED', b'/invalidate/background-native'), reply
    background_files = records('/invalidate/background-native', 2)
    assert all(item['metadata'].startswith(b'NFR1') for item in background_files), background_files
    background_paths = [ready_file(item) for item in background_files]
    background_inodes = {path.stat().st_ino for path in background_paths}
    cleanup_log_start = (run / 'error.log').stat().st_size
    fence('background-native', 1, 'http://cache.test/invalidate/background-native', submit=True)
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
        return stage == 4 and done and not pending and not failed
    until(background_done)
    cleanup_log = (run / 'error.log').read_bytes()[cleanup_log_start:].decode()
    cleaned = {int(inode): (int(blocks), int(size)) for inode, blocks, size in
               re.findall(r'cache purge native cleanup inode:(\d+) blocks:(\d+) size:(\d+)', cleanup_log)}
    assert background_inodes <= cleaned.keys(), (background_inodes, cleaned)
    assert all(cleaned[inode][0] > 0 for inode in background_inodes)
    assert query('http://cache.test/invalidate/background-native') == []
    fresh_bodies['/invalidate/background-native'] = b'new-after-background'
    assert get('/invalidate/background-native') == (200, 'MISS', b'new-after-background')
    assert get('/invalidate/background-native?z=b') == (200, 'MISS', b'new-after-background')
    assert {item['id'] for item in records('/invalidate/background-native', 2)}.isdisjoint(
        {item['id'] for item in background_files})
    checks.append('durable submission autonomously activates Delete in the running worker; both hot zone instances are removed and their native size accounting is acknowledged before enumeration completes')
    assert get('/alias/one') == (200, 'MISS', b'/alias/one')
    alias = records('/alias/shared', 1)[0]; ready_file(alias)
    assert get('/alias/two') == (200, 'HIT', b'/alias/one')
    assert not query('http://cache.test/alias/one') and not query('http://cache.test/alias/two')
    fence('unmapped-request-url', 1, 'http://cache.test/alias/two')
    assert get('/alias/three') == (200, 'HIT', b'/alias/one')
    checks.append('different raw request paths share only their configured canonical URL; raw paths are not indexed as aliases')
    fence('canonical-url-prefix', 3, 'http://cache.test/alias/sh')
    fresh_bodies['/alias/one'] = b'canonical-fresh'
    deleted_response('/alias/one', b'canonical-fresh')
    original_id = alias['id']
    alias = until(lambda: (item if (item := records('/alias/shared', 1)[0])['id'] != original_id else None))
    ready_file(alias)
    conditional_paths.add('/alias/one')
    fence('canonical-url-invalidate', 1, 'http://cache.test/alias/shared', mode=1)
    assert get('/alias/one') == (200, 'REVALIDATED', b'canonical-fresh')
    assert get('/alias/three') == (200, 'HIT', b'canonical-fresh')
    assert records('/alias/shared', 1)[0]['id'] == alias['id']
    checks.append('canonical URL prefix Delete and exact Invalidate preserve shared routing and 304 semantics')
    for name, kind in (('url', 1), ('key', 0), ('url-prefix/item', 3)):
        uri = '/invalidate/' + name
        conditional_paths.add(uri)
        assert get(uri) == (200, 'MISS', uri.encode())
        variants = [uri, uri + '?z=b'] if kind == 1 else [uri]
        if kind == 1: assert get(variants[1]) == (200, 'MISS', uri.encode())
        original = records(uri, len(variants))
        original_ids = {item['id'] for item in original}
        original_origins = {item['id']: watermark(item) for item in original}
        if kind == 0: value = original[0]['key']
        elif kind == 3: value = 'http://cache.test' + uri.rsplit('/', 1)[0] + '/'
        else: value = 'http://cache.test' + uri
        fence('direct-invalidate-' + name, kind, value, mode=1)
        for variant in variants:
            assert get(variant) == (200, 'REVALIDATED', uri.encode()), variant
        after = records(uri, len(variants))
        assert {item['id'] for item in after} == original_ids
        assert all(watermark(item) == original_origins[item['id']] for item in after)
        for variant in variants:
            assert get(variant) == (200, 'HIT', uri.encode()), variant
        checks.append('current ' + name + ' Invalidate reuses 304-validated entities and permits covered HITs while its FENCE remains')
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
    deleted_response('/invalidate/304', b'new-full-entity')
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
    conditional_paths.add('/invalidate/failure')
    assert get('/invalidate/failure') == (200, 'REVALIDATED', b'/invalidate/failure')
    assert records('/invalidate/failure', 1)[0]['id'] == failed_original['id']
    assert get('/invalidate/failure') == (200, 'HIT', b'/invalidate/failure')
    checks.append('a failed Invalidate attempt still requires successful validation before the old entity can HIT again')
    conditional_paths.add('/invalidate/race')
    assert get('/invalidate/race') == (200, 'MISS', b'/invalidate/race')
    race_file = records('/invalidate/race', 1)[0]
    fence('invalidate-race-first', 1, 'http://cache.test/invalidate/race', mode=1)
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        request = pool.submit(get, '/invalidate/race')
        assert invalidate_waiting.wait(5)
        sequence = fence('invalidate-race-second', 1, 'http://cache.test/invalidate/race', mode=1)
        until(lambda: (count := writers(sequence)) is not None and count > 0)
        fresh_bodies['/invalidate/race'] = b'after-second-invalidate'
        invalidate_release.set()
        assert request.result(timeout=15) == (200, 'BYPASS', b'after-second-invalidate')
    until(lambda: records('/invalidate/race', 1)[0]['id'] != race_file['id'])
    assert conditional_calls.count('/invalidate/race') == 1
    assert calls.count('/invalidate/race') == 3
    assert get('/invalidate/race') == (200, 'HIT', b'after-second-invalidate')
    until(lambda: writers(sequence) == 0)
    checks.append('an older forced 304 cannot clear a second Invalidate; a new full entity is required before HIT')
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
    checks.append('a successful 304 persists a newly received origin must-revalidate policy while clearing only purged in the extension build')
    assert get('/unmarked')[1] == 'MISS'
    unmarked = records('/unmarked', 1)[0]
    os.removexattr(ready_file(unmarked), 'user.ngx_cache_purge.id')
    assert get('/unmarked') == (200, 'BYPASS', b'/unmarked')
    until(lambda: records('/unmarked', 1)[0]['id'] != unmarked['id'])
    assert get('/unmarked')[1] == 'HIT' and calls.count('/unmarked') == 2
    assert retired(unmarked['id'])
    checks.append('a native HIT with no publication marker refills and replaces its old index instance before later reuse')
    missing_index_read()
    assert get('/read-wait')[1] == 'MISS'
    with prepared_replacement(records('/read-wait', 1)[0]) as finish:
        with socket.create_connection(('127.0.0.1', port), timeout=5) as held:
            held.sendall(b'GET /read-wait HTTP/1.1\r\nHost: cache.test\r\nConnection: close\r\n\r\n')
            held.settimeout(.05)
            try:
                early = held.recv(1)
                raise AssertionError(('unverified response bytes escaped', early))
            except socket.timeout: pass
            finish()
            held.settimeout(5)
            response = http.client.HTTPResponse(held); response.begin()
            assert response.status == 200 and response.getheader('X-Status') == 'HIT'
            assert response.read() == b'/read-wait'
    assert calls.count('/read-wait') == 1
    checks.append('reads of a real PREPARE_FILE transaction wait asynchronously until FINISH_FILE without exposing bytes or fetching a new entity')
    with prepared_replacement(records('/read-wait', 1)[0]) as finish:
        with socket.create_connection(('127.0.0.1', port), timeout=5) as abandoned:
            abandoned.sendall(b'GET /read-wait HTTP/1.1\r\nHost: cache.test\r\nConnection: close\r\n\r\n')
            abandoned.settimeout(.05)
            try:
                early = abandoned.recv(1)
                raise AssertionError(('unverified response bytes escaped', early))
            except socket.timeout: pass
        finish()
    assert get('/read-wait') == (200, 'HIT', b'/read-wait')
    assert calls.count('/read-wait') == 1
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
    identities = []
    for item in varied:
        language = 'en' if ready_file(item).read_bytes().endswith(b'/varyen') else 'de'
        identities.append(variant_hashes(item, language))
    assert identities[0][0] == identities[1][0] and identities[0][1] != identities[1][1]
    checks.append('Vary metadata stores native main and variant hashes while ordinary files keep the original metadata size')
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
    assert len({x['key'] for x in sliced}) == 1 and len({x['path'] for x in sliced}) == 3
    assert {x['range'] for x in sliced} == {b''}
    assert get('/slice')[2] == slice_body and calls.count('/slice') == 3
    checks.append('slice subrequests publish all ranges under one base key')
    middle_digest = hashlib.md5(b'tenant\x1fhttp\x1fcache.test\x1f/slice\x1f\x1fbytes=1024-2047').hexdigest().encode()
    middle = next(item for item in sliced if item['path'].split(b'/')[-1] == middle_digest)
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
    assert new['tags'] == [b'blue'] and retired(old['id'])
    assert new['id'] in [x[0] for x in query('blue', kind=5, scope='tenant')]
    assert not query('blue', kind=5, scope='another-tenant')
    checks.append('BYPASS replacement captures before origin, retires the prior instance and updates scoped TAG membership')
    assert get('/raw?m=native')[1] == 'MISS'
    item = records('/raw', 1)[0]; ready_file(item)
    assert item['key'] == identity_key('rawtenant', 'http://cache.test/raw', 'native')
    assert item['namespace'] == b'rawtenant'
    checks.append('independent identity expressions index an arbitrary native key format without key-module outputs')
    if extension and a.expr:
        uri = '/tag-when/object'
        policies[uri] = 'when-old'
        assert get(uri + '?policy=1')[1] == 'MISS'
        item = records(uri, 1)[0]
        assert set(item['tags']) == {b'when-old', b'conditional'}
        conditional_paths.add(uri)
        for selected, tag in ((0, 'when-new'), (1, 'when-last')):
            validation_tags[uri] = tag
            fence('conditional-tag-' + str(selected), 1, 'http://cache.test' + uri, mode=1)
            assert get(uri + '?policy=' + str(selected)) == (200, 'REVALIDATED', uri.encode())
            expected = {tag.encode()} | ({b'conditional'} if selected else set())
            until(lambda: set(record(item['id'])['tags']) == expected)
        checks.append('TAG when selection and inherited fallback apply on fill and on successful 304 replacement')
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
            return matches if value[1] == 'HIT' and matches and all(item[2] == 2 for item in matches) and pending() == 0 else None
        until(recovered, timeout=10)
        ready_file(records('/after-capacity', 1)[0])
        checks.append('publication pressure preserves ordinary HITs and response bodies; subsequent fills recover')
    until(lambda: pending() == 0)
    before = {x[0] for x in query('http://cache.test/asset')}
    conditional_paths.add('/invalidate/restart')
    assert get('/invalidate/restart') == (200, 'MISS', b'/invalidate/restart')
    restart_file = records('/invalidate/restart', 1)[0]
    restart_path = ready_file(restart_file)
    restart_contents = restart_path.read_bytes()
    fence('invalidate-before-restart', 1, 'http://cache.test/invalidate/restart', mode=1)
    assert restart_path.read_bytes() == restart_contents
    stop(process); process = start()
    until(lambda: (run / 'index/component.sock').exists())
    until(lambda: struct.unpack('>IQ', rpc(16))[0] == 6)
    assert {x[0] for x in query('http://cache.test/asset')} == before
    assert get('/asset')[1] == 'HIT'
    assert {watermark(record(ident))[0] for ident in before} == {ledger}
    checks.append('full nginx restart reuses persisted file relations without rebuilding the index')
    assert restart_path.read_bytes() == restart_contents
    assert get('/invalidate/restart') == (200, 'REVALIDATED', b'/invalidate/restart')
    assert records('/invalidate/restart', 1)[0]['id'] == restart_file['id']
    assert get('/invalidate/restart') == (200, 'HIT', b'/invalidate/restart')
    checks.append('persistent invalidation survives a full restart without writing a purge marker into the native cache file')
    assert get('/alias/three') == (200, 'HIT', b'canonical-fresh')
    assert records('/alias/shared', 1)[0]['id'] == alias['id']
    assert not query('http://cache.test/alias/two')
    checks.append('restart preserves the canonical identity without discovering request-path aliases')
    for uri, canonical, request_host, body in host_cases:
        suffix = '?cache_host=' + canonical
        assert get(uri + suffix, {'Host': request_host}) == (200, 'HIT', body)
        assert get(uri + suffix + '&z=b', {'Host': request_host}) == (200, 'HIT', body)
        assert len(query(canonical, kind=4)) == 2 and not query(request_host, kind=4)
    checks.append('canonical host identity and satisfied host rules survive restart')
    validation_files = {}
    assert get('/revalidate-vary', {'Accept-Language': 'en'}) == (200, 'MISS', b'/revalidate-varyen')
    varying_validation = records('/revalidate-vary', 1)[0]
    varying_hashes = variant_hashes(varying_validation, 'en')
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
    assert get('/revalidate-vary', {'Accept-Language': 'en'}) == (200, 'REVALIDATED', b'/revalidate-varyen')
    varying_after = records('/revalidate-vary', 1)[0]
    assert varying_after['id'] == varying_validation['id']
    assert variant_hashes(varying_after, 'en') == varying_hashes
    checks.append('304 keeps native Vary hashes alongside the refreshed conditional watermark')
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        request = pool.submit(get, '/revalidate-race')
        assert validation_waiting.wait(5)
        race_sequence = fence('invalidate-during-304', 7,
                              tag_group('race-old', 'race-unused'), 'tenant', mode=1)
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
    checks.append('a TAG-group FENCE accepted while origin 304 is pending forces a fresh entity even when the 304 replaces its matched TAG')
    policies['/late-fill'] = 'late-tag'
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        request = pool.submit(get, '/late-fill')
        assert late_waiting.wait(5)
        late_sequence = fence('late-fill-rule', 7,
                              tag_group('late-tag', 'late-unused'), 'tenant')
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
    until(lambda: writers(late_sequence) == 0)
    until(lambda: not target.exists() and pending() == 0)
    assert query('http://cache.test/late-fill') == []
    reply = get('/late-fill')
    assert reply[0] == 200 and reply[1] in ('MISS', 'BYPASS') and reply[2] == b'new-late-entity', reply
    fresh = records('/late-fill', 1)[0]; ready_file(fresh)
    assert watermark(fresh)[0] == ledger and watermark(fresh)[1] >= late_sequence
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
    url_cutoff = fence('read-url', 1, 'http://cache.test/fenced-url')
    deleted_response('/fenced-url', b'/fenced-url')
    deleted_response('/fenced-url?z=b', b'/fenced-url')
    until(lambda: all(watermark(item)[1] >= url_cutoff for item in records('/fenced-url', 2)))
    assert get('/fenced-url')[1] == get('/fenced-url?z=b')[1] == 'HIT'
    assert get('/asset')[1] == 'HIT'
    checks.append('native read admission checks the published URL FENCE across both zones while unrelated cached files remain HITs')
    assert get('/fenced-prefix/a')[1] == 'MISS'; records('/fenced-prefix/a', 1)
    prefix_cutoff = fence('read-prefix', 3, 'http://cache.test/fenced-prefix/')
    deleted_response('/fenced-prefix/a', b'/fenced-prefix/a')
    until(lambda: watermark(records('/fenced-prefix/a', 1)[0])[1] >= prefix_cutoff)
    assert get('/fenced-prefix/a')[1] == 'HIT'
    checks.append('native read admission applies bytewise prefix FENCEs to cached files')
    assert get('/fenced-key')[1] == 'MISS'
    fenced_key = records('/fenced-key', 1)[0]['key']
    key_cutoff = fence('read-key', 0, fenced_key)
    deleted_response('/fenced-key', b'/fenced-key')
    until(lambda: watermark(records('/fenced-key', 1)[0])[1] >= key_cutoff)
    assert get('/fenced-key')[1] == 'HIT'
    checks.append('native read admission applies exact base-key FENCEs')
    policies['/fenced-tag'] = 'only-this-tag'
    assert get('/fenced-tag')[1] == 'MISS'; records('/fenced-tag', 1)
    fence('read-wrong-tag', 5, 'only-this-tag', 'other-tenant')
    assert get('/fenced-tag')[1] == 'HIT'
    tag_cutoff = fence('read-tag', 5, 'only-this-tag', 'tenant')
    deleted_response('/fenced-tag', b'/fenced-tag')
    until(lambda: watermark(records('/fenced-tag', 1)[0])[1] >= tag_cutoff)
    assert get('/fenced-tag')[1] == 'HIT' and calls.count('/fenced-tag') == 2
    assert get('/asset')[1] == 'HIT'
    checks.append('native TAG admission uses persisted entity tags with namespace isolation')
    policies['/revalidate-covered'] = 'covered-tag'
    assert get('/revalidate-covered')[1] == 'MISS'
    original = records('/revalidate-covered', 1)[0]
    fence('covered-tag-rule', 5, 'covered-tag', 'tenant')
    deleted_response('/revalidate-covered', b'/revalidate-covered')
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
    assert negative_result == (200, 'MISS', b'/negative-cache'), negative_result
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
    deleted_response('/redirect-tag', b'redirect-new')
    assert calls.count('/redirect-tag') == 2
    checks.append('scoped TAG FENCE matches persisted metadata before a cached redirect executes')
    redirects['/redirect-cache'] = '/redirect-new'
    fence('read-redirect', 1, 'http://cache.test/redirect-cache')
    deleted_response('/redirect-cache', b'redirect-new')
    assert calls.count('/redirect-cache') == 2
    checks.append('URL FENCE bypasses a cached X-Accel-Redirect before it selects the old target')
    uri = '/invalidate/host'
    conditional_paths.add(uri)
    assert get(uri) == (200, 'MISS', uri.encode())
    host_before = records(uri, 1)[0]
    fence('direct-invalidate-host', 4, 'cache.test', mode=1)
    assert get(uri) == (200, 'REVALIDATED', uri.encode())
    host_after = records(uri, 1)[0]
    assert host_before['id'] == host_after['id'] and watermark(host_before) == watermark(host_after)
    assert get(uri) == (200, 'HIT', uri.encode())
    checks.append('current host Invalidate conditionally validates without replacing the entity or repeatedly bypassing its covered file')
    fence('read-host', 4, 'cache.test')
    deleted_response('/asset', b'/asset')
    checks.append('native read admission applies effective host FENCEs before background file processing')
    marker = 'changing/removing cache purge store or state zone requires a full restart'
    rejected = (run / 'error.log').read_text().count(marker)
    (run / 'nginx.conf').write_text(legacy_config)
    process.send_signal(signal.SIGHUP)
    until(lambda: (run / 'error.log').read_text().count(marker) > rejected)
    assert get('/negative-cache') == (200, 'BYPASS', b'/negative-cache')
    checks.append('disabling indexing by reload is rejected and the existing workers continue enforcing resident Delete FENCEs')
    (run / 'nginx.conf').write_text(config)
    status, headers, caps = until(lambda: (reply if
        (reply := api('/__purge/v2/capabilities'))[0] == 200 and reply[2]['ready']
        else None))
    assert status == 200 and headers['Cache-Control'] == 'no-store' and caps['ready'], (status, caps)
    assert caps['authentication'] == 'nginx-configuration' and 'signed_events' not in caps
    assert caps['propagation_timeout_ms'] == '120000'
    assert caps['index_schema'] == 12
    assert caps['purge_method'] and 'key' not in caps['json_selectors']
    assert caps['control_headers']['purge_type'] == 'X-Cache-Purge-Type'
    assert caps['control_headers']['purge_action'] == 'X-Cache-Purge-Action'
    assert caps['control_headers']['purge_tags'] == 'X-Cache-Purge-Tags'
    assert caps['control_headers']['operation_id'] == 'X-Cache-Purge-Operation-Id'
    assert caps['control_headers']['issued_at'] == 'X-Cache-Purge-Issued-At'
    assert caps['control_headers']['tag_namespace'] == 'X-Cache-Purge-Namespace'
    assert caps['index_id'] == ledger.hex() and caps['max_tags'] == 64
    assert caps['invalidate_stale_policy'] == ('forbid' if extension else 'native')
    assert not caps['proxy_propagation_ready'] and caps['propagation_protocols'] == []
    assert api('/__purge/v2/capabilities', method='HEAD')[0::2] == (200, None)
    custom = api('/__custom/v2/capabilities')[2]
    assert custom['control_headers']['tag_namespace'] == 'X-Test-Purge-Scope'
    assert get('/__denied/v2/capabilities', details=True)[0] == 403
    addresses = [ip for ip in subprocess.check_output(['hostname', '-I'], text=True).split()
                 if '.' in ip and not ip.startswith('127.')]
    if addresses:
        conn = http.client.HTTPConnection('127.0.0.1', port, timeout=10,
                                          source_address=(addresses[0], 0))
        conn.request('GET', '/__open/v2/capabilities', headers={'Host': 'cache.test'})
        assert conn.sock.getsockname()[0] == addresses[0]
        response = conn.getresponse()
        assert response.status == 200 and json.loads(response.read())['authentication'] == 'nginx-configuration'
        conn.request('GET', '/__purge/v2/capabilities',
                     headers={'Host': 'cache.test', 'X-Forwarded-For': '127.0.0.1'})
        response = conn.getresponse(); response.read()
        assert response.status == 403
        conn.close()
    assert api('/__purge/v2/operations/missing-api-operation')[0] == 404
    denied = api('/__purge/v2/capabilities', method='POST', payload={})
    assert denied[0] == 405 and denied[1]['Allow'] == 'GET, HEAD'
    checks.append('HTTP capabilities and status use real readiness, instance identity, macro policy and effective header names without advertising unavailable L2/signatures')

    uri = '/invalidate/api-expired'
    assert get(uri)[0] == 200 and get(uri + '?z=b')[0] == 200
    files = records(uri, 2)
    paths = [ready_file(item) for item in files]
    original_contents = [path.read_bytes() for path in paths]
    conditional_paths.add(uri)
    request_body = api_input('api-invalidate', {'type': 'url', 'value': 'http://cache.test' + uri}, 'invalidate')
    status, headers, receipt = api('/__purge/v2/operations', request_body)
    assert status == 202 and receipt['effective'] and receipt['ready'] and receipt['durable'], (status, receipt)
    assert len(receipt['request_digest']) == 64 and receipt['index_id'] == ledger.hex()
    assert [path.read_bytes() for path in paths] == original_contents
    assert get(uri) == (200, 'REVALIDATED', uri.encode())
    assert get(uri + '?z=b') == (200, 'REVALIDATED', uri.encode())
    readback = api(receipt['status_url'])[2]
    replay = api('/__purge/v2/operations', raw=json.dumps(request_body, sort_keys=True, indent=2).encode())[2]
    for name in ('accepted_at', 'effective_at', 'propagation_until', 'sequence', 'request_digest'):
        assert receipt[name] == readback[name] == replay[name], name
    conflict = dict(request_body, mode='delete')
    assert api('/__purge/v2/operations', conflict)[0] == 409
    checks.append('HTTP 202 waits for durable effectiveness, Invalidate covers both zones, and canonical retries preserve the first receipt while changed input conflicts')

    fresh_bodies[uri] = b'api-new-entity'
    deleted = api_input('api-delete-prefix', {'type': 'prefix', 'field': 'url', 'value': 'http://cache.test/invalidate/api-'})
    assert api('/__purge/v2/operations', deleted)[0] == 202
    for suffix in ('', '?z=b'):
        reply = get(uri + suffix)
        assert reply[0] == 200 and reply[1] in ('MISS', 'BYPASS') and reply[2] == b'api-new-entity', reply
    until(lambda: api('/__purge/v2/operations/api-delete-prefix')[2]['cleanup']['state'] == 'done')
    def refilled_api_zones():
        # A fresh response may skip its cache write while background deletion
        # owns the path lease. Verify reuse after cleanup with another request.
        for suffix in ('', '?z=b'):
            reply = get(uri + suffix)
            assert reply[0] == 200 and reply[2] == b'api-new-entity', reply
        found = query('http://cache.test' + uri)
        return len(found) == 2 and all(item[2] == 2 for item in found)
    until(refilled_api_zones)
    assert {item['id'] for item in records(uri, 2)}.isdisjoint({item['id'] for item in files})
    checks.append('HTTP URL-prefix Delete rejects old hits immediately and finishes physical cleanup independently of response delivery')

    uri = '/api-key'
    assert get(uri)[0] == 200
    key_file = records(uri, 1)[0]; ready_file(key_file)
    operation = api_input('api-key', {'type': 'key', 'value': key_file['key'].decode()})
    rejected = api('/__purge/v2/operations', operation)
    assert rejected[0] == 412 and rejected[2]['error'] == 'use_purge_key'
    assert api('/__purge/v2/operations/api-key')[0] == 404
    assert get(uri)[1] == 'HIT'
    import base64
    raw_key = b'\xff\x00binary-prefix'
    operation = api_input('api-binary', {'type': 'prefix', 'field': 'key',
                          'encoding': 'base64', 'value': base64.b64encode(raw_key).decode()})
    binary_receipt = api('/__purge/v2/operations', operation)
    assert binary_receipt[0] == 412 and binary_receipt[2]['error'] == 'key_prefix_not_supported'
    assert api('/__purge/v2/operations/api-binary')[0] == 404
    assert api('/__purge/v2/operations', api_input('api-host',
               {'type': 'host', 'value': 'unpopulated-api.test', 'include_subdomains': False}))[0] == 202
    unicode_input = api_input('api-unicode', {'type': 'url', 'value': 'https://cache.test/absent/\U0001f680'})
    unicode_reply = api('/__purge/v2/operations', raw=json.dumps(unicode_input, ensure_ascii=True).encode())
    assert unicode_reply[0] == 202 and unicode_reply[2]['selector']['value'] == 'https://cache.test/absent/\U0001f680'
    checks.append('JSON rejects whole-key and key-prefix submissions without acceptance while URL/host selectors preserve their bytes')

    uri = '/api-tag'
    policies[uri] = 'api-red'
    assert get(uri)[0] == 200
    tag_before = records(uri, 1)[0]; ready_file(tag_before)
    operation = api_input('api-tag', {'type': 'tag', 'values': ['api-red'], 'match': 'any'})
    assert api('/__custom/v2/operations', operation, headers={'X-Cache-Purge-Namespace': 'tenant'})[0] == 400
    assert api('/__custom/v2/operations/api-tag')[0] == 404
    reply = api('/__custom/v2/operations', operation, headers={'X-Test-Purge-Scope': 'tenant'})
    assert reply[0] == 202 and reply[2]['tag_namespace'] == 'tenant'
    assert api('/__purge/v2/operations', operation, headers={'X-Cache-Purge-Namespace': 'tenant'})[0] == 202
    assert get(uri)[0] == 200
    until(lambda: records(uri, 1)[0]['id'] != tag_before['id'])
    ready_file(records(uri, 1)[0])
    operation = api_input('api-host-tag', {'type': 'tag', 'values': ['api-red'], 'scope': 'host'})
    reply = api('/__purge/v2/operations', operation, headers={'X-Cache-Purge-Namespace': 'cache.test'})
    assert reply[0] == 202 and reply[2]['tag_host'] == 'cache.test'
    checks.append('HTTP namespace/host TAG scope uses the configured control header, and header renaming does not change semantic idempotency')
    empty_scope = api_input('api-empty-scope', {'type': 'tag',
        'values': ['empty-scope-absent'], 'scope': 'namespace'})
    empty_scope_reply = api('/__purge/v2/operations', empty_scope,
                            headers={'X-Cache-Purge-Namespace': ''})
    assert empty_scope_reply[0] == 202 and empty_scope_reply[2]['tag_namespace'] == ''
    empty_host = api_input('api-empty-host', {'type': 'tag',
        'values': ['empty-scope-absent'], 'scope': 'host'})
    assert api('/__purge/v2/operations', empty_host,
               headers={'X-Cache-Purge-Namespace': ''})[0] == 400
    checks.append('HTTP accepts an explicit empty namespace and still rejects an empty host scope')

    group_a, group_b = '/invalidate/api-group-a', '/invalidate/api-group-b'
    group_other = '/api-group-unrelated'
    policies[group_a] = 'multi-blue,multi-red'
    policies[group_b] = 'multi-blue'
    policies[group_other] = 'multi-green'
    conditional_paths.update((group_a, group_b))
    for path in (group_a, group_a + '?z=b', group_b, group_other):
        assert get(path)[0] == 200
    group_files = records(group_a, 2) + records(group_b, 1)
    for item in group_files: ready_file(item)
    ready_file(records(group_other, 1)[0])
    scope_headers = {'X-Cache-Purge-Namespace': 'tenant'}
    group_input = api_input('api-tag-group',
        {'type': 'tag', 'values': ['multi-red', 'multi-blue', 'multi-red']}, 'invalidate')
    wrong = dict(group_input, operation_id='api-tag-group-wrong-scope')
    assert api('/__purge/v2/operations', wrong,
               headers={'X-Cache-Purge-Namespace': 'another-tenant'})[0] == 202
    assert get(group_a)[1] == 'HIT'
    grouped = api('/__purge/v2/operations', group_input, headers=scope_headers)
    assert grouped[0] == 202 and grouped[2]['selector']['values'] == ['multi-blue', 'multi-red'], grouped
    # Count selector deduplication without concurrent 304 leases: visits also
    # counts retry attempts, so validating during the scan makes it nondeterministic.
    until(lambda: api('/__purge/v2/operations/api-tag-group')[2]['cleanup']['scan_done'])
    for path in (group_a, group_a + '?z=b', group_b):
        reply = get(path)
        assert reply[0] == 200 and reply[1] == 'REVALIDATED', reply
    assert get(group_other)[1] == 'HIT'
    retry = dict(group_input, selector={'type': 'tag', 'values': ['multi-blue', 'multi-red']})
    repeated = api('/__purge/v2/operations', retry, headers=scope_headers)
    assert repeated[0] == 202
    for name in ('request_digest', 'sequence', 'accepted_at', 'effective_at'):
        assert repeated[2][name] == grouped[2][name], name
    until(lambda: api('/__purge/v2/operations/api-tag-group')[2]['cleanup']['state'] == 'done')
    finished = api('/__purge/v2/operations/api-tag-group')[2]
    assert finished['cleanup']['visits'] == '3', finished
    assert {item['id'] for item in group_files} == {item['id'] for item in records(group_a, 2) + records(group_b, 1)}
    checks.append('HTTP multi-TAG Invalidate normalizes order/duplicates, covers both zones once per file, preserves 304 entities and isolates its namespace')

    host_group = api_input('api-host-tag-group',
        {'type': 'tag', 'values': ['multi-red', 'multi-blue'], 'scope': 'host'})
    for path in (group_a, group_b): fresh_bodies[path] = b'new-group-body'
    host_group_reply = api('/__purge/v2/operations', host_group,
                          headers={'X-Cache-Purge-Namespace': 'cache.test'})
    assert host_group_reply[0] == 202 and host_group_reply[2]['tag_host'] == 'cache.test'
    for path in (group_a, group_a + '?z=b', group_b):
        reply = get(path)
        assert reply[0] == 200 and reply[2] == b'new-group-body', reply
    assert get(group_other)[1] == 'HIT'
    until(lambda: api('/__purge/v2/operations/api-host-tag-group')[2]['cleanup']['state'] == 'done')
    checks.append('HTTP host TAG-group Delete prevents old responses across zones while unrelated TAGs remain HITs')

    many_tags = ['unused-tag-' + str(i) for i in range(64)]
    maximum = api('/__purge/v2/operations', api_input('api-64-tags',
                  {'type': 'tag', 'values': many_tags}), headers=scope_headers)
    assert maximum[0] == 202 and maximum[2]['selector']['values'] == sorted(many_tags)
    binary_values = [b'normal', b'\xff\x00']
    binary_group = api('/__purge/v2/operations', api_input('api-binary-tags',
        {'type': 'tag', 'values': [base64.b64encode(value).decode() for value in binary_values],
         'encoding': 'base64'}), headers=scope_headers)
    assert binary_group[0] == 202 and binary_group[2]['selector']['encoding'] == 'base64'
    assert [base64.b64decode(value) for value in binary_group[2]['selector']['values']] == binary_values
    singleton = api_input('api-singleton-tag', {'type': 'tag', 'values': ['same', 'same']})
    singleton_reply = api('/__purge/v2/operations', singleton, headers=scope_headers)
    singleton['selector']['values'] = ['same']
    singleton_retry = api('/__purge/v2/operations', singleton, headers=scope_headers)
    assert singleton_reply[0] == singleton_retry[0] == 202
    assert singleton_reply[2]['request_digest'] == singleton_retry[2]['request_digest']
    checks.append('HTTP accepts 64 TAG members, preserves binary TAG arrays and canonicalizes duplicate singleton sets')

    malformed = api_input('api-invalid', {'type': 'url', 'value': 'absent'})
    raw = json.dumps(malformed).encode()
    cases = [raw[:-1] + b',"mode":"delete"}', raw + b' trailing',
             raw.replace(b'absent', b'\xff'), raw.replace(b'absent', b'\\ud800'),
             raw[:-1] + b',"unexpected":true}', b'[]', b'{}']
    for body in cases:
        result = api('/__purge/v2/operations', raw=body)
        assert result[0] == 400, (body, result)
    assert api('/__purge/v2/operations', malformed, headers={'Content-Type': 'text/plain'})[0] == 415
    bad_base64 = api_input('api-invalid', {'type': 'url', 'encoding': 'base64', 'value': 'YQ==garbage'})
    assert api('/__purge/v2/operations', bad_base64)[0] == 400
    signed = dict(malformed, event={**malformed['event'], 'signature': 'not-verified'})
    assert api('/__purge/v2/operations', signed)[0] == 400
    assert api('/__purge/v2/operations', api_input('api-too-many-tags',
               {'type': 'tag', 'values': ['tag-' + str(i) for i in range(65)]}),
               headers={'X-Cache-Purge-Namespace': 'tenant'})[0] == 412
    oversized = [str(i).zfill(2) + 'x' * 1022 for i in range(33)]
    assert api('/__purge/v2/operations', api_input('api-tags-too-large',
               {'type': 'tag', 'values': oversized}), headers=scope_headers)[0] == 400
    assert api('/__purge/v2/operations/api-tags-too-large')[0] == 404
    assert api('/__purge/v2/operations/api-too-many-tags')[0] == 404
    assert api('/__purge/v2/operations/api-invalid')[0] == 404
    checks.append('HTTP parser rejects duplicate keys, trailing input, malformed UTF-8/surrogates, unknown fields, unsupported signatures and oversized TAG groups without acceptance')

    chunked = api_input('api-chunked', {'type': 'url', 'value': 'http://absent.test/chunked'})
    raw = json.dumps(chunked).encode()
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=15)
    conn.request('POST', '/__purge/v2/operations', [raw[:17], raw[17:]],
                 {'Host': 'cache.test', 'Content-Type': 'application/json'}, encode_chunked=True)
    response = conn.getresponse()
    assert response.status == 202, response.read()
    assert json.loads(response.read())['operation_id'] == 'api-chunked'
    conn.close()
    duplicate = api_input('api-duplicate-header', {'type': 'tag', 'values': ['one']})
    raw = json.dumps(duplicate).encode()
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=15)
    conn.putrequest('POST', '/__purge/v2/operations', skip_host=True)
    for name, value in [('Host', 'cache.test'), ('Content-Type', 'application/json'),
                        ('Content-Length', str(len(raw))), ('X-Cache-Purge-Namespace', 'one'),
                        ('X-Cache-Purge-Namespace', 'two')]:
        conn.putheader(name, value)
    conn.endheaders(raw)
    response = conn.getresponse()
    assert response.status == 400, response.read()
    response.read(); conn.close()
    assert api('/__purge/v2/operations/api-duplicate-header')[0] == 404
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=15)
    conn.request('POST', '/__purge/v2/operations', b' ' * (262144 + 1),
                 {'Host': 'cache.test', 'Content-Type': 'application/json'})
    response = conn.getresponse()
    assert response.status == 413
    response.read(); conn.close()
    checks.append('HTTP handles chunked JSON and rejects duplicate namespace headers and oversized bodies')

    held_uri = '/invalidate/api-held'
    policies[held_uri] = 'api-held-member'
    assert get(held_uri)[0] == 200
    held_file = records(held_uri, 1)[0]
    held_path = ready_file(held_file)
    _, lease = rpc(17, string(str(held_path)), receive_fd=True)
    try:
        conn = http.client.HTTPConnection('127.0.0.1', port, timeout=15)
        conn.request('PURGE', '/push/disconnected', headers={
            'Host': 'purge.test', 'X-Namespace': 'tenant',
            'X-Cache-Purge-Type': 'tag',
            'X-Cache-Purge-Tags': 'api-held-member, api-held-unused',
            'X-Cache-Purge-Operation-Id': 'api-disconnected',
            'X-Cache-Purge-Issued-At': str(int(time.time() * 1000))})
        response = conn.getresponse()
        assert response.status == 202
        response.close()
        conn.close()
        assert held_path.exists()
        accepted = api('/__purge/v2/operations/api-disconnected')[2]
        assert accepted['effective'] and accepted['cleanup']['state'] != 'done'
        stop(process); process = start()
        until(lambda: (run / 'index/component.sock').exists())
        until(lambda: struct.unpack('>IQ', rpc(16))[0] == 6)
        restored = api('/__purge/v2/operations/api-disconnected')[2]
        assert restored['effective_at'] == accepted['effective_at'] and held_path.exists()
    finally:
        os.close(lease)
    try:
        until(lambda: api('/__purge/v2/operations/api-disconnected')[2]['cleanup']['state'] == 'done')
    except AssertionError:
        print('cleanup diagnostic', api('/__purge/v2/operations/api-disconnected')[2],
              'writers', writers(int(restored['sequence'])), 'file_exists', held_path.exists(), flush=True)
        raise
    assert not held_path.exists()
    checks.append('PURGE TAG-group acceptance does not wait for a held file lease; disconnection and nginx restart preserve autonomous cleanup and the original effective time')

    def propagation_headers(path, language=None, span=None):
        return next(headers for uri, headers in reversed(propagation_calls)
                    if uri == path and (language is None or headers.get('accept-language') == language)
                    and (span is None or headers.get('range') == span))

    def propagated(headers, expected=None):
        names = {name for name in headers if name.startswith('x-cache-purge-')}
        if expected is None:
            assert not names, headers
            return
        assert names == {'x-cache-purge-time', 'x-cache-purge-mode', 'x-cache-purge-fence'}, headers
        packet = base64.b64decode(headers['x-cache-purge-fence'], validate=True)
        assert packet == b'NPF2' + struct.pack('>QQ', *expected), headers
        assert headers['x-cache-purge-time'] == str(max(expected))
        assert headers['x-cache-purge-mode'] == ('mixed' if all(expected) else 'invalidate' if expected[0] else 'delete')

    def tag_push(name, tag, mode='delete'):
        operation = api_input(name, {'type': 'tag', 'values': [tag]}, mode)
        result = api('/__purge/v2/operations', operation,
                     headers={'X-Cache-Purge-Namespace': 'prop'})
        assert result[0] == 202 and result[2]['effective'], result
        until(lambda: struct.unpack('>IQ', rpc(16))[0] == 6)
        return calendar.timegm(time.strptime(operation['event']['issued_at'], '%Y-%m-%dT%H:%M:%SZ')) * 1000

    def prop_get(path, headers=None):
        return get(path, {'Host': 'prop.test', **(headers or {})})

    def prop_records(path, count):
        found = until(lambda: (v if len(v := query('http://prop.test' + path)) == count
                               and all(item[2] == 2 for item in v) else None))
        return [record(item[0]) for item in found]

    uri = '/propagation/entry-hit'
    policies[uri] = 'entry-hit-tag'
    assert prop_get(uri) == (200, 'MISS', uri.encode())
    ready_file(prop_records(uri, 1)[0])
    tag_push('entry-unrelated', 'entry-unrelated-tag')
    until(lambda: api('/__purge/v2/operations/entry-unrelated')[2]['cleanup']['state'] == 'done')
    until(lambda: struct.unpack('>IQ', rpc(16))[0] == 6)
    response = prop_get(uri)
    assert response == (200, 'HIT', uri.encode()), response
    initial_requests = int.from_bytes(rpc(1)[40:48], 'big')
    initial_origins = calls.count(uri)
    for _ in range(10):
        assert prop_get(uri) == (200, 'HIT', uri.encode())
    final_requests = int.from_bytes(rpc(1)[40:48], 'big')
    hit_query_budget = {'hits': 10, 'component_requests': final_requests - initial_requests,
                        'maximum_requests_including_final_ping': 1}
    assert hit_query_budget['component_requests'] == 1, hit_query_budget
    assert calls.count(uri) == initial_origins
    checks.append('HITs do not query the representation index before cache lookup while an unrelated TAG propagation window remains active')

    uri = '/propagation/vary'
    policies[(uri, 'en')] = 'known-red'
    policies[(uri, 'de')] = 'known-blue'
    policies[(uri, 'fr')] = 'unknown-green'
    for number, (suffix, language) in enumerate((('', 'en'), ('', 'de'), ('?z=b', 'en')), 1):
        assert prop_get(uri + suffix, {'Accept-Language': language}) == (200, 'MISS', (uri + language).encode())
        propagated(propagation_headers(uri, language))
        prop_records(uri, number)
    variants = prop_records(uri, 3)
    removed = [item for item in variants if b'known-red' in item['tags']]
    assert len(removed) == 2
    deleted_at = tag_push('prop-known-red', 'known-red')
    until(lambda: all(record(item['id'], optional=True) is None for item in removed))
    assert prop_get(uri + '?bypass=1', {'Accept-Language': 'de'})[0] == 200
    propagated(propagation_headers(uri, 'de'))
    checks.append('TAG propagation selects the current native Vary representation and never borrows a different language across zones')
    stop(process); process = start()
    until(lambda: (run / 'index/component.sock').exists())
    until(lambda: struct.unpack('>IQ', rpc(16))[0] == 6)
    assert prop_get(uri + '?z=b', {'Accept-Language': 'en'}) == (200, 'MISS', (uri + 'en').encode())
    propagated(propagation_headers(uri, 'en'), (0, deleted_at))
    checks.append('Delete retains known TAG associations through physical collection and full restart and emits only the fixed object timestamp')
    assert prop_get(uri, {'Accept-Language': 'fr'})[0] == 200
    propagated(propagation_headers(uri, 'fr'))
    checks.append('a completely unknown variant proceeds upstream without TAG compensation or partial propagation headers')

    uri = '/propagation/validate'
    policies[uri] = 'validate-old'
    conditional_paths.add(uri)
    assert prop_get(uri) == (200, 'MISS', uri.encode())
    original = prop_records(uri, 1)[0]
    invalidated_at = tag_push('prop-validate', 'validate-old', 'invalidate')
    validation_tags[uri] = 'validate-new'
    response = prop_get(uri)
    assert response == (200, 'REVALIDATED', uri.encode()), response
    propagated(propagation_headers(uri), (invalidated_at, 0))
    until(lambda: record(original['id'])['tags'] == [b'validate-new'])
    assert prop_get(uri + '?bypass=1')[0] == 200
    propagated(propagation_headers(uri), (invalidated_at, 0))
    checks.append('Invalidate emits its requirement for 304 and preserves the previous TAG association after the response replaces TAGs')

    uri = '/propagation/slice'
    policies[(uri, 'bytes=0-1023')] = 'slice-first'
    policies[(uri, 'bytes=1024-2047')] = 'slice-other'
    policies[(uri, 'bytes=2048-3071')] = 'slice-other'
    assert prop_get(uri)[2] == slice_body
    sliced = prop_records(uri, 3)
    first_slice = next(item for item in sliced if b'slice-first' in item['tags'])
    deleted_at = tag_push('prop-slice', 'slice-first')
    until(lambda: record(first_slice['id'], optional=True) is None)
    assert prop_get(uri + '?bypass=1')[2] == slice_body
    propagated(propagation_headers(uri, span='bytes=0-1023'), (0, deleted_at))
    propagated(propagation_headers(uri, span='bytes=1024-2047'))
    propagated(propagation_headers(uri, span='bytes=2048-3071'))
    checks.append('known TAG propagation respects native slice identities instead of expanding a match to all files with the same key')

    uri = '/propagation/groups/object'
    policies[uri] = 'many-keys'
    variants = [uri + '?m=' + str(number) for number in range(97)]
    for target in variants:
        assert prop_get(target) == (200, 'MISS', uri.encode())
    original = prop_records(uri, 97)
    assert len({item['key'] for item in original}) == 97
    deleted_at = tag_push('prop-many-keys', 'many-keys')
    until(lambda: all(record(item['id'], optional=True) is None for item in original), timeout=20)
    policies[uri] = 'group-old'
    assert prop_get(variants[-1])[0] == 200
    propagated(propagation_headers(uri), (0, deleted_at))
    checks.append('background TAG Delete preserves known propagation for 97 misc keys of one canonical URL')

    current = prop_records(uri, 1)[0]
    conditional_paths.add(uri)
    validation_tags[uri] = 'group-new'
    operation = api_input('prop-group-invalidate',
                         {'type': 'url', 'value': 'http://prop.test' + uri}, 'invalidate')
    assert api('/__purge/v2/operations', operation)[0] == 202
    response = prop_get(variants[-1])
    assert response == (200, 'REVALIDATED', uri.encode()), response
    until(lambda: record(current['id'])['tags'] == [b'group-new'])
    assert prop_get(variants[-1]) == (200, 'HIT', uri.encode())
    checks.append('URL Invalidate validates the selected misc key with 304 and replaces its TAGs')
    for number, overlap in enumerate((False, True)):
        uri = '/propagation/send-race-' + str(number)
        tag = 'send-race-tag-' + str(number)
        policies[uri] = tag
        assert prop_get(uri) == (200, 'MISS', uri.encode())
        prop_records(uri, 1)
        invalidated = tag_push('send-invalidate-' + str(number), tag,
                               'invalidate') if overlap else 0
        host = 'held-' + str(number) + '.send.test'
        arrived, release = threading.Event(), threading.Event()
        dns_holds[host] = arrived, release
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(prop_get, uri + '?bypass=1&gate=' + host)
            try:
                assert arrived.wait(5), 'proxy did not reach DNS after create_request'
                deleted = tag_push('send-delete-' + str(number), tag)
                policies[uri] = 'send-new-' + str(number)
            finally:
                release.set()
            response = future.result(timeout=12)
        assert response == (200, 'BYPASS', uri.encode()), response
        propagated(propagation_headers(uri), (invalidated, deleted))
        latest = prop_records(uri, 1)[0]
        assert latest['tags'] == [('send-new-' + str(number)).encode()], latest
        checks.append('real TAG acceptance during DNS wait refreshes ' +
                      ('an existing Invalidate header to mixed' if overlap else 'an empty header set to Delete') +
                      ' before the first send, then admits the new response')

    uri = '/propagation/send-inflight'
    policies[uri] = 'send-inflight-tag'
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(prop_get, uri)
        try:
            assert send_waiting.wait(5), 'request did not reach the upstream'
            tag_push('send-inflight', 'send-inflight-tag')
            operation = api('/__purge/v2/operations/send-inflight')[2]
        finally:
            send_release.set()
        response = future.result(timeout=12)
    assert response == (200, 'MISS', uri.encode()), response
    until(lambda: writers(int(operation['sequence'])) == 0)
    assert not any(item[2] == 2 for item in query('http://prop.test' + uri))
    fresh_bodies[uri] = b'send-new-entity'
    response = prop_get(uri)
    assert response[0] == 200 and response[1] in ('MISS', 'BYPASS'), response
    assert response[2] == b'send-new-entity' and calls.count(uri) == 2, response
    prop_records(uri, 1)
    assert prop_get(uri) == (200, 'HIT', b'send-new-entity')
    checks.append('a request sent before TAG acceptance can finish its response but cannot publish the old entity')

    uri = '/propagation/uncached-seed'
    policies[(uri, 'en')] = 'detached-red'
    policies[(uri, 'de')] = 'detached-blue'
    for number, language in enumerate(('en', 'de'), 1):
        assert prop_get(uri, {'Accept-Language': language})[0] == 200
        prop_records(uri, number)
    initial = prop_records(uri, 2)
    red = next(item for item in initial if b'detached-red' in item['tags'])
    issued = tag_push('detached-red', 'detached-red')
    until(lambda: record(red['id'], optional=True) is None)
    remaining = query('http://prop.test' + uri)
    for language, requirement in [('en', (0, issued)), ('de', None), ('fr', None)]:
        response = prop_get('/propagation/uncached-vary', {'Accept-Language': language})
        assert response == (200, None, (uri + language).encode()), response
        propagated(propagation_headers(uri, language), requirement)
        assert query('http://prop.test' + uri) == remaining
    checks.append('proxy_cache off resolves current and retained Vary associations without borrowing unknown variants or publishing cache files')
    response = get(uri, {'Host': 'prop.test', 'Accept-Language': 'en'}, method='POST')
    assert response == (200, None, (uri + 'en').encode()), response
    assert propagation_methods[-1] == (uri, 'POST')
    propagated(propagation_headers(uri, 'en'), (0, issued))
    response = prop_get(uri + '?z=off', {'Accept-Language': 'en'})
    assert response == (200, None, (uri + 'en').encode()), response
    propagated(propagation_headers(uri, 'en'), (0, issued))
    assert query('http://prop.test' + uri) == remaining
    checks.append('excluded POST methods and a runtime cache zone resolving to off retain propagation and do not create index records')

    uri = '/propagation/slice-uncached-seed'
    policies[(uri, 'bytes=0-1023')] = 'detached-slice-red'
    assert prop_get(uri)[2] == slice_body
    original = prop_records(uri, 3)
    first = next(item for item in original if b'detached-slice-red' in item['tags'])
    issued = tag_push('detached-slice', 'detached-slice-red')
    until(lambda: record(first['id'], optional=True) is None)
    response = prop_get('/propagation/uncached-slice')
    assert response == (200, None, slice_body), response
    propagated(propagation_headers(uri, span='bytes=0-1023'), (0, issued))
    propagated(propagation_headers(uri, span='bytes=1024-2047'))
    propagated(propagation_headers(uri, span='bytes=2048-3071'))
    assert len(query('http://prop.test' + uri)) == 2
    checks.append('uncached slice subrequests compute their own native main digest and propagate only the matching retained TAG')

    incoming_time = int(time.time()) * 1000
    incoming = {'X-Cache-Purge-Time': str(incoming_time), 'X-Cache-Purge-Mode': 'delete',
                'X-Cache-Purge-Fence': base64.b64encode(b'NPF2' + struct.pack('>QQ', 0, incoming_time)).decode()}
    assert prop_get('/propagation/uncached-vary', {**incoming, 'Accept-Language': 'fr'})[0] == 200
    propagated(propagation_headers('/propagation/uncached-seed', 'fr'), (0, incoming_time))
    assert prop_get('/propagation/uncached-origin', incoming)[0] == 200
    propagated(propagation_headers('/propagation/uncached-origin'))
    malformed = {**incoming, 'X-Cache-Purge-Fence': 'broken'}
    assert prop_get('/propagation/uncached-ignore', malformed)[0] == 200
    propagated(propagation_headers('/propagation/uncached-ignore'))
    before = len(propagation_calls)
    assert prop_get('/propagation/uncached-vary', malformed)[0] == 400
    assert len(propagation_calls) == before
    checks.append('uncached transit forwards valid requirements, strips them on the origin leg, honors ignore and rejects malformed input before dispatch')
    response = prop_get('/propagation/uncached-manual', incoming)
    assert response[0] == 200, response
    propagated(propagation_headers('/propagation/uncached-manual'), (0, incoming_time))
    checks.append('independent uncached identity expressions do not evaluate unused key-module components')

finally:
    finish_release.set()
    send_release.set()
    for hold in dns_holds.values(): hold[1].set()
    dns_server.shutdown(); dns_server.server_close()
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
          'dynamic': a.dynamic, 'key_last': a.key_last, 'sanitizer': a.sanitize, 'expr': bool(a.expr), 'extension': extension,
          'sanitizer_exclusions': ['nonnull-attribute', 'alignment instrumentation for native ngx_md5.o only'] if a.sanitize else [],
          'workers': a.workers, 'checks': checks,
          'hit_query_budget': hit_query_budget,
          'sources': {group: {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.rglob('*') if p.is_file()}
                      for group, directory in [('addon', addon), ('key', key)]},
          'library_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in a.libraries.glob('libngx_http_cache_*.a')},
          'scope': 'Real cache capture, admission, durable Invalidate/Delete HTTP API, nginx-configured access control and restart-safe background cleanup; L2 propagation remains incomplete'}
a.output.write_text(json.dumps(result, indent=2) + '\n')
print('PASS', len(checks), 'native index capture checks')
