#!/usr/bin/env python3
"""Verify native expiry, in-memory purged, and restart degradation."""
import argparse
import fcntl
import hashlib
import http.client
import http.server
import io
import json
import os
import re
from pathlib import Path
import signal
import socket
import socketserver
import struct
import subprocess
import tempfile
import tarfile
import threading
import time

parser = argparse.ArgumentParser()
parser.add_argument('--nginx', type=Path, required=True)
parser.add_argument('--nginx-ref', default='HEAD')
parser.add_argument('--extension', type=int, choices=(0, 1), required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--reuse-build', type=Path)
parser.add_argument('--backend', type=Path,
                    help='Enable the embedded backend using these CMake archives')
parser.add_argument('--sanitize', action='store_true')
parser.add_argument('--public-module', action='store_true')
parser.add_argument('--no-thread-pool', action='store_true')
parser.add_argument('--expr', type=Path)
parser.add_argument('--proxy-filter', type=Path)
parser.add_argument('--slice', action='store_true')
parser.add_argument('--dynamic', action='store_true')
parser.add_argument('--protocols', action='store_true')
parser.add_argument('--ssl', action='store_true')
parser.add_argument('--cache-io', action='store_true',
                    help='Exercise sendfile and cached native file descriptors')
args = parser.parse_args()
cache_io = '''sendfile on;
    open_file_cache max=10000 inactive=20s;
    open_file_cache_valid 10s;
    open_file_cache_min_uses 2;
    open_file_cache_errors on;''' if args.cache_io else '''
    open_file_cache max=128 inactive=30s;
    open_file_cache_valid 30s;
    open_file_cache_min_uses 1;'''
if args.public_module:
    assert args.backend, '--public-module requires --backend'
if args.slice or args.dynamic or args.protocols:
    assert args.public_module, 'extended HTTP checks require --public-module'
repo = Path(__file__).resolve().parent.parent
root = args.reuse_build or Path(tempfile.mkdtemp(prefix='cache-native-runtime-'))
build, addon, run = root / 'nginx', root / 'addon', root / 'run'
if args.reuse_build and run.exists():
    run = Path(tempfile.mkdtemp(prefix='run-', dir=root))
for directory in (build, addon, run, run / 'cache', run / 'logs'):
    directory.mkdir(exist_ok=bool(args.reuse_build))
if args.backend:
    (run / 'store').mkdir()
    (run / 'cache2').mkdir()
git = ['git', '-c', 'safe.directory=' + str(args.nginx), '-C', str(args.nginx)]
revision = subprocess.check_output(git + ['rev-parse', args.nginx_ref], text=True).strip()
archive = subprocess.check_output(git + ['archive', revision])
if not args.reuse_build:
    subprocess.run(['tar', '-xf', '-', '-C', str(build)], input=archive, check=True)
with tarfile.open(fileobj=io.BytesIO(archive)) as source:
    core_files = [entry for entry in source if entry.isfile() and entry.name.startswith('src/')]
    for entry in core_files:
        assert (build / entry.name).read_bytes() == source.extractfile(entry).read(), entry.name
inputs = ('ngx_http_cache_purge_files.c', 'ngx_http_cache_purge.h',
          'ngx_http_cache_purge_internal.h', 'rocksdb/ngx_http_cache_rocksdb.h',
          't/ngx_http_cache_purge_native_probe.c')
if args.backend:
    inputs += ('ngx_http_cache_purge_runtime.c',)
if args.public_module:
    inputs += ('ngx_http_cache_purge_module.c', 'ngx_http_cache_purge_request.c',
               'rocksdb/ngx_http_cache_rocksdb_module.c',
               'key/ngx_http_cache_key_module.c', 'config')
hashes = {}
for name in inputs:
    target = addon / name
    target.parent.mkdir(parents=True, exist_ok=True)
    data = (repo / name).read_bytes().replace(b'\r\n', b'\n')
    target.write_bytes(data)
    hashes[name] = hashlib.sha256(data).hexdigest()
backend_sources = ''
backend_libraries = ''
if args.backend:
    backend_sources = ' $ngx_addon_dir/ngx_http_cache_purge_runtime.c'
    backend_libraries = ('ngx_module_libs="' +
        str(args.backend / 'libngx_http_cache_purge_index.a') + ' ' +
        str(args.backend / 'libngx_http_cache_rocksdb.a') + ' ' +
        subprocess.check_output(['pkg-config', '--libs', '--static', 'rocksdb'],
                                text=True).strip() + ' -lstdc++ -pthread"\n')
probe_config = ('''ngx_addon_name=ngx_http_cache_purge_native_probe_module
ngx_module_type=HTTP
ngx_module_name=$ngx_addon_name
ngx_module_incs="$ngx_addon_dir"
ngx_module_srcs="$ngx_addon_dir/ngx_http_cache_purge_files.c $ngx_addon_dir/t/ngx_http_cache_purge_native_probe.c'''
    + backend_sources + '"\n' + backend_libraries + '. auto/module\n')
if not args.public_module:
    (addon / 'config').write_text(probe_config)
else:
    os.environ['CACHE_PURGE_ROCKSDB_LIB_DIR'] = str(args.backend)
configure = 'auto/configure' if (build / 'auto/configure').exists() else 'configure'
command = ['sh', configure, '--prefix=' + str(run), '--with-threads',
           '--with-cc-opt=-O2 -DNGX_RESTY_EXT=' + str(args.extension)
           + (' -DNGX_HTTP_CACHE_PURGE_OWNER_TEST=1'
              if args.backend and not args.public_module else ''),
           ('--add-dynamic-module=' if args.dynamic else '--add-module=') + str(addon)]
if args.no_thread_pool:
    command.remove('--with-threads')
if args.slice:
    command.append('--with-http_slice_module')
if args.ssl:
    command.append('--with-http_ssl_module')
for dependency in (args.expr, args.proxy_filter):
    if dependency:
        command.insert(-1, '--add-module=' + str(dependency))
if args.sanitize:
    cc = next(i for i, value in enumerate(command) if value.startswith('--with-cc-opt='))
    command[cc] += ' -g -fsanitize=address,undefined -fno-omit-frame-pointer'
    command.append('--with-ld-opt=-fsanitize=address,undefined')
    os.environ['ASAN_OPTIONS'] = 'detect_leaks=0:abort_on_error=1'
    os.environ['UBSAN_OPTIONS'] = 'halt_on_error=1:print_stacktrace=1'
print('BUILD', root, flush=True)
backend_hashes = {name: hashlib.sha256((args.backend / name).read_bytes()).hexdigest()
                  for name in ('libngx_http_cache_purge_index.a', 'libngx_http_cache_rocksdb.a')} if args.backend else {}
with (root / 'build.log').open('a' if args.reuse_build else 'w') as log:
    if args.reuse_build:
        assert '-DNGX_RESTY_EXT=' + str(args.extension) in (build / 'objs/Makefile').read_text()
        assert ('-DNGX_HTTP_CACHE_PURGE_OWNER_TEST=1' in
                (build / 'objs/Makefile').read_text()) == bool(args.backend and not args.public_module)
        assert ('-fsanitize=address,undefined' in
                (build / 'objs/Makefile').read_text()) == args.sanitize
        assert ('ngx_http_slice_filter_module' in
                (build / 'objs/ngx_modules.c').read_text()) == args.slice
        assert ('ngx_http_ssl_module' in
                (build / 'objs/ngx_modules.c').read_text()) == args.ssl
        assert ('ngx_http_cache_key_module' in
                (build / 'objs/ngx_modules.c').read_text()) == (args.public_module and not args.dynamic)
        assert ('--add-dynamic-module=' + str(addon) in
                (build / 'objs/ngx_auto_config.h').read_text()) == args.dynamic
    reconfigure = not args.reuse_build or (args.backend and any(
        str(args.backend / name) not in (build / 'objs/Makefile').read_text()
        for name in backend_hashes))
    if reconfigure:
        subprocess.run(command, cwd=build, stdout=log, stderr=subprocess.STDOUT, check=True)
    subprocess.run(['make', '-j2'], cwd=build, stdout=log, stderr=subprocess.STDOUT, check=True)

state = {'fail': False, 'version': 1, 'calls': [], 'ranges': [], 'protocol_calls': []}
slow_started = threading.Event()
slow_release = threading.Event()

def slice_body():
    return (f'slice-v{state["version"]}-'.encode() * 384)[:3072]

class Origin(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        state['calls'].append(self.headers.get('If-None-Match'))
        version = state['version']
        if self.path == '/bulk/zz-slow' and not slow_release.is_set():
            slow_started.set()
            assert slow_release.wait(5), 'in-flight request was not released'
        if self.path.startswith('/slice/'):
            body = slice_body()
            match = re.fullmatch(r'bytes=(\d+)-(\d+)', self.headers.get('Range', ''))
            assert match, dict(self.headers)
            start, end = map(int, match.groups())
            end = min(end, len(body) - 1)
            state['ranges'].append((self.path, start, end))
            self.send_response(206)
            self.send_header('Content-Range', f'bytes {start}-{end}/{len(body)}')
            self.send_header('Cache-Control', 'max-age=3600')
            self.send_header('ETag', f'"slice-v{state["version"]}"')
            body = body[start:end + 1]
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if state['fail']:
            self.send_response(500)
            body = b'failure'
        else:
            etag = '"v' + str(version) + '"'
            status = 304 if self.headers.get('If-None-Match') == etag else 200
            self.send_response(status)
            self.send_header('ETag', etag)
            self.send_header('Cache-Control', 'max-age=3600')
            self.send_header('Cache-Tag', self.headers.get('Accept-Language', 'native')
                             if self.path == '/variants/vary' else self.headers.get('X-Test-Tags', 'native'))
            body = b'' if status == 304 else ('body-v' + str(version)).encode()
            if self.path == '/variants/vary':
                self.send_header('Vary', 'Accept-Language')
                body += self.headers.get('Accept-Language', 'default').encode()
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *arguments):
        pass

origin = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Origin)
threading.Thread(target=origin.serve_forever, daemon=True).start()
protocol_servers = {}


class ProtocolOrigin(socketserver.StreamRequestHandler):
    def handle(self):
        protocol = self.server.protocol
        if protocol == 'fastcgi':
            data = b''
            while True:
                header = self.rfile.read(8)
                if not header:
                    return
                version, kind, identifier, length, padding, _ = struct.unpack('!BBHHBB', header)
                assert version == 1
                content = self.rfile.read(length)
                self.rfile.read(padding)
                if kind == 4:
                    data += content
                if kind == 5 and length == 0:
                    break
            params, offset = {}, 0
            while offset < len(data):
                lengths = []
                for _ in range(2):
                    length = data[offset]
                    size = 4 if length & 128 else 1
                    lengths.append(int.from_bytes(data[offset:offset + size], 'big') & 0x7fffffff)
                    offset += size
                key, value = lengths
                params[data[offset:offset + key].decode()] = data[offset + key:offset + key + value].decode()
                offset += key + value
        elif protocol == 'uwsgi':
            _, length, _ = struct.unpack('<BHB', self.rfile.read(4))
            data, offset, params = self.rfile.read(length), 0, {}
            while offset < len(data):
                key = struct.unpack_from('<H', data, offset)[0]
                offset += 2
                name = data[offset:offset + key].decode()
                offset += key
                length = struct.unpack_from('<H', data, offset)[0]
                offset += 2
                params[name] = data[offset:offset + length].decode()
                offset += length
        else:
            size = b''
            while (byte := self.rfile.read(1)) != b':':
                assert byte
                size += byte
            data = self.rfile.read(int(size)).split(b'\0')
            assert self.rfile.read(1) == b','
            params = {key.decode(): value.decode() for key, value in zip(data[::2], data[1::2])}
        state['protocol_calls'].append((protocol, params))
        etag = f'"v{state["version"]}"'
        status = 500 if state['fail'] else 304 if params.get('HTTP_IF_NONE_MATCH') == etag else 200
        body = b'' if status == 304 else b'failure' if status == 500 else f'body-v{state["version"]}'.encode()
        first = f'Status: {status}\r\n' if protocol == 'fastcgi' else f'HTTP/1.1 {status} Test\r\n'
        policy = '' if status == 500 else f'Cache-Control: max-age=3600\r\nETag: {etag}\r\nCache-Tag: protocol\r\n'
        response = (first + f'Content-Type: text/plain\r\nContent-Length: {len(body)}\r\n'
                    + policy + '\r\n').encode() + body
        if protocol == 'fastcgi':
            response = (struct.pack('!BBHHBB', 1, 6, identifier, len(response), 0, 0) + response
                        + struct.pack('!BBHHBB', 1, 6, identifier, 0, 0, 0)
                        + struct.pack('!BBHHBB', 1, 3, identifier, 8, 0, 0) + bytes(8))
        self.wfile.write(response)


if args.protocols:
    for protocol in ('fastcgi', 'uwsgi', 'scgi'):
        server = socketserver.ThreadingTCPServer(('127.0.0.1', 0), ProtocolOrigin)
        server.daemon_threads = True
        server.protocol = protocol
        threading.Thread(target=server.serve_forever, daemon=True).start()
        protocol_servers[protocol] = server
with socket.socket() as available:
    available.bind(('127.0.0.1', 0))
    port = available.getsockname()[1]
owner_config = f'native_cache_test_store {run}/store;' if args.backend else ''
control_config = 'native_cache_test;'
business_config = 'proxy_cache_bypass $native_cache_test_guard;'
api_config = ''
refused = None
if args.public_module:
    refused = socket.socket()
    refused.bind(('127.0.0.1', 0))
    owner_config = f'''cache_purge_index_store {run}/store;
    cache_purge_state_zone purge 1m;
    map $http_x_test_identity $native_purge_url {{
        default http://cache.test$uri;
        invalid "";
    }}
    cache_purge_key_url $native_purge_url;
    cache_purge_tag $upstream_http_cache_tag inherited;'''
    control_config = 'proxy_cache_purge on; cache_purge_key_url $http_x_target;'
    business_config = '''proxy_cache_purge on;
            proxy_set_header X-Cache-Purge-Time $cache_purge_propagation_time;
            proxy_set_header X-Cache-Purge-Action $cache_purge_propagation_action;'''
    api_config = 'location /management/ { cache_purge_api; }'
    api_config += f'''
        location = /memory-error {{
            proxy_cache_purge on;
            proxy_cache test;
            proxy_cache_key $uri;
            proxy_cache_valid 502 504 1h;
            proxy_next_upstream off;
            proxy_pass http://127.0.0.1:{refused.getsockname()[1]};
            add_header X-Cache $upstream_cache_status always;
        }}
        location = /intercepted {{
            proxy_cache_purge on;
            proxy_cache test;
            proxy_cache_key $uri;
            proxy_cache_valid 500 1h;
            proxy_intercept_errors on;
            error_page 500 = @fallback;
            proxy_pass http://127.0.0.1:{origin.server_port};
        }}
        location = /disk-error {{
            proxy_cache_purge on;
            proxy_cache test;
            proxy_cache_key $uri;
            proxy_cache_valid 500 1h;
            proxy_pass http://127.0.0.1:{origin.server_port};
            add_header X-Cache $upstream_cache_status always;
        }}
        location @fallback {{ return 200 "fallback"; }}'''
    if args.expr:
        owner_config += '''
    expr is_purge str_eq $request_method PURGE;
    expr is_get str_eq $request_method GET;'''
        control_config = '''
            when is_purge {
                proxy_cache_purge on;
                cache_purge_key_url $http_x_target;
                cache_purge_type_header X-Test-Purge-Type;
                cache_purge_action_header X-Test-Purge-Action;
            }
            proxy_cache_purge off;
            cache_purge_key_url http://unused.test/;'''
        business_config = business_config.replace('proxy_cache_purge on;', '''
            when is_get { proxy_cache_purge on; }
            proxy_cache_purge off;''')
    control_config += '''
        cache_purge_key_misc $http_x_test_misc;
        cache_purge_key_namespace $http_x_test_namespace;'''
    variant_identity = '''cache_key_host variants.test;
            cache_key_uri $uri;
            cache_key_misc $http_x_test_misc;
            cache_key_namespace $http_x_test_namespace;
            cache_purge_key_url $cache_key_url;
            cache_purge_key_misc $cache_key_misc;
            cache_purge_key_namespace $cache_key_namespace;'''
    tag_config = 'cache_purge_tag $upstream_http_cache_tag $http_x_extra_tag extra-a extra-b;'
    if args.expr:
        tag_config = 'when is_get { ' + tag_config + ' } cache_purge_tag fallback;'
    api_config += f'''
        location /tag-input/ {{
            proxy_cache_purge on;
            cache_purge_key_url http://tags.test$uri;
            cache_purge_key_namespace tag-input;
            {tag_config}
            proxy_cache test;
            proxy_cache_key $uri;
            proxy_cache_valid 200 1h;
            proxy_pass http://127.0.0.1:{origin.server_port};
            add_header X-Cache $upstream_cache_status always;
        }}
        location /variants/ {{
            proxy_cache_purge on;
            {variant_identity}
            proxy_cache test;
            proxy_cache_key $cache_key_full;
            proxy_cache_valid 200 1h;
            proxy_cache_use_stale error http_500;
            proxy_pass http://127.0.0.1:{origin.server_port};
            add_header X-Cache $upstream_cache_status always;
        }}'''
    if args.slice:
        slice_identity = ('cache_key_uri $uri; cache_key_range $slice_range; '
                          'cache_purge_key_url $cache_key_url; '
                          'cache_purge_key_misc $cache_key_misc; '
                          'cache_purge_key_namespace $cache_key_namespace;')
        api_config += f'''
        location /slice/ {{
            {'slice on; slice_size 1k;' if args.extension else 'slice 1k;'}
            log_subrequest off;
            proxy_cache_purge on;
            {slice_identity}
            proxy_cache test;
            proxy_cache_key $cache_key_full;
            proxy_set_header Range $slice_range;
            proxy_cache_valid 200 206 1h;
            proxy_pass http://127.0.0.1:{origin.server_port};
            add_header X-Cache $upstream_cache_status always;
        }}'''
    for protocol, server in protocol_servers.items():
        (run / (protocol + '-cache')).mkdir()
        owner_config += f'''
    {protocol}_cache_path {run}/{protocol}-cache levels=1:2 keys_zone={protocol}:1m;'''
        api_config += f'''
        location /{protocol}/ {{
            {protocol}_cache_purge on;
            {protocol}_cache {protocol};
            {protocol}_cache_key $uri;
            {protocol}_cache_valid 200 1h;
            {protocol}_cache_revalidate on;
            {protocol}_cache_use_stale error http_500;
            {protocol}_param REQUEST_URI $uri;
            {protocol}_param HTTP_IF_NONE_MATCH $upstream_cache_etag;
            {protocol}_pass 127.0.0.1:{server.server_address[1]};
            add_header X-Cache $upstream_cache_status always;
        }}'''
second_cache = (f'proxy_cache_path {run}/cache2 levels=1:2 keys_zone=test2:1m;\n'
                'map $http_x_zone $native_cache_zone { default test; second test2; }'
                if args.backend else '')
loads = ('\n'.join(f'load_module {build}/objs/{name}.so;'
                  for name in ('ngx_http_cache_key_module', 'ngx_http_cache_rocksdb_module',
                               'ngx_http_cache_purge_module'))
         if args.dynamic else '')
config = f'''{loads}
daemon off;
master_process {'on' if args.backend else 'off'};
worker_processes {2 if args.backend else 1};
pid {run}/nginx.pid;
error_log {run}/error.log notice;
events {{ worker_connections 128; }}
http {{
    access_log off;
    {owner_config}
    proxy_cache_path {run}/cache levels=1:2 keys_zone=test:1m;
    {second_cache}
    {cache_io}
    server {{
        listen 127.0.0.1:{port};
        location = /control {{ {control_config} }}
        {api_config}
        location / {{
            proxy_pass http://127.0.0.1:{origin.server_port};
            proxy_cache {'$native_cache_zone' if args.backend else 'test'};
            proxy_cache_key $uri;
            {business_config}
            proxy_cache_revalidate on;
            proxy_cache_valid 200 1h;
            proxy_cache_use_stale error timeout http_500 updating;
            proxy_next_upstream off;
            add_header X-Cache $upstream_cache_status always;
        }}
    }}
}}
'''
(run / 'nginx.conf').write_text(config)
binary = build / 'objs/nginx'
process = None
output = (root / 'runtime.log').open('w')
checks, responses = [], []
expected_manager_exits = []

if args.public_module:
    store_directive = f'cache_purge_index_store {run}/store;'
    assert config.count(store_directive) == 1
    for value, valid in (('0', True), ('1k', True), ('16m', True),
                         ('64m', True), ('1', False), ('1023', False),
                         ('-1', False), ('bad', False),
                         ('16m io_rate=32m', False)):
        candidate = config.replace(store_directive,
            f'cache_purge_index_store {run}/store io_rate={value};')
        candidate = candidate.replace(str(run / 'error.log'), str(run / 'config-check.log'))
        (run / 'config-check.conf').write_text(candidate)
        result = subprocess.run([str(binary), '-t', '-p', str(run),
            '-c', str(run / 'config-check.conf')], capture_output=True, text=True)
        assert (result.returncode == 0) == valid, (value, result.stderr)
    checks.append('store I/O rate accepts disabled and sized limits, rejects invalid and duplicate values')

def start():
    proc = subprocess.Popen([str(binary), '-p', str(run), '-c', str(run / 'nginx.conf')],
                            stdout=output, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            error = run / 'error.log'
            raise AssertionError((error if error.exists() else root / 'runtime.log').read_text())
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=.1):
                return proc
        except OSError:
            time.sleep(.02)
    proc.terminate()
    proc.wait(5)
    raise AssertionError('nginx did not start')

def request(path='/item', headers=None, method='GET'):
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
    connection.request(method, path, headers={'Host': 'cache.test', **(headers or {})})
    response = connection.getresponse()
    result = {'path': path, 'status': response.status,
              'cache': response.getheader('X-Cache'), 'body': response.read().decode()}
    connection.close()
    responses.append(result)
    return result

def files():
    return [p for p in (run / 'cache').rglob('*') if p.is_file()]

def hit(path='/item', headers=None):
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        response = request(path, headers)
        assert response['status'] == 200, response
        if response['cache'] == 'HIT':
            return response
        time.sleep(.01)
    raise AssertionError(('cache fill did not settle to HIT', responses[-5:]))

def ready():
    if args.backend:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            response = request('/management/capabilities' if args.public_module
                               else '/control?action=ready')
            if (response['status'] == 200 and json.loads(response['body'])['ready']
                if args.public_module else response['status'] == 204):
                return
            time.sleep(.02)
        raise AssertionError('cache index owner did not become ready')


def owner_placement():
    children = Path(f'/proc/{process.pid}/task/{process.pid}/children').read_text().split()
    owners = []
    for pid in children:
        directory = Path('/proc') / pid
        try:
            name = (directory / 'cmdline').read_bytes()
            files = [str(path.resolve()) for path in (directory / 'fd').iterdir()]
            owns_store = any(path.startswith(str(run / 'store') + '/') for path in files)
            if name.startswith(b'nginx: worker process'):
                assert not owns_store, (pid, files)
                if args.no_thread_pool:
                    assert len(list((directory / 'task').iterdir())) == 1, pid
            if owns_store:
                assert name.startswith(b'nginx: cache manager process'), (pid, name)
                owners.append(pid)
        except FileNotFoundError:
            pass
    assert len(owners) == 1, owners
    return owners[0]

def purge(action, target, identifier):
    if not args.backend:
        assert request(f'/control?action={action}&target={target}')['status'] == 204
        return None
    # Native timestamps deliberately retain the accepted one-second boundary.
    time.sleep(1.05)
    response = accepted(action, target, identifier)
    assert response['status'] == 202, response
    return finished(identifier)

def finished(identifier):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        response = progress(identifier)
        assert response['status'] == 200, response
        status = json.loads(response['body'])
        if args.public_module:
            status = status['cleanup']
            status['done'] = status['state'] == 'done'
        if status['done']:
            assert status['affected'] > 0, status
            return status
        time.sleep(.02)
    raise AssertionError('background operation did not finish')

def accepted(action, target, identifier, kind='url', headers=None):
    if args.public_module:
        return request('/control', {
            'X-Target': target if target.startswith('http') else 'http://cache.test' + target,
            ('X-Test-Purge-Action' if args.expr else 'X-Cache-Purge-Action'): action,
            ('X-Test-Purge-Type' if args.expr else 'X-Cache-Purge-Type'): kind,
            'X-Cache-Purge-Operation-Id': f'test-{identifier}',
            'X-Cache-Purge-Issued-At': str(int(time.time() * 1000)),
            **(headers or {})}, method='PURGE')
    return request(f'/control?action={action}&target={target}&id={identifier}')

def progress(identifier):
    return request(f'/management/operations/test-{identifier}' if args.public_module
                   else f'/control?action=status&id={identifier}')

try:
    if args.public_module:
        with (run / 'store/index.lock').open('a+b') as lease:
            fcntl.flock(lease, fcntl.LOCK_EX)
            process = start()
            time.sleep(1.1)
            response = request('/management/capabilities')
            assert response['status'] == 200 and not json.loads(response['body'])['ready'], response
        checks.append('initial database lease conflict is retried and does not break manager cleanup at exit')
    else:
        process = start()
    ready()
    if args.public_module:
        owner_placement()
        checks.append('only the native cache manager owns database descriptors; HTTP workers remain single-threaded without nginx thread pools')
    assert request()['body'] == 'body-v1'
    hit()
    paths = files()
    assert len(paths) == 1
    cached = paths[0]
    inode = cached.stat().st_ino
    original = cached.read_bytes()
    assert not os.listxattr(cached)
    checks.append('native fill/HIT with open_file_cache and no module attributes')

    purge('invalidate', '/item', 1)
    expired = cached.read_bytes()
    assert expired != original and cached.stat().st_ino == inode
    state['fail'] = True
    response = request()
    if args.extension:
        assert response['status'] == 500 and response['body'] == 'failure', response
    else:
        assert response['status'] == 200 and response['cache'] == 'STALE', response
        assert response['body'] == 'body-v1'
    checks.append('purged blocks failed-origin stale in extension; macro-off keeps native fallback')

    state['fail'] = False
    response = request()
    assert response['cache'] == 'REVALIDATED' and response['body'] == 'body-v1', response
    assert state['calls'][-1] == '"v1"'
    assert cached.stat().st_ino == inode
    hit()
    checks.append('304 reuses the same inode and clears in-memory purged')

    purge('invalidate', '/item', 2)
    state['fail'] = True
    expired = cached.read_bytes()
    process.send_signal(signal.SIGQUIT)
    assert process.wait(10) == 0
    process = start()
    ready()
    response = request()
    assert response['status'] == 200 and response['cache'] == 'STALE', response
    assert response['body'] == 'body-v1' and cached.read_bytes() == expired
    checks.append('restart loses purged and permits ordinary expired-cache stale without restoring metadata')

    purge('delete', '/item', 3)
    assert not cached.exists()
    state['fail'] = False
    state['version'] = 2
    response = request()
    assert response['cache'] == 'MISS' and response['body'] == 'body-v2', response
    hit()
    assert all(not os.listxattr(p) for p in files())
    checks.append('Delete clears the native node and refills despite open_file_cache')
    if args.backend:
        for headers in ({}, {'X-Zone': 'second'}):
            assert request('/multi', headers)['cache'] == 'MISS'
            hit('/multi', headers)
        status = purge('delete', '/multi', 4)
        assert status['affected'] == 2, status
        for headers in ({}, {'X-Zone': 'second'}):
            assert request('/multi', headers)['cache'] == 'MISS'
            hit('/multi', headers)
        checks.append('background owner drains two-worker queues and deletes one URL across two cache zones')
        assert os.geteuid() != 0, 'permission-failure check requires an unprivileged worker'
        assert request('/hold')['cache'] == 'MISS'
        hit('/hold')
        digest = hashlib.md5(b'/hold').hexdigest()
        held = next(p for p in files() if p.name == digest)
        mode = held.stat().st_mode & 0o7777
        time.sleep(1.05)
        held.chmod(0)
        try:
            response = accepted('delete', '/hold', 5)
            assert response['status'] == 202, response
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                response = progress(5)
                assert response['status'] == 200, response
                current = json.loads(response['body'])
                if args.public_module:
                    current = current['cleanup']
                    current['done'] = current['state'] == 'done'
                assert not current['done'], current
                if current['error']:
                    break
                time.sleep(.02)
            assert current['error'] == 12 and held.exists(), current
            for _ in range(10):
                assert request()['cache'] == 'HIT'
        finally:
            held.chmod(mode)
        assert finished(5)['affected'] == 1
        assert not held.exists()
        checks.append('202 is returned while file processing fails; unrelated HITs continue and the background retry finishes')

        previous = re.findall(r'start worker process (\d+)',
                              (run / 'error.log').read_text())
        before = len(previous)
        old_workers = previous[-2:]
        process.send_signal(signal.SIGHUP)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            assert process.poll() is None
            if (run / 'error.log').read_text().count('start worker process ') >= before + 2:
                break
            time.sleep(.02)
        else:
            raise AssertionError('reload did not start replacement workers')
        while time.monotonic() < deadline:
            messages = (run / 'error.log').read_text()
            if all(f'worker process {pid} exited with code 0' in messages
                   for pid in old_workers):
                break
            time.sleep(.02)
        else:
            raise AssertionError('old workers did not finish reload handoff')
        ready()
        if args.public_module:
            owner = owner_placement()
            before = (run / 'error.log').read_text().count('start cache manager process ')
            os.kill(int(owner), signal.SIGKILL)
            expected_manager_exits.append(owner)
            deadline = time.monotonic() + 10
            while (run / 'error.log').read_text().count('start cache manager process ') <= before:
                assert time.monotonic() < deadline, 'master did not respawn cache manager'
                time.sleep(.02)
            ready()
            assert owner_placement() != owner
            checks.append('nginx master respawns a killed cache manager without restarting HTTP workers')
        assert purge('delete', '/multi', 6)['affected'] == 2
        checks.append('reload transfers the single RocksDB owner and retains cross-zone index data')
        if args.public_module:
            targets = [f'/bulk/{i:04d}' for i in range(510)] + ['/bulk/zz-head', '/bulk/zz-last']
            for target in targets:
                response = request(target)
                assert response['status'] == 200, response
            hit(targets[-1])
            last_digest = hashlib.md5(targets[-1].encode()).hexdigest()
            last_file = next(p for p in files() if p.name == last_digest)
            last_inode = last_file.stat().st_ino
            time.sleep(1.05)
            response = accepted('invalidate', '/bulk/', 7, 'prefix')
            assert response['status'] == 202, response
            pending = json.loads(progress(7)['body'])['cleanup']
            assert pending['state'] == 'pending' and pending['visited'] < 256, pending
            if not args.expr:
                response = request('/bulk/zz-head', method='HEAD')
                assert response['status'] == 200 and response['cache'] == 'REVALIDATED' and not response['body'], response
            response = request(targets[-1])
            assert response['cache'] == 'REVALIDATED' and response['body'] == 'body-v2', response
            assert last_file.stat().st_ino == last_inode
            response = request(targets[-1], {'X-Test-Identity': 'invalid'})
            assert response['status'] == 200 and response['cache'] == 'HIT', response
            assert json.loads(progress(7)['body'])['cleanup']['state'] == 'pending'
            finished(7)
            assert request(targets[-1])['cache'] == 'HIT'
            checks.append('pending prefix Invalidate revalidates the last cached object before background processing reaches it')

            inflight = []
            slow = threading.Thread(target=lambda: inflight.append(request('/bulk/zz-slow')))
            slow.start()
            assert slow_started.wait(2)
            time.sleep(1.05)
            state['version'] = 3
            response = accepted('delete', '/bulk/', 8, 'prefix')
            assert response['status'] == 202, response
            pending = json.loads(progress(8)['body'])['cleanup']
            assert pending['state'] == 'pending' and pending['visited'] < 256, pending
            slow_release.set()
            slow.join(5)
            assert not slow.is_alive() and inflight[0]['body'] == 'body-v2', inflight
            response = request('/bulk/zz-slow')
            assert response['cache'] == 'MISS' and response['body'] == 'body-v3', response
            response = request(targets[-1])
            assert response['cache'] == 'MISS' and response['body'] == 'body-v3', response
            response = request(targets[-1], {'X-Test-Identity': 'invalid'})
            assert response['status'] == 200 and response['cache'] == 'HIT', response
            assert json.loads(progress(8)['body'])['cleanup']['state'] == 'pending'
            finished(8)
            assert request(targets[-1])['cache'] == 'HIT'
            checks.append('pending prefix Delete fetches a new entity and background work preserves the refreshed object')
            checks.append('fresh HITs skip URL evaluation while older prefix tasks remain pending')
            checks.append('a pre-purge in-flight response completes without becoming a cache HIT while its rule is pending')
            for _ in range(2):
                response = request('/memory-error')
                assert response['status'] == 502 and response['cache'] == 'MISS', response
            state['fail'] = True
            calls = len(state['calls'])
            for _ in range(2):
                response = request('/intercepted')
                assert response['status'] == 200 and response['body'] == 'fallback', response
            assert len(state['calls']) == calls + 2, state['calls'][calls:]
            response = request('/disk-error')
            assert response['status'] == 500, response
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                response = request('/disk-error')
                assert response['status'] == 500, response
                if response['cache'] == 'HIT':
                    break
                time.sleep(.01)
            assert response['cache'] == 'HIT', response
            state['fail'] = False
            checks.append('memory-only connection and intercepted errors are not cached; ordinary disk-cached errors remain cacheable')
            if args.slice:
                target = '/slice/item'
                before = set(files())
                response = hit(target)
                assert response['body'].encode() == slice_body(), response
                slices = set(files()) - before
                assert len(slices) == 3, slices
                assert {x[1:] for x in state['ranges']} == {(0, 1023), (1024, 2047), (2048, 3071)}, state['ranges']
                assert purge('delete', target, 9)['affected'] == 3
                assert all(not p.exists() for p in slices), slices
                state['version'] += 1
                response = request(target)
                assert response['cache'] == 'MISS' and response['body'].encode() == slice_body(), response
                hit(target)
                assert purge('invalidate', target, 10)['affected'] == 3
                state['version'] += 1
                response = request(target)
                assert response['cache'] == 'EXPIRED' and response['body'].encode() == slice_body(), response
                hit(target)
                checks.append('URL Delete and Invalidate cover all three native slices with log_subrequest off')
            variants = [{'X-Test-Namespace': ns, 'X-Test-Misc': misc}
                        for ns, misc in [('', 'a'), ('a', 'a'), ('a', 'b'), ('b', 'a')]]
            target = '/variants/item'
            url = 'http://variants.test' + target
            for headers in variants:
                hit(target, headers)
            time.sleep(1.05)
            issued = str(int(time.time() * 1000))
            selected = {**variants[1], 'X-Cache-Purge-Issued-At': issued}
            assert accepted('delete', url, 20, 'key', selected)['status'] == 202
            assert finished(20)['affected'] == 1
            assert accepted('delete', url, 20, 'key', selected)['status'] == 202
            assert finished(20)['affected'] == 1
            assert accepted('invalidate', url, 20, 'key', selected)['status'] == 409
            for i, headers in enumerate(variants):
                assert request(target, headers)['cache'] == ('MISS' if i == 1 else 'HIT')
                hit(target, headers)
            checks.append('key uses configured URL plus exact misc and namespace; operation replay is idempotent and conflicting reuse returns 409')

            time.sleep(1.05)
            assert accepted('delete', 'unused', 21, 'tag', {
                'X-Test-Namespace': 'a', 'X-Cache-Purge-Tags': 'native'})['status'] == 202
            assert finished(21)['affected'] == 2
            for i, headers in enumerate(variants):
                assert request(target, headers)['cache'] == ('MISS' if i in (1, 2) else 'HIT')
                hit(target, headers)
            checks.append('namespace TAG purge matches only that namespace and does not require a target URL')

            for identifier, kind in [(22, 'host-tag'), (23, 'url'), (24, 'host')]:
                time.sleep(1.05)
                hit('/item')
                assert accepted('delete', url, identifier, kind, {
                    'X-Cache-Purge-Tags': 'native'})['status'] == 202
                assert finished(identifier)['affected'] == 4
                assert request('/item')['cache'] == 'HIT'
                for headers in variants:
                    assert request(target, headers)['cache'] == 'MISS'
                    hit(target, headers)
            checks.append('URL, host and host-TAG purge cover all misc/namespace variants using configured URL authority instead of management Host')

            target = '/variants/vary'
            before = set(files())
            for language in ('en', 'zh'):
                response = hit(target, {'Accept-Language': language})
                assert response['body'].endswith(language), response
            varying = set(files()) - before
            assert len(varying) == 2, varying
            time.sleep(1.05)
            assert accepted('invalidate', '', 26, 'tag', {
                'X-Cache-Purge-Tags': 'zh'})['status'] == 202
            assert finished(26)['affected'] == 1
            state['fail'] = True
            response = request(target, {'Accept-Language': 'zh'})
            assert response['status'] == (500 if args.extension else 200), response
            if not args.extension:
                assert response['cache'] == 'STALE', response
            assert request(target, {'Accept-Language': 'en'})['cache'] == 'HIT'
            state['fail'] = False
            hit(target, {'Accept-Language': 'zh'})
            checks.append('TAG Invalidate of a secondary Vary file enforces extension stale policy without invalidating the primary variant')
            time.sleep(1.05)
            assert accepted('delete', 'http://variants.test' + target, 25, 'key')['status'] == 202
            assert finished(25)['affected'] == 2
            assert all(not p.exists() for p in varying)
            checks.append('one configured key purge removes both native Vary files')

            scoped = {'X-Test-Namespace': 'tag-input'}
            limit_tags = ','.join(f'limit-{i}' for i in range(125))
            hit('/tag-input/limit', {'X-Test-Tags': limit_tags, 'X-Extra-Tag': 'source-extra'})
            time.sleep(1.05)
            assert accepted('delete', '', 40, 'tag', {
                **scoped, 'X-Cache-Purge-Tags': 'limit-124,source-extra,extra-b'})['status'] == 202
            assert finished(40)['affected'] == 1
            assert request('/tag-input/limit')['cache'] == 'MISS'
            checks.append('128 response TAGs combine upstream, variable and literal arguments and overlapping selectors affect a file once')

            for path, tags in [('a', 'batch-0,batch-1'), ('b', 'batch-31'), ('c', 'unselected')]:
                hit('/tag-input/' + path, {'X-Test-Tags': tags})
            time.sleep(1.05)
            issued = str(int(time.time() * 1000))
            batch = [f'batch-{i}' for i in range(32)]
            headers = {**scoped, 'X-Cache-Purge-Issued-At': issued,
                       'X-Cache-Purge-Tags': ', '.join(batch)}
            assert accepted('invalidate', '', 41, 'tag', headers)['status'] == 202
            assert finished(41)['affected'] == 2
            headers['X-Cache-Purge-Tags'] = ','.join(reversed(batch))
            assert accepted('invalidate', '', 41, 'tag', headers)['status'] == 202
            assert finished(41)['affected'] == 2
            assert request('/tag-input/a')['cache'] == 'EXPIRED'
            assert request('/tag-input/b')['cache'] == 'EXPIRED'
            assert request('/tag-input/c')['cache'] == 'HIT'
            checks.append('32-TAG Invalidate uses OR semantics, preserves unrelated files and replays reordered selectors idempotently')
            for value in (','.join(batch + ['overflow']), ' , , ', 'x' * 1025):
                assert accepted('delete', '', 42, 'tag', {
                    **scoped, 'X-Cache-Purge-Tags': value})['status'] == 400
            time.sleep(1.05)
            assert accepted('delete', '', 42, 'tag', {
                **scoped, 'X-Cache-Purge-Tags': ' extra-b, extra-b, , '})['status'] == 202
            assert finished(42)['affected'] == 4
            checks.append('TAG submission rejects 33 items and overlong or empty lists, while duplicates and empty comma fields are canonicalized')
            for number, protocol in enumerate(protocol_servers):
                target = '/' + protocol + '/item'
                hit(target)
                assert purge('invalidate', target, 30 + number * 3)['affected'] == 1
                state['fail'] = True
                response = request(target)
                assert response['status'] == (500 if args.extension else 200), response
                if not args.extension:
                    assert response['cache'] == 'STALE', response
                state['fail'] = False
                response = request(target)
                assert response['cache'] == 'REVALIDATED' and response['status'] == 200, response
                hit(target)
                assert purge('delete', target, 31 + number * 3)['affected'] == 1
                state['version'] += 1
                response = request(target)
                assert response['cache'] == 'MISS' and response['body'] == f'body-v{state["version"]}', response
                calls = [params for name, params in state['protocol_calls'] if name == protocol]
                assert not calls[-1].get('HTTP_IF_NONE_MATCH'), calls[-1]
                assert all(not params.get('HTTP_X_CACHE_PURGE_TIME') and not params.get('HTTP_X_CACHE_PURGE_ACTION') for params in calls)
                checks.append(protocol + ' independently supports HIT, Invalidate, stale policy, 304 and Delete without propagation headers')
except BaseException as exc:
    (run / 'failure.json').write_text(json.dumps({
        'error': repr(exc), 'responses': responses, 'origin_calls': state['calls'],
        'protocol_calls': state['protocol_calls']},
        indent=2) + '\n')
    raise
finally:
    if process is not None and process.poll() is None:
        process.send_signal(signal.SIGQUIT)
        assert process.wait(10) == 0
    origin.shutdown()
    origin.server_close()
    for server in protocol_servers.values():
        server.shutdown()
        server.server_close()
    if refused is not None:
        refused.close()
    output.close()

errors = (run / 'error.log').read_text()
for pid in expected_manager_exits:
    errors = re.sub(r'^.*\[alert\].*cache manager process ' + pid + r' exited on signal 9\n', '', errors, flags=re.M)
assert '[alert]' not in errors and '[crit]' not in errors and '[emerg]' not in errors, errors
for name, expected in backend_hashes.items():
    assert hashlib.sha256((args.backend / name).read_bytes()).hexdigest() == expected, name
report = {'status': 'passed', 'root': str(root), 'run': str(run),
          'backend': str(args.backend) if args.backend else None,
          'backend_sha256': backend_hashes,
          'executor': 'cache-manager' if args.public_module else 'worker-thread' if args.backend else None,
          'public_module': args.public_module,
          'expr': str(args.expr) if args.expr else None,
          'proxy_filter': str(args.proxy_filter) if args.proxy_filter else None,
          'key_module': str(repo / 'key') if args.public_module else None,
          'slice': args.slice,
          'dynamic': args.dynamic,
          'protocols': list(protocol_servers),
          'nginx_thread_pool': not args.no_thread_pool,
          'sanitize': args.sanitize,
          'cache_io': args.cache_io,
          'extension': args.extension,
          'core_revision': revision, 'core_files_unchanged': len(core_files),
          'checks': checks, 'responses': responses,
          'sources': hashes, 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
          'module_sha256': {name: hashlib.sha256((build / 'objs' / name).read_bytes()).hexdigest()
                            for name in ('ngx_http_cache_key_module.so', 'ngx_http_cache_rocksdb_module.so',
                                         'ngx_http_cache_purge_module.so')}
                           if args.dynamic else {},
          'scope': ('Public PURGE, task status, native operations, shared request rules and the native cache manager; this is not full feature or performance acceptance.'
                    if args.public_module else
                    'Native operations and optional worker-0 owner through a test HTTP adapter; public HTTP integration is outside this run.')}
args.output.write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report, indent=2))
