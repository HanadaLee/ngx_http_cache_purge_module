#!/usr/bin/env python3
"""Exercise one-way propagation between two real nginx cache instances."""
import argparse
import hashlib
import http.client
import http.server
import json
import os
import pwd
import grp
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import threading
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--binary', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--modules', type=Path)
parser.add_argument('--root', type=Path)
parser.add_argument('--framework', action='store_true')
parser.add_argument('--expr', action='store_true')
parser.add_argument('--custom-headers', action='store_true')
parser.add_argument('--tls', action='store_true', help='Verify the L2 TLS certificate and hostname')
parser.add_argument('--streaming', action='store_true', help='Disable request buffering in both proxy tiers')
args = parser.parse_args()
root = args.root.resolve() if args.root else Path(tempfile.mkdtemp(prefix='cache-propagation-'))
if args.root:
    root.mkdir()
os.environ['ASAN_OPTIONS'] = 'detect_leaks=0:abort_on_error=1'
os.environ['UBSAN_OPTIONS'] = 'halt_on_error=1:print_stacktrace=1'
state = {'version': 1, 'calls': []}
processes, checks, responses = [], [], []
time_header = 'X-Test-Purge-Time' if args.custom_headers else 'X-Cache-Purge-Time'
action_header = 'X-Test-Purge-Action' if args.custom_headers else 'X-Cache-Purge-Action'


class Origin(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.headers.get('Transfer-Encoding', '').lower() == 'chunked':
            chunks = []
            while True:
                length = int(self.rfile.readline().split(b';', 1)[0], 16)
                if length == 0:
                    assert self.rfile.readline() == b'\r\n'
                    break
                chunks.append(self.rfile.read(length))
                assert self.rfile.read(2) == b'\r\n'
            payload = b''.join(chunks)
        else:
            payload = self.rfile.read(int(self.headers.get('Content-Length', '0')))
        state['calls'].append({**dict(self.headers),
                               'body_sha256': hashlib.sha256(payload).hexdigest()})
        etag = f'"v{state["version"]}"'
        status = 304 if self.headers.get('If-None-Match') == etag else 200
        body = b'' if status == 304 else f'body-v{state["version"]}'.encode()
        self.send_response(status)
        self.send_header('ETag', etag)
        self.send_header('Cache-Control', 'max-age=3600')
        self.send_header('Cache-Tag', 'known')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *unused):
        pass


origin = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Origin)
threading.Thread(target=origin.serve_forever, daemon=True).start()


def reserve_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


ports = {'l1': reserve_port(), 'l2': reserve_port()}
tls_port = reserve_port() if args.tls else None
if args.tls:
    subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
        '-keyout', str(root / 'server.key'), '-out', str(root / 'server.crt'),
        '-subj', '/CN=cache.test', '-addext', 'subjectAltName=DNS:cache.test',
        '-days', '1'], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
header_config = f'''proxy_set_header {time_header} $cache_purge_propagation_time;
            proxy_set_header {action_header} $cache_purge_propagation_action;'''
if args.framework:
    header_config = ''


def request(tier, path, method='GET', headers=None, body=None, chunked=False):
    connection = http.client.HTTPConnection('127.0.0.1', ports[tier], timeout=10)
    connection.request(method, path, body=body,
                       headers={'Host': 'cache.test', **(headers or {})},
                       encode_chunked=chunked)
    response = connection.getresponse()
    result = {'tier': tier, 'path': path, 'status': response.status,
              'cache': response.getheader('X-Test-Cache'),
              'headers': dict(response.getheaders()), 'body': response.read().decode()}
    connection.close()
    assert not any(name.lower().startswith('x-cache-purge-') for name in result['headers']), result
    assert time_header not in result['headers'] and action_header not in result['headers'], result
    responses.append(result)
    return result


def hit(tier, path):
    deadline = time.monotonic() + 3
    while True:
        result = request(tier, path)
        assert result['status'] == 200, result
        if result['cache'] == 'HIT':
            return result
        assert time.monotonic() < deadline, result
        time.sleep(.01)


def submit(path, action, kind='url'):
    time.sleep(1.05)
    result = request('l1', '/control', 'PURGE', {
        'X-Target': 'http://cache.test' + path,
        'X-Cache-Purge-Action': action, 'X-Cache-Purge-Type': kind,
        'X-Cache-Purge-Tags': 'aaa-absent, known'})
    assert result['status'] == 202, result
    operation = json.loads(result['body'])
    deadline = time.monotonic() + 5
    while True:
        result = request('l1', '/management/operations/' + operation['operation_id'])
        cleanup = json.loads(result['body'])['cleanup']
        if cleanup['state'] == 'done':
            assert cleanup['affected'] >= 1, cleanup
            return operation
        assert time.monotonic() < deadline, cleanup
        time.sleep(.01)


def parent_log(path):
    deadline = time.monotonic() + 1
    while True:
        entries = [json.loads(line) for line in (root / 'l2/access.log').read_text().splitlines()]
        entries = [entry for entry in entries if entry['uri'] == path]
        if entries:
            return entries[-1]
        assert time.monotonic() < deadline
        time.sleep(.01)


try:
    for tier in ('l2', 'l1'):
        run = root / tier
        for name in ('cache', 'store', 'logs'):
            (run / name).mkdir(parents=True, exist_ok=True)
        target = ports['l2'] if tier == 'l1' else origin.server_port
        scheme, tls_server, tls_client = 'http', '', ''
        if args.tls and tier == 'l2':
            tls_server = f'''listen 127.0.0.1:{tls_port} ssl;
        ssl_certificate {root}/server.crt;
        ssl_certificate_key {root}/server.key;'''
        if args.tls and tier == 'l1':
            scheme, target = 'https', tls_port
            tls_client = f'''proxy_ssl_verify on;
            proxy_ssl_trusted_certificate {root}/server.crt;
            proxy_ssl_server_name on;
            proxy_ssl_name cache.test;'''
        propagation = 'on' if tier == 'l1' else 'off'
        conditions = ''
        flags = f'proxy_cache_purge_propagation {propagation};'
        if args.expr:
            conditions = '''expr allow_propagation str_eq $http_x_origin "";
    expr ignore_propagation str_eq $http_x_ignore 1;'''
            flags = ('''when allow_propagation { proxy_cache_purge_propagation on; }
            proxy_cache_purge_propagation off;''' if tier == 'l1' else flags)
            flags += '''
            when ignore_propagation { cache_purge_ignore_propagation on; }
            cache_purge_ignore_propagation off;'''
        loads = ('\n'.join(f'load_module {args.modules.resolve() / name};'
                          for name in ('ngx_http_cache_rocksdb_module.so', 'ngx_http_cache_purge_module.so'))
                 if args.modules else '')
        user = ('user ' + pwd.getpwuid(0).pw_name + ' ' + grp.getgrgid(os.getegid()).gr_name + ';') if os.geteuid() == 0 else ''
        config = f'''{loads}
{user}
daemon off;
master_process on;
worker_processes 1;
pid {run}/nginx.pid;
error_log {run}/error.log notice;
events {{ worker_connections 128; }}
http {{
    log_format trace escape=json '{{"uri":"$uri","time":"$http_{time_header.lower().replace('-', '_')}","action":"$http_{action_header.lower().replace('-', '_')}","tag":"$http_x_cache_purge_tags","cache":"$upstream_cache_status"}}';
    access_log {run}/access.log trace;
    cache_purge_store {run}/store;
    cache_purge_state_zone purge 1m;
    cache_purge_propagation_time_header {time_header};
    cache_purge_propagation_action_header {action_header};
    {'cache_purge_propagation_timeout 3s;' if tier == 'l1' else ''}
    cache_purge_key_url http://cache.test$uri;
    cache_purge_tag $upstream_http_cache_tag;
    proxy_cache_path {run}/cache levels=1:2 keys_zone=cache:1m;
    {f'proxy_cache_path {run}/cleanup levels=1:2 keys_zone=cleanup:1m inactive=1s;' if tier == 'l1' else ''}
    {'map $uri $test_cache_zone { default cache; ~^/cleanup/ cleanup; }' if tier == 'l1' else ''}
    {conditions}
    server {{
        listen 127.0.0.1:{ports[tier]};
        {tls_server}
        location /management/ {{ cache_purge_api; }}
        location = /control {{
            proxy_cache_purge on;
            cache_purge_key_url $http_x_target;
        }}
        location / {{
            proxy_cache_purge on;
            {flags}
            {header_config}
            proxy_cache {'$test_cache_zone' if tier == 'l1' else 'cache'};
            proxy_cache_key $uri;
            proxy_cache_bypass $http_x_bypass;
            proxy_cache_revalidate on;
            proxy_hide_header X-Test-Cache;
            proxy_http_version 1.1;
            {'proxy_request_buffering off;' if args.streaming else ''}
            {tls_client}
            proxy_pass {scheme}://127.0.0.1:{target};
            add_header X-Test-Cache $upstream_cache_status always;
        }}
    }}
}}
'''
        (run / 'nginx.conf').write_text(config)
        with (run / 'stdout.log').open('w') as log:
            process = subprocess.Popen([str(args.binary), '-p', str(run), '-c', str(run / 'nginx.conf')],
                                       stdout=log, stderr=subprocess.STDOUT)
        processes.append(process)
        deadline = time.monotonic() + 10
        while True:
            assert process.poll() is None, (run / 'stdout.log').read_text()
            try:
                result = request(tier, '/management/capabilities')
                capability = json.loads(result['body'])
                if capability['ready']:
                    assert capability['propagation_timeout_ms'] == (3000 if tier == 'l1' else 120000)
                    break
            except OSError:
                pass
            assert time.monotonic() < deadline
            time.sleep(.02)

    path = '/invalidate'
    hit('l2', path)
    hit('l1', path)
    operation = submit(path, 'invalidate')
    previous = len(state['calls'])
    result = request('l1', path)
    assert result['body'] == 'body-v1' and result['cache'] == 'REVALIDATED', result
    assert len(state['calls']) == previous + 1 and state['calls'][-1]['If-None-Match'] == '"v1"'
    seen = parent_log(path)
    assert seen['time'] == str(operation['effective_at']) and seen['action'] == 'invalidate', seen
    assert seen['tag'] == '', seen
    previous = len(state['calls'])
    assert request('l2', path, headers={time_header: seen['time'],
                                     action_header: 'invalidate'})['cache'] == 'HIT'
    assert len(state['calls']) == previous
    checks.append('L1 Invalidate makes L2 revalidate; 304 is reused and the same timestamp then permits HIT')

    path = '/delete'
    hit('l2', path)
    hit('l1', path)
    operation = submit(path, 'delete')
    state['version'] = 2
    result = request('l1', path)
    assert result['body'] == 'body-v2' and result['cache'] == 'MISS', result
    seen = parent_log(path)
    assert seen['time'] == str(operation['effective_at']) and seen['action'] == 'delete', seen
    assert 'If-None-Match' not in state['calls'][-1], state['calls'][-1]
    checks.append('L1 Delete makes an unpurged L2 replace its old cache without conditional reuse')

    time.sleep(3.1)
    assert request('l1', path, headers={'X-Bypass': '1'})['status'] == 200
    seen = parent_log(path)
    assert seen['time'] == '' and seen['action'] == '', seen
    checks.append('outgoing timeout stops propagation; parent has no sender-window requirement')

    path = '/tag-miss'
    hit('l2', path)
    hit('l1', path)
    operation = submit(path, 'delete', 'tag')
    state['version'] = 3
    payload = b'body must survive the asynchronous lookup' * 1024
    result = request('l1', path, body=payload)
    assert result['body'] == 'body-v3' and result['cache'] == 'MISS', result
    assert state['calls'][-1]['body_sha256'] == hashlib.sha256(payload).hexdigest()
    seen = parent_log(path)
    assert seen['time'] == str(operation['effective_at']) and seen['action'] == 'delete', seen
    assert seen['tag'] == '', seen
    checks.append('a batched TAG selector uses a known association after deletion to propagate on MISS without sending TAG names')

    result = request('l1', '/unknown-body', body=payload)
    assert result['body'] == 'body-v3', result
    assert state['calls'][-1]['body_sha256'] == hashlib.sha256(payload).hexdigest()
    seen = parent_log('/unknown-body')
    assert seen['time'] == '' and seen['action'] == '', seen
    checks.append('known and unknown TAG MISS preserve a Content-Length request body while lookup is pending')

    if args.streaming:
        def chunks():
            for offset in range(0, len(payload), 2048):
                yield payload[offset:offset + 2048]
                time.sleep(.005)

        result = request('l1', '/chunked-body', body=chunks(), chunked=True)
        assert result['body'] == 'body-v3', result
        assert state['calls'][-1]['body_sha256'] == hashlib.sha256(payload).hexdigest()
        assert state['calls'][-1]['Transfer-Encoding'] == 'chunked'
        checks.append('unbuffered chunked request body survives asynchronous lookup and both proxy tiers')

    for number in range(32):
        with socket.create_connection(('127.0.0.1', ports['l1']), timeout=2) as connection:
            connection.sendall(f'GET /cancel-{number} HTTP/1.1\r\nHost: cache.test\r\n\r\n'.encode())
    assert request('l1', '/management/capabilities')['status'] == 200
    checks.append('client disconnects during scoped TAG MISS requests leave both nginx instances healthy')

    if args.expr:
        path = '/ignore'
        hit('l2', path)
        time.sleep(1.05)
        previous = len(state['calls'])
        result = request('l2', path, headers={'X-Ignore': '1',
            time_header: str(int(time.time() * 1000)), action_header: 'delete'})
        assert result['cache'] == 'HIT' and len(state['calls']) == previous, result
        hit('l1', path)
        submit(path, 'delete')
        request('l1', path, headers={'X-Origin': '1'})
        seen = parent_log(path)
        assert seen['time'] == '' and seen['action'] == '', seen
        checks.append('expr when independently disables incoming trust and outgoing propagation')
    before = json.loads(request('l1', '/management/capabilities')['body'])['index_cleanup']
    path = '/cleanup/entry'
    hit('l2', path)
    hit('l1', path)
    digest = hashlib.md5(path.encode()).hexdigest()
    cached = list((root / 'l1/cleanup').rglob(digest))
    assert len(cached) == 1
    deadline = time.monotonic() + 15
    while True:
        cleanup = json.loads(request('l1', '/management/capabilities')['body'])['index_cleanup']
        if not cached[0].exists() and cleanup['retained'] > before['retained']:
            break
        assert time.monotonic() < deadline, cleanup
        time.sleep(.02)
    assert cleanup['errors'] == before['errors'], cleanup
    operation = submit(path, 'delete', 'tag')
    state['version'] += 1
    result = request('l1', path)
    assert result['cache'] == 'MISS' and result['body'] == f'body-v{state["version"]}', result
    seen = parent_log(path)
    assert seen['time'] == str(operation['effective_at']) and seen['action'] == 'delete', seen
    assert seen['tag'] == '', seen
    checks.append('native inactive eviction is swept in the cache manager while its known TAG association still propagates the next purge')

    if os.geteuid() != 0:
        path = '/permission'
        hit('l1', path)
        digest = hashlib.md5(path.encode()).hexdigest()
        cached = list((root / 'l1/cache').rglob(digest))
        assert len(cached) == 1
        directory = cached[0].parent
        permissions = directory.stat().st_mode
        before = json.loads(request('l1', '/management/capabilities')['body'])['index_cleanup']
        try:
            directory.chmod(0)
            deadline = time.monotonic() + 15
            while True:
                cleanup = json.loads(request('l1', '/management/capabilities')['body'])['index_cleanup']
                if cleanup['errors'] > before['errors']:
                    break
                assert time.monotonic() < deadline, cleanup
                time.sleep(.05)
        finally:
            directory.chmod(permissions)
        assert cached[0].exists()
        assert request('l1', path)['cache'] == 'HIT'
        submit(path, 'delete')
        assert not cached[0].exists()
        checks.append('filesystem permission errors preserve the cache file and its index entry for a later explicit purge')

    assert all(time_header not in call and action_header not in call for call in state['calls']), state['calls']
    checks.append('parent with propagation off sends no control headers to origin; business responses contain no purge receipt')
    if args.tls:
        checks.append('all parent requests use TLS with certificate and hostname verification enabled')
finally:
    for process in reversed(processes):
        if process.poll() is None:
            process.send_signal(signal.SIGQUIT)
        assert process.wait(10) == 0
    origin.shutdown()
    origin.server_close()
    (root / 'responses.json').write_text(json.dumps(responses, indent=2) + '\n')

for tier in ports:
    errors = (root / tier / 'error.log').read_text()
    assert not any(marker in errors for marker in ('[alert]', '[crit]', '[emerg]', 'AddressSanitizer', 'runtime error:')), errors
args.output.write_text(json.dumps({'status': 'passed', 'root': str(root),
    'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
    'modules': {name: hashlib.sha256((args.modules / name).read_bytes()).hexdigest()
                for name in ('ngx_http_cache_rocksdb_module.so', 'ngx_http_cache_purge_module.so')}
               if args.modules else {},
    'framework': args.framework, 'expr': args.expr, 'tls': args.tls, 'streaming': args.streaming,
    'permission_check': os.geteuid() != 0,
    'time_header': time_header, 'action_header': action_header, 'checks': checks,
    'responses': responses, 'origin_calls': state['calls']}, indent=2) + '\n')
print(json.dumps({'root': str(root), 'checks': checks}))
