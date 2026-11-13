#!/usr/bin/env python3
"""Verify independent origin revalidation and purge invalidation policies."""
import argparse
import http.client
import http.server
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import threading
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--binary', type=Path, required=True)
parser.add_argument('--must-revalidate', type=int, choices=(0, 1), required=True)
parser.add_argument('--strict', type=int, choices=(0, 1), required=True)
parser.add_argument('--without-module', action='store_true')
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
if os.geteuid() == 0:
    parser.error('run as an ordinary user')
root = Path(tempfile.mkdtemp(prefix='cache-policy-'))
for name in ('cache', 'store', 'logs'):
    (root / name).mkdir()
policies = {}
checks = []
responses = []
process = None


class Origin(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        policy = policies[self.path]
        etag = '"v' + str(policy['version']) + '"'
        status = 500 if policy['fail'] else 304 if self.headers.get('If-None-Match') == etag else 200
        body = b'failure' if status == 500 else b'' if status == 304 else ('body-v' + str(policy['version'])).encode()
        if 'release' in policy:
            with policy['lock']:
                policy['entered'] += 1
                policy['started'].set()
                if policy['entered'] == 2:
                    policy['second'].set()
            assert policy['release'].wait(10), 'held upstream request was not released'
        self.send_response(status)
        if status != 500:
            control = 'max-age=' + str(policy['ttl'])
            if policy['must']:
                control += ', must-revalidate'
            self.send_header('Cache-Control', control)
            self.send_header('ETag', etag)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        policy['calls'].append(status)

    def log_message(self, *unused):
        pass


origin = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Origin)
threading.Thread(target=origin.serve_forever, daemon=True).start()
with socket.socket() as listener:
    listener.bind(('127.0.0.1', 0))
    port = listener.getsockname()[1]
settings = ''
locations = ''
cache_key = '$uri'
managed = ''
if not args.without_module:
    settings = f'''
    cache_purge_index_store {root}/store;
    cache_purge_state_zone zone=purge:1m;
    cache_key_scheme http;
    cache_key_host policy.test;
    cache_key_uri $uri;
    '''
    locations = '''
        location = /purge {
            proxy_cache_purge on;
            cache_key_uri $http_x_target;
        }
        location /management/ { cache_purge_api; }
    '''
    cache_key = '$cache_key_full'
    managed = 'proxy_cache_purge on;'
(root / 'nginx.conf').write_text(f'''daemon off;
master_process on;
worker_processes 1;
pid {root}/nginx.pid;
error_log {root}/error.log info;
events {{ worker_connections 128; }}
http {{
    access_log off;
    proxy_cache_path {root}/cache keys_zone=test:1m;
    {settings}
    server {{
        listen 127.0.0.1:{port};
        {locations}
        location / {{
            {managed}
            proxy_pass http://127.0.0.1:{origin.server_port};
            proxy_cache test;
            proxy_cache_key {cache_key};
            proxy_cache_revalidate on;
            proxy_cache_background_update on;
            proxy_cache_use_stale error timeout http_500 updating;
            add_header X-Cache $upstream_cache_status always;
        }}
    }}
}}
''')


def request(path, method='GET', headers=None):
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
    connection.request(method, path, headers={'Host': 'policy.test', **(headers or {})})
    response = connection.getresponse()
    result = {'path': path, 'status': response.status, 'cache': response.getheader('X-Cache'),
              'body': response.read().decode()}
    connection.close()
    responses.append(result)
    return result


def wait(check, message, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(.02)
    raise AssertionError(message)


def hit(path):
    def check():
        response = request(path)
        assert response['status'] == 200, response
        return response if response['cache'] == 'HIT' else None
    return wait(check, 'response did not settle to HIT: ' + path)


def create(path, must, ttl):
    policies[path] = {'must': must, 'ttl': ttl, 'fail': False, 'version': 1, 'calls': []}
    response = request(path)
    assert response['status'] == 200 and response['body'] == 'body-v1', response
    hit(path)
    checks.append(path + ': fresh response is reusable')


def failure(path, blocked):
    policy = policies[path]
    policy['fail'] = True
    prior = len(policy['calls'])
    response = request(path)
    expected = 500 if blocked else 200
    assert response['status'] == expected, (path, blocked, response)
    if not blocked:
        assert response['cache'] in ('STALE', 'UPDATING'), response
    wait(lambda: 500 in policy['calls'][prior:], 'upstream failure was not exercised')
    checks.append(path + ': ' + ('blocks stale and background reuse' if blocked else 'retains native stale fallback'))


def invalidate(path, identifier):
    # Native cache dates deliberately have second precision.
    time.sleep(1.05)
    response = request('/purge', 'PURGE', {
        'X-Target': path, 'X-Cache-Purge-Type': 'url', 'X-Cache-Purge-Action': 'invalidate',
        'X-Cache-Purge-Operation-Id': identifier,
        'X-Cache-Purge-Issued-At': str(int(time.time() * 1000)),
    })
    assert response['status'] == 202, response
    def complete():
        response = request('/management/operations/' + identifier)
        assert response['status'] == 200, response
        cleanup = json.loads(response['body'])['cleanup']
        if cleanup['state'] == 'done':
            assert cleanup['affected'] > 0, cleanup
            return True
        return False
    wait(complete, 'purge did not finish')


def recover(path, status):
    policy = policies[path]
    prior = len(policy['calls'])
    policy['fail'] = False
    if status == 200:
        policy['version'] += 1
    response = hit(path)
    assert response['body'] == 'body-v' + str(policy['version']), response
    assert status in policy['calls'][prior:], (path, policy['calls'][prior:])
    checks.append(path + ': ' + str(status) + ' recovery permits subsequent HIT')


def updating():
    path = '/updating'
    create(path, False, 3600)
    invalidate(path, 'policy-updating')
    policy = policies[path]
    policy.update(fail=True, entered=0, lock=threading.Lock(),
                  started=threading.Event(), second=threading.Event(),
                  release=threading.Event())
    results = []
    errors = []
    clients = []
    def fetch():
        try:
            results.append(request(path))
        except Exception as error:
            errors.append(error)
    try:
        clients.append(threading.Thread(target=fetch))
        clients[-1].start()
        assert policy['started'].wait(3), 'first upstream request did not start'
        clients.append(threading.Thread(target=fetch))
        clients[-1].start()
        if args.strict:
            assert policy['second'].wait(3), 'updating request served an invalidated entity'
            assert not results, results
        else:
            for client in clients:
                client.join(3)
            assert len(results) == 2, (results, errors)
            assert {response['cache'] for response in results} == {'STALE', 'UPDATING'}, results
    finally:
        policy['release'].set()
        for client in clients:
            client.join(6)
    assert not errors and len(results) == 2, (results, errors)
    assert all(response['status'] == (500 if args.strict else 200) for response in results), results
    checks.append('concurrent requests respect strict invalidation while the cache node is updating')


result = {'root': str(root), 'status': 'running', 'checks': checks,
          'must_revalidate': bool(args.must_revalidate), 'strict': bool(args.strict),
          'without_module': args.without_module, 'responses': responses}
try:
    with (root / 'runtime.log').open('w') as log:
        process = subprocess.Popen([str(args.binary.resolve()), '-p', str(root), '-c', str(root / 'nginx.conf')], stdout=log, stderr=subprocess.STDOUT)
    def started():
        if process.poll() is not None:
            raise AssertionError((root / 'runtime.log').read_text())
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=.1):
                return True
        except OSError:
            return False
    wait(started, 'nginx did not start')
    if not args.without_module:
        wait(lambda: json.loads(request('/management/capabilities')['body'])['ready'], 'index owner did not become ready')
    create('/ordinary', False, 1)
    create('/origin-policy', True, 1)
    time.sleep(2.1)
    failure('/ordinary', False)
    failure('/origin-policy', bool(args.must_revalidate))
    if not args.without_module:
        create('/invalidate', False, 3600)
        invalidate('/invalidate', 'policy-invalidate')
        failure('/invalidate', bool(args.strict))
        failure('/invalidate', bool(args.strict))
        recover('/invalidate', 304)
        invalidate('/invalidate', 'policy-replace')
        recover('/invalidate', 200)

        updating()

        create('/combined', True, 1)
        invalidate('/combined', 'policy-combined')
        failure('/combined', bool(args.strict or args.must_revalidate))
        recover('/combined', 304)
        time.sleep(2.1)
        failure('/combined', bool(args.must_revalidate))
        recover('/combined', 200)
        time.sleep(2.1)
        failure('/combined', bool(args.must_revalidate))
        checks.append('purge completion does not remove the origin revalidation policy')
    result['status'] = 'passed'
except Exception as error:
    result['status'] = 'failed'
    result['error'] = str(error)
    raise
finally:
    if process is not None and process.poll() is None:
        process.send_signal(signal.SIGQUIT)
        try:
            rc = process.wait(10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(5)
            raise
        assert rc == 0, rc
    origin.shutdown()
    args.output.write_text(json.dumps(result, indent=2) + '\n')
print('PASS cache policy checks:', len(checks), root)
