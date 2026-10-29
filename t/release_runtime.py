#!/usr/bin/env python3
"""Exercise the packaged provider/purge modules with the matching key module."""
import argparse
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import pwd
import grp
import signal
import socket
import subprocess
import sys
import threading
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--nginx', type=Path, required=True)
p.add_argument('--modules', type=Path, required=True)
p.add_argument('--key-module', type=Path, required=True)
p.add_argument('--root', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
root = a.root.resolve()
root.mkdir()
(root / 'logs').mkdir()
(root / 'cache').mkdir()
version = 1
origin_calls = 0


class Origin(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        global origin_calls
        origin_calls += 1
        body = ('entity-' + str(version)).encode()
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'max-age=3600')
        self.end_headers()
        self.wfile.write(body)


origin = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Origin)
threading.Thread(target=origin.serve_forever, daemon=True).start()
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
user = ''
if os.geteuid() == 0:
    user = 'user ' + pwd.getpwuid(0).pw_name + ' ' + grp.getgrgid(os.getegid()).gr_name + ';'
modules = [a.key_module.resolve(), a.modules.resolve() / 'ngx_http_cache_rocksdb_module.so',
           a.modules.resolve() / 'ngx_http_cache_purge_module.so']
config = '\n'.join('load_module ' + str(path) + ';' for path in modules) + f'''
{user}
daemon off; master_process on; worker_processes 1;
pid {root}/pid; error_log {root}/error.log notice;
events {{ worker_connections 128; }}
http {{
    access_log off;
    client_body_temp_path {root}/body;
    proxy_temp_path {root}/temp;
    cache_purge_store {root}/index cache=16m write_buffer=4m;
    cache_purge_state_zone release_control 4m;
    proxy_cache_path {root}/cache levels=1:2 keys_zone=release:1m;
    server {{
        listen 127.0.0.1:{port};
        location / {{
            proxy_cache release;
            proxy_cache_key $cache_key_full;
            add_header X-Test-Cache $upstream_cache_status always;
            proxy_pass http://127.0.0.1:{origin.server_port};
        }}
        location /__cache_purge/ {{
            cache_purge_api; allow 127.0.0.1; deny all;
        }}
    }}
}}
'''
(root / 'nginx.conf').write_text(config)
command = [str(a.nginx.resolve()), '-p', str(root) + '/', '-c', str(root / 'nginx.conf')]
subprocess.run([*command, '-t'], check=True)
checks = ['packaged modules load with the pinned key API and real business selectors']


def request(path, payload=None):
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=10)
    try:
        data = json.dumps(payload).encode() if payload is not None else None
        conn.request('POST' if data is not None else 'GET', path, body=data,
                     headers={'Host': 'cache.test', 'Content-Type': 'application/json'})
        response = conn.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        conn.close()


def until(callback):
    end = time.monotonic() + 10
    while time.monotonic() < end:
        if process.poll() is not None:
            raise AssertionError('nginx exited during package verification')
        try:
            result = callback()
            if result:
                return result
        except ConnectionRefusedError:
            pass
        time.sleep(.02)
    raise AssertionError('package verification timed out')


with (root / 'stdout.log').open('w') as log:
    process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
    try:
        until(lambda: (reply if (reply := request('/__cache_purge/capabilities'))[0] == 200
                       and json.loads(reply[2])['ready'] else None))
        first = request('/asset')
        assert first[0] == 200 and first[2] == b'entity-1', first

        def published():
            for path in (root / 'cache').rglob('*'):
                if path.is_file():
                    try:
                        if os.getxattr(path, 'user.ngx_cache_purge.id').startswith(b'NCF1\0\0\0\2'):
                            return True
                    except OSError:
                        pass
            return False

        until(published)
        hit = until(lambda: (reply if (reply := request('/asset'))[1].get('X-Test-Cache') == 'HIT' else None))
        assert hit[2] == b'entity-1' and origin_calls == 1, (hit, origin_calls)
        checks.append('native fill is indexed and reused as HIT without another origin request')
        version = 2
        payload = {'version': 'ngx-http-cache-purge/v2', 'operation_id': 'release-check',
                   'mode': 'delete', 'selector': {'type': 'url', 'value': 'http://cache.test/asset'},
                   'event': {'issuer': 'release-test', 'stream_id': 'smoke', 'sequence': '1',
                             'issued_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}}
        status, _, data = request('/__cache_purge/operations', payload)
        receipt = json.loads(data)
        assert status == 202 and receipt['effective'] and receipt['durable'], (status, receipt)
        fresh = request('/asset')
        assert fresh[0] == 200 and fresh[2] == b'entity-2', fresh
        checks.append('durable Delete acceptance prevents reuse of the old cached response')
        until(lambda: (state if (state := json.loads(request(receipt['status_url'])[2]))['cleanup']['state'] == 'done' else None))
        checks.append('the accepted operation completes background cleanup')
    except Exception:
        if (root / 'error.log').exists():
            print((root / 'error.log').read_text()[-6000:], file=sys.stderr)
        raise
    finally:
        process.send_signal(signal.SIGQUIT) if process.poll() is None else None
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise
        origin.shutdown()
assert process.returncode == 0
logs = (root / 'error.log').read_text() + (root / 'stdout.log').read_text()
assert not any(marker in logs for marker in ('[alert]', 'exited on signal', 'AddressSanitizer'))
checks.append('packaged runtime shuts down without an abnormal worker exit')
a.output.write_text(json.dumps({'checks': checks,
    'modules': {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in modules}}, indent=2) + '\n')
print('PASS', len(checks), 'packaged module checks', flush=True)
