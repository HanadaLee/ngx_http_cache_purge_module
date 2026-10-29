#!/usr/bin/env python3
"""Check independent identity expressions with arbitrary native cache keys."""
import argparse
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import tempfile
import threading
import time

p = argparse.ArgumentParser()
p.add_argument('--build', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
binary = a.build / 'objs/nginx'
assert 'ngx_http_cache_key_module' not in (a.build / 'objs/ngx_modules.c').read_text()
run = Path(tempfile.mkdtemp(prefix='cache-format-'))
for name in ('cache', 'index', 'logs'):
    (run / name).mkdir()
(run / 'index').chmod(0o700)
checks = []


def encode(fields):
    return b'\x1f'.join(field.replace(b'\\', b'\\\\').replace(b'\x1f', b'\\x1F')
                      for field in fields)


def quote(value):
    return '"' + value.decode().replace('\\', '\\\\').replace('"', '\\"') + '"'


valid = [
    [b'', b'http', b'format.test', b'/plain', b'', b''],
    [b'tenant', b'http', b'format.test', b'/escape', b'a\\b\x1fc', b'bytes=0-9'],
    [b'tenant', b'http', b'format.test', b'/literal', b'\\x1F', b''],
    [b'tenant', b'http', b'format.test', '/utf8/汉'.encode(), b'misc', b''],
]
base = encode(valid[0])
invalid = [
    base[:-1], base + b'\x1f', base + b'\\', base + b'\\q',
    base + b'\\x1f', base + b'\\x1', base + b'\\x',
    encode([b'', *valid[0][1:]]),
    encode([b'bad name', *valid[0][1:]]),
    encode([b'bad,name', *valid[0][1:]]),
    encode([b'n' * 256, *valid[0][1:]]),
    *(encode([*valid[0][:i], b'', *valid[0][i + 1:]]) for i in (1, 2, 3)),
]


class Origin(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = self.path.encode()
        self.send_response(200)
        self.send_header('Cache-Control', 'max-age=3600')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


origin = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Origin)
threading.Thread(target=origin.serve_forever, daemon=True).start()
with socket.socket() as reserved:
    reserved.bind(('127.0.0.1', 0))
    port = reserved.getsockname()[1]
cases = [(fields[0], fields[1] + b'://' + fields[2] + fields[3], fields[4],
          b'opaque/' + str(i).encode()) for i, fields in enumerate(valid)]
cases += [(b'', b'http://format.test/arbitrary/' + str(i).encode(), b'variant', native)
          for i, native in enumerate(invalid)]
locations = []
for i, (namespace, url, misc, native) in enumerate(cases):
    identity = f'cache_purge_key_url {quote(url)};'
    if namespace: identity += f'cache_purge_key_namespace {quote(namespace)};'
    if misc: identity += f'cache_purge_key_misc {quote(misc)};'
    locations.append(f'location = /case-{i} {{ proxy_cache_key {quote(native)}; '
                     f'proxy_cache_purge on; {identity}'
                     f'proxy_pass http://127.0.0.1:{origin.server_port}; }}')
locations.append(f'location /default-off {{ proxy_cache_key $uri; '
                 f'proxy_pass http://127.0.0.1:{origin.server_port}; }}')
loads = ''.join(f'load_module {a.build}/objs/{name}.so;\n' for name in
                ('ngx_http_cache_rocksdb_module', 'ngx_http_cache_purge_module')
                if (a.build / 'objs' / (name + '.so')).exists())
# No key-module variable, header or API is used.
config = loads + f'''daemon off;
master_process on;
worker_processes 1;
pid {run}/nginx.pid;
error_log {run}/error.log info;
events {{ worker_connections 64; }}
http {{
    access_log off;
    proxy_cache_path {run}/cache keys_zone=cache:1m levels=1:2;
    cache_purge_store {run}/index cache=8m write_buffer=2m;
    cache_purge_state_zone control 2m;
    cache_purge_tag topic;
    server {{
        listen 127.0.0.1:{port};
        proxy_cache cache;
        add_header X-Cache-Status $upstream_cache_status always;
        location /api/ {{ cache_purge_api; }}
        {os.linesep.join(locations)}
    }}
}}
'''
(run / 'nginx.conf').write_text(config)
process = None


def request(path, payload=None):
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=8)
    try:
        conn.request('POST' if payload is not None else 'GET', path,
                     json.dumps(payload) if payload is not None else None,
                     {'Host': 'request-host.test', 'Content-Type': 'application/json'})
        reply = conn.getresponse()
        return reply.status, reply.getheader('X-Cache-Status'), reply.read()
    finally:
        conn.close()


def until(fn):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        assert process.poll() is None
        result = fn()
        if result:
            return result
        time.sleep(.01)
    raise AssertionError('condition timeout')


def string(value):
    return struct.pack('>I', len(value)) + value


def rpc(op, payload=b''):
    with socket.socket(socket.AF_UNIX) as conn:
        conn.settimeout(8)
        conn.connect(str(run / 'index/component.sock'))
        conn.sendall(struct.pack('>4sHHQII', b'NCP1', 2, op, 1, len(payload), 0) + payload)

        def read(size):
            value = b''
            while len(value) < size:
                part = conn.recv(size - len(value))
                assert part
                value += part
            return value

        magic, version, operation, ident, size, code = struct.unpack('>4sHHQII', read(24))
        body = read(size)
        assert (magic, version, operation, ident, code) == (b'NCP1', 2, op, 1, 0), body
        return body


class Decode:
    def __init__(self, data):
        self.data = data
        self.at = 0

    def take(self, size):
        assert size <= len(self.data) - self.at
        result = self.data[self.at:self.at + size]
        self.at += size
        return result

    def number(self, size):
        return int.from_bytes(self.take(size), 'big')

    def string(self):
        return self.take(self.number(4))


def record(url):
    d = Decode(rpc(8, b'\1' + string(url) + string(b'') + string(b'') + b'\x20'))
    count, done = d.number(1), d.number(1)
    d.string()
    assert done and count <= 1
    if count == 0:
        return None
    ident = d.take(16)
    d.number(8)
    if d.number(1) != 2:
        return None
    d = Decode(rpc(2, ident))
    d.take(25)
    fields = [d.string() for _ in range(11)]
    tags = [d.string() for _ in range(d.number(1))]
    flags = d.take(2)
    components = [d.string() for _ in range(6)]
    assert d.at == len(d.data)
    return fields, tags, flags, components


try:
    with (run / 'stdout.log').open('w') as log:
        process = subprocess.Popen([str(binary), '-p', str(run) + '/', '-c', str(run / 'nginx.conf')],
                                   stdout=log, stderr=subprocess.STDOUT)
        until(lambda: (run / 'index/component.sock').exists())
        until(lambda: struct.unpack('>IQ', rpc(16))[0] == 6)
        for i, (namespace, url, misc, native) in enumerate(cases):
            path = f'/case-{i}'
            assert request(path) == (200, 'MISS', path.encode())
            stored, tags, flags, components = until(lambda: record(url))
            expected = hashlib.sha256(b'NCI1' + string(namespace) + string(url) + string(misc)).digest()
            assert stored[:4] == [expected.hex().encode(), url, b'format.test', namespace]
            assert stored[10] == misc and flags == bytes(2) and components == [b''] * 6
            assert tags == [b'topic']
            target = Path(os.fsdecode(stored[4])) / os.fsdecode(stored[6])
            until(lambda: target.exists() and os.getxattr(target, 'user.ngx_cache_purge.id')[4:8] == b'\0\0\0\2')
            assert os.getxattr(target, 'user.ngx_cache_purge.id')[120:152] == expected
            assert target.read_bytes().split(b'\nKEY: ', 1)[1].split(b'\n', 1)[0] == native
            assert request(path)[1] == 'HIT'
            checks.append(f'case {i}: independent identity accepts arbitrary native key bytes and preserves the physical key')
        assert request('/default-off')[1] == 'MISS'
        assert request('/default-off')[1] == 'HIT'
        assert record(b'http://format.test/default-off') is None
        checks.append('store/state resources alone leave locations disabled; omitted namespace and misc are empty')
        payload = {'version': 'ngx-http-cache-purge/v2', 'operation_id': 'format-delete',
                   'mode': 'delete', 'selector': {'type': 'url', 'value': 'http://format.test/plain'},
                   'event': {'issuer': 'test', 'stream_id': 'format', 'sequence': '1',
                             'issued_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}}
        status, _, body = request('/api/operations', payload)
        receipt = json.loads(body)
        assert status == 202 and receipt['effective'] and receipt['durable'], receipt
        assert request('/case-0')[1] in ('MISS', 'BYPASS')
        checks.append('URL Delete remains effective with an independent native key producer')
        children = Path(f'/proc/{process.pid}/task/{process.pid}/children').read_text().split()
        assert children
        for pid in children:
            assert 'ngx_http_cache_key_module.so' not in Path(f'/proc/{pid}/maps').read_text()
        checks.append('no worker or cache helper has loaded the key module')
finally:
    if process is not None and process.poll() is None:
        process.send_signal(signal.SIGQUIT)
        process.wait(timeout=15)
    origin.shutdown()
    origin.server_close()
assert process.returncode == 0
logs = (run / 'error.log').read_text() + (run / 'stdout.log').read_text()
assert not any(word in logs for word in ('[alert]', 'AddressSanitizer', 'LeakSanitizer', 'runtime error:', 'exited on signal'))
a.output.write_text(json.dumps({'build': str(a.build), 'run': str(run), 'checks': checks,
                               'key_module_loaded': False,
                               'independent_identity_directives': True,
                               'binaries': {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                            for path in [binary, *(a.build / 'objs').glob('*.so')]}}, indent=2) + '\n')
print('PASS', len(checks), 'independent identity checks')
