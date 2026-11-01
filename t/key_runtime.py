#!/usr/bin/env python3
"""Exercise cache-key variables in the combined module build."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--binary', type=Path, required=True)
parser.add_argument('--modules', type=Path)
parser.add_argument('--expr', action='store_true')
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
repo = Path(__file__).resolve().parents[1]
root = Path(tempfile.mkdtemp(prefix='cache-key-runtime-'))
run = root / 'run'
run.mkdir()
(run / 'logs').mkdir()
os.environ['ASAN_OPTIONS'] = 'detect_leaks=0:abort_on_error=1'
os.environ['UBSAN_OPTIONS'] = 'halt_on_error=1:print_stacktrace=1'

with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
load = (''.join(f'load_module {args.modules}/{name}.so;\n' for name in (
    'ngx_http_cache_key_module', 'ngx_http_cache_rocksdb_module',
    'ngx_http_cache_purge_module')) if args.modules else '')
prefix = load + f'''daemon off;
master_process on;
worker_processes 2;
pid {run}/pid;
error_log {run}/error.log notice;
events {{ worker_connections 128; }}
'''
http = '''map $arg_loop $loop_value { default $cache_key_misc; }
map $arg_valid $invalid_namespace { default "bad,comma"; }
'''
locations = '''
location /default { return 200 "$cache_key_namespace|$cache_key_scheme|$cache_key_host|$cache_key_uri|$cache_key_misc|$cache_key_range"; }
location /full { return 200 "$cache_key_full"; }
location /base { return 200 "$cache_key_base"; }
location /custom { cache_key_namespace scope; cache_key_scheme http; cache_key_host shared.test; cache_key_uri /asset?b=2&a=1; cache_key_misc ""; cache_key_range "bytes=0-99"; return 200 "$cache_key_full"; }
location /ns { cache_key_namespace $arg_ns; return 200 "$cache_key_base"; }
location /empty { cache_key_namespace ""; return 200 "$cache_key_base"; }
location /cycle { cache_key_misc $loop_value; return 200 "[$cache_key_full]"; }
location /unused { cache_key_namespace $invalid_namespace; return 200 "$cache_key_uri"; }
location /rewrite { cache_key_uri $uri; set $old_uri $cache_key_uri; rewrite ^ /final last; }
location /final { cache_key_uri $uri; return 200 "$old_uri|$cache_key_uri"; }
location /changed { set $source_uri /before; cache_key_uri $source_uri; set $old_uri $cache_key_uri; set $source_uri /after; return 200 "$old_uri|$cache_key_uri"; }
location /escape { cache_key_misc "a\\\\b\x1f"; return 200 "$cache_key_full"; }
'''
if args.expr:
    http += '''expr shared str_eq $arg_shared 1;
when shared { cache_key_namespace parent; cache_key_scheme https; cache_key_host parent.test; cache_key_uri /parent; cache_key_misc parent; cache_key_range parent; }
'''
    locations += '''
location /when { when shared { cache_key_namespace child; cache_key_scheme ftp; cache_key_host child.test; cache_key_uri /child; cache_key_misc ""; cache_key_range child; } return 200 "$cache_key_full"; }
location /inherit { when shared { cache_key_misc child; } return 200 "$cache_key_full"; }
location /order { cache_key_misc first; when shared { cache_key_misc ignored; } return 200 "$cache_key_misc"; }
'''
conf = run / 'nginx.conf'
checks = []


def check_config(allowed=True, extra='', body=locations):
    conf.write_text(prefix + 'http { access_log off; ' + http
                    + f' server {{ listen 127.0.0.1:{port}; server_name example.test; '
                    + extra + body + ' }}')
    result = subprocess.run([str(args.binary), '-t', '-p', str(run) + '/', '-c', str(conf)],
                            capture_output=True, text=True)
    assert (result.returncode == 0) == allowed, (extra, result.stderr)


for extra in ['cache_key_misc one; cache_key_misc two;',
              'cache_key_namespace "bad space";', 'cache_key_namespace "bad,comma";',
              'cache_key_uri $cache_key_uri;', 'cache_key_uri ${cache_key_base};',
              'cache_key_host $cache_key_url;']:
    check_config(False, extra)
    checks.append('reject ' + extra)
check_config(False, body='location / { set $cache_key_misc bad; return 200 ok; }')
checks.append('read-only outputs')
if args.expr:
    check_config(False, body='location / { when shared { cache_key_misc a; cache_key_misc b; } }')
    checks.append('duplicate conditional field')
check_config()
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def get(path):
    request = urllib.request.Request(f'http://127.0.0.1:{port}' + path,
                                     headers={'Host': 'example.test'})
    with opener.open(request, timeout=4) as response:
        return response.status, response.read()


def equal(path, expected):
    actual = get(path)
    assert actual == (200, expected), (path, actual, expected)
    checks.append(path)


def key(*parts):
    return b'\x1f'.join(part.replace(b'\\', b'\\\\').replace(b'\x1f', b'\\x1F') for part in parts)


with (root / 'stderr.log').open('w') as error:
    proc = subprocess.Popen([str(args.binary), '-p', str(run) + '/', '-c', str(conf)], stderr=error)
    try:
        for _ in range(100):
            try:
                get('/default')
                break
            except OSError:
                time.sleep(.05)
        equal('/default?b=2&a=1', b'|http|example.test|/default?b=2&a=1||')
        equal('/full?x=1', key(b'', b'http', b'example.test', b'/full?x=1', b'', b''))
        equal('/base', key(b'', b'http', b'example.test', b'/base', b''))
        equal('/empty', key(b'', b'http', b'example.test', b'/empty', b''))
        equal('/custom', key(b'scope', b'http', b'shared.test', b'/asset?b=2&a=1', b'', b'bytes=0-99'))
        equal('/escape', key(b'', b'http', b'example.test', b'/escape', b'a\\b\x1f', b''))
        equal('/rewrite', b'/rewrite|/final')
        equal('/changed', b'/before|/after')
        equal('/unused', b'/unused')
        equal('/ns', key(b'', b'http', b'example.test', b'/ns', b''))
        equal('/ns?ns=valid', key(b'valid', b'http', b'example.test', b'/ns?ns=valid', b''))
        # nginx's ordinary variable semantics: errors may become empty values.
        equal('/ns?ns=bad,comma', b'')
        equal('/cycle', b'[]')
        log = (run / 'error.log').read_text()
        assert 'cache key namespace must contain' in log
        assert 'cache key evaluation cycle' in log
        if args.expr:
            equal('/when?shared=1', key(b'child', b'ftp', b'child.test', b'/child', b'', b'child'))
            equal('/when', key(b'', b'http', b'example.test', b'/when', b'', b''))
            equal('/inherit?shared=1', key(b'parent', b'https', b'parent.test', b'/parent', b'child', b'parent'))
            equal('/order?shared=1', b'first')
        old = Path(f'/proc/{proc.pid}/task/{proc.pid}/children').read_text().split()
        os.kill(proc.pid, signal.SIGHUP)
        for _ in range(100):
            current = Path(f'/proc/{proc.pid}/task/{proc.pid}/children').read_text().split()
            if len(current) == 2 and not set(old) & set(current):
                break
            time.sleep(.05)
        assert len(current) == 2 and not set(old) & set(current)
        equal('/base', key(b'', b'http', b'example.test', b'/base', b''))
        checks.append('reload replaces both workers')
    finally:
        proc.send_signal(signal.SIGQUIT)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
assert proc.returncode == 0, proc.returncode
log = (run / 'error.log').read_text() + (root / 'stderr.log').read_text()
assert not any(marker in log for marker in ('exited on signal', '[alert]', 'AddressSanitizer', 'runtime error:')), log
result = {'status': 'passed', 'root': str(root), 'expr': args.expr,
          'checks': checks,
          'source_sha256': hashlib.sha256((repo / 'key/ngx_http_cache_key_module.c').read_bytes()).hexdigest()}
if args.modules:
    name = 'ngx_http_cache_key_module.so'
    result['modules'] = {name: hashlib.sha256((args.modules / name).read_bytes()).hexdigest()}
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps(result, indent=2) + '\n')
print('PASS', len(checks), 'cache key checks', flush=True)
