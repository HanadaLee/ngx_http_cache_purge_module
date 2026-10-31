#!/usr/bin/env python3
"""Check native Delete/Invalidate without any cache-file metadata writes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile

parser = argparse.ArgumentParser()
parser.add_argument('--nginx', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--sanitize', action='store_true')
parser.add_argument('--backend', type=Path,
                    help='CMake build directory for native background-job checks')
parser.add_argument('--state', action='store_true',
                    help='Check shared queues/rules using native nginx slab and mutex code')
args = parser.parse_args()
repo = Path(__file__).resolve().parent.parent
root = Path(tempfile.mkdtemp(prefix='cache-native-files-'))
binary = root / 'native-files'
command = ['cc', '-std=gnu89', '-Wall', '-Wextra', '-Werror', '-O2', '-g',
           '-Dngx_http_cache_purge_test_main=main', '-ffunction-sections',
           '-fdata-sections', '-Wl,--gc-sections']
if args.sanitize:
    command += ['-fsanitize=address,undefined', '-fno-omit-frame-pointer']
for name in ('objs', 'src/core', 'src/event', 'src/event/modules',
             'src/event/quic', 'src/os/unix', 'src/http', 'src/http/modules',
             'src/http/v2', 'src/http/v3'):
    command += ['-I', str(args.nginx / name)]
compile_command = command + ['-I', str(repo)]
command += ['-I', str(repo), str(repo / 'ngx_http_cache_purge_files.c'),
            str(repo / 't/ngx_http_cache_purge_native_files_test.c'),
            '-o', str(binary)]
subprocess.run(command, check=True)

def run(*arguments):
    result = subprocess.run([str(binary), *map(str, arguments)],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert not result.stderr, result.stderr
    return json.loads(result.stdout)

checks = []
layout = run('layout')
offset, size = layout['offset'], layout['size']
cache = root / 'cache'
inode = run('create', cache)['inode']
original = cache.read_bytes()
assert not os.listxattr(cache)
checks.append('native fixture has no module attributes')

result = run('apply', cache, 1, inode)
assert result['rc'] == 0 and result['inode'] == inode
assert cache.stat().st_ino == inode
assert cache.read_bytes() == original[:offset] + bytes(size) + original[offset + size:]
assert not os.listxattr(cache)
checks.append('Invalidate changes only native valid_sec; preserves inode, date, policy, headers and body')

expired = cache.read_bytes()
assert run('apply', cache, 1, inode)['rc'] == 0
assert cache.read_bytes() == expired
checks.append('repeated Invalidate is idempotent')

old_fd = os.open(cache, os.O_RDONLY)
cache.unlink()
replacement = run('create', cache)['inode']
assert replacement != inode
new_bytes = cache.read_bytes()
for operation in (0, 1):
    assert run('apply', cache, operation, inode)['rc'] == -5
    assert cache.read_bytes() == new_bytes
os.close(old_fd)
checks.append('an already replaced inode is preserved for Delete and Invalidate')

assert run('apply', cache, 0, replacement)['rc'] == 0
assert not cache.exists()
assert run('apply', cache, 0, replacement)['rc'] == -5
assert run('apply', cache, 1, replacement)['rc'] == -5
checks.append('Delete removes the native file; missing files decline')

for label, data in (('short', original[:8]), ('version', bytes(8) + original[8:])):
    path = root / label
    path.write_bytes(data)
    assert run('apply', path, 1, 0)['rc'] == -1
    assert path.read_bytes() == data and not os.listxattr(path)
checks.append('invalid native headers are rejected without writes')

target = root / 'target'
run('create', target)
target_bytes = target.read_bytes()
link = root / 'link'
link.symlink_to(target)
for operation in (0, 1):
    assert run('apply', link, operation, 0)['rc'] == -1
assert link.is_symlink() and target.read_bytes() == target_bytes
checks.append('symlinks are not followed or removed')

fifo = root / 'fifo'
os.mkfifo(fifo)
for path in (root, fifo):
    for operation in (0, 1):
        assert run('apply', path, operation, 0)['rc'] == -1
assert root.is_dir() and fifo.exists()
checks.append('directories and non-regular files are rejected without blocking')

assert run('apply', target, 1, 0)['rc'] == 0
assert not os.listxattr(target)
assert sorted(p.name for p in root.iterdir()) == [
    'fifo', 'link', 'native-files', 'short', 'target', 'version']
checks.append('no metadata or sidecar files are created')

jobs_result = None
if args.backend:
    objects = []
    for source in ('ngx_http_cache_purge_files.c',
                   't/ngx_http_cache_purge_jobs_test.c'):
        obj = root / (Path(source).stem + '.o')
        subprocess.run(compile_command + [
            '-DNGX_HTTP_CACHE_PURGE_NATIVE_TEST=1',
            '-Wdeclaration-after-statement', '-c', str(repo / source),
            '-o', str(obj)], check=True)
        objects.append(str(obj))
    jobs_binary = root / 'native-jobs'
    libraries = shlex.split(subprocess.check_output(
        ['pkg-config', '--libs', '--static', 'rocksdb'], text=True))
    link = ['c++', '-Wl,--gc-sections', *objects,
            str(args.backend / 'libngx_http_cache_purge_index.a'),
            str(args.backend / 'libngx_http_cache_rocksdb.a'),
            *libraries, '-pthread', '-o', str(jobs_binary)]
    if args.sanitize:
        link += ['-fsanitize=address,undefined']
    subprocess.run(link, check=True)
    jobs_result = subprocess.run([str(jobs_binary)], capture_output=True,
                                 text=True, timeout=120)
    assert jobs_result.returncode == 0, (jobs_result.stdout, jobs_result.stderr)
    assert not jobs_result.stderr, jobs_result.stderr
    checks.append('durable jobs execute native file operations through the compact index')

state_result = None
if args.state:
    state_binary = root / 'shared-state'
    state_sources = [repo / 'ngx_http_cache_purge_runtime.c',
                     repo / 't/ngx_http_cache_purge_state_test.c',
                     args.nginx / 'src/core/ngx_slab.c',
                     args.nginx / 'src/core/ngx_shmtx.c',
                     args.nginx / 'src/core/ngx_string.c',
                     args.nginx / 'src/core/ngx_hash.c',
                     args.nginx / 'src/os/unix/ngx_alloc.c']
    state_backend = []
    if args.backend:
        state_backend = ['-DNGX_HTTP_CACHE_PURGE_BACKEND_TEST=1',
                         str(args.backend / 'libngx_http_cache_purge_index.a'),
                         str(args.backend / 'libngx_http_cache_rocksdb.a'),
                         *libraries, '-lstdc++']
    subprocess.run(compile_command + ['-Wdeclaration-after-statement',
                   '-Wno-unused-parameter',
                   *map(str, state_sources), *state_backend, '-pthread',
                   '-o', str(state_binary)], check=True)
    state_result = subprocess.run([str(state_binary)], capture_output=True,
                                  text=True, timeout=120)
    assert state_result.returncode == 0, (state_result.stdout, state_result.stderr)
    assert not state_result.stderr, state_result.stderr
    checks.append('bounded shared queues and rules work across processes')
    if args.backend:
        checks.append('durable task acceptance precedes shared-rule publication and completion')

report = {'status': 'passed', 'checks': checks, 'root': str(root),
          'sanitize': args.sanitize, 'nginx': str(args.nginx), 'layout': layout,
          'jobs': jobs_result.stdout.strip() if jobs_result else None,
          'state': state_result.stdout.strip() if state_result else None,
          'state_backend': bool(args.state and args.backend),
          'scope': 'Native file, task and shared-state primitives; no full HTTP request integration.',
          'sources': {name: hashlib.sha256((repo / name).read_bytes()).hexdigest()
                      for name in ('ngx_http_cache_purge_files.c',
                                   'ngx_http_cache_purge.h',
                                   'ngx_http_cache_purge_component.c',
                                   'ngx_http_cache_purge_internal.h',
                                   'ngx_http_cache_purge_index.c',
                                   'ngx_http_cache_purge_runtime.c',
                                   'rocksdb/ngx_http_cache_rocksdb.cc',
                                   't/ngx_http_cache_purge_jobs_test.c',
                                   't/ngx_http_cache_purge_state_test.c',
                                   't/ngx_http_cache_purge_native_files_test.c',
                                   't/native_files_runtime.py')}}
args.output.write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report, indent=2))
