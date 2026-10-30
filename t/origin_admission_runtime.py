#!/usr/bin/env python3
"""Inject one writer-registration BUSY without changing the file proof gate."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

p = argparse.ArgumentParser()
p.add_argument('--source', type=Path, default=Path(__file__).resolve().parents[1])
p.add_argument('--build', type=Path, required=True)
p.add_argument('--nginx', type=Path, required=True)
p.add_argument('--key', type=Path, required=True)
p.add_argument('--libraries', type=Path, required=True)
p.add_argument('--expected', choices=('HIT', 'BYPASS'), default='HIT')
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
driver = a.source / 't/capture_runtime.py'
sys.argv = [str(driver), '--nginx', str(a.nginx), '--key', str(a.key),
            '--libraries', str(a.libraries), '--dynamic', '--open-file-cache',
            '--reuse-build', str(a.build), '--output', str(a.output)]
scope = {'__file__': str(driver), '__name__': '__main__'}
code = driver.read_text()
exec(compile(code[:code.index('\nchecks = []\n')], str(driver), 'exec'), scope)
q = scope
run = q['run']
module = q['build'] / 'objs/ngx_http_cache_purge_module.so'
symbols = subprocess.check_output(['nm', '-S', '--defined-only', str(module)], text=True)
ranges = []
for line in symbols.splitlines():
    fields = line.split()
    if len(fields) == 4 and fields[3].split('.')[0] == 'ngx_http_cache_purge_capture_origin':
        ranges.append((int(fields[0], 16), int(fields[1], 16)))
assert ranges, 'writer registration must have a concrete symbol range'
arm, fired = run / 'origin-busy-arm', run / 'origin-busy-fired'
condition = ' || '.join(f'(offset >= {start} && offset < {start + size})'
                        for start, size in ranges)
probe = r'''
#define _GNU_SOURCE
#include "ngx_http_cache_purge_internal.h"
#include <dlfcn.h>
#include <fcntl.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

int
ngx_http_cache_purge_control_watermark(
    ngx_http_cache_purge_control_reader_t *reader,
    ngx_http_cache_purge_watermark_t *watermark, uint64_t *publication,
    ngx_http_cache_rocksdb_error_t *error)
{
    typedef int (*next_t)(ngx_http_cache_purge_control_reader_t *,
        ngx_http_cache_purge_watermark_t *, uint64_t *,
        ngx_http_cache_rocksdb_error_t *);
    next_t     next;
    Dl_info    info;
    uintptr_t  caller, offset;
    int        fd;

    next = (next_t) dlsym(RTLD_NEXT, "ngx_http_cache_purge_control_watermark");
    if (next == NULL) {
        _exit(90);
    }
    caller = (uintptr_t) __builtin_return_address(0);
    if (syscall(SYS_gettid) == getpid() && dladdr((void *) caller, &info)) {
        offset = caller - (uintptr_t) info.dli_fbase;
        if ((@CONDITION@) && unlink(@ARM@) == 0) {
            memset(watermark, 0, sizeof(*watermark));
            *publication = 0;
            memset(error, 0, sizeof(*error));
            error->code = NGX_HTTP_CACHE_ROCKSDB_BUSY;
            strcpy(error->message, "injected origin registration BUSY");
            fd = open(@FIRED@, O_WRONLY | O_CREAT | O_EXCL, 0600);
            if (fd < 0 || write(fd, "1", 1) != 1) {
                _exit(91);
            }
            close(fd);
            return NGX_HTTP_CACHE_ROCKSDB_BUSY;
        }
    }
    return next(reader, watermark, publication, error);
}
'''.replace('@CONDITION@', condition).replace('@ARM@', json.dumps(str(arm))).replace('@FIRED@', json.dumps(str(fired)))
probe_source = run / 'origin_busy.c'
probe_library = run / 'origin_busy.so'
probe_source.write_text(probe)
subprocess.run(['cc', '-shared', '-fPIC', '-O2', '-Wall', '-Wextra', '-Werror',
                '-I' + str(a.source), str(probe_source), '-ldl', '-o', str(probe_library)], check=True)
q['env']['LD_PRELOAD'] = str(probe_library)
process = q['start']()
try:
    q['until'](lambda: (run / 'index/component.sock').exists())
    q['until'](lambda: q['struct'].unpack('>IQ', q['rpc'](16))[0] == 6)
    path = '/origin-admission-race'
    assert q['get'](path) == (200, 'MISS', path.encode())
    q['ready_file'](q['records'](path, 1)[0])
    assert q['get'](path) == (200, 'HIT', path.encode())
    count = q['calls'].count(path)
    arm.write_text('1')
    reply = q['get'](path)
    assert fired.read_text() == '1' and not arm.exists(), 'the exact origin path was intercepted'
    assert reply == (200, a.expected, path.encode()), reply
    assert q['calls'].count(path) == count + (a.expected == 'BYPASS')
    assert q['get'](path) == (200, 'HIT', path.encode())
finally:
    for child, log in q['processes']:
        q['stop'](child)
        log.close()
    for server in [q['origin'], q['dns_server'], *q['gateways'].values()]:
        server.shutdown()
        server.server_close()
assert process.returncode == 0
report = {'expected': a.expected, 'reply': [reply[0], reply[1]],
          'additional_origins': q['calls'].count(path) - count,
          'origin_symbol_ranges': ranges, 'run': str(run),
          'module_sha256': hashlib.sha256(module.read_bytes()).hexdigest(),
          'request_source_sha256': hashlib.sha256((a.source / 'ngx_http_cache_purge_request.c').read_bytes().replace(b'\r\n', b'\n')).hexdigest(),
          'probe_sha256': hashlib.sha256(probe_source.read_bytes()).hexdigest(),
          'scope': 'One injected BUSY at writer registration; actual native HIT and file proof remain active.'}
a.output.write_text(json.dumps(report, indent=2) + '\n')
print('PASS origin registration BUSY:', reply[:2], 'additional origins:', report['additional_origins'])
