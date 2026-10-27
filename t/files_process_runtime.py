"""Recover the disk marker + index boundary using independent exec processes."""
import argparse
import fcntl
import hashlib
import json
import os
import select
import socket
import struct
import subprocess
import tempfile
import time
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--binary', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--component', type=Path)
a = p.parse_args()
checks = []
roots = []
for cut in ('prepared', 'rename', 'index', 'marker', 'missing'):
    root = Path(tempfile.mkdtemp(prefix='cache-purge-file-publication-', dir='/tmp'))
    roots.append(str(root))
    mode = '--crash-' + ('index' if cut == 'missing' else cut)
    checks.append(subprocess.check_output([str(a.binary), mode, str(root)], text=True, timeout=30).strip())
    if cut == 'missing':
        (root / 'cache/file').unlink()
    unrelated = root / 'cache/unrelated'
    unrelated.write_bytes(b'untouched')
    for _ in range(2):
        checks.append(subprocess.check_output([str(a.binary), '--recover', str(root)], text=True, timeout=30).strip())
    assert unrelated.read_bytes() == b'untouched'
def automatic(root):
    target = root / 'cache/file'
    offset = int.from_bytes(hashlib.sha256(str(target).encode()).digest()[16:24], 'big') & 0x3fffffffffffffff
    lease = os.open(root / 'db/file.leases', os.O_RDWR | os.O_CLOEXEC)
    fcntl.fcntl(lease, fcntl.F_OFD_SETLK, struct.pack('hhqqi', fcntl.F_WRLCK, os.SEEK_SET, offset, 1, 0) + b'\0' * 4)
    environment = os.environ.copy()
    environment.update(CACHE_PURGE_TEST_CACHE_ROOT=str(root / 'cache'),
                       CACHE_PURGE_TEST_TEMP_ROOT=str(root / 'cache'),
                       CACHE_PURGE_TEST_CACHE_ZONE='cache')
    log = (root / 'component.log').open('w')
    process = subprocess.Popen([str(a.component), str(root / 'db'), '8', '3000', '1100000', '524288'], env=environment,
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log, text=True, bufsize=1)
    def line():
        assert select.select([process.stdout], [], [], 8)[0]
        return process.stdout.readline().strip()
    def revision():
        with socket.socket(socket.AF_UNIX) as client:
            client.settimeout(4); client.connect(str(root / 'db/component.sock'))
            client.sendall(struct.pack('>4sHHQII', b'NCP1', 1, 2, 1, 16, 0) + (42).to_bytes(16, 'big'))
            def read(n):
                data = b''
                while len(data) < n:
                    part = client.recv(n - len(data)); assert part; data += part
                return data
            magic, version, operation, request, size, result = struct.unpack('>4sHHQII', read(24))
            data = read(size)
            assert (magic, version, operation, request, result) == (b'NCP1', 1, 2, 1, 0), data
            return int.from_bytes(data[1:9], 'big')
    try:
        assert line() == 'STARTED'
        deadline = time.monotonic() + 8
        while not (root / 'db/component.sock').exists():
            assert process.poll() is None and time.monotonic() < deadline
            time.sleep(.01)
        time.sleep(.15)
        assert revision() == 3
        os.close(lease); lease = -1
        while revision() != 4:
            assert time.monotonic() < deadline
            time.sleep(.02)
        assert target.read_bytes() == b'new-head' + b'B' * 65536
        process.stdin.write('q\n'); process.stdin.flush()
        assert line() == 'STOP 0 1' and process.wait(timeout=8) == 0
    finally:
        if lease >= 0: os.close(lease)
        if process.poll() is None: process.kill(); process.wait(timeout=5)
        log.close()
    diagnostics = (root / 'component.log').read_text()
    assert not any(word in diagnostics for word in ('AddressSanitizer', 'LeakSanitizer', 'runtime error:')), diagnostics
    checks.append('automatic validation recovery respects a held OFD lease and completes without a finish request')

for cut in ('basic', 'intent', 'prefix', 'torn', 'done', 'missing', 'replaced', 'automatic',
            'changed-prefix', 'changed-body', 'changed-size'):
    root = Path(tempfile.mkdtemp(prefix='cache-purge-validation-', dir='/tmp'))
    roots.append(str(root))
    if cut == 'automatic' and not a.component: continue
    reject = cut.startswith('changed-')
    mode = '--validate-basic' if cut == 'basic' else '--validate-crash-' + ('intent' if reject or cut in ('missing', 'replaced', 'automatic') else cut)
    checks.append(subprocess.check_output([str(a.binary), mode, str(root)], text=True, timeout=30).strip())
    if cut in ('missing', 'replaced'):
        (root / 'cache/file').unlink()
    if cut == 'replaced':
        (root / 'cache/file').write_bytes(b'unrelated replacement')
    if cut == 'automatic':
        automatic(root)
    if reject:
        target = root / 'cache/file'
        contents = bytearray(target.read_bytes())
        if cut == 'changed-size': contents.append(0)
        elif cut == 'changed-body': contents[-1] = ord('C')
        else: contents[0] = ord('X')
        # Force a different mtime even on a filesystem with coarse timestamps.
        stamp = target.stat().st_mtime_ns + 2_000_000_000
        target.write_bytes(contents)
        os.utime(target, ns=(stamp, stamp))
    if cut != 'basic':
        for _ in range(2):
            recover = '--validate-recover-reject' if reject else '--validate-recover'
            checks.append(subprocess.check_output([str(a.binary), recover, str(root)], text=True, timeout=30).strip())
        if reject: assert target.read_bytes() == contents
    if cut == 'replaced':
        assert (root / 'cache/file').read_bytes() == b'unrelated replacement'

for cut in ('basic', 'retired', 'unlinked'):
    root = Path(tempfile.mkdtemp(prefix='cache-purge-removal-', dir='/tmp'))
    roots.append(str(root))
    mode = '--remove-basic' if cut == 'basic' else '--remove-crash-' + cut
    checks.append(subprocess.check_output([str(a.binary), mode, str(root)], text=True, timeout=30).strip())
    unrelated = root / 'cache/a/00/unrelated'; unrelated.write_bytes(b'untouched')
    for _ in range(2):
        checks.append(subprocess.check_output([str(a.binary), '--remove-recover', str(root)], text=True, timeout=30).strip())
    assert unrelated.read_bytes() == b'untouched'

a.output.write_text(json.dumps({'checks': checks, 'directories': roots,
    'scope': 'Real PREPARE/FINISH, xattrs, rename and durable index publication recovery; not nginx integration or power-loss testing'}, indent=2) + '\n')
print('PASS file/index publication crash cuts and repeated recovery', flush=True)
