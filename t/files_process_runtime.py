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

for cut in ('basic', 'intent', 'prefix', 'torn', 'done', 'automatic'):
    if cut == 'automatic' and not a.component: continue
    root = Path(tempfile.mkdtemp(prefix='cache-purge-conditional-', dir='/tmp'))
    roots.append(str(root))
    mode = '--validate-origin-basic' if cut == 'basic' else '--validate-origin-crash-' + ('intent' if cut == 'automatic' else cut)
    checks.append(subprocess.check_output([str(a.binary), mode, str(root)], text=True, timeout=30).strip())
    if cut == 'automatic': automatic(root)
    if cut != 'basic':
        for _ in range(2):
            checks.append(subprocess.check_output([str(a.binary), '--validate-origin-recover', str(root)], text=True, timeout=30).strip())

for cut in ('basic', 'retired', 'unlinked'):
    root = Path(tempfile.mkdtemp(prefix='cache-purge-removal-', dir='/tmp'))
    roots.append(str(root))
    mode = '--remove-basic' if cut == 'basic' else '--remove-crash-' + cut
    checks.append(subprocess.check_output([str(a.binary), mode, str(root)], text=True, timeout=30).strip())
    unrelated = root / 'cache/a/00/unrelated'; unrelated.write_bytes(b'untouched')
    for _ in range(2):
        checks.append(subprocess.check_output([str(a.binary), '--remove-recover', str(root)], text=True, timeout=30).strip())
    assert unrelated.read_bytes() == b'untouched'

def background_delete(cut):
    root = Path(tempfile.mkdtemp(prefix='cache-purge-background-', dir='/tmp'))
    roots.append(str(root))
    validation_case = cut.startswith('validation-')
    checks.append(subprocess.check_output(
        [str(a.binary), '--validate-race-intent' if validation_case else
         '--background-seed-invalidate' if cut == 'invalidate' else
         '--background-seed-many' if cut == 'native-many' else '--background-seed', str(root)], text=True, timeout=30).strip())
    first = root / ('cache/file' if validation_case else 'cache/a/first')
    offset = int.from_bytes(hashlib.sha256(str(first).encode()).digest()[16:24], 'big') & 0x3fffffffffffffff
    lease = os.open(root / 'db/file.leases', os.O_RDWR | os.O_CLOEXEC)
    fcntl.fcntl(lease, fcntl.F_OFD_SETLK,
                struct.pack('hhqqi', fcntl.F_WRLCK, os.SEEK_SET, offset, 1, 0) + b'\0' * 4)
    environment = os.environ.copy()
    environment.update(CACHE_PURGE_TEST_CACHE_ROOT=str(root / 'cache'),
                       CACHE_PURGE_TEST_TEMP_ROOT=str(root / 'cache'),
                       CACHE_PURGE_TEST_CACHE_ZONE='cache',
                       CACHE_PURGE_TEST_SECOND_ROOT=str(root / 'cache-b'))
    if cut == 'invalidate':
        environment.update(CACHE_PURGE_TEST_HOLD_ORIGIN='1',
                           CACHE_PURGE_TEST_MATCH_URL='https://example.test/work/first')
    if cut.startswith('native'):
        environment['CACHE_PURGE_TEST_NATIVE_ROOT'] = '1'
    if validation_case:
        environment.pop('CACHE_PURGE_TEST_SECOND_ROOT')
    log = (root / 'component.log').open('w')
    process = None

    def until(test):
        deadline = time.monotonic() + 12
        while True:
            result = test()
            if result: return result
            assert process.poll() is None and time.monotonic() < deadline
            time.sleep(.02)

    def start():
        nonlocal process
        process = subprocess.Popen(
            [str(a.component), str(root / 'db'), '8', '3000', '1100000', '524288', str(root / 'controls')],
            env=environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log, text=True, bufsize=1)
        assert select.select([process.stdout], [], [], 8)[0]
        assert process.stdout.readline().strip() == 'STARTED'
        until(lambda: (root / 'db/component.sock').exists())
        # A stale pathname can survive the killed owner until its successor binds.
        deadline = time.monotonic() + 8
        while True:
            try:
                if struct.unpack('>IQ', rpc(16))[0] == 6: break
            except (ConnectionRefusedError, FileNotFoundError): pass
            assert process.poll() is None and time.monotonic() < deadline
            time.sleep(.02)

    def rpc(opcode, payload=b'', fds=(), code=0):
        with socket.socket(socket.AF_UNIX) as client:
            client.settimeout(4); client.connect(str(root / 'db/component.sock'))
            packet = struct.pack('>4sHHQII', b'NCP1', 1, opcode, 1, len(payload), 0) + payload
            if fds:
                assert client.sendmsg([packet[:1]], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                      struct.pack(f'{len(fds)}i', *fds))]) == 1
                client.sendall(packet[1:])
            else:
                client.sendall(packet)
            def read(n):
                data = b''
                while len(data) < n:
                    part = client.recv(n - len(data)); assert part; data += part
                return data
            magic, version, echoed, request, size, result = struct.unpack('>4sHHQII', read(24))
            data = read(size)
            assert (magic, version, echoed, request, result) == (b'NCP1', 1, opcode, 1, code), data
            return data

    def operation(name='background'):
        raw = rpc(15, struct.pack('>I', len(name)) + name.encode())
        at = 0
        def number(width):
            nonlocal at
            value = int.from_bytes(raw[at:at + width], 'big'); at += width; return value
        def string():
            nonlocal at
            length = number(4); value = raw[at:at + length]; at += length; return value
        assert string() == name.encode()
        number(1); number(8); number(1); string(); string(); string()
        for _ in range(5): number(8)
        stage = number(1); number(4); number(1)
        bookmark = string(); done = number(1); number(8)
        visits, pending, failed, checkpoint = [number(8) for _ in range(4)]
        message = string(); assert at == len(raw)
        return dict(stage=stage, bookmark=bookmark, done=done, visits=visits,
                    pending=pending, failed=failed, checkpoint=checkpoint, message=message)

    def stop(kill=False):
        if kill:
            process.kill(); process.wait(timeout=8)
        else:
            process.stdin.write('q\n'); process.stdin.flush()
            assert select.select([process.stdout], [], [], 8)[0]
            assert process.stdout.readline().strip() == 'STOP 0 1'
            assert process.wait(timeout=8) == 0

    try:
        start()
        if validation_case:
            def string(value):
                value = value.encode()
                return struct.pack('>I', len(value)) + value
            def tagged(tag):
                return rpc(8, b'\x05' + string(tag) + string('tenant') + string('') + b'\x20')[0]
            assert tagged('red') == 1 and tagged('blue') == tagged('green') == 0
            raw = rpc(2, (42).to_bytes(16, 'big'))
            assert raw[:9] == b'\x02' + struct.pack('>Q', 3) and b'NFV2' in raw
            old = first.open('rb')
            original = old.read()
            kind = cut.split('-')[1]
            tag = {'old': 'red', 'new': 'blue', 'unrelated': 'other'}[kind]
            mode = 1 if cut.endswith('invalidate') else 2
            rpc(14, string('race') + bytes([mode]) + struct.pack('>Q', 1) + b'\x05'
                + string(tag) + string('tenant') + string(''))
            until(lambda: struct.unpack('>IQ', rpc(16))[0] == 6)
            assert first.read_bytes() == original and tagged('red') == 1
            # The intent and accepted rule both survive loss of the executor.
            stop(kill=True)
            start()
            assert tagged('red') == 1 and tagged('blue') == 0
            os.close(lease); lease = -1
            if kind == 'unrelated':
                until(lambda: int.from_bytes(rpc(2, (42).to_bytes(16, 'big'))[1:9], 'big') == 4)
                assert tagged('red') == 0 and tagged('blue') == tagged('green') == 1
                assert first.read_bytes() == b'new-head' + b'B' * 65536
            else:
                until(lambda: not first.exists())
                rpc(2, (42).to_bytes(16, 'big'), code=1)
                old.seek(0)
                assert old.read() == original
                assert tagged('red') == tagged('blue') == tagged('green') == 0
            old.close()
            stop()
        elif cut.startswith('native'):
            def native():
                process.stdin.write('n\n'); process.stdin.flush()
                assert select.select([process.stdout], [], [], 8)[0]
                reply = process.stdout.readline().split()
                assert reply[0] == 'NATIVE'
                if reply[1] == '2': return None
                assert reply[1] == '0', reply
                return reply
            def ack(command, expected='ACK 0'):
                process.stdin.write(command + '\n'); process.stdin.flush()
                assert select.select([process.stdout], [], [], 8)[0]
                assert process.stdout.readline().strip() == expected
            request = until(native)
            assert operation('independent')['pending'] == 1
            raw = rpc(2, (6).to_bytes(16, 'big'))
            rpc(7, b'\x01' + (6).to_bytes(16, 'big') + raw[1:9], code=4)
            assert not (root / 'cache-b/a/other').exists()
            assert len(rpc(1)) == 72
            ack('z', 'ACK 7')
            assert until(native)[2] == request[2]
            probe = os.open(root / 'db/file.leases', os.O_RDWR | os.O_CLOEXEC)
            target = root / 'cache-b' / request[5]
            offset = int.from_bytes(hashlib.sha256(str(target).encode()).digest()[16:24], 'big') & 0x3fffffffffffffff
            lock = struct.pack('hhqqi', fcntl.F_WRLCK, os.SEEK_SET, offset, 1, 0) + b'\0' * 4
            def acquired():
                try:
                    fcntl.fcntl(probe, fcntl.F_OFD_SETLK, lock)
                    return True
                except BlockingIOError:
                    return False
            assert not acquired()
            ack('h', 'REQUEST_STOP 0')
            until(acquired)
            assert process.poll() is None
            os.close(probe)
            stop()
            start()
            request = until(native)
            stop(kill=True)
            os.close(lease); lease = -1
            start()
            request = until(native)
            ack('x')
            # A busy consumer releases its file lease and retries; the exact
            # retired instance remains in the durable task checkpoint.
            completed = 0
            seen = {}
            restarted_collection = False
            task_streak = 0
            expected_paths = {'a/first': 1, 'a/second': 2, 'a/other': 6}
            if cut == 'native-many':
                expected_paths.update({f'a/bulk-{n}': n for n in range(10, 50)})
            while True:
                request = until(lambda: native() or
                                ('done' if operation()['stage'] == operation('independent')['stage'] == 4
                                 and rpc(10, b'\0' * 4 + b'\x01')[0] == 0 else None))
                if request == 'done': break
                assert int(request[3]) > 0 and request[5] in expected_paths
                number = expected_paths[request[5]]
                raw = rpc(2, number.to_bytes(16, 'big'))
                rpc(7, b'\x01' + number.to_bytes(16, 'big') + raw[1:9], code=4)
                if seen.get(request[5], 0) == 1 and not restarted_collection:
                    if cut == 'native-many':
                        assert len(seen) <= 8, ('collection starved behind task removals', len(seen))
                    # The task checkpoint has released this ID; the retired
                    # queue alone must recover an unacknowledged GC removal.
                    assert raw[0] == 3
                    stop(kill=True)
                    start()
                    assert rpc(2, number.to_bytes(16, 'big'))[0] == 3
                    restarted_collection = True
                    continue
                task_streak = 0 if seen.get(request[5], 0) else task_streak + 1
                if cut == 'native-many':
                    assert task_streak <= 8, ('collection starved behind task removals', task_streak, seen)
                seen[request[5]] = seen.get(request[5], 0) + 1
                ack('y')
                completed += 1
                assert completed <= len(expected_paths) * 2
            assert completed == len(expected_paths) * 2 and set(seen.values()) == {2} and restarted_collection, seen
            assert not first.exists() and not (root / 'cache-b/a/second').exists()
            assert (root / 'cache/a/fresh').exists()
            stop()
        elif cut == 'invalidate':
            until(lambda: operation()['done'] and operation('independent')['done'])
            assert operation()['stage'] == operation('independent')['stage'] == 3
            assert int.from_bytes(rpc(23, struct.pack('>Q', 1)), 'big') == 1
            checkpoint = operation()['checkpoint']
            time.sleep(1.1)
            assert operation()['stage'] == 3 and operation()['checkpoint'] == checkpoint

            def string(value):
                if isinstance(value, str): value = value.encode()
                return struct.pack('>I', len(value)) + value

            fields = ['pending-key', 'https://other.test/pending', 'other.test', 'tenant',
                      str(root / 'cache'), 'cache', 'a/pending', '', '']
            record = ((99).to_bytes(16, 'big') + b''.join(map(string, fields))
                      + string('unknown-origin') + b'\0\0\0' + string('') * 6 + b'\0' * 16)
            rpc(3, b'\x01' + record)
            process.stdin.write('e\n'); process.stdin.flush()
            assert select.select([process.stdout], [], [], 8)[0]
            assert process.stdout.readline().split()[:2] == ['ORIGIN', '0']
            assert int.from_bytes(rpc(23, struct.pack('>Q', 1)), 'big') == 0
            time.sleep(1.1)
            assert operation()['stage'] == 3 and operation()['checkpoint'] == checkpoint
            stop(kill=True)
            environment.pop('CACHE_PURGE_TEST_HOLD_ORIGIN')
            start()
            time.sleep(1.1)
            assert operation()['stage'] == 3 and rpc(9, string('') + b'\x01')[0] == 1
            process.stdin.write('m\n'); process.stdin.flush()
            assert select.select([process.stdout], [], [], 8)[0]
            assert process.stdout.readline().split()[:2] == ['MATCH', '0']
            process.stdin.write('g\n'); process.stdin.flush()
            assert select.select([process.stdout], [], [], 8)[0]
            assert (guard_reply := process.stdout.readline().strip()) == 'GUARD 0', guard_reply
            rpc(5, (99).to_bytes(16, 'big') + struct.pack('>Q', 1))
            until(lambda: 4 in (operation()['stage'], operation('independent')['stage']))
            time.sleep(.1)
            assert struct.unpack('>IQ', rpc(16))[0] != 6
            process.stdin.write('r\n'); process.stdin.flush()
            assert select.select([process.stdout], [], [], 8)[0]
            assert process.stdout.readline().strip() == 'RELEASE 0'
            until(lambda: operation()['stage'] == operation('independent')['stage'] == 4)
            assert operation()['message'] == operation('independent')['message'] == b''
            until(lambda: struct.unpack('>IQ', rpc(16))[0] == 6)
            process.stdin.write('m\n'); process.stdin.flush()
            assert select.select([process.stdout], [], [], 8)[0]
            matched = process.stdout.readline().split()
            assert matched[:4] == ['MATCH', '0', '1', '2'], matched
            before = operation()['checkpoint']
            # A delayed publication with no valid origin must be rejected by
            # the persisted retirement floor before any filesystem mutation.
            with first.open('rb') as old:
                rpc(18, record + string(str(root / 'cache/temporary')),
                    (lease, old.fileno()), code=7)
            stop()
            start()
            assert operation()['stage'] == 4 and operation()['checkpoint'] == before
            for path in ('cache/a/first', 'cache-b/a/second', 'cache/a/outside', 'cache/a/fresh'):
                assert (root / path).read_bytes() == b'cache entity bytes\0'
            stop()
        else:
            until(lambda: operation()['pending'] == 1)
            until(lambda: operation('independent')['done'])
            assert first.exists() and not (root / 'cache-b/a/other').exists()
            assert operation()['bookmark'].startswith(b'NDW1\x01')
            if cut == 'retired':
                os.chmod(first.parent, 0o500)
                os.close(lease); lease = -1
                until(lambda: operation()['failed'] == 1)
                raw = rpc(2, (1).to_bytes(16, 'big'))
                assert raw[0] == 3 and first.exists(), (raw[:25], operation())
            stop(kill=True)
            if cut == 'replaced':
                os.close(lease); lease = -1
                checks.append(subprocess.check_output(
                    [str(a.binary), '--background-replace', str(root)], text=True, timeout=30).strip())
            start()
            if cut == 'lease':
                assert operation()['pending'] == 1 and first.exists()
                os.close(lease); lease = -1
            elif cut == 'retired':
                assert operation()['pending'] == 1 and operation()['failed'] == 1
                os.chmod(first.parent, 0o700)
            until(lambda: operation()['stage'] == 4)
            final = operation()
            # Visits count processing attempts, including busy/error retries and
            # the replacement's separate ID; they are not a deletion total.
            assert final['stage'] == 4 and final['visits'] >= (4 if cut == 'replaced' else 3), (cut, final)
            assert not final['pending'] and not final['failed'], (cut, final)
            assert not (root / 'cache-b/a/second').exists()
            assert first.exists() == (cut == 'replaced')
            for path in ('cache/a/fresh', 'cache/a/outside'):
                assert (root / path).read_bytes() == b'cache entity bytes\0'
            if cut == 'replaced':
                assert os.getxattr(first, 'user.ngx_cache_purge.id')[8:24] == (8).to_bytes(16, 'big')
            stop()
    finally:
        if lease >= 0: os.close(lease)
        os.chmod(first.parent, 0o700)
        if process is not None and process.poll() is None: process.kill(); process.wait(timeout=5)
        log.close()
    diagnostics = (root / 'component.log').read_text()
    assert not any(word in diagnostics for word in ('AddressSanitizer', 'LeakSanitizer', 'runtime error:')), diagnostics
    checks.append('304 recovery retains old TAG membership, rechecks old/new rules before header mutation and atomically publishes new TAGs: ' + cut if validation_case else
                  'native cleanup alternates task removal and collection without draining a 43-file task first' if cut == 'native-many' else
                  'native cleanup notification survives owner death, rejects stale replies, retries busy consumers and preserves durable per-file progress' if cut == 'native' else
                  'background Invalidate waits for origins and durable intents, resumes after owner death, retires FENCE while retaining PROPAGATE and rejects delayed old writes' if cut == 'invalidate' else
                  'background Delete survives ' + cut + ' restart, handles both zones, preserves new entities and retires the FENCE')

if a.component:
    for cut in ('lease', 'retired', 'replaced', 'invalidate', 'native', 'native-many'):
        background_delete(cut)
    for cut in ('validation-old-invalidate', 'validation-new-invalidate',
                'validation-old-delete', 'validation-new-delete',
                'validation-unrelated-delete'):
        background_delete(cut)

a.output.write_text(json.dumps({'checks': checks, 'directories': roots,
    'scope': 'Real PREPARE/FINISH, xattrs, rename and durable index publication recovery; not nginx integration or power-loss testing'}, indent=2) + '\n')
print('PASS file/index publication crash cuts and repeated recovery', flush=True)
