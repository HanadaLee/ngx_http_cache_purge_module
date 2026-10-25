#!/usr/bin/env python3
"""Bounded index IPC, ambiguous replies, owner handoff and crash recovery."""
import argparse
import json
import os
import select
import socket
import struct
import subprocess
import tempfile
import time
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--binary', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
root = Path(tempfile.mkdtemp(prefix='ncp-component-', dir='/tmp'))
checks = []
processes = []


def check(value, name):
    assert value, name
    checks.append(name)


def wait_for(function, timeout=10):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = function()
        if result:
            return result
        time.sleep(.01)
    raise AssertionError('condition deadline')


class Component:
    def __init__(self, path, connections=8, timeout=3000, budget=1100000, payload=512 * 1024, shared=None):
        self.path = path
        path.mkdir(exist_ok=True)
        self.stderr = (root / f'stderr-{len(processes)}.log').open('w')
        command = [str(args.binary), str(path), str(connections), str(timeout), str(budget), str(payload)]
        if shared:
            command.append(str(shared))
        self.process = subprocess.Popen(command,
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.stderr, text=True, bufsize=1)
        processes.append(self)
        assert self.line() == 'STARTED'

    def line(self):
        assert select.select([self.process.stdout], [], [], 10)[0], 'component stdout timeout'
        line = self.process.stdout.readline().strip()
        assert line, f'component exited {self.process.poll()}'
        return line

    def status(self):
        self.process.stdin.write('s\n')
        self.process.stdin.flush()
        return list(map(int, self.line().split()))

    def opened(self):
        return wait_for(lambda: self.status()[0] == 2)

    def match(self):
        self.process.stdin.write('m\n')
        self.process.stdin.flush()
        line = self.line().split()
        assert line[0] == 'MATCH'
        return list(map(int, line[1:]))

    def guard(self, release=False):
        self.process.stdin.write('r\n' if release else 'g\n')
        self.process.stdin.flush()
        assert self.line() == ('RELEASE 0' if release else 'GUARD 0')

    def stop(self, expected=0):
        self.process.stdin.write('q\n')
        self.process.stdin.flush()
        assert self.line() == f'STOP {expected} 1'
        assert self.process.wait(timeout=10) == expected


def string(value):
    if isinstance(value, str):
        value = value.encode()
    return struct.pack('>I', len(value)) + value


def identity(value):
    return value.to_bytes(16, 'big')


def record(n, root='/cache/a', uri='/dir/a', misc='', metadata='proof-v1'):
    fields = ['team\x1fhttps\x1fa.test\x1f' + uri + '\x1f' + misc,
              'https://a.test' + uri, 'business.test', 'team', root,
              'zone-a' if root == '/cache/a' else 'zone-b', f'a/00/{n}', 'bytes=0-99', 'lang']
    return (identity(n) + b''.join(map(string, fields)) + string(metadata)
            + b'\x02' + string('photo') + string('common') + b'\0\0'
            + string('') * 6 + identity(0))


def batch(items):
    return bytes([len(items)]) + b''.join(identity(n) + struct.pack('>Q', revision) for n, revision in items)


def query(kind, value, scope='', bookmark=b'', limit=32):
    return bytes([kind]) + string(value) + string(scope) + string(bookmark) + bytes([limit])


def operation_input(name='operation-1', mode=1):
    return (string(name) + bytes([mode]) + struct.pack('>Q', 1000)
            + b'\x03' + string('https://a.test/dir/') + string('') + string('config-v1'))


class Client:
    def __init__(self, path):
        self.socket = socket.socket(socket.AF_UNIX)
        self.socket.settimeout(5)
        self.socket.connect(str(path / 'component.sock'))
        self.counter = 0

    def close(self):
        self.socket.close()

    def packet(self, operation, data=b''):
        self.counter += 1
        return struct.pack('>4sHHQII', b'NCP1', 1, operation, self.counter, len(data), 0) + data

    def read(self, size):
        chunks = []
        while size:
            part = self.socket.recv(size)
            if not part:
                raise EOFError('component closed connection')
            chunks.append(part)
            size -= len(part)
        return b''.join(chunks)

    def reply(self, operation, code=0):
        magic, version, op, request, size, result = struct.unpack('>4sHHQII', self.read(24))
        assert (magic, version, op, request) == (b'NCP1', 1, operation, self.counter)
        data = self.read(size)
        assert result == code, (result, code, data)
        return data

    def call(self, operation, data=b'', code=0):
        self.socket.sendall(self.packet(operation, data))
        return self.reply(operation, code)

    def page(self, kind, value, scope='', bookmark=b'', limit=32):
        data = self.call(8, query(kind, value, scope, bookmark, limit))
        count, done, size = struct.unpack('>BBI', data[:6])
        bookmark = data[6:6 + size]
        rest = data[6 + size:]
        assert len(rest) == count * 25
        return [int.from_bytes(rest[n:n + 16], 'big') for n in range(0, len(rest), 25)], done, bookmark


try:
    path = root / 'index'
    first = Component(path)
    first.opened()
    check(os.stat(path / 'component.sock').st_mode & 0o777 == 0o600, 'socket owner-only mode')
    c = Client(path)
    check(struct.unpack('>9Q', c.call(1))[0] == first.process.pid, 'executor answers with owning process')
    packet = c.packet(1)
    for part in (packet[:3], packet[3:13], packet[13:]):
        c.socket.sendall(part)
    check(len(c.reply(1)) == 72, 'fragmented header accepted')
    c.call(999, code=4)
    c.call(1, b'extra', code=4)
    check(len(c.call(1)) == 72, 'malformed operation does not lose connection framing')
    operation = operation_input()
    receipt = c.call(14, operation)
    fields = struct.unpack('>5Q', receipt[len(operation):len(operation) + 40])
    check(receipt.startswith(operation) and fields[3:] == (0, 0) and receipt[len(operation) + 40] == 1,
          'durable operation receipt remains preparing until FENCE publication')
    check(c.call(14, operation) == receipt and c.call(15, string('operation-1')) == receipt,
          'operation acceptance and status are idempotent over IPC')
    c.call(14, operation_input(mode=2), code=7)
    check(c.call(15, string('operation-1')) == receipt, 'changed operation mode does not replace original receipt')
    records = b'\x03' + record(1) + record(2, root='/cache/b') + record(3, misc='fr')
    check(c.call(3, records)[0] == 3, 'batch prepare over IPC')
    c.call(4, batch([(1, 1), (2, 1), (3, 999)]), code=7)
    check(c.call(2, identity(1))[0] == 1, 'conflicting IPC batch makes no partial commit')
    check(c.call(4, batch([(1, 1), (2, 1), (3, 1)]))[0] == 3, 'batch commit over IPC')
    check(c.page(1, 'https://a.test/dir/a')[0] == [1, 2, 3], 'URL lookup covers keys and zones')
    check(c.page(5, 'photo', 'team')[0] == [1, 2, 3], 'namespace TAG lookup')
    check(c.page(6, 'photo', 'wrong.test')[0] == [], 'host TAG isolation')
    found, done, bookmark = c.page(3, 'https://a.test/', limit=1)
    while not done:
        items, done, bookmark = c.page(3, 'https://a.test/', bookmark=bookmark, limit=1)
        found += items
    check(found == [1, 2, 3], 'bounded pages with client-owned bookmark')
    c.call(13, identity(1) + struct.pack('>Q', 2) + string('validated') + b'\x01' + string('new'))
    check(c.page(5, 'new', 'team')[0] == [1], 'metadata TAG replacement over IPC')
    c.call(12, string('team\x1fhttps\x1fa.test\x1f/dir/a\x1f') + string('https://alias.test/a'))
    check(c.page(1, 'https://alias.test/a')[0] == [1, 2], 'URL alias lookup')
    c.close()

    contender = Component(path)
    wait_for(lambda: contender.status()[0] == 1)
    started = time.monotonic()
    contender.stop()
    check(time.monotonic() - started < 1, 'standby shutdown is cancellable')
    c = Client(path)
    check(struct.unpack('>9Q', c.call(1))[0] == first.process.pid, 'standby does not unlink active socket')
    c.close()
    successor = Component(path)
    wait_for(lambda: successor.status()[0] == 1)
    first.stop()
    successor.opened()
    c = Client(path)
    check(c.page(1, 'https://alias.test/a')[0] == [1, 2], 'new owner reopens persistent relationships')
    check(c.call(15, string('operation-1')) == receipt, 'owner handoff preserves operation ledger')
    c.close()
    successor.process.kill()
    successor.process.wait(timeout=5)
    recovered = Component(path)
    recovered.opened()
    c = Client(path)
    check(c.page(1, 'https://a.test/dir/a')[0] == [1, 2, 3], 'owner SIGKILL recovers WAL and stale socket')
    check(c.call(14, operation) == receipt, 'crash recovery preserves original operation timestamps')
    c.call(6, batch([(1, 3), (2, 2), (3, 2)]))
    c.call(7, batch([(1, 4), (2, 3), (3, 3)]))
    while struct.unpack('>I', c.call(11, b'\x08'))[0]:
        pass
    check(c.page(1, 'https://a.test/dir/a')[0] == [], 'retirement and bounded collection over IPC')
    c.close()
    recovered.stop()
    check(not (path / 'component.sock').exists(), 'normal stop removes owned socket')

    # Every retained payload is charged before receiving it. Two advertised
    # 512 KiB bodies fit; a third is rejected, without allocating its body.
    limited = Component(root / 'limited', timeout=700)
    limited.opened()
    peers = [Client(limited.path) for _ in range(3)]
    header = struct.pack('>4sHHQII', b'NCP1', 1, 3, 1, 512 * 1024, 0)
    for peer in peers:
        peer.socket.sendall(header)
    status = wait_for(lambda: (s if (s := limited.status())[4] >= 1 else None))
    check(status[5] <= 1100000 and status[6] <= 1100000, 'connection buffer budget rejects overflow')
    quick = Client(limited.path)
    check(len(quick.call(1)) == 72, 'partial large frames do not block other peers')
    quick.close()
    for peer in peers:
        peer.close()
    wait_for(lambda: limited.status()[2] == 0)
    slow = Client(limited.path)
    slow.socket.sendall(b'NCP')
    check(slow.socket.recv(1) == b'', 'incomplete frame deadline closes slow peer')
    slow.close()
    oversized = Client(limited.path)
    oversized.socket.sendall(struct.pack('>4sHHQII', b'NCP1', 1, 3, 1, 2**32 - 1, 0))
    check(oversized.socket.recv(1) == b'', 'oversized frame rejected before payload allocation')
    oversized.close()
    peers = [Client(limited.path) for _ in range(12)]
    time.sleep(.05)
    check(limited.status()[2] <= 8, 'connection count remains bounded')
    for peer in peers:
        peer.close()
    limited.stop()

    # Disconnect after sending a mutation is deliberately ambiguous. Retry the
    # fill ID and inspect the state; never infer rollback from a lost reply.
    ambiguous = Component(root / 'ambiguous')
    ambiguous.opened()
    abandoned = Client(ambiguous.path)
    pending = b'\x01' + record(9)
    abandoned.socket.sendall(abandoned.packet(3, pending))
    abandoned.close()
    retry = Client(ambiguous.path)
    check(retry.call(3, pending)[0] == 1, 'retry stable fill identity after discarded reply')
    retry.call(5, identity(9) + struct.pack('>Q', 1))
    retry.close()
    ambiguous.stop()

    protected = root / 'protected'
    protected.mkdir()
    (protected / 'component.sock').write_text('preserve me')
    refused = Component(protected)
    wait_for(lambda: refused.status()[0] == 5)
    refused.stop(expected=4)
    check((protected / 'component.sock').read_text() == 'preserve me', 'non-socket path is preserved')

    shared = root / 'control.shm'
    controlled = Component(root / 'controlled', shared=shared)
    controlled.opened()
    c = Client(controlled.path)
    wait_for(lambda: struct.unpack('>IQ', c.call(16))[0] == 6)
    check(controlled.match()[:3] == [0, 0, 0], 'coordinator publishes empty recovered table')
    controlled.guard()
    accepted = c.call(14, operation_input('guarded-operation'))
    check(accepted[len(operation_input('guarded-operation')) + 40] == 1, 'coordinated admission still reports PREPARING')
    wait_for(lambda: struct.unpack('>IQ', c.call(16))[0] == 5)
    check(controlled.match()[0] == 6, 'old pinned admission keeps new gate closed')
    check(len(c.call(1)) == 72, 'blocked publication does not block component IPC')
    controlled.guard(release=True)
    wait_for(lambda: struct.unpack('>IQ', c.call(16))[0] == 6)
    check(controlled.match()[:3] == [0, 1, 5], 'durable operation appears as FENCE plus PREPARING')
    check(c.call(15, string('guarded-operation')) == accepted, 'publication alone does not claim native effectiveness')
    c.close()
    controlled.process.kill()
    controlled.process.wait(timeout=5)
    restored = Component(controlled.path, shared=shared)
    restored.opened()
    c = Client(restored.path)
    wait_for(lambda: struct.unpack('>IQ', c.call(16))[0] == 6)
    check(restored.match()[:3] == [0, 1, 5], 'manager crash reloads durable controls into shared memory')
    check(c.call(14, operation_input('guarded-operation')) == accepted, 'recovered control keeps original durable identity')
    c.close()
    restored.stop()
finally:
    for component in processes:
        if component.process.poll() is None:
            component.process.kill()
            component.process.wait(timeout=5)
        component.stderr.close()

result = {'checks': checks, 'count': len(checks), 'root': str(root),
          'scope': 'component/index/ledger IPC and PREPARING control publication; native cache hooks and physical jobs are not wired'}
args.output.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result), flush=True)
