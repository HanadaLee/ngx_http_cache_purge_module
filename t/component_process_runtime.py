#!/usr/bin/env python3
"""Bounded index IPC, ambiguous replies, owner handoff and crash recovery."""
import argparse
import array
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
    def __init__(self, path, connections=8, timeout=3000, budget=1100000, payload=512 * 1024, shared=None, roots=None):
        self.path = path
        path.mkdir(exist_ok=True)
        self.stderr = (root / f'stderr-{len(processes)}.log').open('w')
        command = [str(args.binary), str(path), str(connections), str(timeout), str(budget), str(payload)]
        if shared:
            command.append(str(shared))
        env = os.environ.copy()
        env.pop('CACHE_PURGE_TEST_CACHE_ROOT', None)
        env.pop('CACHE_PURGE_TEST_TEMP_ROOT', None)
        env.pop('CACHE_PURGE_TEST_CACHE_ZONE', None)
        if roots:
            env['CACHE_PURGE_TEST_CACHE_ROOT'], env['CACHE_PURGE_TEST_TEMP_ROOT'] = map(str, roots)
        self.process = subprocess.Popen(command, env=env,
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

    def watermark(self):
        self.process.stdin.write('w\n'); self.process.stdin.flush()
        kind, code, sequence, ledger = self.line().split()
        assert kind == 'WATERMARK' and code == '0', (kind, code)
        return b'NFW1' + bytes.fromhex(ledger) + struct.pack('>Q', int(sequence))

    def origin(self, release=False):
        self.process.stdin.write('e\n' if release else 'b\n'); self.process.stdin.flush()
        kind, code, sequence = self.line().split()
        assert kind == 'ORIGIN' and code == '0', (kind, code)
        return int(sequence)

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


def record(n, root='/cache/a', uri='/dir/a', misc='', metadata='proof-v1', zone=None, path=None,
           tags=('photo', 'common')):
    fields = ['team\x1fhttps\x1fa.test\x1f' + uri + '\x1f' + misc,
              'https://a.test' + uri, 'business.test', 'team', root,
              zone or ('zone-a' if root == '/cache/a' else 'zone-b'), path or f'a/00/{n}', 'bytes=0-99', 'lang']
    return (identity(n) + b''.join(map(string, fields)) + string(metadata)
            + bytes([len(tags)]) + b''.join(map(string, tags)) + b'\0\0'
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

    def lease(self, address, target=None, previous=0):
        operation = 20 if target else 17
        request_data = b''.join(map(string, target)) if target else string(address)
        self.socket.sendall(self.packet(operation, request_data))
        first, control, flags, _ = self.socket.recvmsg(1, socket.CMSG_SPACE(8), socket.MSG_CMSG_CLOEXEC)
        rights = array.array('i')
        for level, kind, data in control:
            assert level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS
            rights.frombytes(data)
        try:
            magic, version, op, request, size, result = struct.unpack('>4sHHQII', first + self.read(23))
            payload = self.read(size)
            assert (magic, version, op, request, result, flags & socket.MSG_CTRUNC) == (b'NCP1', 1, operation, self.counter, 0, 0)
            assert len(rights) == 1
            if target:
                canonical = str(Path(target[0]).resolve())
                prefix = string(canonical) + identity(previous)
                assert payload.startswith(prefix)
                payload = payload[len(prefix):]
            assert len(payload) == 24
            digest = hashlib.sha256(address.encode()).digest()
            assert payload == digest[:16] + struct.pack('>Q', int.from_bytes(digest[16:24], 'big') & 0x3fffffffffffffff)
            assert fcntl.fcntl(rights[0], fcntl.F_GETFD) & fcntl.FD_CLOEXEC
            return rights.pop()
        finally:
            for fd in rights:
                os.close(fd)

    def prepare_file(self, data, fds, code=0):
        packet = self.packet(18, data)
        assert self.socket.sendmsg([packet[:1]], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', fds))]) == 1
        self.socket.sendall(packet[1:])
        return self.reply(18, code)

    def finish_file(self, n, lease, revision=1, code=0):
        packet = self.packet(19, identity(n) + struct.pack('>Q', revision))
        assert self.socket.sendmsg([packet[:1]], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [lease]))]) == 1
        self.socket.sendall(packet[1:])
        return self.reply(19, code)

    def inspect_file(self, target, fd, code=0):
        packet = self.packet(21, b''.join(map(string, target)))
        assert self.socket.sendmsg([packet[:1]], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [fd]))]) == 1
        self.socket.sendall(packet[1:])
        return self.reply(21, code)

    def remove_file(self, n, lease, revision=2, code=0):
        packet = self.packet(24, identity(n) + struct.pack('>Q', revision))
        assert self.socket.sendmsg([packet[:1]], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [lease]))]) == 1
        self.socket.sendall(packet[1:])
        return self.reply(24, code)

    def validate_file(self, data, lease, selected, code=0):
        packet = self.packet(22, data)
        assert self.socket.sendmsg([packet[:1]], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                                  array.array('i', [lease, selected]))]) == 1
        self.socket.sendall(packet[1:])
        reply = self.reply(22, code)
        if code == 0:
            assert len(reply) >= 41
            self.admission_stamp = int.from_bytes(reply[-16:-8], 'big')
            self.invalidate_required = int.from_bytes(reply[-8:], 'big')
            return reply[:-16]
        return reply

    def admit_file(self, target, key, url, fd, code=0, host='business.test'):
        fields = (*target, key, url)
        if host is not None:
            fields += (host,)
        packet = self.packet(25, b''.join(map(string, fields)))
        assert self.socket.sendmsg([packet[:1]], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [fd]))]) == 1
        self.socket.sendall(packet[1:])
        reply = self.reply(25, code)
        if code == 0:
            assert len(reply) >= 41
            self.admission_stamp = int.from_bytes(reply[-16:-8], 'big')
            self.invalidate_required = int.from_bytes(reply[-8:], 'big')
            assert self.admission_stamp != 0
            return reply[:-16]
        return reply

    def page(self, kind, value, scope='', bookmark=b'', limit=32):
        data = self.call(8, query(kind, value, scope, bookmark, limit))
        count, done, size = struct.unpack('>BBI', data[:6])
        bookmark = data[6:6 + size]
        rest = data[6 + size:]
        assert len(rest) == count * 25
        return [int.from_bytes(rest[n:n + 16], 'big') for n in range(0, len(rest), 25)], done, bookmark


try:
    cache_root, temp_root = root / 'cache', root / 'temp'
    cache_root.mkdir(); temp_root.mkdir()
    publisher = Component(root / 'publication', roots=(cache_root, temp_root))
    publisher.opened()
    client = Client(publisher.path)
    temp = temp_root / 'body'
    temp.write_bytes(b'complete cached entity')
    target = cache_root / 'a/00/401'
    client.call(20, string(str(cache_root)) + string('wrong-zone') + string('a/00/401'), code=4)
    client.call(20, string(str(cache_root)) + string('zone-a') + string('../escape'), code=4)
    lease = client.lease(str(target), target=(str(cache_root), 'zone-a', 'a/00/401'))
    check(True, 'ACQUIRE_PUBLICATION validates root/zone/path and returns canonical identity plus an exclusive lease')
    temporary = os.open(temp, os.O_RDWR | os.O_CLOEXEC)
    payload = record(401, root=str(cache_root), zone='zone-a') + string(str(temp))
    prepared = client.prepare_file(payload, [lease, temporary])
    assert prepared == identity(401) + struct.pack('>QB', 1, 1)
    marker = os.getxattr(temporary, 'user.ngx_cache_purge.id')
    assert marker == b'NCF1\0\0\0\1' + identity(401) + hashlib.sha256(str(target).encode()).digest()[:16]
    assert temp.read_bytes() == b'complete cached entity'
    assert client.prepare_file(payload, [lease, temporary]) == prepared
    client.call(4, batch([(401, 1)]), code=4)
    client.call(5, identity(401) + struct.pack('>Q', 1), code=4)
    check(client.call(2, identity(401))[0] == 1, 'raw index mutations cannot bypass physical publication checks')
    check(True, 'PREPARE_FILE persists the index intent and matching xattr without changing entity bytes; retries are stable')
    replacement = temp_root / 'other'
    replacement.write_bytes(b'complete cached entity')
    replacement_fd = os.open(replacement, os.O_RDWR | os.O_CLOEXEC)
    client.prepare_file(payload, [lease, replacement_fd], code=4)
    client.prepare_file(record(401, root=str(cache_root), zone='zone-a') + string(str(replacement)),
                        [lease, replacement_fd], code=7)
    assert not os.listxattr(replacement_fd)
    check(True, 'a path/FD mismatch or reuse of an intent for another physical temp file cannot stamp that file')
    client.prepare_file(record(402, root=str(cache_root), zone='wrong-zone') + string(str(replacement)),
                        [lease, replacement_fd], code=4)
    client.prepare_file(record(402, root=str(cache_root), zone='zone-a', path='../escape') + string(str(replacement)),
                        [lease, replacement_fd], code=4)
    outside = root / 'outside'
    outside.write_bytes(b'untouched')
    outside_fd = os.open(outside, os.O_RDWR | os.O_CLOEXEC)
    client.prepare_file(record(402, root=str(cache_root), zone='zone-a') + string(str(outside)),
                        [lease, outside_fd], code=4)
    alias = temp_root / 'symlink'
    alias.symlink_to(replacement)
    client.prepare_file(record(402, root=str(cache_root), zone='zone-a') + string(str(alias)),
                        [lease, replacement_fd], code=4)
    assert not os.listxattr(replacement_fd) and not os.listxattr(outside_fd)
    check(True, 'unregistered zones, traversal, external temp paths and symlinks are rejected before mutation')
    # A competing OFD description cannot substitute for the protected lease.
    wrong_lease = os.open(publisher.path / 'file.leases', os.O_RDWR | os.O_CLOEXEC)
    client.prepare_file(payload, [wrong_lease, temporary], code=6)
    check(True, 'PREPARE_FILE requires exclusive access to its canonical cache address')
    cache_root.rename(root / 'old-cache')
    cache_root.mkdir()
    client.prepare_file(record(402, root=str(cache_root), zone='zone-a') + string(str(replacement)),
                        [lease, replacement_fd], code=4)
    cache_root.rmdir()
    (root / 'old-cache').rename(cache_root)
    check(not os.listxattr(replacement_fd), 'replacing a configured root cannot silently redirect prepared file identity')
    for fd in (temporary, replacement_fd, outside_fd, wrong_lease):
        os.close(fd)
    client.close()
    publisher.process.kill(); publisher.process.wait(timeout=5)
    publisher = Component(publisher.path, roots=(cache_root, temp_root))
    publisher.opened(); client = Client(publisher.path)
    assert client.call(2, identity(401))[0] == 1
    temporary = os.open(temp, os.O_RDWR | os.O_CLOEXEC)
    assert client.prepare_file(payload, [lease, temporary]) == prepared
    check(True, 'owner crash preserves a lease-held PREPARE_FILE intent and permits an identical file retry without index rebuild')
    target.parent.mkdir(parents=True, exist_ok=True)
    temp.rename(target)
    selected = (str(cache_root), 'zone-a', 'a/00/401')
    client.inspect_file(selected, temporary, code=6)
    check(True, 'a renamed PREPARED descriptor is not admitted before durable publication')
    finished = client.finish_file(401, lease)
    assert finished == identity(401) + struct.pack('>QB', 2, 2)
    assert os.getxattr(target, 'user.ngx_cache_purge.id')[7] == 2
    assert client.finish_file(401, lease) == finished
    assert client.call(9, string('') + bytes([32]))[0] == 0
    check(True, 'FINISH_FILE publishes the renamed instance and clears its durable intent; retries are idempotent')
    verified = client.call(2, identity(401))
    writes = struct.unpack('>9Q', client.call(1))[1]
    selected_fd = os.open(target, os.O_RDONLY | os.O_CLOEXEC)
    assert client.inspect_file(selected, selected_fd) == verified
    assert os.lseek(selected_fd, 0, os.SEEK_CUR) == 0
    check(True, 'INSPECT_FILE validates the actual read-only descriptor and returns the full durable record without changing its offset')
    client.inspect_file((str(cache_root), 'wrong-zone', 'a/00/401'), selected_fd, code=4)
    client.inspect_file((str(cache_root), 'zone-a', '../escape'), selected_fd, code=4)
    client.inspect_file((str(cache_root), 'zone-a', 'a/00/other'), selected_fd, code=7)
    check(True, 'read inspection binds the selected instance to its registered root, zone and exact path')
    marker = os.getxattr(selected_fd, 'user.ngx_cache_purge.id')
    replacement_fd = os.open(replacement, os.O_RDONLY | os.O_CLOEXEC)
    client.inspect_file(selected, replacement_fd, code=1)
    os.setxattr(replacement_fd, 'user.ngx_cache_purge.id', marker)
    client.inspect_file(selected, replacement_fd, code=7)
    assert os.getxattr(replacement_fd, 'user.ngx_cache_purge.id') == marker
    os.close(replacement_fd)
    check(True, 'unknown and copied-marker files are rejected without adoption or repair')
    write_only = os.open(target, os.O_WRONLY | os.O_CLOEXEC)
    path_only = os.open(target, os.O_PATH | os.O_CLOEXEC)
    directory_fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    for fd in (write_only, path_only, directory_fd):
        client.inspect_file(selected, fd, code=4)
        os.close(fd)
    check(True, 'write-only, O_PATH and directory descriptors cannot pass cache read inspection')
    linked = target.parent / 'hardlink'
    os.link(target, linked)
    client.inspect_file(selected, selected_fd, code=7)
    linked.unlink()
    detached = target.parent / 'detached'
    target.rename(detached)
    client.inspect_file(selected, selected_fd, code=7)
    target.write_bytes(detached.read_bytes())
    os.setxattr(target, 'user.ngx_cache_purge.id', marker)
    client.inspect_file(selected, selected_fd, code=7)
    target.unlink(); detached.rename(target)
    check(True, 'hardlinks, detached descriptors and replaced named instances fail even when marker bytes agree')
    directory = cache_root / 'a'
    moved_directory = cache_root / 'moved'
    directory.rename(moved_directory); directory.symlink_to(moved_directory, target_is_directory=True)
    client.inspect_file(selected, selected_fd, code=4)
    directory.unlink(); moved_directory.rename(directory)
    cache_root.rename(root / 'detached-cache'); cache_root.mkdir()
    client.inspect_file(selected, selected_fd, code=4)
    cache_root.rmdir(); (root / 'detached-cache').rename(cache_root)
    check(True, 'read inspection rejects intermediate symlinks and replaced root directories')
    original_stat = os.fstat(selected_fd)
    os.pwrite(temporary, b'changed entity with a different size', 0)
    client.inspect_file(selected, selected_fd, code=7)
    os.pwrite(temporary, b'complete cached entity', 0)
    os.ftruncate(temporary, original_stat.st_size)
    os.utime(temporary, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
    os.setxattr(temporary, 'user.ngx_cache_purge.id', b'malformed')
    client.inspect_file(selected, selected_fd, code=7)
    os.setxattr(temporary, 'user.ngx_cache_purge.id', marker)
    check(True, 'changed physical proof and malformed file markers are rejected without failing the database owner')
    for _ in range(100):
        assert client.inspect_file(selected, selected_fd) == verified
    assert struct.unpack('>9Q', client.call(1))[1] == writes
    assert target.read_bytes() == b'complete cached entity'
    assert os.getxattr(selected_fd, 'user.ngx_cache_purge.id') == marker
    check(True, 'repeated verified reads leave database writes, cache contents and markers unchanged')
    client.call(13, identity(401) + struct.pack('>Q', 2) + string('replacement') + b'\0', code=4)
    target.unlink()
    client.inspect_file(selected, selected_fd, code=7)
    os.close(selected_fd)
    check(True, 'an evicted open descriptor cannot be accepted using its formerly committed marker')
    assert client.finish_file(401, lease) == finished and not target.exists()
    check(True, 'a late finish retry returns its durable result without recreating an evicted file')
    os.close(temporary); os.close(lease)

    for number, scenario in ((402, 'missing'), (403, 'directory'), (404, 'unrelated'), (405, 'changed')):
        source = temp_root / f'body-{number}'
        source.write_bytes(b'new complete entity')
        destination = cache_root / f'a/00/{number}'
        lease = client.lease(str(destination))
        temporary = os.open(source, os.O_RDWR | os.O_CLOEXEC)
        prepared_data = record(number, root=str(cache_root), zone='zone-a') + string(str(source))
        client.prepare_file(prepared_data, [lease, temporary])
        if scenario == 'directory': destination.mkdir()
        elif scenario == 'unrelated': destination.write_bytes(b'leave this file unchanged')
        elif scenario == 'changed':
            source.write_bytes(b'content changed after prepare')
            source.rename(destination)
            client.finish_file(number, lease, code=7)
            assert os.getxattr(destination, 'user.ngx_cache_purge.id')[7] == 1
            destination.unlink()  # Test-owned damaged candidate; FINISH itself never unlinks it.
        result = client.finish_file(number, lease)
        assert result[-1] == 3 and client.finish_file(number, lease) == result
        if scenario == 'unrelated': assert destination.read_bytes() == b'leave this file unchanged'
        if scenario == 'directory': assert destination.is_dir()
        os.close(temporary); os.close(lease)
    check(True, 'missing, failed and changed candidates retire safely without altering unrelated final paths')
    client.close(); publisher.stop()

    recovery_cache, recovery_temp = root / 'recovery-cache', root / 'recovery-temp'
    recovery_cache.mkdir(); recovery_temp.mkdir()
    recovering = Component(root / 'recovery-owner', roots=(recovery_cache, recovery_temp))
    recovering.opened(); recovery_client = Client(recovering.path)
    for number, scenario in ((501, 'missing'), (502, 'renamed'), (503, 'restart'),
                             (504, 'damaged'), (505, 'lost-prepare')):
        source = recovery_temp / str(number)
        source.write_bytes(b'complete recovery entity')
        destination = recovery_cache / f'a/00/{number}'
        destination.parent.mkdir(parents=True, exist_ok=True)
        lease = recovery_client.lease(str(destination))
        temporary = os.open(source, os.O_RDWR | os.O_CLOEXEC)
        prepare_data = record(number, root=str(recovery_cache), zone='zone-a',
                              uri=f'/recovery/{number}') + string(str(source))
        if scenario == 'lost-prepare':
            packet = recovery_client.packet(18, prepare_data)
            assert recovery_client.socket.sendmsg([packet[:1]], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                                     array.array('i', [lease, temporary]))]) == 1
            recovery_client.socket.sendall(packet[1:])
            wait_for(lambda: 'user.ngx_cache_purge.id' in os.listxattr(temporary))
            recovery_client.close()  # Drop the unread reply after durable preparation.
            recovery_client = Client(recovering.path)
        else:
            recovery_client.prepare_file(prepare_data, [lease, temporary])
        if scenario not in ('missing', 'lost-prepare'):
            source.rename(destination)
        if scenario == 'restart':
            recovery_client.close()
            recovering.process.kill(); recovering.process.wait(timeout=5)
            recovering = Component(recovering.path, roots=(recovery_cache, recovery_temp))
            recovering.opened(); recovery_client = Client(recovering.path)
        time.sleep(1.2)
        assert recovery_client.call(2, identity(number))[0] == 1
        assert os.getxattr(temporary, 'user.ngx_cache_purge.id')[7] == 1
        check(True, f'background recovery does not advance a live OFD-held {scenario} publication')
        if scenario == 'renamed':
            retained = os.dup(lease)
            os.close(lease); lease = retained
            time.sleep(1.2)
            assert recovery_client.call(2, identity(number))[0] == 1
            check(True, 'a duplicate OFD reference continues to exclude background recovery')
        if scenario == 'damaged':
            os.setxattr(temporary, 'user.ngx_cache_purge.id', b'broken-marker')
        os.close(temporary); os.close(lease)
        if scenario == 'damaged':
            time.sleep(1.2)
            assert recovery_client.call(2, identity(number))[0] == 1
            assert os.getxattr(destination, 'user.ngx_cache_purge.id') == b'broken-marker'
            assert recovery_client.call(9, string('') + b'\x20')[0] == 1
            check(True, 'background recovery retains a malformed candidate without stamping it READY')
            destination.unlink()  # Remove only this test-owned damaged candidate.
        expected = 3 if scenario in ('missing', 'damaged', 'lost-prepare') else 2
        wait_for(lambda: recovery_client.call(2, identity(number))[0] == expected)
        wait_for(lambda: recovery_client.call(9, string('') + b'\x20')[0] == 0)
        if expected == 2:
            assert os.getxattr(destination, 'user.ngx_cache_purge.id')[7] == 2
            assert destination.read_bytes() == b'complete recovery entity'
        else:
            assert not destination.exists()
        check(True, f'background recovery completes abandoned {scenario} publication without a FINISH RPC')
    recovery_client.close(); recovering.stop()

    lease_component = Component(root / 'leases')
    lease_component.opened()
    lease_client = Client(lease_component.path)
    lease_fd = lease_client.lease('/cache/root/one')
    lease_client.call(17, string('/cache/root/one'), code=6)
    other_fd = lease_client.lease('/cache/root/two')
    os.close(other_fd)
    check(True, 'component returns a real OFD lease and serializes competing addresses')
    lease_client.close()
    lease_component.stop()
    successor = Component(lease_component.path)
    successor.opened()
    lease_client = Client(successor.path)
    lease_client.call(17, string('/cache/root/one'), code=6)
    os.close(lease_fd)
    os.close(lease_client.lease('/cache/root/one'))
    check(True, 'lease retained by a worker survives component owner replacement')
    before = len(list(Path(f'/proc/{successor.process.pid}/fd').iterdir()))
    for count in (1, 2, 8):
        unexpected = Client(successor.path)
        packet = unexpected.packet(1)
        with open('/dev/null', 'rb') as source:
            unexpected.socket.sendmsg([packet], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                                   array.array('i', [source.fileno()] * count))])
        try:
            assert unexpected.socket.recv(1) == b''
        except ConnectionResetError:
            pass
        unexpected.close()
    wait_for(lambda: len(list(Path(f'/proc/{successor.process.pid}/fd').iterdir())) <= before)
    check(len(lease_client.call(1)) == 72, 'unexpected and truncated request rights are rejected without descriptor leaks or owner failure')
    lease_client.close()
    successor.stop()

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
    participant = Component(controlled.path, shared=shared)
    assert participant.origin() == 0
    controlled.guard()
    accepted = c.call(14, operation_input('guarded-operation'))
    check(accepted[len(operation_input('guarded-operation')) + 40] == 1, 'coordinated admission still reports PREPARING')
    wait_for(lambda: struct.unpack('>IQ', c.call(16))[0] == 5)
    check(controlled.match()[0] == 6, 'old pinned admission keeps new gate closed')
    check(len(c.call(1)) == 72, 'blocked publication does not block component IPC')
    controlled.guard(release=True)
    wait_for(lambda: struct.unpack('>IQ', c.call(16))[0] == 6)
    check(controlled.match()[:3] == [0, 1, 5], 'durable operation appears as FENCE plus PREPARING')
    assert int.from_bytes(c.call(23, struct.pack('>Q', 1)), 'big') == 1
    participant.origin(release=True)
    assert int.from_bytes(c.call(23, struct.pack('>Q', 1)), 'big') == 0
    assert participant.origin() == 1
    assert int.from_bytes(c.call(23, struct.pack('>Q', 1)), 'big') == 0
    c.call(14, operation_input('second-writer-cutoff'))
    wait_for(lambda: struct.unpack('>IQ', c.call(16))[0] == 6)
    assert int.from_bytes(c.call(23, struct.pack('>Q', 2)), 'big') == 1
    participant.process.kill(); participant.process.wait(timeout=5)
    assert int.from_bytes(c.call(23, struct.pack('>Q', 2)), 'big') == 0
    check(True, 'writer status tracks another process across cutoffs and reclaims it only after verified death')
    check(c.call(15, string('guarded-operation')) == accepted, 'publication alone does not claim native effectiveness')
    c.close()
    controlled.process.kill()
    controlled.process.wait(timeout=5)
    restored = Component(controlled.path, shared=shared)
    restored.opened()
    c = Client(restored.path)
    wait_for(lambda: struct.unpack('>IQ', c.call(16))[0] == 6)
    check(restored.match()[:3] == [0, 2, 5], 'manager crash reloads durable controls into shared memory')
    check(c.call(14, operation_input('guarded-operation')) == accepted, 'recovered control keeps original durable identity')
    c.close()
    restored.stop()

    validation_cache, validation_temp = root / 'validation-cache', root / 'validation-temp'
    validation_cache.mkdir(); validation_temp.mkdir()
    validation_shared = root / 'validation.shm'
    validating = Component(root / 'validating', shared=validation_shared,
                           roots=(validation_cache, validation_temp))
    validating.opened(); v = Client(validating.path)
    wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
    validation_origin = validating.watermark()

    def make_validation_file(n, uri, metadata='proof-v1'):
        target = validation_cache / f'a/00/{n}'
        target.parent.mkdir(parents=True, exist_ok=True)
        source = validation_temp / str(n)
        source.write_bytes(b'old-head' + b'body' * 16384)
        lease = v.lease(str(target))
        fd = os.open(source, os.O_RDWR | os.O_CLOEXEC)
        v.prepare_file(record(n, root=str(validation_cache), zone='zone-a', uri=uri,
                              tags=(f'tag-{n}', 'shared'), metadata=metadata)
                       + string(str(source)), [lease, fd])
        source.rename(target); v.finish_file(n, lease)
        os.close(fd)
        return target, lease, os.open(target, os.O_RDONLY | os.O_CLOEXEC)

    def validation_packet(n, revision=2, token=1, before=b'old-head', after=b'new-head', tags=None, origin=None):
        if origin is None:
            origin = validation_origin
        return (identity(n) + struct.pack('>Q', revision) + identity(token) + string(before) + string(after)
                + bytes([tags is not None, len(tags or [])]) + b''.join(map(string, tags or [])) + origin[4:])

    def validation_operation(name, kind, value, scope='', mode=2):
        data = (string(name) + bytes([mode]) + struct.pack('>Q', 1000) + bytes([kind])
                + string(value) + string(scope) + string('validation-test'))
        v.call(14, data)

    target, lease, selected = make_validation_file(605, '/read-alias', metadata=validation_origin)
    address = (str(validation_cache), 'zone-a', 'a/00/605')
    key = 'team\x1fhttps\x1fa.test\x1f/read-alias\x1f'
    original = v.call(2, identity(605))
    writes = struct.unpack('>9Q', v.call(1))[1]
    v.admit_file(address, key, 'https://a.test/forbidden-alias', selected, code=4, host=None)
    v.admit_file(address, key, 'https://a.test/forbidden-alias', selected, code=7, host='')
    v.admit_file(address, 'another-key', 'https://a.test/forbidden-alias', selected, code=7)
    assert struct.unpack('>9Q', v.call(1))[1] == writes and v.page(1, 'https://a.test/forbidden-alias')[0] == []
    assert v.admit_file(address, key, 'https://a.test/discovered-alias', selected) == original
    stamp = v.admission_stamp
    assert stamp == struct.unpack('>IQ', v.call(16))[1]
    assert v.page(1, 'https://a.test/discovered-alias')[0] == [605]
    writes = struct.unpack('>9Q', v.call(1))[1]
    assert v.admit_file(address, key, 'https://a.test/discovered-alias', selected) == original
    assert struct.unpack('>9Q', v.call(1))[1] == writes
    check(True, 'ADMIT_FILE requires an explicit host, binds the native key and atomically registers request identities without rewriting repeated observations')
    validation_operation('discovered-alias-rule', 1, 'https://a.test/discovered-alias')
    wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
    assert stamp != struct.unpack('>IQ', v.call(16))[1]
    v.admit_file(address, key, 'https://a.test/read-alias', selected, code=7)
    assert v.inspect_file(address, selected) == original
    check(True, 'a FENCE on another persisted alias rejects read admission while physical inspection remains diagnostic only')
    os.close(selected)
    source = validation_temp / 'fresh-alias-606'; source.write_bytes(b'fresh alias entity')
    fd = os.open(source, os.O_RDWR | os.O_CLOEXEC)
    payload = record(606, root=str(validation_cache), zone='zone-a', uri='/read-alias',
                     path='a/00/605', metadata=validating.watermark())
    v.prepare_file(payload[:-16] + identity(605) + string(str(source)), [lease, fd])
    source.rename(target); v.finish_file(606, lease); os.close(fd)
    selected = os.open(target, os.O_RDONLY | os.O_CLOEXEC)
    assert v.admit_file(address, key, 'https://a.test/discovered-alias', selected) == v.call(2, identity(606))
    os.close(lease); os.close(selected)
    check(True, 'a newer full entity satisfies the persisted alias FENCE and retains its key URL associations')

    full_origin = validating.watermark()
    target, lease, selected = make_validation_file(607, '/conditional', metadata=full_origin)
    address = (str(validation_cache), 'zone-a', 'a/00/607')
    key = 'team\x1fhttps\x1fa.test\x1f/conditional\x1f'
    validation_operation('conditional-first', 5, 'tag-607', 'team', mode=1)
    wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
    origin = validating.watermark()
    v.admit_file(address, key, 'https://a.test/conditional', selected)
    assert v.invalidate_required == int.from_bytes(origin[20:], 'big')
    result = v.validate_file(validation_packet(607, origin=origin), lease, selected)
    assert v.invalidate_required == 0 and v.admission_stamp != 0
    at = result.index(b'NFW2')
    assert result[at + 4:at + 28] == origin[4:] and result[at + 32:at + 60] == full_origin
    assert target.read_bytes().startswith(b'new-head')
    validation_operation('conditional-second', 5, 'tag-607', 'team', mode=1)
    wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
    origin = validating.watermark()
    again = v.validate_file(validation_packet(607, revision=4, token=2,
                            before=b'new-head', after=b'rev-head', origin=origin), lease, selected)
    assert len(again) == len(result) and v.invalidate_required == 0
    at = again.index(b'NFW2')
    assert again[at + 32:at + 60] == full_origin
    check(True, 'successful conditional validation advances Invalidate coverage without changing full-entity origin or nesting metadata')
    validation_operation('conditional-delete', 5, 'tag-607', 'team', mode=2)
    wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
    v.admit_file(address, key, 'https://a.test/conditional', selected, code=7)
    before = v.call(2, identity(607)); contents = target.read_bytes()
    v.validate_file(validation_packet(607, revision=6, token=3, before=b'rev-head',
                    origin=validating.watermark()), lease, selected, code=7)
    assert v.call(2, identity(607)) == before and target.read_bytes() == contents
    os.close(lease); os.close(selected)
    check(True, 'Delete still requires a newer full entity even after successful Invalidate validation and a newer conditional request')

    target, lease, selected = make_validation_file(601, '/validate')
    packet = validation_packet(601, tags=['new-tag'])
    outcome = v.validate_file(packet, lease, selected)
    assert outcome[:9] == b'\x02' + struct.pack('>Q', 4)
    assert target.read_bytes() == b'new-head' + b'body' * 16384
    assert v.page(5, 'new-tag', 'team')[0] == [601]
    assert v.page(5, 'tag-601', 'team')[0] == []
    assert v.inspect_file((str(validation_cache), 'zone-a', 'a/00/601'), selected) == outcome
    writes = struct.unpack('>9Q', v.call(1))[1]
    assert v.validate_file(packet, lease, selected) == outcome
    v.validate_file(validation_packet(601, after=b'bad-head', tags=['new-tag']), lease, selected, code=7)
    assert struct.unpack('>9Q', v.call(1))[1] == writes
    check(True, 'VALIDATE_FILE atomically changes TAGs and the bounded header, preserves the body and retries without writes')

    validation_operation('unrelated-validation-rule', 1, 'https://a.test/unrelated')
    wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
    packet = validation_packet(601, revision=4, token=2, before=b'new-head')
    committed_before_alias = packet
    assert v.validate_file(packet, lease, selected)[:9] == b'\x02' + struct.pack('>Q', 6)
    check(True, 'an unrelated accepted rule permits validation through the bounded URL visitor')

    key = 'team\x1fhttps\x1fa.test\x1f/validate\x1f'
    v.call(12, string(key) + string('https://a.test/alias'))
    assert validating.match()[0] == 0
    validating.guard()
    validation_operation('alias-before-validation', 1, 'https://a.test/alias')
    wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 5)
    packet = validation_packet(601, revision=6, token=3, before=b'new-head', tags=[])
    previous = v.call(2, identity(601))
    v.validate_file(packet, lease, selected, code=6)
    assert v.call(2, identity(601)) == previous
    validating.guard(release=True)
    wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
    v.validate_file(packet, lease, selected, code=7)
    assert v.call(2, identity(601)) == previous and v.page(5, 'new-tag', 'team')[0] == [601]
    assert v.validate_file(committed_before_alias, lease, selected) == previous
    assert v.admission_stamp == 0
    check(True, 'a completed validation receipt can replay under a new alias FENCE but its zero stamp cannot authorize response reuse')
    check(True, 'accepted but unpublished controls stop validation; a persisted alias FENCE blocks TAG removal after publication')
    os.close(lease); os.close(selected)

    # Each operation uses a distinct file/tag until the final broad host rule.
    cases = [(610, 0, 'team\x1fhttps\x1fa.test\x1f/scope-610\x1f', ''),
             (611, 2, 'team\x1fhttps\x1fa.test\x1f/scope-611', ''),
             (612, 3, 'https://a.test/scope-612', ''),
             (613, 5, 'tag-613', 'team'), (614, 6, 'tag-614', 'business.test')]
    for n, kind, value, scope in cases:
        target, lease, selected = make_validation_file(n, f'/scope-{n}')
        if kind in (5, 6):
            validation_operation(f'wrong-scope-{n}', kind, value, 'other')
            wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
            v.validate_file(validation_packet(n), lease, selected)
            revision, before = 4, b'new-head'
        else:
            revision, before = 2, b'old-head'
        validation_operation(f'validation-rule-{n}', kind, value, scope)
        wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
        previous = v.call(2, identity(n)); contents = target.read_bytes()
        v.validate_file(validation_packet(n, revision, token=2, before=before, tags=[]), lease, selected, code=7)
        assert v.call(2, identity(n)) == previous and target.read_bytes() == contents
        os.close(lease); os.close(selected)
    check(True, 'validation checks exact/prefix keys, URL prefixes and scoped old TAGs before catalogue mutation')

    target, lease, selected = make_validation_file(620, '/alias-limit')
    key = 'team\x1fhttps\x1fa.test\x1f/alias-limit\x1f'
    for i in range(64):
        v.call(12, string(key) + string(f'https://a.test/many/{i}'))
    previous = v.call(2, identity(620))
    v.validate_file(validation_packet(620), lease, selected, code=5)
    v.admit_file((str(validation_cache), 'zone-a', 'a/00/620'), key,
                 'https://a.test/alias-limit', selected, code=5)
    assert v.call(2, identity(620)) == previous and target.read_bytes().startswith(b'old-head')
    os.close(lease); os.close(selected)
    check(True, 'excess URL aliases fail validation without partial TAG or header mutation')

    target, lease, selected = make_validation_file(621, '/host-rule')
    validation_operation('validation-host', 4, 'business.test')
    wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
    previous = v.call(2, identity(621))
    v.validate_file(validation_packet(621), lease, selected, code=7)
    assert v.call(2, identity(621)) == previous
    ledger_before_restart = validating.watermark()
    v.close(); validating.process.kill(); validating.process.wait(timeout=5)
    validating = Component(validating.path, shared=validation_shared,
                           roots=(validation_cache, validation_temp))
    validating.opened(); v = Client(validating.path)
    wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
    v.validate_file(validation_packet(621), lease, selected, code=7)
    assert v.call(2, identity(621)) == previous
    assert validating.watermark() == ledger_before_restart
    os.close(lease); os.close(selected)
    check(True, 'host FENCE rejection survives owner crash and control recovery')

    watermark = validating.watermark()
    early_watermark = watermark
    target, lease, selected = make_validation_file(630, '/late-tag', metadata=watermark)
    assert v.call(2, identity(630))[0] == 2
    os.close(lease); os.close(selected)
    source = validation_temp / 'late-631'; source.write_bytes(b'old request with changed TAG')
    lease = v.lease(str(target), target=(str(validation_cache), 'zone-a', 'a/00/630'), previous=630)
    fd = os.open(source, os.O_RDWR | os.O_CLOEXEC)
    payload = record(631, root=str(validation_cache), zone='zone-a', uri='/late-tag',
                     path='a/00/630', tags=('replacement-tag',), metadata=watermark)
    payload = payload[:-16] + identity(630) + string(str(source))
    v.prepare_file(payload, [lease, fd])
    validation_operation('old-tag-after-prepare', 5, 'tag-630', 'team')
    wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
    source.rename(target)
    retired = v.finish_file(631, lease)
    assert retired[-1] == 3 and v.call(2, identity(631))[0] == 3
    assert os.getxattr(target, 'user.ngx_cache_purge.id')[7] == 1
    os.close(fd)
    check(True, 'FINISH rejects a late prepared replacement whose old TAG was invalidated before its catalogue commit')

    watermark = validating.watermark()
    source = validation_temp / 'fresh-632'; source.write_bytes(b'fresh request with changed TAG')
    fd = os.open(source, os.O_RDWR | os.O_CLOEXEC)
    payload = record(632, root=str(validation_cache), zone='zone-a', uri='/late-tag',
                     path='a/00/630', tags=('replacement-tag',), metadata=watermark)
    v.prepare_file(payload[:-16] + identity(630) + string(str(source)), [lease, fd])
    source.rename(target)
    assert v.finish_file(632, lease)[-1] == 2
    assert os.getxattr(target, 'user.ngx_cache_purge.id')[7] == 2
    os.close(fd)
    check(True, 'an origin watermark after the accepted rule permits a new entity to replace that old TAG')

    source = validation_temp / 'regressed-634'; source.write_bytes(b'older request started first')
    fd = os.open(source, os.O_RDWR | os.O_CLOEXEC)
    payload = record(634, root=str(validation_cache), zone='zone-a', uri='/late-tag',
                     path='a/00/630', tags=('replacement-tag',), metadata=early_watermark)
    v.prepare_file(payload[:-16] + identity(632) + string(str(source)), [lease, fd])
    source.rename(target)
    assert v.finish_file(634, lease)[-1] == 3
    assert os.getxattr(target, 'user.ngx_cache_purge.id')[7] == 1
    os.close(fd)
    check(True, 'a late fill cannot regress its predecessor watermark even when its current TAGs do not match a newer rule')

    source = validation_temp / 'abandoned-633'; source.write_bytes(b'abandoned late replacement')
    fd = os.open(source, os.O_RDWR | os.O_CLOEXEC)
    payload = record(633, root=str(validation_cache), zone='zone-a', uri='/late-tag',
                     path='a/00/630', tags=('third-tag',), metadata=watermark)
    v.prepare_file(payload[:-16] + identity(632) + string(str(source)), [lease, fd])
    validation_operation('old-tag-before-recovery', 5, 'replacement-tag', 'team')
    wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
    source.rename(target)
    v.close(); validating.process.kill(); validating.process.wait(timeout=5)
    os.close(fd); os.close(lease)
    validating = Component(validating.path, shared=validation_shared,
                           roots=(validation_cache, validation_temp))
    validating.opened(); v = Client(validating.path)
    wait_for(lambda: v.call(2, identity(633))[0] == 3)
    assert os.getxattr(target, 'user.ngx_cache_purge.id')[7] == 1
    check(True, 'automatic recovery applies the same old-instance FENCE check without a FINISH request')

    wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
    watermark = validating.watermark()
    source = validation_temp / 'invalid-watermark'; source.write_bytes(b'unmodified')
    destination = validation_cache / 'a/00/640'
    lease = v.lease(str(destination)); fd = os.open(source, os.O_RDWR | os.O_CLOEXEC)
    for invalid, code in ((b'NFW1', 4), (b'NFW2' + watermark[4:] + string(watermark), 4), (b'NFW1' + b'X' * 16 + watermark[20:], 7),
                          (watermark[:20] + struct.pack('>Q', 999999), 7)):
        payload = record(640, root=str(validation_cache), zone='zone-a', metadata=invalid) + string(str(source))
        v.prepare_file(payload, [lease, fd], code=code)
        assert not os.listxattr(fd)
    v.call(2, identity(640), code=1)
    os.close(fd); os.close(lease)
    check(True, 'malformed, foreign-ledger and future origin watermarks are rejected before file or index mutation')
    v.close(); validating.stop()

    # Offline retirement supplies only the ledger transition. It deliberately
    # does not stand in for a native physical-work/quiescence implementation.
    validation_shared = root / 'retired.shm'
    validating = Component(root / 'retired', shared=validation_shared,
                           roots=(validation_cache, validation_temp))
    validating.opened(); v = Client(validating.path)
    wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
    validation_origin = validating.watermark()
    target, lease, selected = make_validation_file(651, '/unrelated-committed', metadata=validation_origin)
    replay = validation_packet(651)
    committed = v.validate_file(replay, lease, selected)
    os.close(lease); os.close(selected)
    pending = []
    for n in (652, 653):
        source = validation_temp / str(n); source.write_bytes(b'abandoned full entity')
        destination = validation_cache / f'a/00/{n}'
        lease = v.lease(str(destination)); fd = os.open(source, os.O_RDWR | os.O_CLOEXEC)
        payload = record(n, root=str(validation_cache), zone='zone-a', uri=f'/pending-{n}',
                         metadata=validation_origin) + string(str(source))
        v.prepare_file(payload, [lease, fd]); source.rename(destination)
        os.close(fd); pending.append((n, destination, lease))
    validation_operation('retired-rule', 1, 'https://a.test/no-matching-file')
    wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
    v.close(); validating.stop()
    subprocess.check_output([str(args.binary.with_name('cache_purge_operations_test')),
                             '--retire-fixture', str(validating.path), 'retired-rule'],
                            text=True, timeout=30)
    os.close(pending[1][2])
    validating = Component(validating.path, shared=validation_shared,
                           roots=(validation_cache, validation_temp))
    validating.opened(); v = Client(validating.path)
    wait_for(lambda: struct.unpack('>IQ', v.call(16))[0] == 6)
    assert v.finish_file(652, pending[0][2])[-1] == 3
    os.close(pending[0][2])
    wait_for(lambda: v.call(2, identity(653))[0] == 3)
    assert all(os.getxattr(path, 'user.ngx_cache_purge.id')[7] == 1 for _, path, _ in pending)
    check(True, 'persisted retirement floor rejects explicit FINISH and automatic recovery after the matching FENCE is removed')

    lease = v.lease(str(target), target=(str(validation_cache), 'zone-a', 'a/00/651'), previous=651)
    selected = os.open(target, os.O_RDONLY | os.O_CLOEXEC)
    assert v.inspect_file((str(validation_cache), 'zone-a', 'a/00/651'), selected) == committed
    writes = struct.unpack('>9Q', v.call(1))[1]
    assert v.validate_file(replay, lease, selected) == committed
    stale = validation_packet(651, revision=4, token=2, before=b'new-head', after=b'nexthead')
    v.validate_file(stale, lease, selected, code=7)
    assert struct.unpack('>9Q', v.call(1))[1] == writes and target.read_bytes().startswith(b'new-head')
    check(True, 'retirement preserves existing files and committed validation replay while rejecting a new old-origin 304 write')
    current_origin = validating.watermark()
    fresh = validation_packet(651, revision=4, token=2, before=b'new-head', after=b'nexthead', origin=current_origin)
    assert v.validate_file(fresh, lease, selected)[:9] == b'\x02' + struct.pack('>Q', 6)
    assert target.read_bytes().startswith(b'nexthead')
    os.close(lease); os.close(selected)
    check(True, 'a current request may validate an unrelated old entity without changing its full-entity origin watermark')

    source = validation_temp / 'late-prepare'; source.write_bytes(b'unmodified')
    destination = validation_cache / 'a/00/654'
    lease = v.lease(str(destination)); fd = os.open(source, os.O_RDWR | os.O_CLOEXEC)
    for origin in (validation_origin, b'legacy-no-watermark'):
        payload = record(654, root=str(validation_cache), zone='zone-a', metadata=origin) + string(str(source))
        v.prepare_file(payload, [lease, fd], code=7)
        assert not os.listxattr(fd)
    v.call(2, identity(654), code=1)
    payload = record(654, root=str(validation_cache), zone='zone-a', metadata=current_origin) + string(str(source))
    v.prepare_file(payload, [lease, fd]); source.rename(destination)
    assert v.finish_file(654, lease)[-1] == 2
    os.close(fd); os.close(lease)
    check(True, 'queued old or unknown PREPARE cannot resurrect retired work; a current full entity remains publishable')

    target, lease, selected = make_validation_file(660, '/remove', metadata=current_origin)
    original = v.call(2, identity(660)); contents = target.read_bytes()
    v.remove_file(660, lease, revision=1, code=7)
    assert v.call(2, identity(660)) == original and target.read_bytes() == contents
    removed = v.remove_file(660, lease)
    assert removed == struct.pack('>I', 1) + identity(660) + struct.pack('>Q', 3) + b'\x03'
    assert not target.exists() and os.pread(selected, len(contents), 0) == contents
    v.inspect_file((str(validation_cache), 'zone-a', 'a/00/660'), selected, code=7)
    assert v.remove_file(660, lease, revision=3)[:4] == struct.pack('>I', 2)
    assert v.page(1, 'https://a.test/remove')[0] == [] and v.call(2, identity(660))[0] == 3
    os.close(lease); os.close(selected)
    check(True, 'REMOVE_FILE retires before unlink, refuses stale revisions and retains metadata for subsequent node/task cleanup')

    target, lease, selected = make_validation_file(661, '/remove-missing', metadata=current_origin)
    target.unlink()
    assert v.remove_file(661, lease)[:4] == struct.pack('>I', 2)
    assert v.call(2, identity(661))[0] == 3
    os.close(lease); os.close(selected)
    check(True, 'an already evicted target is retired and its absent directory entry is synchronized')

    target, lease, selected = make_validation_file(662, '/remove-retry', metadata=current_origin)
    original = v.call(2, identity(662))
    target.parent.chmod(0o500)
    try:
        v.remove_file(662, lease, code=64)
        assert target.read_bytes().startswith(b'old-head') and v.call(2, identity(662))[0] == 3
    finally:
        target.parent.chmod(0o700)
    assert v.remove_file(662, lease, revision=3)[:4] == struct.pack('>I', 1)
    assert not target.exists()
    os.close(lease); os.close(selected)
    check(True, 'unlink failure leaves a durable retired record and a later retry completes physical removal')

    target, lease, selected = make_validation_file(663, '/remove-link', metadata=current_origin)
    saved = target.with_name('saved-663'); target.rename(saved); target.symlink_to(saved)
    original = v.call(2, identity(663))
    v.remove_file(663, lease, code=64)
    assert v.call(2, identity(663)) == original and saved.read_bytes().startswith(b'old-head')
    target.unlink(); saved.rename(target)
    hardlink = target.with_name('hard-663'); os.link(target, hardlink)
    v.remove_file(663, lease, code=4)
    assert v.call(2, identity(663)) == original
    hardlink.unlink()
    wrong = v.lease(str(target.with_name('wrong-lease')))
    v.remove_file(663, wrong, code=6); os.close(wrong)
    assert target.read_bytes().startswith(b'old-head') and v.call(2, identity(663)) == original
    assert v.remove_file(663, lease)[:4] == struct.pack('>I', 1)
    os.close(lease); os.close(selected)
    check(True, 'symlinks and multiple hardlinks are refused; another lease cannot bypass the held path lease')

    target, lease, selected = make_validation_file(664, '/remove-replaced', metadata=current_origin)
    os.close(selected)
    source = validation_temp / 'new-665'; source.write_bytes(b'new replacement')
    fd = os.open(source, os.O_RDWR | os.O_CLOEXEC)
    payload = record(665, root=str(validation_cache), zone='zone-a', uri='/remove-replaced',
                     path='a/00/664', metadata=current_origin)
    v.prepare_file(payload[:-16] + identity(664) + string(str(source)), [lease, fd])
    v.remove_file(664, lease, code=6)
    source.rename(target); v.finish_file(665, lease); os.close(fd)
    replacement = v.call(2, identity(665))
    assert v.remove_file(664, lease, revision=3)[:4] == struct.pack('>I', 3)
    assert target.read_bytes() == b'new replacement' and v.call(2, identity(665)) == replacement
    os.close(lease)
    check(True, 'an unfinished replacement blocks retirement and removing its old instance never unlinks the committed replacement')

    target = validation_cache / 'a/00/651'
    lease = v.lease(str(target))
    assert v.remove_file(651, lease, revision=6)[:4] == struct.pack('>I', 1)
    os.close(lease)
    target = validation_cache / 'a/00/652'
    lease = v.lease(str(target))
    assert v.remove_file(652, lease, revision=2)[:4] == struct.pack('>I', 1)
    os.close(lease)
    check(True, 'physical removal accepts completed validation receipts and retired PREPARED-marker candidates')
    v.close(); validating.stop()
finally:
    for component in processes:
        if component.process.poll() is None:
            component.process.kill()
            component.process.wait(timeout=5)
        component.stderr.close()

result = {'checks': checks, 'count': len(checks), 'root': str(root),
    'scope': 'component/index/ledger IPC, same-filesystem PREPARE/FINISH and automatic recovery; this fixture does not prove native read admission, immediate invalidation or background purge'}
args.output.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result), flush=True)
