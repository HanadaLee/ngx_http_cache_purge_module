"""Worker-side IPC checks using the real component and a fault-injection peer."""
import concurrent.futures
import http.client
import json
import os
import signal
import socket
import socketserver
import struct
import tempfile
import threading
import time
from pathlib import Path


def run_client_cases(Run, root, checks):
    frames, lock = [], threading.Lock()
    gate, waiting = threading.Event(), threading.Event()

    def count(op):
        with lock:
            return sum(x == op for x in frames)

    def until(fn):
        end = time.monotonic() + 6
        while time.monotonic() < end:
            if fn():
                return
            time.sleep(.01)
        raise AssertionError('worker IPC deadline')

    class Peer(socketserver.BaseRequestHandler):
        def take(self, n):
            result = b''
            while len(result) < n:
                value = self.request.recv(n - len(result))
                if not value:
                    raise EOFError()
                result += value
            return result

        def handle(self):
            self.request.settimeout(3)
            try:
                while True:
                    magic, version, op, label, size, result = struct.unpack('>4sHHQII', self.take(24))
                    assert magic == b'NCP1' and version == 1 and result == 0 and size <= 524288
                    data = self.take(size)
                    with lock:
                        frames.append(op)
                    if op == 108:
                        return
                    if op == 109:
                        time.sleep(.4)
                    if op == 110:
                        waiting.set()
                        assert gate.wait(5)
                    response = struct.pack('>4sHHQII', b'FAIL' if op == 102 else magic,
                        2 if op == 104 else version, op + 1 if op == 105 else op,
                        label + 1 if op == 103 else label, 524289 if op == 106 else len(data), 0)
                    if op in (101, 107):
                        for byte in response:
                            self.request.sendall(bytes([byte]))
                            time.sleep(.03 if op == 107 else .001)
                        for start in range(0, len(data), 4096):
                            self.request.sendall(data[start:start + 4096])
                    else:
                        self.request.sendall(response + data)
            except (EOFError, ConnectionResetError, BrokenPipeError, TimeoutError):
                pass

    directory = Path(tempfile.mkdtemp(prefix='client-peer-', dir=root))
    directory.chmod(0o755)
    path = directory / 'peer.sock'
    server = socketserver.ThreadingUnixStreamServer(str(path), Peer)
    server.daemon_threads = True
    path.chmod(0o666)  # Isolate SO_PEERCRED from filesystem access in UID tests.
    threading.Thread(target=server.serve_forever, daemon=True).start()
    case = Run(workers=1, rpc_socket=path)
    owner = case.start()

    def rpc(uri, body=b'', connection=None):
        own = connection is None
        client = connection or http.client.HTTPConnection('127.0.0.1', case.port, timeout=5)
        try:
            client.request('POST', uri, body=body)
            response = client.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            if own:
                client.close()

    def stats():
        status, _, body = rpc('/rpc-fake/?stats=1')
        assert status == 200
        return json.loads(body)

    def text(value):
        data = value.encode()
        return struct.pack('>I', len(data)) + data

    try:
        value = rpc('/rpc/')
        assert value[0] == 200 and int(value[1]['X-IPC-Transport']) == 0
        assert struct.unpack('>9Q', value[2])[0] == owner
        checks.append('HTTP worker asynchronously queries the real agent with verified server UID')

        payload = text('worker-client-op') + b'\x01' + struct.pack('>Q', 1000) + b'\x02' + text('/worker-test/') + text('') + text('probe')
        receipt = rpc('/rpc/?op=14', payload)
        assert receipt[0] == 200 and receipt[1]['X-IPC-Result'] == '0'
        assert receipt[2][len(payload) + 40] == 1  # Internal PREPARING, never public 202.
        assert rpc('/rpc/?op=15', text('worker-client-op'))[2] == receipt[2]
        assert rpc('/rpc/?op=14', payload)[2] == receipt[2]
        conflict = rpc('/rpc/?op=14', payload[:-5] + b'other')
        assert conflict[0] == 200 and conflict[1]['X-IPC-Result'] == '7'
        checks.append('worker IPC persists and retrieves an idempotent PREPARING operation and preserves application errors')

        connection = http.client.HTTPConnection('127.0.0.1', case.port, timeout=5)
        try:
            for i in range(8):
                data = ('round-' + str(i)).encode()
                assert rpc('/rpc-fake/?op=100', data, connection)[2] == data
            assert rpc('/rpc-fake/?op=100', b'', connection)[2] == b''
        finally:
            connection.close()
        assert stats()['connections'] == 1
        checks.append('sequential worker calls reuse one Unix connection without retaining completed buffers')

        data = bytes(range(256)) * 128
        assert rpc('/rpc-fake/?op=101', data)[2] == data
        checks.append('fragmented headers and binary response payloads are assembled without truncation')

        for op, expected in ((102, 3), (103, 3), (104, 3), (105, 3), (106, 4)):
            value = rpc('/rpc-fake/?op=' + str(op), b'probe')
            assert value[0] == 503 and int(value[1]['X-IPC-Transport']) == expected, value
            assert value[1]['X-IPC-Ambiguous'] == '1'
            assert rpc('/rpc-fake/?op=100', b'reconnected')[2] == b'reconnected'
        checks.append('bad magic, correlation, version, opcode and oversized response close the channel and allow later calls')

        before = count(108)
        value = rpc('/rpc-fake/?op=108', b'possible-mutation')
        assert value[0] == 503 and value[1]['X-IPC-Ambiguous'] == '1' and count(108) == before + 1
        checks.append('lost response after send is ambiguous and never automatically replayed')

        started = time.monotonic()
        value = rpc('/rpc-fake/?op=107&timeout=100', b'trickle')
        assert value[0] == 504 and value[1]['X-IPC-Ambiguous'] == '1'
        assert time.monotonic() - started < .6
        checks.append('partial progress does not extend the absolute call deadline')

        before = count(109)
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            pending = pool.submit(rpc, '/rpc-fake/?op=109', b'first')
            until(lambda: count(109) == before + 1)
            sent = count(100)
            value = rpc('/rpc-fake/?op=100&timeout=30', b'queued')
            assert value[0] == 504 and value[1]['X-IPC-Ambiguous'] == '0'
            assert pending.result()[2] == b'first' and count(100) == sent
        checks.append('queued timeout is unsent and leaves the active call intact')

        assert case.get('/other')[0] == 200
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            pending = pool.submit(rpc, '/rpc-fake/?op=110', b'gate')
            until(waiting.is_set)
            assert case.get('/other')[:2] == (200, 'HIT') and not pending.done()
            gate.set()
            assert pending.result()[2] == b'gate'
        checks.append('a single HTTP worker continues serving cache HITs while its IPC peer is blocked')

        with concurrent.futures.ThreadPoolExecutor(6) as pool:
            futures = [pool.submit(rpc, '/rpc-fake/?op=109&timeout=3000', f'bounded-{i}'.encode()) for i in range(6)]
            replies = [f.result() for f in futures]
        assert sorted(v[0] for v in replies) == [200, 200, 200, 200, 503, 503]
        assert all(value[2] == f'bounded-{i}'.encode() for i, value in enumerate(replies) if value[0] == 200)
        checks.append('pending-call capacity rejects excess work without unbounded queuing')

        before = count(109)
        large = b'x' * 400000
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            pending = pool.submit(rpc, '/rpc-fake/?op=109', large)
            until(lambda: count(109) == before + 1)
            assert rpc('/rpc-fake/?op=100', b'y' * 200000)[0] == 503
            assert pending.result()[2] == large
        checks.append('request byte budget applies independently of count and large frames preserve binary content')

        before = stats()['cancelled']
        seen = count(109)
        raw = socket.create_connection(('127.0.0.1', case.port))
        raw.sendall(b'GET /rpc-fake/?op=109 HTTP/1.1\r\nHost: probe.test\r\n\r\n')
        until(lambda: count(109) == seen + 1)
        raw.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        raw.close()
        until(lambda: stats()['cancelled'] == before + 1)
        assert rpc('/rpc-fake/?op=100', b'after-cancel')[2] == b'after-cancel'
        checks.append('HTTP cancellation closes its active call, suppresses callback and permits a new connection')

        before = stats()['cancelled']
        seen = count(109)
        sent = count(100)
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            pending = pool.submit(rpc, '/rpc-fake/?op=109', b'queue-cancel')
            until(lambda: count(109) == seen + 1)
            raw = socket.create_connection(('127.0.0.1', case.port))
            raw.sendall(b'GET /rpc-fake/?op=100 HTTP/1.1\r\nHost: probe.test\r\n\r\n')
            until(lambda: stats()['pending'] == 2)
            raw.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
            raw.close()
            until(lambda: stats()['cancelled'] == before + 1)
            assert pending.result()[2] == b'queue-cancel' and count(100) == sent
        checks.append('cancelling a queued request neither sends it nor interrupts the active frame')

        value = rpc('/rpc-wrong/?op=100', b'wrong-owner')
        assert value[0] == 503 and value[1]['X-IPC-Transport'] == '5' and value[1]['X-IPC-Ambiguous'] == '0'
        value = rpc('/rpc-missing/')
        assert value[0] == 503 and value[1]['X-IPC-Ambiguous'] == '0'
        checks.append('wrong peer UID and connect failure fail before sending a frame')

        old_connections = stats()['connections']
        time.sleep(1.1)
        assert rpc('/rpc-fake/?op=100', b'new-idle-connection')[0] == 200
        assert stats()['connections'] == old_connections + 1
        checks.append('idle connections expire and reopen on demand')

        killed_owner = owner
        os.kill(owner, signal.SIGKILL)
        owner = case.ready(owner)
        until(lambda: rpc('/rpc/')[2][:8] == struct.pack('>Q', owner))
        assert rpc('/rpc/?op=15', text('worker-client-op'))[2] == receipt[2]
        checks.append('agent respawn reconnects the worker client and preserves the durable operation')

        final = stats()
        assert final['pending'] == final['request_bytes'] == final['response_bytes'] == 0, final
        assert final['accepted'] == final['completed'] + final['cancelled'], final
        assert final['peak_bytes'] <= 524312 + 524288, final
        checks.append('completed/cancelled accounting balances and buffers return to zero within the configured bound')

        seen = count(109)
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            first = pool.submit(rpc, '/rpc-fake/?op=109', b'stop-active')
            until(lambda: count(109) == seen + 1)
            second = pool.submit(rpc, '/rpc-fake/?op=100', b'stop-queued')
            until(lambda: stats()['pending'] == 2)
            assert rpc('/rpc-fake/?stop=1')[2] == b'stopped'
            assert first.result()[1]['X-IPC-Transport'] == second.result()[1]['X-IPC-Transport'] == '6'
        final = stats()
        assert final['pending'] == final['request_bytes'] == final['response_bytes'] == 0
        assert final['accepted'] == final['completed'] + final['cancelled']
        checks.append('explicit client stop completes active and queued calls once and releases their buffers')

        # Recreate the stopped test client in a new worker before testing a
        # further graceful reload with an outstanding RPC.
        os.kill(case.process.pid, signal.SIGHUP)
        owner = case.ready(owner)

        before = count(109)
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            pending = pool.submit(rpc, '/rpc-fake/?op=109', b'reload')
            until(lambda: count(109) == before + 1)
            os.kill(case.process.pid, signal.SIGHUP)
            assert pending.result()[2] == b'reload'
        owner = case.ready(owner)
        assert rpc('/rpc/?op=15', text('worker-client-op'))[2] == receipt[2]
        checks.append('HUP drains the old worker call and new workers reconnect to the replacement agent')
    finally:
        gate.set()
        case.stop()
        server.shutdown()
        server.server_close()
        path.unlink()
    logs = (case.path / 'error.log').read_text() + (case.path / 'stderr.log').read_text()
    expected = f'privileged agent process {killed_owner} exited on signal 9'
    assert logs.count(expected) == 1
    unexpected = '\n'.join(line for line in logs.splitlines() if expected not in line)
    assert '[alert]' not in unexpected and 'AddressSanitizer' not in unexpected and 'runtime error:' not in unexpected, case.path
    return {'runtime_path': str(case.path), 'final_stats_before_reload': final,
            'fixture_pending_limit': 4, 'request_byte_limit': 524312,
            'response_byte_limit': 524288}
