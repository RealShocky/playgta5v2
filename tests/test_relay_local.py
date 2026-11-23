"""End-to-end test for the in-process /relay WebSocket server in serve_local.py
(protocol v2: one big open room, optional access code, vehicle mode).
Uses raw sockets (no third-party deps). Verifies the exchange the browser
handshake (launcher_player_flow.js) and the GTAMultiplayerGame client rely on:
check, join, welcome, pose fan-out to many peers, left, and key handling.
Run: py tests/test_relay_local.py
"""
import base64, json, os, socket, struct, sys, threading, time, uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import serve_local as S

PORT = 8917
BUILD = '8b0b5899ed-replica-20261009-v1'


def ws_connect():
    s = socket.create_connection(('127.0.0.1', PORT), timeout=5)
    key = base64.b64encode(uuid.uuid4().bytes).decode()
    req = (f'GET /relay HTTP/1.1\r\nHost: 127.0.0.1:{PORT}\r\n'
           f'Upgrade: websocket\r\nConnection: Upgrade\r\n'
           f'Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n')
    s.sendall(req.encode())
    head = b''
    while b'\r\n\r\n' not in head:
        head += s.recv(4096)
    assert b'101' in head.split(b'\r\n')[0], f'upgrade failed: {head[:120]!r}'
    return s


def ws_send(s, obj):
    payload = json.dumps(obj).encode()
    mask = os.urandom(4)
    header = bytes([0x81])
    n = len(payload)
    if n < 126:
        header += bytes([0x80 | n])
    elif n < 65536:
        header += bytes([0x80 | 126]) + struct.pack('>H', n)
    else:
        header += bytes([0x80 | 127]) + struct.pack('>Q', n)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    s.sendall(header + mask + masked)


def ws_recv(s, timeout=5):
    s.settimeout(timeout)
    head = b''
    while len(head) < 2:
        head += s.recv(2 - len(head))
    op = head[0] & 0x0F
    length = head[1] & 0x7F
    if length == 126:
        length = struct.unpack('>H', s.recv(2))[0]
    elif length == 127:
        length = struct.unpack('>Q', s.recv(8))[0]
    payload = b''
    while len(payload) < length:
        payload += s.recv(length - len(payload))
    return op, payload


def recv_json(s, timeout=5):
    op, payload = ws_recv(s, timeout)
    if op == 0x8:
        code = struct.unpack('>H', payload[:2])[0]
        raise AssertionError(f'close frame code={code} reason={payload[2:].decode(errors="replace")}')
    return json.loads(payload.decode())


def start_server(code, capacity=4):
    S.RELAY = S.Relay(code, capacity=capacity)
    server = S.ThreadingHTTPServer(('127.0.0.1', PORT), S.Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    time.sleep(0.3)
    return server


def main():
    failures = []

    def check(name, cond):
        print(('PASS' if cond else 'FAIL'), '-', name)
        if not cond:
            failures.append(name)

    # ---- A. Open room (no code) ----
    server = start_server('')
    # Empty room -> check returns checked players:0 capacity:1024 open:true
    b = ws_connect()
    ws_send(b, {'type': 'check', 'protocol': 2, 'key': '', 'room': 'open', 'build': BUILD, 'map': 'gta5'})
    m = recv_json(b)
    check('open-room check players:0 open:true', m.get('type') == 'checked' and m.get('players') == 0 and m.get('open') is True and m.get('capacity') == 4)
    b.close()

    # Four joins all succeed (capacity=4): each gets welcome with prior peer count
    sockets = []
    peer_ids = []
    welcome_players = []
    for i in range(4):
        p = ws_connect()
        ws_send(p, {'type': 'join', 'protocol': 2, 'key': '', 'room': 'open', 'build': BUILD, 'map': 'gta5'})
        w = recv_json(p)
        sockets.append(p)
        peer_ids.append(w.get('id'))
        welcome_players.append((w.get('type'), w.get('players')))
    check('open room admits 4 peers (no 2-player cap)', welcome_players == [('welcome', 0), ('welcome', 1), ('welcome', 2), ('welcome', 3)])

    # Fifth join -> capacity (4) hit: close 1008 Room full
    p5 = ws_connect()
    ws_send(p5, {'type': 'join', 'protocol': 2, 'key': '', 'room': 'open', 'build': BUILD, 'map': 'gta5'})
    op, payload = ws_recv(p5, timeout=5)
    check('room-full at capacity 4 -> 1008', op == 0x8 and struct.unpack('>H', payload[:2])[0] == 1008)
    p5.close()

    # Pose fan-out: peer0 sends a pose; each of the other 3 should get a pose frame.
    ws_send(sockets[0], {'type': 'pose', 'seq': 1, 'model': 0, 'mode': 'vehicle', 'position': [1.0, 2.0, 3.0], 'rotation': [0, 0, 0]})
    got = 0
    for i in (1, 2, 3):
        try:
            m = recv_json(sockets[i], timeout=3)
            if m.get('type') == 'pose' and m.get('seq') == 1 and m.get('mode') == 'vehicle' and m.get('position') == [1.0, 2.0, 3.0]:
                got += 1
        except AssertionError:
            pass
    check('pose fanned out to all 3 other peers (vehicle mode)', got == 3)

    # Chat forwarding: sockets[0] sends chat; 1,2,3 receive it tagged with id0.
    ws_send(sockets[0], {'type': 'chat', 'text': 'hello room'})
    chat_ok = 0
    for i in (1, 2, 3):
        try:
            m = recv_json(sockets[i], timeout=3)
            if m.get('type') == 'chat' and m.get('text') == 'hello room' and m.get('id') == peer_ids[0]:
                chat_ok += 1
        except AssertionError:
            pass
    check('chat forwarded to all 3 other peers', chat_ok == 3)

    # Shoot forwarding: sockets[0] sends shoot; 1,2,3 receive it tagged with id0.
    ws_send(sockets[0], {'type': 'shoot', 'seq': 7, 'position': [5.0, 6.0, 7.0]})
    shoot_ok = 0
    for i in (1, 2, 3):
        try:
            m = recv_json(sockets[i], timeout=3)
            if m.get('type') == 'shoot' and m.get('seq') == 7 and m.get('position') == [5.0, 6.0, 7.0] and m.get('id') == peer_ids[0]:
                shoot_ok += 1
        except AssertionError:
            pass
    check('shoot forwarded to all 3 other peers', shoot_ok == 3)

    # Hit targeting: sockets[0] sends hit aimed at peer_ids[1]; sockets[1] receives it, sockets[2] does not.
    ws_send(sockets[0], {'type': 'hit', 'seq': 3, 'target': peer_ids[1], 'dmg': 12.5})
    hit_to_target = None
    for _ in range(3):
        try:
            m = recv_json(sockets[1], timeout=3)
            if m.get('type') == 'hit' and m.get('target') == peer_ids[1] and m.get('dmg') == 12.5:
                hit_to_target = m
                break
        except AssertionError:
            pass
    check('hit delivered to the targeted peer', hit_to_target is not None and hit_to_target.get('id') == peer_ids[0])

    # Invalid chat (empty) -> close 1008
    bad = ws_connect()
    ws_send(bad, {'type': 'join', 'protocol': 2, 'key': '', 'room': 'open2', 'build': BUILD, 'map': 'gta5'})
    recv_json(bad)  # welcome
    ws_send(bad, {'type': 'chat', 'text': '   '})
    op, payload = ws_recv(bad, timeout=5)
    check('invalid chat closes with 1008', op == 0x8 and struct.unpack('>H', payload[:2])[0] == 1008)
    bad.close()

    for s in sockets:
        try:
            s.close()
        except OSError:
            pass
    server.shutdown()
    time.sleep(0.2)

    # ---- B. Locked room (code required) ----
    server = start_server('secret')
    # wrong/empty key rejected
    a = ws_connect()
    ws_send(a, {'type': 'check', 'protocol': 2, 'key': 'wrong', 'room': 'lock', 'build': BUILD, 'map': 'gta5'})
    op, payload = ws_recv(a, timeout=5)
    check('locked room rejects wrong key 1008', op == 0x8 and struct.unpack('>H', payload[:2])[0] == 1008)
    a.close()
    # correct key accepted
    c = ws_connect()
    ws_send(c, {'type': 'check', 'protocol': 2, 'key': 'secret', 'room': 'lock', 'build': BUILD, 'map': 'gta5'})
    m = recv_json(c)
    check('locked room accepts correct key', m.get('type') == 'checked' and m.get('open') is False)
    c.close()
    # old protocol v1 rejected as mismatch
    v1 = ws_connect()
    ws_send(v1, {'type': 'check', 'protocol': 1, 'key': 'secret', 'room': 'lock', 'build': BUILD, 'map': 'gta5'})
    op, payload = ws_recv(v1, timeout=5)
    check('protocol v1 now rejected (needs v2)', op == 0x8 and struct.unpack('>H', payload[:2])[0] == 1008)
    v1.close()
    server.shutdown()

    print('\n' + ('ALL RELAY TESTS PASSED' if not failures else f'FAILURES: {failures}'))
    sys.exit(1 if failures else 0)


if __name__ == '__main__':
    main()
