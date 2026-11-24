"""Local mirror server: isolation headers, byte ranges, and engine batch I/O."""
import argparse, base64, gzip, hashlib, hmac, io, json, math, mimetypes, re, socket, string, struct, threading, time, uuid, webbrowser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, unquote, parse_qs

ROOT = Path(__file__).resolve().parent / 'mirror' / 'playgta5.com'
SUPPORT = Path(__file__).resolve().parent / 'support'
if not (ROOT / 'index.html').is_file():
    nested_root = ROOT.parent / 'mirror' / 'playgta5.com'
    if (nested_root / 'index.html').is_file():
        ROOT = nested_root
mimetypes.add_type('application/wasm', '.wasm')
mimetypes.add_type('text/javascript', '.js')

def improve_game_page(code):
    """Improve support text at serving/build time, preserving verified release files."""
    if 'game-progress' not in code:
        return code
    if 'name="viewport"' not in code:
        code = code.replace('<head>', '<head><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">', 1)
    # Mobile taps must not call an absent pointer-lock API. Relative camera input
    # comes from the touch pad instead; desktop pointer lock remains unchanged.
    code = code.replace('if (!locked()) { canvas.requestPointerLock({ unadjustedMovement: true }).catch(() => canvas.requestPointerLock()); }',
                        'if (!locked() && canvas.requestPointerLock && !matchMedia("(pointer:coarse)").matches) { Promise.resolve(canvas.requestPointerLock({ unadjustedMovement: true })).catch(() => Promise.resolve(canvas.requestPointerLock()).catch(() => {})); }')
    code = code.replace(': the device may be out of memory. Close other tabs, or reload.',
                        ': loading has paused. Use Help / browser checks and collect the first console error.')
    code = code.replace('If the tab shows "Aw, Snap!" or SIGILL it ran out of memory.',
                        'A tab crash needs its error details; it does not by itself prove insufficient RAM.')
    link = (SUPPORT / 'overlay.html').read_text(encoding='utf-8')
    marker = 'Atomics.store(w, MY, H >> 1);'
    if code.count(marker) == 1:
        code = code.replace('<head>', '<head><script src="/__support/controller.js"></script><script defer src="/__support/touch.js"></script>', 1)
        code = code.replace(marker, marker + '''
if (!window.PlayGTA5Enhanced) {
    const mode = document.createElement('button');
    mode.setAttribute('data-game-overlay', '');
    mode.title = 'Click to switch on-foot/driving controls; View + Menu also switches';
    mode.style.cssText = 'position:fixed;right:12px;bottom:12px;z-index:10003;max-width:48vw;min-height:44px;padding:10px 14px;background:#182621e8;color:white;border:1px solid #789184;border-radius:24px;font:14px system-ui;cursor:pointer';
    mode.hidden = true;
    document.body.append(mode);
    const controller = PlayGTA5Controller.attach({keys,w,VK,canvas,status:text => {
        mode.hidden = text === 'Press a controller button to connect';
        if (mode.textContent !== text) mode.textContent = text;
    }});
    mode.onclick = () => controller.toggleMode();
}
''', 1)
    return code.replace('<body>', '<body>\n' + link, 1)

# --- WebSocket / multiplayer relay (protocol v1, mirrors LauncherApp/rust/src/relay.rs) ---
_WS_GUID = b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11'

def _ws_accept(key):
    return base64.b64encode(hashlib.sha1(key.encode('latin1') + _WS_GUID).digest()).decode('ascii')

def _ws_send_frame(sock, payload, opcode=0x1):
    if isinstance(payload, str):
        payload = payload.encode('utf-8')
    length = len(payload)
    if length < 126:
        header = bytes([0x80 | opcode, length])
    elif length < 65536:
        header = bytes([0x80 | opcode, 126]) + struct.pack('>H', length)
    else:
        header = bytes([0x80 | opcode, 127]) + struct.pack('>Q', length)
    sock.sendall(header + payload)

def _ws_recv_exact(sock, n):
    data = b''
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError('closed')
        data += chunk
    return data

def _ws_recv_frame(sock):
    head = _ws_recv_exact(sock, 2)
    opcode = head[0] & 0x0F
    masked = head[1] & 0x80
    length = head[1] & 0x7F
    if length == 126:
        length = struct.unpack('>H', _ws_recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack('>Q', _ws_recv_exact(sock, 8))[0]
    mask = _ws_recv_exact(sock, 4) if masked else b'\x00' * 4
    payload = bytearray(_ws_recv_exact(sock, length)) if length else b''
    if masked:
        for i in range(len(payload)):
            payload[i] ^= mask[i % 4]
    return opcode, bytes(payload)

def _ws_send_close(sock, code, reason):
    try:
        _ws_send_frame(sock, struct.pack('>H', code) + reason.encode('utf-8'), 0x8)
    except OSError:
        pass

def _is_identifier(v, max_len, dots):
    if not isinstance(v, str) or not v or len(v) > max_len:
        return False
    allowed = set(string.ascii_letters + string.digits + ('_.-' if dots else '_-'))
    return all(c in allowed for c in v)

PROTOCOL = 2
DEFAULT_CAPACITY = 1024
POSE_HZ = 20


def _vec_ok(arr, limit):
    return (isinstance(arr, list) and len(arr) == 3 and
            all(isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) and abs(x) <= limit for x in arr))


def _is_name(v):
    return (isinstance(v, str) and 1 <= len(v) <= 24 and
            all(c.isalnum() or c in ' _-.(' for c in v)) and v.strip() != ''


def _snapshot(v):
    if v.get('type') != 'pose':
        raise ValueError('Unsupported message')
    seq = v.get('seq')
    if not (isinstance(seq, int) and not isinstance(seq, bool) and 0 <= seq <= 2147483647):
        raise ValueError('Invalid sequence')
    model = v.get('model', 0)
    if not (isinstance(model, int) and not isinstance(model, bool) and 0 <= model <= 0xFFFFFFFF):
        raise ValueError('Invalid model')
    mode = v.get('mode', 'foot')
    if mode not in ('foot', 'vehicle'):
        raise ValueError('Invalid mode')
    position, rotation = v.get('position'), v.get('rotation')
    if not _vec_ok(position, 20000.0):
        raise ValueError('Invalid position')
    if not _vec_ok(rotation, 360.0):
        raise ValueError('Invalid rotation')
    hp = v.get('hp', 100)
    if not (isinstance(hp, (int, float)) and not isinstance(hp, bool) and 0 <= hp <= 100):
        raise ValueError('Invalid hp')
    name = v.get('name')
    if name is not None and not _is_name(name):
        raise ValueError('Invalid name')
    snap = {'type': 'pose', 'seq': seq, 'model': model, 'mode': mode, 'position': position,
            'rotation': rotation, 'hp': hp, 'down': v.get('down') is True}
    if name is not None:
        snap['name'] = name.strip()
    return snap


def _forwarded(v):
    """Validate and normalise non-pose room messages: shoot, hit, down, respawn, chat."""
    if not isinstance(v, dict):
        raise ValueError('Unsupported message')
    t = v.get('type')
    if t == 'shoot':
        if not _vec_ok(v.get('position'), 20000.0):
            raise ValueError('Invalid position')
        seq = v.get('seq')
        if not (isinstance(seq, int) and not isinstance(seq, bool) and 0 <= seq <= 2147483647):
            raise ValueError('Invalid sequence')
        return {'type': 'shoot', 'seq': seq, 'position': v['position']}
    if t == 'hit':
        target = v.get('target')
        if not isinstance(target, str) or not (1 <= len(target) <= 24):
            raise ValueError('Invalid target')
        dmg = v.get('dmg')
        if not (isinstance(dmg, (int, float)) and not isinstance(dmg, bool) and 0 < dmg <= 100):
            raise ValueError('Invalid damage')
        seq = v.get('seq')
        if not (isinstance(seq, int) and not isinstance(seq, bool) and 0 <= seq <= 2147483647):
            raise ValueError('Invalid sequence')
        return {'type': 'hit', 'seq': seq, 'target': target, 'dmg': dmg}
    if t == 'chat':
        text = v.get('text')
        if not isinstance(text, str) or not text.strip() or len(text) > 200:
            raise ValueError('Invalid chat')
        return {'type': 'chat', 'text': text[:200]}
    if t in ('down', 'respawn'):
        return {'type': t}
    raise ValueError('Unsupported message')


class Relay:
    """One big open room. The engine's mailbox only exposes player position and a
    single replica/vehicle command channel, so the relay carries every peer's
    position (and foot/vehicle mode); each client renders the closest peer as a
    replica. Combat (bullets/health) is not exposed by the engine.

    Access code is optional: if ``code`` is non-empty the client must present the
    same key; if empty, the room is open to everyone. The 2-player cap is gone.
    """

    def __init__(self, code, capacity=DEFAULT_CAPACITY):
        self.code = code
        self.capacity = max(1, int(capacity))
        self.lock = threading.Lock()
        self.rooms = {}

    def _close_with(self, sock, code, reason):
        _ws_send_close(sock, code, reason)

    def _key_ok(self, hello_key):
        if not self.code:
            return True
        return isinstance(hello_key, str) and hmac.compare_digest(hello_key, self.code)

    def _broadcast(self, room_name, sender_id, data):
        """Send ``data`` bytes to every other live peer in the room."""
        with self.lock:
            others = [p['sock'] for pid, p in self.rooms.get(room_name, {}).get('peers', {}).items() if pid != sender_id and p.get('alive')]
        for ps in others:
            try:
                _ws_send_frame(ps, data)
            except OSError:
                pass

    def status(self):
        """Live room overview for /__mp/ and diagnostics."""
        with self.lock:
            out = {'open': not self.code, 'capacity': self.capacity, 'rooms': {}}
            for name, room in self.rooms.items():
                peers = []
                for pid, p in room['peers'].items():
                    pose = p.get('pose') or {}
                    peers.append({'id': pid, 'name': pose.get('name') or None, 'hp': pose.get('hp'), 'mode': pose.get('mode'), 'pos': pose.get('position'), 'alive': bool(p.get('alive'))})
                out['rooms'][name] = {'build': room.get('build'), 'map': room.get('map'), 'players': len(peers), 'peers': peers}
            return out

    def log(self, msg):
        try:
            print('[relay] ' + str(msg), flush=True)
        except Exception:
            pass

    def serve_session(self, sock, hello):
        hello = hello if isinstance(hello, dict) else {}
        error = None
        if hello.get('type') not in ('check', 'join') or hello.get('protocol') != PROTOCOL:
            error = 'Protocol mismatch'
        elif not self._key_ok(hello.get('key')):
            error = 'Join key rejected'
        elif not _is_identifier(hello.get('room'), 32, False):
            error = 'Invalid room'
        elif not _is_identifier(hello.get('build'), 96, True) or hello.get('map') not in ('gta5', 'env_test'):
            error = 'Invalid build/map'
        if error:
            self._close_with(sock, 1008, error)
            return
        checked = hello['type'] == 'check'
        room_name, build, mapp = hello['room'], hello['build'], hello['map']
        with self.lock:
            room = self.rooms.get(room_name)
            if room and (room['build'] != build or room['map'] != mapp):
                error = 'Room build/map mismatch'
            elif room and len(room['peers']) >= self.capacity:
                error = 'Room full'
            else:
                players = 0 if room is None else len(room['peers'])
        if error:
            self._close_with(sock, 1008, error)
            return
        if checked:
            try:
                _ws_send_frame(sock, json.dumps({'type': 'checked', 'players': players, 'capacity': self.capacity, 'open': not self.code}))
            except OSError:
                return
            self._close_with(sock, 1000, 'Check complete')
            return
        peer_id = uuid.uuid4().hex[:16]
        with self.lock:
            room = self.rooms.setdefault(room_name, {'build': build, 'map': mapp, 'peers': {}})
            existing = [(pid, room['peers'][pid]['pose']) for pid in room['peers']]
            room['peers'][peer_id] = {'sock': sock, 'pose': None, 'alive': True, 'poses': 0, 'last_pose_at': 0}
        self.log('join %s in room "%s" (%d peers, build=%s map=%s)' % (peer_id, room_name, len(existing), build, mapp))
        try:
            # Send the newcomer every other peer's latest pose so it can place them
            # immediately instead of waiting for the next tick.
            for pid, pose in existing:
                if pose is not None:
                    _ws_send_frame(sock, json.dumps(pose))
            _ws_send_frame(sock, json.dumps({'type': 'welcome', 'protocol': PROTOCOL, 'id': peer_id, 'players': len(existing), 'hz': POSE_HZ}))
        except OSError:
            self._remove(room_name, peer_id)
            return
        sock.settimeout(2)
        last_seq, tokens, last = None, 30.0, time.monotonic()
        ping_sent_at = None
        try:
            while True:
                try:
                    opcode, payload = _ws_recv_frame(sock)
                except socket.timeout:
                    if ping_sent_at is None:
                        _ws_send_frame(sock, b'', 0x9)
                        ping_sent_at = time.monotonic()
                    elif time.monotonic() - ping_sent_at > 2:
                        self._close_with(sock, 1008, 'Heartbeat timeout')
                        return
                    continue
                if opcode == 0x9:
                    _ws_send_frame(sock, payload, 0xA)
                elif opcode == 0xA:
                    ping_sent_at = None
                elif opcode == 0x8:
                    break
                elif opcode == 0x1:
                    now = time.monotonic()
                    tokens = min(30.0, tokens + (now - last) * 25.0)
                    last = now
                    if tokens < 1.0:
                        self._close_with(sock, 1008, 'Message rate exceeded')
                        return
                    tokens -= 1.0
                    try:
                        parsed = json.loads(payload.decode('utf-8'))
                    except json.JSONDecodeError:
                        parsed = None
                    if not isinstance(parsed, dict):
                        self._close_with(sock, 1008, 'Unsupported message')
                        return
                    mtype = parsed.get('type')
                    if mtype == 'pose':
                        try:
                            snap = _snapshot(parsed)
                        except (ValueError, TypeError, KeyError) as exc:
                            self._close_with(sock, 1008, str(exc))
                            return
                        if last_seq is not None and snap['seq'] <= last_seq:
                            continue
                        last_seq = snap['seq']
                        snap['id'] = peer_id
                        snap['seq'] = last_seq
                        data = json.dumps(snap).encode('utf-8')
                        with self.lock:
                            peer = self.rooms.get(room_name, {}).get('peers', {}).setdefault(peer_id, {})
                            peer.update(pose=snap, alive=True)
                            peer['poses'] = peer.get('poses', 0) + 1
                            peer['last_pose_at'] = time.monotonic()
                            if peer['poses'] in (1, 10, 100):
                                self.log('pose %s (room "%s") # %d pos=%s' % (peer_id, room_name, peer['poses'], snap['position']))
                        self._broadcast(room_name, peer_id, data)
                    else:
                        # shoot / hit / down / respawn / chat: validate and forward.
                        try:
                            fwd = _forwarded(parsed)
                        except (ValueError, TypeError, KeyError) as exc:
                            self._close_with(sock, 1008, str(exc))
                            return
                        fwd['id'] = peer_id
                        self._broadcast(room_name, peer_id, json.dumps(fwd).encode('utf-8'))
                else:
                    self._close_with(sock, 1008, 'Unsupported message')
                    return
        except (ConnectionError, OSError):
            pass
        finally:
            self._left(room_name, peer_id)

    def _remove(self, room_name, peer_id):
        with self.lock:
            room = self.rooms.get(room_name)
            if not room:
                return
            room['peers'].pop(peer_id, None)
            if not room['peers']:
                self.rooms.pop(room_name, None)

    def _left(self, room_name, peer_id):
        with self.lock:
            peer = self.rooms.get(room_name, {}).get('peers', {}).get(peer_id)
            poses = peer.get('poses', 0) if peer else 0
        self._remove(room_name, peer_id)
        self.log('leave %s (room "%s", poses=%d)' % (peer_id, room_name, poses))
        msg = json.dumps({'type': 'left', 'id': peer_id}).encode('utf-8')
        with self.lock:
            others = [p['sock'] for p in self.rooms.get(room_name, {}).get('peers', {}).values() if p.get('alive')]
        for ps in others:
            try:
                _ws_send_frame(ps, msg)
            except OSError:
                pass

def _load_access_code(base_dir):
    path = Path(base_dir) / 'multiplayer-access-code.txt'
    if path.is_file():
        code = path.read_text(encoding='utf-8').strip()
        if code:
            return code
    code = 'gtav-' + uuid.uuid4().hex[:8]
    try:
        path.write_text(code + '\n', encoding='utf-8')
    except OSError:
        pass
    return code

RELAY = None

class Handler(SimpleHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(30)

    def __init__(self, *args, **kwargs):
        self.byte_range = None
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def end_headers(self):
        self.send_header('Cross-Origin-Opener-Policy', 'same-origin')
        self.send_header('Cross-Origin-Embedder-Policy', 'require-corp')
        self.send_header('Cross-Origin-Resource-Policy', 'same-origin')
        self.send_header('Accept-Ranges', 'bytes')
        super().end_headers()

    def send_head(self):
        self.byte_range = None
        if urlsplit(self.path).path in ('/', '/index.html') and (ROOT / 'index.html').is_file():
            body = improve_game_page((ROOT / 'index.html').read_text(encoding='utf-8')).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            return io.BytesIO(body)
        support_files = {'/__support/': 'index.html', '/__support/check.js': 'check.js', '/__support/controller.js': 'controller.js', '/__support/touch.js': 'touch.js'}
        support_name = support_files.get(urlsplit(self.path).path)
        if support_name:
            path = SUPPORT / support_name
            if not path.is_file():
                self.send_error(404)
                return None
            self.send_response(200)
            self.send_header('Content-Type', self.guess_type(str(path)))
            self.send_header('Content-Length', str(path.stat().st_size))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            return path.open('rb')
        if not self.headers.get('Range'):
            return super().send_head()
        path = Path(self.translate_path(self.path))
        if not path.is_file():
            self.send_error(404)
            return None
        size = path.stat().st_size
        match = re.fullmatch(r'bytes=(\d*)-(\d*)', self.headers['Range'])
        if not match or not any(match.groups()):
            self.send_error(416)
            return None
        start = int(match[1]) if match[1] else max(0, size - int(match[2]))
        end = min(size - 1, int(match[2])) if match[1] and match[2] else size - 1
        if start >= size or end < start:
            self.send_response(416)
            self.send_header('Content-Range', 'bytes */%d' % size)
            self.send_header('Content-Length', '0')
            self.end_headers()
            return None
        self.byte_range = (start, end)
        self.send_response(206)
        self.send_header('Content-Type', self.guess_type(str(path)))
        self.send_header('Content-Range', 'bytes %d-%d/%d' % (start, end, size))
        self.send_header('Content-Length', str(end - start + 1))
        self.end_headers()
        source = path.open('rb')
        source.seek(start)
        return source

    def copyfile(self, source, outputfile):
        if self.byte_range is None:
            return super().copyfile(source, outputfile)
        remaining = self.byte_range[1] - self.byte_range[0] + 1
        while remaining:
            chunk = source.read(min(1024 * 1024, remaining))
            if not chunk:
                break
            outputfile.write(chunk)
            remaining -= len(chunk)

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == '/relay':
            upgrade = self.headers.get('Upgrade', '').lower() == 'websocket'
            # Log every /relay request so "is it even arriving?" is answerable from
            # the server console alone (this is the line that was missing before).
            print('[relay] GET /relay from %s (upgrade=%s, connection=%r)' % (
                self.client_address[0] if self.client_address else '?', upgrade,
                self.headers.get('Connection', '')), flush=True)
            if upgrade and RELAY is not None:
                self.handle_relay()
                return
            if RELAY is None:
                self.send_error(503, 'Relay not initialised')
                return
            # A non-WebSocket /relay request (e.g. curl, or a proxy that stripped the
            # Upgrade header) gets a clear 426 instead of a confusing 404 file-missing.
            body = json.dumps({'error': '/relay is a WebSocket endpoint; the Upgrade: websocket header is missing.', 'path': path}).encode('utf-8')
            self.send_response(426)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(body)
            return
        if path == '/__mp/' and RELAY is not None:
            body = json.dumps(RELAY.status()).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(body)
            return
        super().do_GET()

    def handle_relay(self):
        key = self.headers.get('Sec-WebSocket-Key', '')
        if not key:
            self.send_error(400, 'Missing Sec-WebSocket-Key')
            return
        self.close_connection = True
        response = ('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n'
                    'Connection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n' % _ws_accept(key))
        self.wfile.write(response.encode('ascii'))
        self.wfile.flush()
        sock = self.connection
        sock.settimeout(5)
        try:
            hello = None
            try:
                opcode, payload = _ws_recv_frame(sock)
                if opcode == 0x1:
                    hello = json.loads(payload.decode('utf-8'))
            except (socket.timeout, ConnectionError, OSError, json.JSONDecodeError):
                hello = None
            sock.settimeout(None)
            RELAY.serve_session(sock, hello)
        except OSError:
            pass
        finally:
            try:
                sock.close()
            except OSError:
                pass

    def do_POST(self):
        url = urlsplit(self.path)
        if url.path != '/data/batch':
            self.send_error(404)
            return
        try:
            length = int(self.headers.get('Content-Length', '0'))
        except ValueError:
            self.send_error(400, 'Invalid Content-Length')
            return
        if not 0 <= length <= 1024 * 1024:
            self.send_error(413, 'Invalid batch size')
            return
        try:
            runs = json.loads(self.rfile.read(length))
            if not isinstance(runs, list) or len(runs) > 1000:
                raise ValueError('invalid batch')
            selected = []
            data_root = (ROOT / 'data').resolve()
            for name, start, end in runs:
                if not isinstance(name, str) or type(start) is not int or type(end) is not int:
                    raise ValueError('invalid file/range types')
                file = (data_root / name).resolve()
                if not file.is_relative_to(data_root) or start < 0 or end < start:
                    raise ValueError('invalid file/range')
                selected.append((file, start, max(0, min(end + 1, file.stat().st_size) - start)))
            total = sum(n for _, _, n in selected)
            if total > 64 * 1024 * 1024:
                raise ValueError('batch exceeds 64 MiB')
            output = io.BytesIO()
            for file, start, n in selected:
                with file.open('rb') as source:
                    source.seek(start)
                    output.write(source.read(n))
            body = output.getvalue()
            compressed = parse_qs(url.query).get('gz') == ['1']
            if compressed:
                body = gzip.compress(body, compresslevel=1)
            self.send_response(200)
            self.send_header('Content-Type', 'application/octet-stream')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('X-Run-Lengths', ','.join(str(n) for _, _, n in selected))
            if compressed:
                self.send_header('Content-Encoding', 'gzip')
            self.end_headers()
            self.wfile.write(body)
        except (ValueError, TypeError, OSError) as exc:
            self.send_error(400, str(exc))

if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument('--port', type=int, default=8000)
    args.add_argument('--host', default='127.0.0.1', help='bind address; 0.0.0.0 to reach the port through a reverse proxy or from other devices (default 127.0.0.1)')
    args.add_argument('--root', type=Path, help='existing game directory containing index.html, b/, and data/')
    args.add_argument('--open', action='store_true', help='open browser compatibility and launch page')
    args.add_argument('--mp-open', action='store_true', help='one big open room: no access code required (default)')
    args.add_argument('--mp-code', help='require this access code to join (one code for the whole room)')
    args.add_argument('--mp-capacity', type=int, default=DEFAULT_CAPACITY, help='max players in a room (default 1024)')
    options = args.parse_args()
    if options.root:
        ROOT = options.root.resolve()
    if not (ROOT / 'index.html').is_file():
        args.error('Game files are missing. Run Download-Game.cmd (Windows) or Download-Game.command (Mac) first.')
    # Open room by default; --mp-code <code> locks the whole room behind one code.
    access_code = options.mp_code or ''
    RELAY = Relay(access_code, capacity=options.mp_capacity)
    try:
        server = ThreadingHTTPServer((options.host, options.port), Handler)
    except OSError as error:
        args.error(f'Cannot start on {options.host}:{options.port}: {error}. Close the previous game server with Ctrl+C, or choose --port 8001.')
    bind_host = 'localhost' if options.host in ('127.0.0.1', '::1') else options.host
    address = 'http://%s:%d/' % (bind_host, server.server_port)
    print('Local mirror: %s (bound to %s, Ctrl+C to stop)' % (address, options.host), flush=True)
    if access_code:
        print('Multiplayer relay: ws://%s:%d/relay  access code: %s  (same room + code on each PC, up to %d players)' % (
            bind_host, server.server_port, access_code, options.mp_capacity), flush=True)
    else:
        print('Multiplayer relay: ws://%s:%d/relay  OPEN room, no access code (up to %d players)' % (
            bind_host, server.server_port, options.mp_capacity), flush=True)
    if options.open:
        webbrowser.open(address + '__support/')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
