// Preview-only policy boundary. Do not substitute ISOLATED for deny-all egress.
// Audited against railway@3.16.0 dist/index.d.ts (see docs/PREVIEW_VM.md).
export const PREVIEW_UNAVAILABLE = 'Preview unavailable: railway 3.16.0 ISOLATED permits public NAT egress; '
  + 'the SDK has no enforceable runtime deny-egress policy. Configure a provider with '
  + 'deny-all IPv4/IPv6 runtime egress (including metadata and control-plane) before enabling previews.';

export function previewCapabilities() {
  return {available: false, reason: PREVIEW_UNAVAILABLE, privateTransport: 'exec-stdin-base64',
    runtimeEgress: false, suspend: false, quota: null};
}

export async function createPreview() {
  // Refuse BEFORE image preparation, source upload, or Sandbox.create. There is no
  // supported provider option to pass here; inventing one would silently allow NAT.
  throw new Error(PREVIEW_UNAVAILABLE);
}

// Private byte tunnel over the SDK's exec control channel, not execHttp, domains,
// port publication, or a control-plane HTTP request. The remote helper connects
// ONLY to the adapter-selected loopback port. HTTP (including upgrade) stays raw.
export const LOOPBACK_TUNNEL = String.raw`import base64, os, socket, sys, threading
try:
    s = socket.create_connection(('127.0.0.1', int(sys.argv[1])), timeout=15)
    s.settimeout(60)
except Exception:
    os._exit(1)
def incoming():
    try:
        while True:
            data = s.recv(49152)
            if not data:
                os._exit(0)
            print(base64.b64encode(data).decode('ascii'), flush=True)
    except Exception:
        # A timeout/reset is NOT clean EOF. Never return truncated success.
        os._exit(1)
threading.Thread(target=incoming, daemon=True).start()
try:
    while True:
        line = sys.stdin.buffer.readline(65538)
        if not line:
            s.shutdown(socket.SHUT_WR)
            threading.Event().wait()
        if len(line) > 65537 or not line.endswith(b'\n'):
            raise ValueError('frame too large')
        data = base64.b64decode(line[:-1], validate=True)
        if len(data) > 49152:
            raise ValueError('frame too large')
        s.sendall(data)
except Exception:
    os._exit(1)
`;

export const FRAME_BYTES = 49152;
export const BUFFER_BYTES = 1024 * 1024;
const BUFFER_FRAMES = 256;
const ENCODED_BYTES = 65536;

function decodeFrame(encoded) {
  if (typeof encoded !== 'string' || encoded.length > ENCODED_BYTES
      || !/^[A-Za-z0-9+/]*={0,2}$/.test(encoded)) throw new Error('Invalid preview frame');
  const bytes = Buffer.from(encoded, 'base64');
  if (bytes.length > FRAME_BYTES || bytes.toString('base64') !== encoded) throw new Error('Invalid preview frame');
  return bytes;
}

export class PrivateTunnel {
  constructor(sandbox, port) {
    if (![3000, 8000, 8080].includes(port)) throw new Error('Unsupported preview app port');
    this.chunks = [];
    this.size = 0;
    this.partial = '';
    this.closed = false;
    this.failed = false;
    this.writing = false;
    // The SDK callbacks are UTF-8 strings, so base64 on the VM is essential.
    const command = 'python3 -I -u -c ' + "'" + LOOPBACK_TUNNEL.replaceAll("'", "'\\''") + "' " + port;
    this.handle = sandbox.exec(command, {stdin: true, ephemeral: true, captureOutput: false,
      timeoutSec: 3600, env: {}, cwd: '/', onStdout: (chunk) => this.receive(chunk),
      onStderr: () => { /* never forward remote diagnostics or app data to logs */ }});
    this.done = Promise.resolve(this.handle).then((result) => {
      this.closed = true;
      if (this.partial || result?.exitCode !== 0 || result?.timedOut) this.overflow();
    }, () => this.overflow());
    if (this.failed) this.overflow(); // a synchronous output callback may have overflowed before assignment
  }

  receive(chunk) {
    if (this.failed || this.closed) return;
    // Bound BEFORE retaining, decoding or serializing remote output.
    if (Buffer.byteLength(chunk) + this.partial.length > BUFFER_BYTES * 2) return this.overflow();
    this.partial += chunk;
    let end;
    while ((end = this.partial.indexOf('\n')) !== -1) {
      const line = this.partial.slice(0, end);
      this.partial = this.partial.slice(end + 1);
      let bytes;
      try { bytes = decodeFrame(line); } catch { return this.overflow(); }
      if (this.size + bytes.length > BUFFER_BYTES || this.chunks.length >= BUFFER_FRAMES) return this.overflow();
      this.chunks.push(bytes);
      this.size += bytes.length;
    }
    if (this.partial.length > ENCODED_BYTES) this.overflow();
  }

  overflow() {
    this.failed = true;
    this.closed = true;
    this.chunks = [];
    this.size = 0;
    this.partial = '';
    void this.handle?.kill('KILL').catch(() => {});
  }

  async write(base64) {
    if (this.failed || this.closed) throw new Error('Preview tunnel closed; requests are never replayed');
    const bytes = decodeFrame(base64);
    // Do not queue unbounded concurrent writes in the SDK stdin/control channel.
    if (this.writing) throw new Error('Preview write in progress; await backpressure');
    this.writing = true;
    try {
      await this.handle.stdin.write(bytes.toString('base64') + '\n');
    } catch {
      this.overflow();
      throw new Error('Preview tunnel lost; requests are never replayed');
    } finally {
      this.writing = false;
    }
  }

  read() {
    if (this.failed) throw new Error('Preview tunnel failed or buffer exceeded; response was not truncated');
    const bytes = this.chunks.shift() || Buffer.alloc(0);
    this.size -= bytes.length;
    return {base64: bytes.toString('base64'), eof: this.closed && !this.chunks.length};
  }

  async close() {
    this.closed = true;
    this.chunks = [];
    this.size = 0;
    this.partial = '';
    await this.handle.kill('KILL');
  }
}
