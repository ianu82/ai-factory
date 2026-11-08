import WebSocket from 'ws';

// Bound HTTP upgrade negotiation without extending the owner's deadline.
// The SDK retains ownership of its one retry before any command is sent.
export class ExecWebSocket extends WebSocket {
  constructor(address, protocols, options) {
    if (protocols && typeof protocols === 'object' && !Array.isArray(protocols)) {
      options = protocols;
      protocols = undefined;
    }
    const supplied = options?.handshakeTimeout;
    const handshakeTimeout = Number.isFinite(supplied) && supplied > 0
      ? Math.min(supplied, 30000) : 30000;
    super(address, protocols, {...options, handshakeTimeout});
  }
}

export function clientOptions(environment = process.env) {
  return {
    token: environment.RAILWAY_API_TOKEN,
    authType: 'project-token',
    environmentId: environment.RAILWAY_ENVIRONMENT_ID,
    // Native Node 22 WebSocket recursively fires error during close while
    // connecting (undici#4741), crashing the SDK on a rejected handshake.
    webSocketImpl: ExecWebSocket,
  };
}
