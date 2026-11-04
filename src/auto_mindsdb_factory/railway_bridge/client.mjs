import WebSocket from 'ws';

export function clientOptions(environment = process.env) {
  return {
    token: environment.RAILWAY_API_TOKEN,
    authType: 'project-token',
    environmentId: environment.RAILWAY_ENVIRONMENT_ID,
    // Native Node 22 WebSocket recursively fires error during close while
    // connecting (undici#4741), crashing the SDK on a rejected handshake.
    webSocketImpl: WebSocket,
  };
}
