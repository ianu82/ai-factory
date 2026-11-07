// Metadata only. These counters never authorize execution or prove delivery.
import WebSocket from 'ws';
import {ExecNotStartedError, ExecInterruptedError, ExecControlUnsupportedError} from 'railway';

export function execFailureObservations(error, observations) {
  try {
    if ([ExecNotStartedError, ExecInterruptedError, ExecControlUnsupportedError].includes(error?.constructor)) {
      return {exec_observations: observations.snapshot()};
    }
  } catch { /* Diagnostics cannot replace the primary bridge failure. */ }
  return {};
}

export function execObservations(onChange = () => {}) {
  const state = {
    connections: 0, opened: 0, errors: 0, closed: 0, http_rejections: 0,
    last_http_status: 0, last_close_code: 0, capabilities_frames: 0, typed_error_frames: 0,
    hello_send_attempts: 0, init_send_attempts: 0, init_send_returned: 0,
    unclassified_control_sends: 0, counters_saturated: false, observation_failed: false,
  };
  function count(key) {
    if (state[key] < 65535) state[key]++;
    else state.counters_saturated = true;
  }
  let last;
  function changed() {
    try {
      const current = JSON.stringify(state);
      if (current !== last) { last = current; onChange(); }
    } catch { /* Observation only. */ }
  }
  function controlType(data) {
    if (Buffer.isBuffer(data)) {
      if (data.length > 4096) return null;
      data = data.toString('utf8');
    }
    if (typeof data !== 'string' || data.length > 4096) return null;
    try { return JSON.parse(data)?.type; } catch { return null; }
  }
  class ObservedWebSocket extends WebSocket {
    constructor(...args) { super(...args); count('connections'); changed(); }
    emit(event, ...args) {
      try {
        if (event === 'open') count('opened');
        if (event === 'error') count('errors');
        if (event === 'close') {
          count('closed');
          state.last_close_code = Number.isInteger(args[0]) && args[0] >= 1000 && args[0] <= 4999 ? args[0] : 0;
        }
        // Observing emit preserves its return value and the absence of a listener:
        // ws must still perform its ordinary rejected-handshake cleanup.
        if (event === 'unexpected-response') {
          count('http_rejections');
          const code = args[1]?.statusCode;
          state.last_http_status = Number.isInteger(code) && code >= 100 && code <= 599 ? code : 0;
        }
        if (event === 'message' && args[1] === false) {
          const type = controlType(args[0]);
          if (type === 'exec_capabilities') count('capabilities_frames');
          if (type === 'error') count('typed_error_frames');
        }
      } catch { state.observation_failed = true; }
      changed();
      return super.emit(event, ...args);
    }
    send(data, ...args) {
      const type = typeof data === 'string' ? controlType(data) : null;
      if (type === 'exec_hello') count('hello_send_attempts');
      else if (type === 'init_exec') count('init_send_attempts');
      else if (typeof data === 'string') count('unclassified_control_sends');
      changed();
      const result = super.send(data, ...args);
      if (type === 'init_exec') count('init_send_returned');
      changed();
      return result;
    }
  }
  return {WebSocketImpl: ObservedWebSocket, snapshot: () => ({...state})};
}
