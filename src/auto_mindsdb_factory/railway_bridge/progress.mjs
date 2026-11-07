// Partial controller observations, never remote execution acknowledgement.
export function bridgeProgress(nonce, snapshot, write = line => process.stderr.write(line)) {
  let sequence = 0, stage = 'dispatch';
  return function report(next = stage) {
    stage = next;
    if (typeof nonce !== 'string' || !/^[a-f0-9]{32}$/.test(nonce) || sequence >= 64) return;
    try {
      write('FACTORY_BRIDGE_PROGRESS ' + JSON.stringify({nonce, sequence: ++sequence,
        stage, exec_observations: snapshot()}) + '\n');
    } catch { /* Diagnostics cannot replace or delay owned retirement. */ }
  };
}
