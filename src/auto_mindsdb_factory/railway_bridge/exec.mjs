// The controller owns the lifecycle deadline; the SDK owns its request drain.
export function execBudget(budgetMs) {
  if (budgetMs === undefined) return () => undefined;
  if (!Number.isSafeInteger(budgetMs) || budgetMs < 1) throw new TypeError('Invalid execution budget');
  const deadline = performance.now() + budgetMs;
  return () => Math.floor(deadline - performance.now());
}

export async function checkedExec(sandbox, command, input, timeoutSec, maxOutputBytes = 65536, budgetMs) {
  let signalMs = (timeoutSec + 30) * 1000;
  if (budgetMs !== undefined) {
    if (!Number.isSafeInteger(budgetMs) || budgetMs < 1) throw new TypeError('Invalid execution budget');
    signalMs = Math.min(signalMs, budgetMs);
  }
  const handle = sandbox.exec(command, {
    ephemeral: true, stdin: input !== undefined, timeoutSec, maxOutputBytes,
    signal: AbortSignal.timeout(signalMs),
  });
  if (input !== undefined) {
    await handle.stdin.write(input);
    await handle.stdin.end();
  }
  const result = await handle;
  if (result.exitCode !== 0 || result.timedOut || result.truncated) {
    throw new Error('Remote execution was incomplete');
  }
  return result.stdout;
}
