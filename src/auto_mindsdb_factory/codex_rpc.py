"""One owned app-server thread/turn; only approved Factory callbacks execute."""

import asyncio
import json

from .product_agent import TOOLS


ACTION_INSTRUCTIONS = (
    "Factory Codex action transport: the complete functions.exec program must be exactly "
    "text(await tools.run(JSON)) or text(await tools.finish(JSON)), where JSON is a literal JSON object "
    "matching the supplied tool schema. For example: text(await tools.run({\"command\":[\"node\",\"--version\"]})). "
    "Do not declare variables, use aliases, comments, loops, nested expressions, parallel calls, "
    "other tools or extra statements. Only run and finish are authorized; visible built-in tools "
    "are unavailable. Factory executes run in the isolated job, not the app-server working directory. "
    "This transport format does not change the frozen role, plan, criteria, permissions or limits."
)


JOB_BOUNDARY_INSTRUCTIONS = (
    "Factory remote job boundary: the read-only filesystem and restricted network policy "
    "of this app-server describes its host environment. The supplied run tool is a "
    "controller-mediated callback into a separate isolated job, never a host shell. "
    "That job's access is enforced by the frozen Factory role, task and job policy; "
    "this explanation grants no additional access. Use run only for work authorized "
    "by that policy. Do not infer a remote job permission denial from the host sandbox "
    "notice. Preserve actual tool failures and report unverified when work cannot be "
    "completed. Built-in host tools remain unavailable."
)


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate RPC key")
        result[key] = value
    return result


def _constant(_):
    raise ValueError("Non-JSON RPC value")


async def codex_turn(owner, gate, *, instructions, context, command):
    """Run approved commands and return a detached report, without accepting it.

    The caller owns process/proxy/client teardown and report acceptance. Pipe
    transports close here on every exit, without a blocking reader thread.
    """
    gate.admit()
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=8_000_000)
    incoming, outgoing = None, None
    try:
        incoming, _ = await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), owner.process.stdout.buffer)
        outgoing, protocol = await loop.connect_write_pipe(lambda: asyncio.streams.FlowControlMixin(loop=loop), owner.process.stdin.buffer)
        writer = asyncio.StreamWriter(outgoing, protocol, None, loop)

        async def send(message):
            gate.admit()
            encoded = (json.dumps(message, allow_nan=False) + "\n").encode()
            if len(encoded) > 8_000_000:
                gate.stop("rpc_policy")
            writer.write(encoded)
            await asyncio.wait_for(writer.drain(), timeout=min(1, max(0, gate.deadline - loop.time())))

        await send({"id": 1, "method": "initialize", "params": {"clientInfo": {
            "name": "ai_factory", "version": "1"}, "capabilities": {"experimentalApi": True}}})
        expected, thread, turn, settings = 1, None, None, None
        while True:
            # Waiting observes an already-admitted inference. Its usage stays
            # incomplete until the provider responds; callbacks still admit fully.
            gate.check_stop()
            try:
                line = await asyncio.wait_for(reader.readline(), timeout=0.05)
            except TimeoutError:
                continue
            if not line or len(line) > 8_000_000:
                gate.stop("rpc_policy")
            message = json.loads(line, object_pairs_hook=_object, parse_constant=_constant)
            if type(message) is not dict or "error" in message:
                gate.stop("rpc_policy")
            method, params = message.get("method"), message.get("params")
            if method == "item/tool/call":
                if (type(params) is not dict or set(params) - {"threadId", "turnId", "callId", "tool", "arguments", "namespace"}
                        or thread is None or turn is None or params.get("threadId") != thread or params.get("turnId") != turn
                        or params.get("namespace") is not None or type(params.get("callId")) is not str or not params["callId"]
                        or type(message.get("id")) not in (str, int) or type(settings) is not dict
                        or settings.get("model") != gate.config.model or settings.get("effort") != gate.config.reasoning_effort
                        or type(settings.get("sandboxPolicy")) is not dict
                        or set(settings["sandboxPolicy"]) != {"type", "networkAccess"}
                        or settings["sandboxPolicy"].get("type") != "readOnly"
                        or settings["sandboxPolicy"].get("networkAccess") is not False):
                    gate.stop("rpc_policy")
                name, arguments = gate.take_action(params.get("tool"), params.get("arguments"))
                if name == "finish":
                    return arguments
                result = await command(arguments["command"])
                gate.action_finished()
                await send({"id": message["id"], "result": {"success": True,
                    "contentItems": [{"type": "inputText", "text": json.dumps(result, allow_nan=False)}]}})
            elif method is not None:
                # No approval, built-in, authentication or other server request
                # is acknowledged. Only notifications may be ignored.
                if "id" in message or method in {"error", "turn/completed"}:
                    gate.stop("rpc_policy")
                if method == "thread/settings/updated":
                    if type(params) is not dict or params.get("threadId") != thread:
                        gate.stop("rpc_policy")
                    settings = params.get("threadSettings")
            else:
                if type(message.get("id")) is not int or message["id"] != expected or "result" not in message:
                    gate.stop("rpc_policy")
                if expected == 1:
                    await send({"method": "initialized", "params": {}})
                    await send({"id": 2, "method": "thread/start", "params": {"model": gate.config.model,
                        "modelProvider": "factory_guarded", "allowProviderModelFallback": False, "ephemeral": True,
                        "cwd": str(owner.work), "approvalPolicy": "never", "sandbox": "read-only",
                        "baseInstructions": instructions + "\n\n" + ACTION_INSTRUCTIONS,
                        "developerInstructions": JOB_BOUNDARY_INSTRUCTIONS,
                        "dynamicTools": [{"name": tool["name"], "description": tool["description"],
                                          "inputSchema": tool["parameters"]} for tool in TOOLS]}})
                    expected = 2
                elif expected == 2:
                    thread = message["result"]["thread"]["id"]
                    if type(thread) is not str or not 1 <= len(thread) <= 200:
                        gate.stop("rpc_policy")
                    gate.record("harness_thread_started", thread_id=thread, resume_supported=False)
                    await send({"id": 3, "method": "turn/start", "params": {"threadId": thread,
                        "model": gate.config.model, "effort": gate.config.reasoning_effort, "approvalPolicy": "never",
                        "cwd": str(owner.work), "input": [{"type": "text", "text": json.dumps(context, allow_nan=False)}]}})
                    expected = 3
                elif expected == 3:
                    turn = message["result"]["turn"]["id"]
                    if type(turn) is not str or not 1 <= len(turn) <= 200:
                        gate.stop("rpc_policy")
                    gate.record("harness_turn_started", thread_id=thread, turn_id=turn)
                    expected = None
                else:
                    gate.stop("rpc_policy")
    except (ValueError, TypeError, KeyError, RecursionError, OSError, TimeoutError):
        gate.stop("rpc_policy")
    finally:
        if outgoing is not None: outgoing.close()
        if incoming is not None: incoming.close()
