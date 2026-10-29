"""Newline-delimited JSON over local TCP (sensor -> edge -> fog links).

Optional `local_delay_ms` emulates the bedside/ward LAN, which is otherwise
~0 ms because every process is on the same PC.
"""
import asyncio
import json


async def send(writer: asyncio.StreamWriter, obj: dict, local_delay_ms: float = 0.0):
    if local_delay_ms:
        await asyncio.sleep(local_delay_ms / 1000.0)
    writer.write((json.dumps(obj, separators=(",", ":")) + "\n").encode())
    await writer.drain()


async def messages(reader: asyncio.StreamReader):
    """Yield decoded messages until the peer disconnects."""
    while True:
        try:
            line = await reader.readline()
        except (ConnectionError, asyncio.IncompleteReadError):
            return
        if not line:
            return
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            continue


async def connect(host: str, port: int, timeout: float = 1.0):
    return await asyncio.wait_for(asyncio.open_connection(host, port), timeout)


async def request_shutdown(host: str, port: int):
    """Ask a local server (edge/fog) to flush its logs and exit."""
    try:
        _, writer = await connect(host, port)
        await send(writer, {"type": "shutdown"})
        writer.close()
    except (OSError, asyncio.TimeoutError):
        pass
