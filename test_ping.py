import asyncio
import websockets
import json
import time

async def test_ping():
    async with websockets.connect("wss://quote.edgex.exchange") as ws:
        t1 = time.time()
        await ws.send(json.dumps({"op": "ping"}))
        msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
        t2 = time.time()
        print(f"Ping RTT: {(t2-t1)*1000:.2f}ms, Reply: {msg}")

asyncio.run(test_ping())
