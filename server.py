import asyncio
import json
import logging
import websockets
from websockets.server import serve

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

# Rooms: room_id -> set of WebSocket connections
rooms: dict[str, set] = {}

async def handler(websocket):
    room_id = None
    try:
        async for raw in websocket:
            msg = json.loads(raw)
            msg_type = msg.get("type")

            # ── JOIN ──────────────────────────────────────────────
            if msg_type == "join":
                room_id = msg["room"]
                rooms.setdefault(room_id, set()).add(websocket)
                peers = rooms[room_id]
                logging.info(f"[{room_id}] peer joined ({len(peers)} total)")

                if len(peers) == 2:
                    # Tell the first peer to start the offer
                    for peer in peers:
                        if peer != websocket:
                            await peer.send(json.dumps({"type": "ready"}))

                elif len(peers) > 2:
                    await websocket.send(json.dumps({
                        "type": "error",
                        "message": "Room is full"
                    }))
                    return

            # ── OFFER / ANSWER / ICE ──────────────────────────────
            elif msg_type in ("offer", "answer", "ice-candidate"):
                if not room_id or room_id not in rooms:
                    continue
                # Forward to all OTHER peers in the room
                for peer in rooms[room_id]:
                    if peer != websocket:
                        await peer.send(raw)
                logging.info(f"[{room_id}] forwarded {msg_type}")

    except websockets.exceptions.ConnectionClosedOK:
        pass
    except websockets.exceptions.ConnectionClosedError as e:
        logging.warning(f"Connection closed with error: {e}")
    finally:
        if room_id and room_id in rooms:
            rooms[room_id].discard(websocket)
            logging.info(f"[{room_id}] peer left ({len(rooms[room_id])} remaining)")
            # Notify remaining peer
            for peer in rooms[room_id]:
                await peer.send(json.dumps({"type": "peer-left"}))
            if not rooms[room_id]:
                del rooms[room_id]

async def main():
    host = "0.0.0.0"
    port = 8765
    logging.info(f"Signaling server starting on ws://{host}:{port}")
    async with serve(handler, host, port):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())