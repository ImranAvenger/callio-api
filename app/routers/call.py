from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import ValidationError
from app.managers.room_manager import room_manager, Participant
from app.schemas import WSMessage

router = APIRouter(prefix="/call", tags=["Call"])


@router.websocket("/ws")
async def call_websocket(websocket: WebSocket):
    await websocket.accept()

    room_key: str = None
    participant_id: str = None
    intentional_leave: bool = False

    try:
        while True:
            raw_data = await websocket.receive_json()

            # Pydantic Schema Validation
            try:
                msg = WSMessage(**raw_data)
            except ValidationError:
                await websocket.send_json({"type": "invalid_request"})
                continue

            # 1. Custom Keep-Alive / Ping-Pong
            if msg.type == "ping":
                await websocket.send_json({"type": "pong"})
                continue

            # 2. Room Creation & Joining
            if msg.type in {"create", "join"}:
                room_key = msg.room_key
                participant_id = msg.participant_id
                display_name = msg.display_name

                if not all(v and v.strip() for v in (room_key, participant_id, display_name)):
                    await websocket.send_json({"type": "invalid_request"})
                    continue

                existing_room = await room_manager.get_room(room_key)

                if msg.type == "create":
                    if existing_room is not None:
                        await websocket.send_json({"type": "room_exists"})
                        continue
                    room = await room_manager.get_or_create_room(room_key)
                else:
                    if existing_room is None:
                        await websocket.send_json({"type": "room_not_found"})
                        await websocket.close()
                        return
                    room = existing_room

                async with room.lock:
                    active = room.active_participants
                    existing_participant = room.participants.get(participant_id)

                    if existing_participant is None and len(active) >= 2:
                        await websocket.send_json({"type": "room_full"})
                        await websocket.close()
                        return

                    if existing_participant is None:
                        participant = Participant(participant_id, display_name.strip(), websocket)
                        room.participants[participant_id] = participant
                    else:
                        existing_participant.websocket = websocket
                        existing_participant.display_name = display_name.strip()
                        participant = existing_participant

                    active_list = room.active_participants
                    count = len(active_list)

                    await websocket.send_json({
                        "type": "joined",
                        "room_key": room_key,
                        "display_name": participant.display_name,
                        "participants": count,
                    })

                    if count == 2:
                        p1, p2 = active_list[0], active_list[1]
                        await room_manager.send_to(p1, {
                            "type": "room_ready",
                            "role": "offerer",
                            "peer_name": p2.display_name,
                        })
                        await room_manager.send_to(p2, {
                            "type": "room_ready",
                            "role": "answerer",
                            "peer_name": p1.display_name,
                        })
                continue

            # 3. Intentional Leave
            if msg.type == "leave":
                intentional_leave = True
                if room_key and participant_id:
                    await room_manager.remove_participant(room_key, participant_id)
                await websocket.close()
                return

            # 4. Relay Signaling Messages
            room = await room_manager.get_room(room_key) if room_key else None
            if not room:
                continue

            if msg.type in {"offer", "answer", "ice", "media_state"}:
                payload_map = {
                    "offer": ("offer", msg.offer),
                    "answer": ("answer", msg.answer),
                    "ice": ("candidate", msg.candidate),
                    "media_state": ("camera_enabled", msg.camera_enabled),
                }
                key_name, payload_val = payload_map[msg.type]

                if payload_val is None:
                    continue

                async with room.lock:
                    for p in room.active_participants:
                        if p.websocket != websocket:
                            await room_manager.send_to(p, {
                                "type": msg.type,
                                key_name: payload_val,
                            })

    except WebSocketDisconnect:
        if room_key and participant_id and not intentional_leave:
            room = await room_manager.get_room(room_key)
            if room:
                async with room.lock:
                    p = room.participants.get(participant_id)
                    if p:
                        p.websocket = None
                        for remaining in room.active_participants:
                            await room_manager.send_to(remaining, {"type": "peer_reconnecting"})
                # ১ মিনিট পর নিষ্ক্রিয় রুম পরিষ্কার করা
                await room_manager.cleanup_room_if_empty(room_key)