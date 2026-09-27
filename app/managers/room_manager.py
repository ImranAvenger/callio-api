import asyncio
from typing import Dict, Optional, List
from fastapi import WebSocket


class Participant:
    def __init__(self, participant_id: str, display_name: str, websocket: WebSocket):
        self.id = participant_id
        self.display_name = display_name
        self.websocket: Optional[WebSocket] = websocket


class Room:
    def __init__(self, room_key: str):
        self.key = room_key
        self.participants: Dict[str, Participant] = {}
        self.lock = asyncio.Lock()

    @property
    def active_participants(self) -> List[Participant]:
        return [p for p in self.participants.values() if p.websocket is not None]


class RoomManager:
    def __init__(self):
        self._rooms: Dict[str, Room] = {}
        self._lock = asyncio.Lock()

    async def get_or_create_room(self, room_key: str) -> Room:
        async with self._lock:
            if room_key not in self._rooms:
                self._rooms[room_key] = Room(room_key)
            return self._rooms[room_key]

    async def get_room(self, room_key: str) -> Optional[Room]:
        async with self._lock:
            return self._rooms.get(room_key)

    async def send_to(self, participant: Participant, message: dict) -> None:
        if participant.websocket is not None:
            try:
                await participant.websocket.send_json(message)
            except Exception:
                participant.websocket = None

    async def cleanup_room_if_empty(self, room_key: str) -> None:
        """Garbage Collector: মেমোরি লিক ঠেকানোর জন্য খালি রুম রিমুভ করে"""
        async with self._lock:
            room = self._rooms.get(room_key)
            if room and len(room.active_participants) == 0:
                del self._rooms[room_key]

    async def remove_participant(self, room_key: str, participant_id: str) -> None:
        room = await self.get_room(room_key)
        if not room:
            return

        async with room.lock:
            room.participants.pop(participant_id, None)
            remaining = room.active_participants
            if remaining:
                await self.send_to(remaining[0], {"type": "peer_left"})

        await self.cleanup_room_if_empty(room_key)


room_manager = RoomManager()