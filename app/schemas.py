from typing import Optional, Any
from pydantic import BaseModel, Field


class WSMessage(BaseModel):
    type: str
    room_key: Optional[str] = None
    participant_id: Optional[str] = None
    display_name: Optional[str] = None
    offer: Optional[Any] = None
    answer: Optional[Any] = None
    candidate: Optional[Any] = None
    camera_enabled: Optional[bool] = None