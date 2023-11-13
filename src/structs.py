from dataclasses import dataclass
from datetime import datetime

@dataclass
class Media:
    media_id: str # PRIMARY KEY
    media_title: str

@dataclass
class Episode:
    episode_id: str
    media_id: Media
    timestamp: datetime
    episode_name: str
    platform: str
    transcript: str