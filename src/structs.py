from dataclasses import dataclass
from datetime import datetime

@dataclass
class Media:
    media_id: str # PRIMARY KEY
    media_title: str

@dataclass
class Episode:
    episode_id: str
    media_id: str
    timestamp: datetime
    episode_name: str
    platform: str
    transcript: str

@dataclass
class Post:
    media_id: str
    post_id: str
    timestamp: datetime
    content: str

@dataclass
class Comment:
    media_id: str
    post_id: str
    user: str
    content: str
    timestamp: datetime
