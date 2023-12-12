from dataclasses import dataclass
from datetime import datetime
from torch.utils.data import Dataset
from transformers import Trainer

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
class Comment:
    media_id: str
    user: str
    content: str
    timestamp: datetime

class MyDataset(Dataset):
    def __init__(self, tokens):
        self.tokens = tokens

    def __len__(self):
        return len(self.tokens['input_ids'])

    def __getitem__(self, i):
        return {key: tensor[i] for key, tensor in self.tokens.items()}
    
class CustomTrainer(Trainer):
    def compute_loss(self, model, inputs):
        labels = inputs["input_ids"]
        outputs = model(**inputs, labels=labels)
        return outputs.loss