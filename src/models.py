# from ktrain import text, get_predictor
import numpy as np
import pandas as pd
import sqlite3
from structs import Comment, CustomTrainer, Episode, Media, MyDataset
import evaluate
from transformers import BertConfig, BertForMaskedLM, BertTokenizer, Trainer, TrainingArguments
import random

DATABASE_CON = sqlite3.connect("../db/podcasts.db")
DATABASE_CURSOR = DATABASE_CON.cursor()

def media_table_contents():
    res = DATABASE_CURSOR.execute("SELECT * FROM Media")
    return res.fetchall()

def episode_table_contents(media_id):
    res = DATABASE_CURSOR.execute(f"SELECT * FROM Episode WHERE media_id='{media_id}'")
    return res.fetchall()

def comment_table_contents(media_id):
    res = DATABASE_CURSOR.execute(f"SELECT * FROM Comment WHERE media_id='{media_id}'")
    return res.fetchall()

# media database -> data structures
media = [Media(id, name) for (id, name) in media_table_contents()]

for m in media:
    if episode_table_contents(m.media_id) is None or len(episode_table_contents(m.media_id)) == 0:
        DATABASE_CURSOR.execute(f"DELETE FROM Comment WHERE media_id='{m.media_id}'")
        media.remove(m)
    elif comment_table_contents(m.media_id) is None or len(comment_table_contents(m.media_id)) == 0:
        DATABASE_CURSOR.execute(f"DELETE FROM Episode WHERE media_id='{m.media_id}'")
        media.remove(m)
print(len(media))

for m in media:
    # print media name
    print(m.media_title)

    # set up training set
    train = []
    episodes = [Episode(episode_id, media_id, timestamp, episode_name, platform, transcript) for (episode_id, media_id, timestamp, episode_name, platform, transcript) in episode_table_contents(m.media_id)]
    for e in episodes:
        with open(e.transcript) as f:
            train += f.readlines()
    train = [t for t in train if len(t) < 512]
    try:
        train = random.sample(train, 2000)
    except:
        train = train
    print(f"there are {len(train)} podcast lines")

    # set up testing set
    test = []
    comments = [Comment(media_id, user, content, timestamp) for (media_id, user, content, timestamp) in comment_table_contents(m.media_id)]
    test = [c.content for c in comments]
    test = [t for t in test if len(t) < 512]
    try:
        test = random.sample(test, 200)
    except:
        test = test
    print(f"there are {len(test)} comments")

    bert = "prajjwal1/bert-mini"
    print("preprocessing data to build model from bert")
    tokenizer = BertTokenizer.from_pretrained(bert)
    train = tokenizer(train, truncation=True, padding=True)
    test = tokenizer(test, truncation=True, padding=True)

    #model = BertForSequenceClassification.from_pretrained(bert, num_labels=len(media))
    model = BertForMaskedLM.from_pretrained(bert)
    print("Put model in train mode")
    model.train()

    print("setting model training arguments")
    training_args = TrainingArguments(
        output_dir='../models/results',          # output directory
        num_train_epochs=3,              # total # of training epochs
        per_device_train_batch_size=16,  # batch size per device during training
        per_device_eval_batch_size=64,   # batch size for evaluation
        warmup_steps=500,                # number of warmup steps for learning rate scheduler
        weight_decay=0.01,               # strength of weight decay
        logging_dir='../models/logs',            # directory for storing logs
    )

    print("setting up model trainer")
    trainer = CustomTrainer(
        model=model,                         # the instantiated 🤗 Transformers model to be trained
        args=training_args,                  # training arguments, defined above
        train_dataset=MyDataset(train),         # training dataset
        eval_dataset=MyDataset(test)            # evaluation dataset
    )

    print("training model...")
    trainer.train()
    print("model training complete.")

    print("saving model to models directory")
    model.save_pretrained(f'../models/{m.media_id}')
    print("model saved!")