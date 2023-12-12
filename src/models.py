# from ktrain import text, get_predictor
import numpy as np
import pandas as pd
import sqlite3
from structs import Comment, CustomTrainer, Episode, Media, MyDataset
import evaluate
from transformers import BertConfig, BertForMaskedLM, BertTokenizer, Trainer, TrainingArguments
import random
import os
from torch.utils.data import DataLoader
import torch
import math

DATABASE_CON = sqlite3.connect("../db/podcasts.db")
DATABASE_CURSOR = DATABASE_CON.cursor()

BERT = "prajjwal1/bert-mini"

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

'''
FINE TUNING BERT AND SAVING MODELS
'''
for m in media:
    # print media name
    print(m.media_title)

    if not os.path.exists(f'../models/{m.media_id}'):

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

        print("preprocessing data to build model from bert")
        tokenizer = BertTokenizer.from_pretrained(BERT)
        train = tokenizer(train, truncation=True, padding=True)

        model = BertForMaskedLM.from_pretrained(BERT)
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
            # eval_dataset=MyDataset(test)            # evaluation dataset
        )

        print("training model...")
        trainer.train()
        print("model training complete.")

        print("saving model to models directory")
        model.save_pretrained(f'../models/{m.media_id}')
        print("model saved!")

'''
CALCULATING MODEL PERPLEXITY
'''
for m in media:
    # set up testing set
    test = []
    comments = [Comment(media_id, user, content, timestamp) for (media_id, user, content, timestamp) in comment_table_contents(m.media_id)]
    test = [c.content for c in comments]
    test = [t for t in test if len(t) < 512]
    try:
        test = random.sample(test, 200)
    except:
        test = test
    # print(f"there are {len(test)} comments")

    # chunk_size = 32  # may need to adjust this based on your GPU memory
    # sliced_test = [test[i:i + chunk_size] for i in range(0, len(test), chunk_size)]

    # print("preprocessing data to calculate perplexity")
    tokenizer = BertTokenizer.from_pretrained(BERT)
    # test = tokenizer(test, truncation=True, padding=True)

    model = BertForMaskedLM.from_pretrained(f"../models/{m.media_id}")
    # print("putting the model in eval mode")
    model.eval()

    training_args = TrainingArguments(
        output_dir='../models/results',          # output directory
        num_train_epochs=3,              # total # of training epochs
        per_device_train_batch_size=16,  # batch size per device during training
        per_device_eval_batch_size=64,   # batch size for evaluation
        warmup_steps=500,                # number of warmup steps for learning rate scheduler
        weight_decay=0.01,               # strength of weight decay
        logging_dir='../models/logs',            # directory for storing logs
        eval_accumulation_steps=20
    )

    trainer = CustomTrainer(
        model=model,                         # the instantiated 🤗 Transformers model to be trained
        args=training_args,                  # training arguments, defined above
        # train_dataset=MyDataset(train),         # training dataset
        eval_dataset=MyDataset(test)            # evaluation dataset
    )
    
    ppl = 0

    model.to("cpu")

    for sentence in test:
        ppl += score(model, tokenizer, sentence)

    # for chunk in sliced_test:
    #     tokenized_chunk = tokenizer(chunk, truncation=True, padding=True)
        
    #     # Update the evaluation dataset for each chunk
    #     trainer.eval_dataset = MyDataset(tokenized_chunk)

    #     # Perform prediction on the current chunk
    #     predictions = trainer.predict(trainer.eval_dataset)

    #     # Process predictions and ground truth as needed
    #     logits = predictions.predictions
    #     labels = tokenized_chunk["input_ids"]

    #     # Compute cross-entropy loss
    #     # print(logits)
        
    #     loss = sum([sum(l) for l in sum(logits)])
    #     # print(loss)

    #     # Update perplexity based on the current chunk's loss
    #     ppl += loss

    # Normalize perplexity by the total number of tokens in the test set
    num_tokens = sum(len(chunk) for chunk in sliced_test)
    ppl = math.exp(ppl / num_tokens)

    print(f'{m.media_title} Perplexity: {ppl}')