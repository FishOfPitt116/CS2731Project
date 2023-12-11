from ktrain import text, get_predictor
import numpy as np
import pandas as pd
import sqlite3
from structs import Comment, Episode, Media

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

# set up pandas dataframe with scripts
train_df = pd.DataFrame(columns=["text", "media"])
for m in media:
    print(m.media_title)
    episodes = [Episode(episode_id, media_id, timestamp, episode_name, platform, transcript) for (episode_id, media_id, timestamp, episode_name, platform, transcript) in episode_table_contents(m.media_id)]
    for e in episodes:
        with open(e.transcript) as f:
            lines = f.readlines()
            e_df = pd.DataFrame({"text": lines, "media": [e.media_id for i in range(len(lines))]})
            train_df = pd.concat([train_df, e_df])
print(train_df.head())
print(len(train_df))

# set up pandas dataframe with comments
test_df = pd.DataFrame(columns=["text", "media"])
for m in media:
    print(m.media_title)
    comments = [Comment(media_id, user, content, timestamp) for (media_id, user, content, timestamp) in comment_table_contents(m.media_id)]
    lines = [c.content for c in comments]
    c_df = pd.DataFrame({"text": lines, "media": [m.media_id for i in range(len(lines))]})
    test_df = pd.concat([test_df, c_df])
        
print(test_df.head())
print(len(test_df))

# establish preprocessing metadata
x_train = np.array(train_df["text"])
y_train = np.array(train_df["media"])
x_test = np.array(test_df["text"])
y_test = np.array(test_df["media"])
print(len(x_train))
print(len(y_train))
print(len(x_test))
print(len(y_test))

class_names = [m.media_id for m in media]

(x_train, y_train), (x_test, y_test), preproc = text.texts_from_array(x_train=x_train,
                                                y_train=y_train,
                                                x_test=x_test,
                                                y_test=y_test,
                                                class_names=class_names,
                                                preprocess_mode='bert',
                                                val_pct=0.5,
                                                ngram_range=1,
                                                maxlen=320)

# set up and train the bert model
model = text.text_classifier('bert', 
                            train_data=(train_df["text"], train_df["media"]),
                            preproc=preproc)

# predict on the test set
predictor = get_predictor(model, preproc)

# calculate accuracy
print(predictor.predict(y_train))