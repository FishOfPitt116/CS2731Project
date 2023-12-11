from ktrain import text
import pandas as pd
import sqlite3
from structs import Episode, Media

DATABASE_CON = sqlite3.connect("../db/podcasts.db")
DATABASE_CURSOR = DATABASE_CON.cursor()

def media_table_contents():
    res = DATABASE_CURSOR.execute("SELECT * FROM Media")
    return res.fetchall()

def episode_table_contents(media_id):
    res = DATABASE_CURSOR.execute(f"SELECT * FROM Episode WHERE media_id='{media_id}'")
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

# set up pandas dataframe with comments

# establish preprocessing metadata


# set up the bert model
# model = text.text_classifier('bert', 
#                             train_data=(train_df["text"], train_df["media"]))

# train the bert model

# validate data on test set

# predict on the test set

# calculate accuracy