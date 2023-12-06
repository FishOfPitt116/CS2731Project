import json, os
from datetime import datetime
from pathlib import Path
from structs import Episode, Media
import sqlite3

MEDIA_SET = [
    ("ChapoTrapHouse", Media("UCokqzNPBJ65raczldVuHAww", "Chapo Trap House")),
    ("PoliticalGabfest", Media("PoliticalGabfest", "Political Gabfest")),
    ("RealTimewithBillMaher", Media("UCy6kyFxaMqGtpE3pQTflK8A", "Real Time with Bill Maher")),
    ("TheAlexJonesShow", Media("TheAlexJonesShow", "The Alex Jones Show")),
    ("TheDaily", Media("TheDaily", "The Daily")),
    ("TheNPRPoliticsPodcast", Media("TheNPRPoliticsPodcast", "The NPR Politics Podcast"))
]

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
DATE_RANGE = (datetime(2017, 10, 1), datetime(2018, 10, 31))

DATABASE_CON = sqlite3.connect("../db/podcasts.db")
DATABASE_CURSOR = DATABASE_CON.cursor()

def media_in_db(media: Media):
    media_exists = DATABASE_CURSOR.execute(f"SELECT media_id FROM Media WHERE media_id='{media.media_id}'")
    return media_exists.fetchone() is not None

def main():
    for (dir_name, media) in MEDIA_SET:
        if not media_in_db(media):
            # create new media entry
            print(f"INSERT INTO Media VALUES ('{media.media_id}', '{media.media_title}')")
            DATABASE_CURSOR.execute(f"INSERT INTO Media VALUES ('{media.media_id}', '{media.media_title}')")
            DATABASE_CON.commit()

        # put all episodes in DB
        dir_path = '../scripts/' + dir_name
        for file in os.listdir(dir_path):
            transcript_path = '../scripts/' + dir_name + '/' + file
            pub_date = datetime.strptime(file[:-4], '%m-%d-%Y')
            if DATE_RANGE[0] > pub_date or DATE_RANGE[1] < pub_date:
                continue
            id = file[:-4]
            episode = Episode(id, media.media_id, pub_date, "", "podcastindex.org", transcript_path)
            print(f"INSERT INTO Episode VALUES ('{episode.episode_id}', '{episode.media_id}', '{str(episode.timestamp)}', '{episode.episode_name}', '{episode.platform}', '{episode.transcript}')")
            DATABASE_CURSOR.execute(f"INSERT INTO Episode VALUES ('{episode.episode_id}', '{episode.media_id}', '{str(episode.timestamp)}', '{episode.episode_name}', '{episode.platform}', '{episode.transcript}')")
            DATABASE_CON.commit()

if __name__ == "__main__":
    main()
