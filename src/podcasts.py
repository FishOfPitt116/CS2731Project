import json, os

import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors

from datetime import datetime
from pathlib import Path
from structs import Episode, Media
import sqlite3
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]

# Disable OAuthlib's HTTPS verification when running locally.
# *DO NOT* leave this option enabled in production.
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

API_SERVICE_NAME = "youtube"
API_VERSION = "v3"
CLIENT_SECRETS_FILE = "../client_secret.json"

# Get credentials and create an API client
FLOW = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
    CLIENT_SECRETS_FILE, SCOPES)
CREDENTIALS = FLOW.run_console()
YOUTUBE = googleapiclient.discovery.build(
    API_SERVICE_NAME, API_VERSION, credentials=CREDENTIALS)

MEDIA_SET = [
    Media("UCnQC_G5Xsjhp9fEJKuIcrSw", "The Ben Shapiro Show"),
    Media("UCKRoXz3hHAu2XL_k3Ef4vJQ", "Pod Save America"),
    Media("UCIveFvW-ARp_B_RckhweNJw", "Louder with Crowder"),
    Media("UC1yBKRuGpC1tSM73A0ZjYjQ", "The Young Turks"),
    Media("UCy6kyFxaMqGtpE3pQTflK8A", "Real Time with Bill Maher"),
    Media("UCXKjhxsfFQUqlNVQzLVnpEA", "FiveThirtyEight"),
    Media("UC-3jIAlnQmbbVMV6gR7K8aQ", "The Majority Report with Sam Seder"),
    Media("UCJdKr0Bgd_5saZYqLCa9mng", "The Rubin Report"),
    Media("UCokqzNPBJ65raczldVuHAww", "Chapo Trap House"),
    Media("UCL_f53ZEJxp8TtlOkHwMV9Q", "Jordan B Peterson"),
    Media("UCNAxrHudMfdzNi6NxruKPLw", "Making Sense with Sam Harris"),
    Media("UC554eY5jNUfDq3yDOJYirOQ", "Destiny"),
    Media("UCldfgbzNILYZA4dmDt4Cd6A", "Secular Talk"),
    Media("UCNvsIonJdJ5E4EXMa65VYpA", "Contrapoints"), # error since there was only 1 page of results
    Media("UCvixJtaXuNdMPUGdOPcY8Ag", "The David Pakman Show"),
    Media("UCLtREJY21xRfCuEKvdki1Kw", "H3 Podcast"),
    Media("UCzQUP1qoWDoEbmsQxvdjxgQ", "The Joe Rogan Experience"),
    Media("UCzpwkXk_GlfmWntZ9v4l3Tg", "The Ramsey Show"),
    Media("UCzuqE7-t13O4NIDYJfakrhw", "Democracy Now!"),
    Media("UC3M7l8ved_rYQ45AVzS0RGA", "The Jimmy Dore Show"),
    Media("UC3XTzVzaHQEd30rQbuvCtTQ", "Last Week Tonight with John Oliver"),
    Media("UCoS-mm5bC5eY07AaKntKzgw", "Human Rights Watch"),
    Media("UClt01z1wHHT7c5lKcU8pxRQ", "hbomberguy")
]

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
DATE_RANGE = (datetime(2017, 10, 1), datetime(2018, 10, 31))

DATABASE_CON = sqlite3.connect("../db/podcasts.db")
DATABASE_CURSOR = DATABASE_CON.cursor()

"""
Main method which fetches podcast information for all podcasts listed in MEDIA_SET which have not already been added to db
"""
def main():
    create_db_tables()
    for media in MEDIA_SET:
        if write_to_media_db(media):
            media_to_videos(media)

"""
Method which creates Media and Episode database tables if they aren't already present in ../db/podcasts.db
"""
def create_db_tables():
    media_exists = DATABASE_CURSOR.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Media';")
    if media_exists.fetchone() is None:
        DATABASE_CURSOR.execute("CREATE TABLE Media(media_id string primary key, title string)")

    episode_exists = DATABASE_CURSOR.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Episode';")
    if episode_exists.fetchone() is None:
        DATABASE_CURSOR.execute("CREATE TABLE Episode(episode_id string, media_id string, timestamp timestamp, episode_name string, platform string, transcript string, foreign key(media_id) references Media(media_id))")

    DATABASE_CON.commit()

"""
Method which writes a media object to the Media database table.

@param media: Media object to be added to ../db/podcasts.db
@return: True if entry is successfully added, False if entry is already present
"""
def write_to_media_db(media: Media):
    media_exists = DATABASE_CURSOR.execute(f"SELECT media_id FROM Media WHERE media_id='{media.media_id}'")
    if media_exists.fetchone() is not None:
        return False
    print(f"INSERT INTO Media VALUES ('{media.media_id}', '{media.media_title}')")
    DATABASE_CURSOR.execute(f"INSERT INTO Media VALUES ('{media.media_id}', '{media.media_title}')")
    DATABASE_CON.commit()
    return True

"""
Method which writes an episode object to the Episode database table.

@param episode: Episode object to be added to ../db/podcasts.db
@return: True if entry is successfully added, False if entry is already present
"""
def write_to_episode_db(episode: Episode):
    episode_exists = DATABASE_CURSOR.execute(f"SELECT episode_id FROM Episode WHERE episode_id='{episode.episode_id}'")
    if episode_exists.fetchone() is not None:
        return False
    print(f"INSERT INTO Episode VALUES ('{episode.episode_id}', '{episode.media_id}', '{str(episode.timestamp)}', '{episode.episode_name}', '{episode.platform}', '{episode.transcript}')")
    DATABASE_CURSOR.execute(f"INSERT INTO Episode VALUES ('{episode.episode_id}', '{episode.media_id}', '{str(episode.timestamp)}', '{episode.episode_name}', '{episode.platform}', '{episode.transcript}')")
    DATABASE_CON.commit()
    return True

"""
Method which fetches YouTube videos given a media object and generates transcripts for all videos it possibly can.
Transcripts are only generated for videos in DATE_RANGE

@param media: Media object for which Episodes should be generated
@return: List of YouTube video IDs which were fetched
"""
def media_to_videos(media: Media):
    channels_request = YOUTUBE.channels().list(
        part="contentDetails",
        id=media.media_id
    )
    channels_response = channels_request.execute()
    uploads_request = YOUTUBE.playlistItems().list(
        part="contentDetails",
        playlistId=channels_response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        maxResults=50
    )
    uploads_response = uploads_request.execute()
    video_ids = []
    with open("sample_response.json", "w") as file:
        json.dump(uploads_response, file)
    while True:
        if 'nextPageToken' in uploads_response:
            print(uploads_response['nextPageToken'])
        for video in uploads_response['items']:
            id = video['contentDetails']['videoId']
            timestamp = datetime.strptime(video['contentDetails']['videoPublishedAt'], DATETIME_FORMAT)
            print(timestamp)
            transcript_fname = None
            if DATE_RANGE[0] <= timestamp <= DATE_RANGE[1]:
                transcript_fname = generate_script(media.media_id, id)
            if transcript_fname != None:
                episode = Episode(id, media.media_id, timestamp, "", API_SERVICE_NAME, transcript_fname)
                write_to_episode_db(episode)
                video_ids.append(episode)
        uploads_request = YOUTUBE.playlistItems().list(
            part="contentDetails",
            playlistId=channels_response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            maxResults=50,
            pageToken=uploads_response['nextPageToken'] if 'nextPageToken' in uploads_response else None
        )
        uploads_response = uploads_request.execute()
        if "nextPageToken" not in uploads_response:
            break
    return video_ids

"""
Method which generates a transcript for video with video_id and puts it into a file at ../scripts/channel_id/video_id.txt

@param channel_id: ID for channel which video comes from
@param video_id: ID for video which transcript is generated for
@return: Name of file where transcript is located
"""
def generate_script(channel_id, video_id):
    Path(f"../scripts/{channel_id}").mkdir(parents=True, exist_ok=True)
    try:
        transcript_object = YouTubeTranscriptApi.get_transcript(video_id)
        if transcript_object[len(transcript_object)-1]['start'] < 900:
            return None # if video length is less than 15 minutes then don't generate transcript
        with open(f"../scripts/{channel_id}/{video_id}.txt", "w") as file:
            for i in range(min(500, len(transcript_object))): # only generate at most 500 lines
            # for line in transcript_object:
                line = transcript_object[i]
                file.write(line['text'] + "\n")
    except TranscriptsDisabled:
        return None
    except NoTranscriptFound:
        return None
    return f"../scripts/{channel_id}/{video_id}.txt"


if __name__ == "__main__":
    # DATABASE_CURSOR.execute("DELETE FROM Media WHERE title='Contrapoints';")
    # DATABASE_CON.commit()
    main()
