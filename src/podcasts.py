# -*- coding: utf-8 -*-

# Sample Python code for youtube.channels.list
# See instructions for running these code samples locally:
# https://developers.google.com/explorer-help/code-samples#python

import json, os

import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors

from datetime import datetime
from src.structs import Episode, Media
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
    Media("UCnQC_G5Xsjhp9fEJKuIcrSw", "The Ben Shapiro Show")
]

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
DATE_RANGE = (datetime(2018, 10, 1), datetime(2019, 10, 31))

DATABASE_CON = sqlite3.connect("../db/podcasts.db")
DATABASE_CURSOR = DATABASE_CON.cursor()
DATABASE_CURSOR.execute("CREATE TABLE Media(media_id, title)")
DATABASE_CURSOR.execute("CREATE TABLE Episode(episode_id, media_id, timestamp, episode_name, platform, transcript)")
DATABASE_CON.commit()

def main():
    for media in MEDIA_SET:
        print(media_to_videos(media))

def media_to_videos(media):
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
    while "nextPageToken" in uploads_response:
        print(uploads_response['nextPageToken'])
        for video in uploads_response['items']:
            id = video['contentDetails']['videoId']
            timestamp = datetime.strptime(video['contentDetails']['videoPublishedAt'], DATETIME_FORMAT)
            print(timestamp)
            transcript_fname = None
            if DATE_RANGE[0] <= timestamp <= DATE_RANGE[1]:
                transcript_fname = generate_script(id)
            video_ids.append(
                Episode(
                    id,
                    media,
                    timestamp,
                    "",
                    API_SERVICE_NAME,
                    transcript_fname
                )
            )
        uploads_request = YOUTUBE.playlistItems().list(
            part="contentDetails",
            playlistId=channels_response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            maxResults=50,
            pageToken=uploads_response['nextPageToken']
        )
        uploads_response = uploads_request.execute()
    for video in uploads_response['items']:
        id = video['contentDetails']['videoId']
        timestamp = datetime.strptime(video['contentDetails']['videoPublishedAt'], DATETIME_FORMAT)
        print(timestamp)
        transcript_fname = None
        if DATE_RANGE[0] <= timestamp <= DATE_RANGE[1]:
            transcript_fname = generate_script(id)
        video_ids.append(
            Episode(
                id,
                media,
                timestamp,
                "",
                API_SERVICE_NAME,
                transcript_fname
            )
        )
    return video_ids

def generate_script(channel_id, video_id):
    try:
        transcript_object = YouTubeTranscriptApi.get_transcript(video_id)
        with open(f"../scripts/{channel_id}/{video_id}.txt", "w") as file:
            for line in transcript_object:
                file.write(line['text'] + "\n")
    except TranscriptsDisabled:
        return None
    except NoTranscriptFound:
        return None
    return f"../scripts/{channel_id}/{video_id}.txt"


if __name__ == "__main__":
    main()