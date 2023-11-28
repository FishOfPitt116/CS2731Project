import json, os, shutil
from datetime import datetime
from pathlib import Path
from structs import Comment, Post
import sqlite3
import zipfile

SUBREDDIT_DICT = {
    "Red Scare Podcast": "redscarepod",
    "The NPR Politics Podcast": "NPR",
    "Planet Money": "nprplanetmoney",
    "The Ben Shapiro Show": "benshapiro",
    "The Journal.": "wsj",
    "Pod Save America": "FriendsofthePod",
    "Louder with Crowder": "stevencrowder",
    "The Young Turks": "tytonreddit",
    "The Rachel Maddow Show": "RachelMaddow",
    "Real Time with Bill Maher": "Maher",
    "The Daily": "Thedaily",
    "FiveThirtyEight": "fivethirtyeight",
    "The Majority Report with Sam Seder": "TheMajorityReport",
    "The Rubin Report": "daverubin",
    "Chapo Trap House": "ChapoTrapHouse",
    "Jordan B Peterson": "JordanPeterson",
    "CNN Inside Politics": "cnn",
    "Making Sense with Sam Harris": "samharris",
    "Destiny": "Destiny",
    "Secular Talk": "seculartalk",
    "Contrapoints": "ContraPoints",
    "The David Pakman Show": "thedavidpakmanshow",
    "H3 Podcast": "h3h3productions",
    "The Joe Rogan Experience": "JoeRogan",
    "Alex Jones Radio Show": "infowarsdotcom",
    "Tucker Carlson Tonight": "tucker_carlson",
    "The Ramsey Show": "DaveRamsey",
    "Democracy Now!": "DemocracyNow",
    "The Jimmy Dore Show": "jimmydore",
    "Last Week Tonight with John Oliver": "lastweektonight",
    "Political Gabfest": "slate",
    "Human Rights Watch": "hrw",
    "hbomberguy": "hbomberguy"
}

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
DATE_RANGE = (datetime(2018, 10, 1), datetime(2019, 10, 31))

DATABASE_CON = sqlite3.connect("../db/podcasts.db")
DATABASE_CURSOR = DATABASE_CON.cursor()

"""
Main method stores raw comment data in database
"""
def main():
    create_db_tables()
    for name in SUBREDDIT_DICT.keys():
        result = DATABASE_CURSOR.execute(f"SELECT media_id FROM Media WHERE title='{name}'")
        media_id = result.fetchone()

        if media_id is None:
            continue

        media_id = media_id[0]

        # DEBUG
        if name != "Louder with Crowder":
            continue

        # delete current contents of working directory
        if os.path.exists('../comments/data/tmp') and os.path.isdir('../comments/data/tmp'):
            shutil.rmtree('../comments/data/tmp')

        # pull subreddit data to working directory
        path_to_zipped = '../comments/data/' + SUBREDDIT_DICT[name] + '.corpus.zip'
        with zipfile.ZipFile(path_to_zipped, 'r') as z:
            z.extractall('../comments/tmp')

        post_file = '../comments/tmp/conversations.json'
        comment_file = '../comments/tmp/utterances.jsonl'

        # save posts within target period
        with open(post_file) as f:
            post_data = json.loads(f.read())
            for post_id in post_data.keys():
                content = post_data[post_id]['title']
                if content == "":
                    continue
                timestamp = datetime.fromtimestamp(post_data[post_id]['timestamp'])
                print(timestamp)
                if DATE_RANGE[0] <= timestamp and timestamp <= DATE_RANGE[1]:
                    write_to_post_db(Post(media_id, post_id, timestamp, content))

        # save comments within target period
        with open(comment_file) as f:
            lines = f.readlines()
            for line in lines:
                comment_data = json.loads(line)
                post_id = comment_data['root']
                user = comment_data['user']
                content = comment_data['text']
                timestamp = datetime.fromtimestamp(comment_data['timestamp'])

                # skip if post_id not in database
                result = DATABASE_CURSOR.execute(f"SELECT post_id FROM Post WHERE post_id=?", (post_id,))
                result_id = result.fetchone()
                if result_id is None:
                    continue

                # skip if no content
                if content == "":
                    continue

                if DATE_RANGE[0] <= timestamp <= DATE_RANGE[1]:
                    write_to_comment_db(Comment(media_id, post_id, user, content, timestamp))

"""
Method which writes a post object to the Post database table.

@param media: Post object to be added to ../db/podcasts.db
@return: True if entry is successfully added, False if entry is already present
"""
def write_to_post_db(post: Post):
    post_exists = DATABASE_CURSOR.execute(f"SELECT post_id FROM Post WHERE post_id='{post.post_id}'")
    if post_exists.fetchone() is not None:
        return False
    DATABASE_CURSOR.execute("INSERT INTO Post VALUES (?, ?, ?, ?)", (post.media_id, post.post_id, post.content, post.timestamp))
    DATABASE_CON.commit()
    return True

"""
Method which writes a comment object to the Comment database table.

@param media: Comment object to be added to ../db/podcasts.db
@return: True if entry is successfully added, False if entry is already present
"""
def write_to_comment_db(comment: Comment):
    comment_exists = DATABASE_CURSOR.execute(f"SELECT post_id FROM Comment WHERE timestamp=? AND user=? AND content=?", (comment.timestamp, comment.user, comment.content))
    if comment_exists.fetchone() is not None:
        return False
    DATABASE_CURSOR.execute("INSERT INTO Comment VALUES (?, ?, ?, ?, ?)", (comment.media_id, comment.post_id, comment.user, comment.content, comment.timestamp))
    DATABASE_CON.commit()
    return True


"""
Method which creates database tables if they aren't already present in ../db/podcasts.db
"""
def create_db_tables():
    post_exists = DATABASE_CURSOR.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Post';")
    if post_exists.fetchone() is None:
        DATABASE_CURSOR.execute("CREATE TABLE Post(media_id string, post_id string, content string, timestamp timestamp, foreign key(media_id) references Media(media_id))")

    comment_exists = DATABASE_CURSOR.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Comment';")
    if comment_exists.fetchone() is None:
        DATABASE_CURSOR.execute("CREATE TABLE Comment(media_id string, post_id string, user string, content string, timestamp timestamp, foreign key(post_id) references Post(post_id), foreign key(media_id) references Media(media_id))")

    DATABASE_CON.commit()

if __name__ == "__main__":
    main()