import json, os, shutil
from datetime import datetime
from pathlib import Path
from structs import Comment
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
    "The Alex Jones Show": "infowarsdotcom",
    "Tucker Carlson Tonight": "tucker_carlson",
    "The Ramsey Show": "DaveRamsey",
    "Democracy Now!": "DemocracyNow",
    "The Jimmy Dore Show": "jimmydore",
    "Last Week Tonight with John Oliver": "lastweektonight",
    "Political Gabfest": "slate",
    "Human Rights Watch": "hrw",
    "hbomberguy": "hbomberguy"
}

# NOTE: We removed the date range for The Alex Jones Show, Democracy Now!, Political Gabfest and Human Rights Watch due to limited data within that range (<100 commnets).

comment_counts = {}

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
DATE_RANGE = (datetime(2017, 10, 1), datetime(2018, 10, 31))

DATABASE_CON = sqlite3.connect("../db/podcasts.db")
DATABASE_CURSOR = DATABASE_CON.cursor()

"""
Main method stores raw comment data in database
"""
def main():
    create_db_tables()
    for name in SUBREDDIT_DICT.keys():
        users = {}

        result = DATABASE_CURSOR.execute(f"SELECT media_id FROM Media WHERE title='{name}'")
        media_id = result.fetchone()

        if media_id is None:
            continue

        media_id = media_id[0]
        comment_counts[name] = 0

        # delete current contents of working directory
        if os.path.exists('../comments/tmp') and os.path.isdir('../comments/tmp'):
            shutil.rmtree('../comments/tmp')

        # pull subreddit data to working directory
        path_to_zipped = '../comments/data/' + SUBREDDIT_DICT[name] + '.corpus.zip'
        with zipfile.ZipFile(path_to_zipped, 'r') as z:
            z.extractall('../comments/tmp')

        comment_file = '../comments/tmp/' + SUBREDDIT_DICT[name] + '.corpus/utterances.jsonl'

        # save comments within target period

        try:
            open(comment_file).close()
        except FileNotFoundError:
            comment_file = '../comments/tmp/utterances.jsonl'

        with open(comment_file) as f:
            lines = f.readlines()
            for line in lines:
                comment_data = json.loads(line)
                user = comment_data['user']
                content = comment_data['text']
                timestamp = datetime.fromtimestamp(comment_data['timestamp'])

                # filter out empty, deleted, and short (<10 characters)
                if content == "":
                    continue
                if "[removed]" in content or "[deleted]" in content:
                    continue
                if len(content) < 10:
                    continue

                if DATE_RANGE[0] <= timestamp <= DATE_RANGE[1]:
                    # only write first 500 comments, and all comments from first 50 users, to DB
                    c = Comment(media_id, user, content, timestamp)
                    if user in users and users[user] < 100:
                        write_to_comment_db(c)
                        comment_counts[name] += 1
                        users[user] += 1
                    elif len(users) < 50:
                        write_to_comment_db(c)
                        comment_counts[name] += 1
                        users[user] = 1
                    elif comment_counts[name] < 500:
                        write_to_comment_db(c)
                        comment_counts[name] += 1
        print("SAVED " + str(comment_counts[name]) + " COMMENTS FOR " + name)

    print("Total Comments Added...")
    print(comment_counts)

"""
Method which writes a comment object to the Comment database table.

@param media: Comment object to be added to ../db/podcasts.db
@return: True if entry is successfully added, False if entry is already present
"""
def write_to_comment_db(comment: Comment):
    comment_exists = DATABASE_CURSOR.execute(f"SELECT media_id FROM Comment WHERE timestamp=? AND user=? AND content=?", (comment.timestamp, comment.user, comment.content))
    if comment_exists.fetchone() is not None:
        return False
    DATABASE_CURSOR.execute("INSERT INTO Comment VALUES (?, ?, ?, ?)", (comment.media_id, comment.user, comment.content, comment.timestamp))
    DATABASE_CON.commit()
    return True


"""
Method which creates database tables if they aren't already present in ../db/podcasts.db
"""
def create_db_tables():
    comment_exists = DATABASE_CURSOR.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Comment';")
    if comment_exists.fetchone() is None:
        DATABASE_CURSOR.execute("CREATE TABLE Comment(media_id string, user string, content string, timestamp timestamp, foreign key(media_id) references Media(media_id))")

    DATABASE_CON.commit()

if __name__ == "__main__":
    main()
