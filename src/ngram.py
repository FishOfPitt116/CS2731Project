import sqlite3
from structs import Comment, Episode, Media
import nltk
from nltk import word_tokenize, ngrams
from nltk.lm import Laplace
from nltk.lm.preprocessing import pad_both_ends
import random
import csv

nltk.download('punkt')
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

print("generating training sets")
TRAIN_SETS = {}
for m in media:
    # set up train set
    train = []
    episodes = [Episode(episode_id, media_id, timestamp, episode_name, platform, transcript) for (episode_id, media_id, timestamp, episode_name, platform, transcript) in episode_table_contents(m.media_id)]
    for e in episodes:
        with open(e.transcript) as f:
            train += f.readlines()
    TRAIN_SETS[m.media_id] = train

print("generating testing sets")
TEST_SETS = {}
for m in media:
    # set up test set
    test = []
    comments = [Comment(media_id, user, content, timestamp) for (media_id, user, content, timestamp) in comment_table_contents(m.media_id)]
    test = [c.content for c in comments]
    TEST_SETS[m.media_id] = test

perplexity_file = open("../stats/unigram_perplexity.csv", "a")
file_writer = csv.writer(perplexity_file)

print("calculating perplexities on own models")
for m in media:
    # Tokenize and process each line
    train_tokens = [word_tokenize(line.lower()) for line in TRAIN_SETS[m.media_id]]
    frequencies = {}
    for sentence in train_tokens:
        for token in sentence:
            if token not in frequencies:
                frequencies[token] = 0
            frequencies[token] += 1
    keep_tokens = sorted(frequencies, key=lambda x: frequencies[x], reverse=True)[:500]
    for i in range(len(train_tokens)):
        for j in range(len(train_tokens[i])):
            if train_tokens[i][j] not in keep_tokens:
                train_tokens[i][j] = "<UNK>"
    test_tokens = [word_tokenize(line.lower()) for line in TEST_SETS[m.media_id]]
    for i in range(len(test_tokens)):
        for j in range(len(test_tokens[i])):
            if test_tokens[i][j] not in keep_tokens:
                test_tokens[i][j] = "<UNK>"

    # unigrams with laplace smoothing
    n = 1
    train_ngrams = [list(ngrams(pad_both_ends(tokens, n=n), n)) for tokens in train_tokens]

    lm = Laplace(order=n)
    lm.fit(train_ngrams, vocabulary_text=[word for tokens in train_tokens for word in tokens])

    # Test the model on the test set
    test_ngrams = [list(ngrams(pad_both_ends(tokens, n=n), n)) for tokens in test_tokens]
    test_perplexity = lm.perplexity(test_ngrams)

    # print(f"Perplexity: {test_perplexity}")
    print(f"Perplexity of {m.media_title} model with {m.media_title} test data: {test_perplexity}")

    file_writer.writerow((m.media_title, m.media_title, test_perplexity))

    other_media = [m]
    while m in other_media:
        other_media = random.sample(media, 5)
        
    for om in other_media:
        test_tokens = [word_tokenize(line.lower()) for line in TEST_SETS[om.media_id]]
        for i in range(len(test_tokens)):
            for j in range(len(test_tokens[i])):
                if test_tokens[i][j] not in keep_tokens:
                    test_tokens[i][j] = "<UNK>"
        test_ngrams = [list(ngrams(pad_both_ends(tokens, n=n), n)) for tokens in test_tokens]
        test_perplexity = lm.perplexity(test_ngrams)

        print(f"Perplexity of {m.media_title} model with {om.media_title} test data: {test_perplexity}")

        file_writer.writerow((m.media_title, om.media_title, test_perplexity))

perplexity_file.close()