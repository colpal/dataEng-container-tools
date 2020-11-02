import sys
import json
import os

default_secret_folder = '/vault/secrets/'

class safe_stdout:
    def __init__(self, bad_words):
        self.__bad_words = list(set(bad_words))
        self.__bad_word_lengths = [len(bad_word) for bad_word in bad_words]
        self.__old_stdout = sys.stdout

    def write(self, message):
        for location, bad_word in enumerate(self.__bad_words):
            bad_word_length = self.__bad_word_lengths[location]
            bad_word_location = message.find(bad_word)
            while(bad_word_location != -1):
                message = (message[0:bad_word_location] + '*'*bad_word_length +
                           message[bad_word_location + bad_word_length:])
                bad_word_location = message.find(bad_word)
        self.__old_stdout.write(message)

    def add_words(self, bad_words):
        self.__bad_words += bad_words
        self.__bad_words = list(set(self.__bad_words))
        self.__bad_word_lengths = [len(bad_word) for bad_word in self.__bad_words]

    def flush(self):
        pass

def setup_stdout(secret_locations):
    bad_words = set()
    for file in secret_locations:
        try:
            secret = json.load(open(file,'r'))
        except ValueError:
            print(file, "is not a properly formatted json file.")
        these_bad_words = set(secret.values())
        bad_words.update(these_bad_words)
        for word in these_bad_words:
            bad_words.add(json.dumps(word))
    sys.stdout.add_words(bad_words)

def setup_default_stdout(folder = default_secret_folder):
    if(not os.path.exists(folder)):
        print("No secret files found in default directory")
        sys.stdout = safe_stdout([])
        return
    bad_words = set()
    files = [os.path.join(dp, f) for dp, dn, fn in os.walk(folder) for f in fn]
    # print("Found these secret files:", files)
    for file in files:
        try:
            secret = json.load(open(file,'r'))
        except ValueError:
            print(file, "is not a properly formatted json file.")
        these_bad_words = set(secret.values())
        bad_words.update(these_bad_words)
        for word in these_bad_words:
            bad_words.add(json.dumps(word))
    sys.stdout = safe_stdout(bad_words)