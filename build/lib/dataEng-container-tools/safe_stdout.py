import sys
import json

class safe_stdout:
    def __init__(self, bad_words):
        self.__bad_words = bad_words
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

    def flush(self):
        pass

def setup_stdout(secret_location):
        secret = {}
        try:
            secret = json.load(open(secret_location,'r'))
        except OSError:
            print("No secret file found")
            return
        bad_words = list(secret.values())
        dumped_words = []
        for word in bad_words:
            dumped_words.append(json.dumps(word))
        bad_words += dumped_words
        #print("\nBad words:", bad_words)
        sys.stdout = safe_stdout(bad_words)