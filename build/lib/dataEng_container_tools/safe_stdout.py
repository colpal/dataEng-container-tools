"""A modified version of standard output for censoring secrets.

Ensures that secrets are not accidentally printed using stdout. Has one
class safe_stdout, two helper methods, setup_stdout and setup_default_stdout,
and one global variable default_secret_folder. On import it automatically searches
for secret files and adds their contents to the list of terms to censor. Also contains
global variables containing the default secret folder, the default GCS secret location,
and the list of secret files automatically found in the default secret folder.

Typical usage example:

    setup_default_stdout()
    print('Secret Information')    # prints normally
    sys.stdout.add_words('Secret')
    print('Secret Information)     # prints '****** Information'

"""

import sys
import json
import os

default_secret_folder = '/vault/secrets/'
default_gcs_secret_locations = [default_secret_folder + 'gcp-sa-storage.json']
secrets_files = []


class safe_stdout:
    """Prints output with secrets removed.

    This class replaces sys.stdout so that when print or display
    calls are made this class is the one that processes them. The
    class maintains a list of 'bad_words' which are replaced with 
    astrices whenever someone tries to print them. By default this
    list is populated with the contents of any secret files that
    are in the default vault secrets folder.
    """

    def __init__(self, bad_words):
        """Initializes safe_stdout with desired configuration.

        Args:
            bad_words: An iterable of words to censor from output.
        """
        self.__bad_words = {}
        for item in bad_words:
            self.__bad_words[item] = len(str(item))
        self.__old_stdout = sys.stdout

    def write(self, message):
        """Outputs the desired message with secrets removed.

        Args:
          message: The message to output. Must either be a string or
            have a working __str__ method associated with it.
        """
        message = str(message)
        for bad_word in self.__bad_words:
            bad_word_location = message.find(str(bad_word))
            bad_word_length = self.__bad_words[str(bad_word)]
            while (bad_word_location != -1):
                message = (message[0:bad_word_location] +
                           '*' * bad_word_length +
                           message[bad_word_location + bad_word_length:])
                bad_word_location = message.find(str(bad_word))
        self.__old_stdout.write(message)

    def add_words(self, bad_words):
        """Adds words to the list of words to censor from output.

        Args:
          bad_words: An iterable containing the words to
            add to the list of words to censor in output.
        """
        for item in bad_words:
            self.__bad_words[item] = len(str(item))

    def flush(self):
        pass


def setup_stdout(secret_locations):
    """Adds the contents of a list of files to the words to censor from output.

    For each JSON location specified in the input, this method will open the JSON,
    parse its contents, and add the contents to the list of of words to censor from
    output. It adds the plaintext, the JSON loaded value, and the JSON dumped version of
    the value.

    Args:
      secret_locations: An iterable of strings representing the locations of JSON files
        whose contents should be censored from output.
    """
    bad_words = set()
    for file in secret_locations:
        try:
            secret = json.load(open(file, 'r'))
        except ValueError:
            print(file, "is not a properly formatted json file.")
        these_bad_words = set(secret.values())
        bad_words.update(these_bad_words)
        for word in these_bad_words:
            bad_words.add(str(json.dumps(word)))
            bad_words.add(str(json.dumps(word)).encode('unicode-escape').decode())
            bad_words.add(str(word).encode('unicode-escape').decode())
    sys.stdout.add_words(bad_words)


def setup_default_stdout(folder=default_secret_folder):
    """Censors the contents of JSONs found in the secrets folder from output.

    Takes an argument that is the string representation of a folder present in
    the container. For each JSON in that folder, this method adds the values present
    to the list of words to censor from output. This method should only be called once,
    and by design is called when this Python package is imported. Calling this method again
    after importation will not cause errors, but will lead to unnecessary delay when attempting
    to output information. To add values to the list of words to censor from output, use either
    setup_stdout() or sys.stdout.add_words().

    Args:
      folder: A string representing the folder in which to look for secret JSONs. By default 
        the value is the  default_secret_folder global variable declared in this file.
    """
    if (not os.path.exists(folder)):
        print("[WARNING] No secret files found in default directory. This is normal when running locally.")
        sys.stdout = safe_stdout([])
        return
    bad_words = set()
    files = [os.path.join(dp, f) for dp, dn, fn in os.walk(folder) for f in fn]
    print("Found these secret files:", files)
    for file in files:
        try:
            secret = json.load(open(file, 'r'))
        except ValueError:
            print(file, "is not a properly formatted json file.")
        secrets_files.append(file)
        these_bad_words = set(secret.values())
        bad_words.update(these_bad_words)
        for word in these_bad_words:
            bad_words.add(str(json.dumps(word)))
            bad_words.add(str(json.dumps(word)).encode('unicode-escape').decode())