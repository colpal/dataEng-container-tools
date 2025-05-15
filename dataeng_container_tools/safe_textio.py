"""A modified version of standard I/O for censoring secrets.

Ensures that secrets are not accidentally printed using stdout or stderr. Has one
class SafeTextIO, two helper methods, add_secrets_folder and setup_default_stdio,
and one global variable default_secret_folder. On import it automatically searches
for secret files and adds their contents to the list of terms to censor. Also contains
global variables containing the default secret folder, the default GCS secret location,
and the list of secret files automatically found in the default secret folder.

Typical usage example:

    setup_default_stdio()
    print("Secret Information")    # prints "Secret Information"
    SafeTextIO.add_words('Secret')
    print("Secret Information)     # prints "******* Information"

"""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING, ClassVar, Protocol, TextIO

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger("Container Tools")


class SupportsStr(Protocol):
    """A protocol that defines a __str__ method."""

    def __str__(self) -> str:
        """Return a string representation of the object."""
        ...


class SafeTextIO(TextIO):
    """Prints output with secrets removed.

    This class wraps any TextIO object so that when print or display
    calls are made this class is the one that processes them. The
    class maintains a list of 'bad_words' which are replaced with
    asterisks whenever someone tries to print them. By default this
    list is populated with the contents of any secret files that
    are in the default vault secrets folder.
    """

    bad_words: ClassVar[dict[str, int]] = {}

    def __init__(self, textio: TextIO | None = None, bad_words: Iterable[str | SupportsStr] = []) -> None:
        """Initialize safe_stdout with desired configuration.

        Args:
            textio: The original TextIO object to wrap.
            bad_words: An iterable of words to censor from output.

        """
        if not textio:
            textio = TextIO()

        self.__old_textio = textio
        SafeTextIO.add_words(bad_words)

    def write(self, message: str | SupportsStr) -> int:
        """Output the desired message with secrets removed.

        Args:
          message: The message to output. Must either be a string or
            have a working __str__ method associated with it.

        """
        message_str = str(message)

        for bad_word in self.__class__.bad_words:
            bad_word_location = message_str.find(bad_word)
            bad_word_length = self.__class__.bad_words[bad_word]
            while bad_word_location != -1:
                message_str = (
                    message_str[0:bad_word_location]
                    + "*" * bad_word_length
                    + message_str[bad_word_location + bad_word_length :]
                )
                bad_word_location = message_str.find(bad_word)
        return self.__old_textio.write(message_str)

    @classmethod
    def add_words(cls, bad_words: Iterable[str | SupportsStr]) -> None:
        """Add words to the list of words to censor from output.

        Args:
          bad_words: An iterable containing the words to
            add to the list of words to censor in output.

        """
        for item in bad_words:
            item_str = str(item)
            cls.bad_words[item_str] = len(item_str)


def setup_default_stdio() -> None:
    """Censors the contents of JSONs found in the secrets folder from output.

    This function sets up both sys.stdout and sys.stderr with the SafeTextIO wrapper.
    This method should only be called once, and by design is called when this Python
    package is imported. To add values to the list of words to censor from output, use
    either add_secrets_folder() or SafeTextIO.add_words().
    """
    sys.stdout = SafeTextIO(textio=sys.stdout)
    sys.stderr = SafeTextIO(textio=sys.stderr)
