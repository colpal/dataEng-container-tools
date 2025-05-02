"""A modified version of standard output for censoring secrets.

Ensures that secrets are not accidentally printed using stdout. Has one
class safe_stdout, two helper methods, setup_stdout and setup_default_stdout,
and one global variable default_secret_folder. On import it automatically searches
for secret files and adds their contents to the list of terms to censor. Also contains
global variables containing the default secret folder, the default GCS secret location,
and the list of secret files automatically found in the default secret folder.

Typical usage example:

    setup_default_stdout()
    print("Secret Information")    # prints "Secret Information"
    sys.stdout.add_words("Secret")
    print("Secret Information)     # prints "******* Information"

"""

import sys
import warnings
from collections.abc import Iterable

from . import safe_textio, secrets_manager

warnings.warn(
    "The 'safe_stdout' module is deprecated. Please import from 'safe_textio' instead.",
    DeprecationWarning,
    stacklevel=2,
)


class safe_stdout(safe_textio.SafeTextIO):  # noqa: N801
    """Compatibility class that wraps SafeTextIO to maintain backward compatibility."""

    def __init__(self, bad_words: Iterable) -> None:
        """Initialize with stdout as the default TextIO object."""
        if bad_words is None:
            bad_words = []
        super().__init__(textio=sys.stdout, bad_words=bad_words)


setup_stdout = secrets_manager.SecretManager.process_secret_folder
setup_default_stdout = safe_textio.setup_default_stdio
