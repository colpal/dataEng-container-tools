"""Collection of various constants and default values."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import ClassVar, Final

from .safe_textio import SafeTextIO

logger = logging.getLogger("Container Tools")


class SecretManager:
    """Stores information about secrets."""

    DEFAULT_SECRET_FOLDER: Final = Path("/vault/secrets/")
    DEFAULT_SECRET_LOCATIONS: Final = {
        "GCS": DEFAULT_SECRET_FOLDER / "gcp-sa-storage.json",
        "SF": DEFAULT_SECRET_FOLDER / "sf_creds.json",
    }

    files: ClassVar[list[Path]] = []
    secrets: ClassVar[dict[str, dict]] = {}

    @classmethod
    def _process_secret(cls, file_path: Path) -> None:
        try:
            with file_path.open() as f:
                secret = json.load(f)
            cls.files.append(file_path)
            cls.secrets[file_path.stem] = secret
        except ValueError:
            logger.exception("%s is not a properly formatted json file.", file_path.as_posix())

    @classmethod
    def process_secret_folder(cls, folder: str | Path = DEFAULT_SECRET_FOLDER) -> None:
        """Process all secret files in the given folder."""
        folder_path = Path(folder)
        if not folder_path.exists():
            logger.info(
                "No secret files found in default directory. This is normal when running locally.",
            )
            return

        files = [file_path for file_path in folder_path.glob("**/*") if file_path.is_file()]
        logger.info("Found these secret files: %s", [file.as_posix() for file in files])
        for file in files:
            cls._process_secret(file)
        cls.update_bad_words()

    @classmethod
    def update_bad_words(cls) -> None:
        """Update the bad words list for SafeTextIO with current secrets."""
        bad_words = set()
        for secret in SecretManager.secrets.values():
            these_bad_words = set(secret.values())
            bad_words.update(these_bad_words)
            for word in these_bad_words:
                bad_words.add(json.dumps(str(word)))
                bad_words.add(json.dumps(str(word)).encode("unicode-escape").decode())
                bad_words.add(str(word).encode("unicode-escape").decode())
        SafeTextIO.add_words(bad_words)
