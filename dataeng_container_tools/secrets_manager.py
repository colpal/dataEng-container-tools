"""Collection of various constants and default values."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, ClassVar, Final

from .safe_textio import SafeTextIO

logger = logging.getLogger("Container Tools")


class SecretManager:
    """Stores information about secrets."""

    DEFAULT_SECRET_FOLDER: Final = Path("/vault/secrets/")

    # Dictionary to store registered module classes
    _module_classes: ClassVar[dict[str, type[Any]]] = {}
    _all_secret_paths: ClassVar[dict[str, str]] = {}

    files: ClassVar[list[Path]] = []
    secrets: ClassVar[dict[str, dict]] = {}

    # States
    _paths_initialized: ClassVar[bool] = False

    @classmethod
    def register_module(cls, module_class: type[Any]) -> None:
        """Register a module class to collect its secret paths.

        Args:
            module_class: A BaseModule subclass with MODULE_NAME and DEFAULT_SECRET_PATHS
        """
        if hasattr(module_class, "MODULE_NAME") and hasattr(module_class, "DEFAULT_SECRET_PATHS"):
            cls._module_classes[module_class.MODULE_NAME] = module_class
            logger.debug("Registered module %s for secret management", module_class.MODULE_NAME)

    @classmethod
    def initialize_secret_paths(cls) -> None:
        """Collect all secret paths from registered modules once.

        This method collects the paths and stores them in the _all_secret_paths class variable.
        It should be called once during application initialization.
        """
        if cls._paths_initialized:
            return

        all_paths = {
            key: path
            for module_class in cls._module_classes.values()
            if hasattr(module_class, "DEFAULT_SECRET_PATHS")
            for key, path in module_class.DEFAULT_SECRET_PATHS.items()
        }

        cls._all_secret_paths = all_paths
        cls._paths_initialized = True
        logger.debug("Secret paths initialized: %s", all_paths)

    @classmethod
    def get_all_secret_paths(cls) -> dict[str, str]:
        """Get all collected secret paths.

        Returns:
            A dictionary with all default secret paths from registered modules.
        """
        if not cls._paths_initialized:
            cls.initialize_secret_paths()
        return cls._all_secret_paths

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
