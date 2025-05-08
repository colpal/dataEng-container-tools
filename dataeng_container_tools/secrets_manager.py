"""Collection of various constants and default values."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, ClassVar, Final, final

from .safe_textio import SafeTextIO

logger = logging.getLogger("Container Tools")


class SecretManager:
    """Stores information about secrets."""

    DEFAULT_SECRET_FOLDER: Final = Path("/vault/secrets/")

    # Dictionary to store registered module classes
    _module_classes: ClassVar[dict[str, type[Any]]] = {}
    _all_secret_paths: ClassVar[dict[str, str]] = {}

    files: ClassVar[list[Path]] = []
    secrets: ClassVar[dict[str, str | dict]] = {}

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

        SecretLocations().update(new_secret_locations=all_paths, set_attr=True)

        cls._all_secret_paths = all_paths
        cls._paths_initialized = True

        logger.debug("Secret paths initialized: %s", all_paths)

    @classmethod
    def get_module_secret_paths(cls) -> dict[str, str]:
        """Get all collected module secret paths.

        Returns:
            A dictionary with all default secret paths from registered modules.
        """
        if not cls._paths_initialized:
            cls.initialize_secret_paths()
        return cls._all_secret_paths

    @staticmethod
    def parse_secret(file_path: Path) -> str | dict:
        """Parses the content of a secret file and returns it as a string or a dictionary.

        This method reads the content of the file specified by the given file path.
        If the content is a valid JSON object, it is parsed and returned as a dictionary.
        Otherwise, the content is returned as a stripped string.

        Args:
            file_path (Path): The path to the secret file to be parsed.

            str | dict: The content of the file as a dictionary if it is a valid JSON object,
            otherwise as a stripped string.

        Raises:
            json.JSONDecodeError: If the content is not a properly formatted JSON object
            and an attempt to parse it as JSON is made.
        """
        content = Path(file_path).read_text().strip()
        try:
            if content.startswith("{") and content.endswith("}"):
                return json.loads(content)  # JSON objects are always considered dicts according to JSONDecoder class
        except json.JSONDecodeError:
            logger.exception("%s is not a properly formatted file.", file_path.as_posix())
        return content

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
            cls.secrets[file.absolute().as_posix()] = cls.parse_secret(file)
            cls.files.append(file)
        cls.update_bad_words()

    @classmethod
    def update_bad_words(cls) -> None:
        """Update the bad words list for SafeTextIO with current secrets."""
        bad_words = set()
        for secret in SecretManager.secrets.values():
            these_bad_words = set(secret.values()) if isinstance(secret, dict) else {secret}
            bad_words.update(these_bad_words)
            for word in these_bad_words:
                bad_words.add(json.dumps(str(word)))
                bad_words.add(json.dumps(str(word)).encode("unicode-escape").decode())
                bad_words.add(str(word).encode("unicode-escape").decode())
        SafeTextIO.add_words(bad_words)


@final
class SecretLocations(dict[str, str]):
    """Dictionary of SecretLocations which can be accessed like a dict or with common attributes.

    Attributes:
        GCS (str): Default location for GCP storage secret.
        SF (str): Default location for Snowflake secret.
        DB (str): Default location for Datastore secret.
        Others: Additional keys from registered modules or input. Accessed like a dict.
    """

    # Add type hints for common attributes
    GCS: str
    SF: str
    DB: str

    _instance: ClassVar[SecretLocations | None] = None

    def __new__(cls, *_args: ..., **_kwargs: ...) -> SecretLocations:
        """Create a new instance of SecretLocations or return the existing one.

        Implements the singleton pattern to ensure only one instance exists.

        Returns:
            SecretLocations: The singleton instance
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def update(self, new_secret_locations: dict[str, str], *, set_attr: bool = False) -> None:
        """Updates the secret locations with new values and optionally sets them as attributes.

        Args:
            new_secret_locations (dict[str, str]): A dictionary containing new secret locations
                where the keys are the secret names and the values are their corresponding locations.
            set_attr (bool, optional): If True, sets the new secret locations as attributes
                of the instance. Defaults to False.
        """
        super().update(new_secret_locations)

        if set_attr:
            for key, value in new_secret_locations.items():
                setattr(self, key, value)
