"""Base module for data engineering container tools.

This module provides a base class for all specialized modules such as GCS, DB, etc.
It offers common functionality and a consistent interface that all module implementations
should follow, ensuring a uniform API across the library.

Typical usage example:

    class GCSModule(BaseModule):
        DEFAULT_MODULE_TYPE = "GCS"
        DEFAULT_SECRET_PATHS = {
            "service_account": "/vault/secrets/gcp-sa-storage.json",
            "config": "/vault/secrets/gcs-config.json"
        }

        def __init__(self, override_secret_paths=None, **kwargs):
            super().__init__(override_secret_paths, **kwargs)
            # GCS-specific initialization
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from dataeng_container_tools import CommandLineArguments

logger = logging.getLogger("Container Tools")


class BaseModule:
    """Base class for all specialized modules.

    This abstract class defines the common interface and functionality that
    all module implementations should follow. It provides methods for handling
    secrets, initialization, and common utilities.

    Attributes:
        MODULE_NAME (ClassVar[str]): Identifies the module type for logging and display.
        DEFAULT_SECRET_PATHS (ClassVar[dict[str, str]]): Default secret file paths for this module.
            Keys are predefined secret types, values are file paths.
        local (bool): Indicates if the module is operating in local mode (no external services).
        secret_paths (dict[str, Path]): Dictionary of secret file paths used by the module.
        client (Any): Client instance used to interact with external services.

    """

    # Class attributes to identify the module type and its default secret paths
    MODULE_NAME: ClassVar[str] = "BASE"
    DEFAULT_SECRET_PATHS: ClassVar[dict[str, str]] = {}

    def __init__(
        self,
        override_secret_paths: dict[str, str | Path] | None = None,
        *,
        local: bool = False,
    ) -> None:
        """Initialize the base module.

        Args:
            override_secret_paths: Dictionary of secret file paths to override the defaults.
                Keys should match those defined in the class's DEFAULT_SECRET_PATHS.
                Values are file paths to the secrets.
                If None, uses the class's DEFAULT_SECRET_PATHS.
            local: If True, operates in local-only mode without connecting to external services.

        """
        self.local = local
        self.client: Any = None

        # Initialize with default secret paths
        self.secret_paths = {}
        if self.DEFAULT_SECRET_PATHS:
            self.secret_paths = {k: Path(v) for k, v in self.DEFAULT_SECRET_PATHS.items()}

        # Update with any provided secret paths - allow overriding existing paths
        if override_secret_paths:
            for key, path in override_secret_paths.items():
                self.secret_paths[key] = Path(path)

    def get_client(self) -> ...:
        """Get the client instance for this module.

        Returns:
            The client instance used to interact with external services.
            Returns None if no client has been initialized.

        """
        return self.client

    def to_dict(self) -> dict[str, Any]:
        """Convert module configuration to a dictionary.

        Returns:
            A dictionary representation of the module's configuration.

        """
        return {
            "module_name": self.MODULE_NAME,
            "local_mode": self.local,
            "secret_paths": {k: str(v) for k, v in self.secret_paths.items()} if self.secret_paths else {},
        }

    def __str__(self) -> str:
        """Return a string representation of the module.

        Returns:
            A string representation of the module's configuration.

        """
        return str(self.to_dict())

    @classmethod
    def get_default_secret_paths(cls) -> dict[str, Path]:
        """Get the default secret paths for this module.

        Returns:
            A dictionary of default secret keys and their file paths.

        """
        return {k: Path(v) for k, v in cls.DEFAULT_SECRET_PATHS.items()}

    @classmethod
    def from_command_line_args(cls, args: CommandLineArguments) -> BaseModule:
        """Create a module instance from command line arguments.

        This factory method simplifies creating module instances from command line arguments
        by automatically resolving the appropriate secret locations.

        Args:
            args: The parsed command line arguments object with get_secret_locations method.

        Returns:
            An initialized module instance.

        """
        override_secret_paths = {}
        try:
            # Try to get the secret locations from command line arguments
            secret_locations = args.get_secret_locations()
            if secret_locations and hasattr(secret_locations, cls.MODULE_NAME):
                secret_path = getattr(secret_locations, cls.MODULE_NAME)
                # If there's a single string secret location from command line args,
                # use the first key from DEFAULT_SECRET_PATHS if available
                if isinstance(secret_path, str):
                    default_keys = list(cls.DEFAULT_SECRET_PATHS.keys())
                    if default_keys:
                        # Use the first default key from the module
                        override_secret_paths[default_keys[0]] = secret_path
                # If the secret is a dictionary, use it directly
                elif isinstance(secret_path, dict):
                    override_secret_paths.update(secret_path)
        except (AttributeError, TypeError):
            # Fall back to module's defaults if command line args don't work
            pass

        return cls(
            override_secret_paths=override_secret_paths or None,  # Use None to trigger default paths if empty
            local=getattr(args, "running_local", False),
        )
