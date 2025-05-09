# ruff: noqa: ANN001, ANN201, ANN204, FBT002, N803, ARG002
"""Package dataEng_container_tools to dataeng_container_tools redirector.

The old package namespace is 'dataEng_container_tools' and uses many old
function names. This makes backwards compatability and deprecation difficult
from within the new package namespace.
This module-level script serves to get around that.
"""

import sys
import types
import warnings
from typing import Any

from dataeng_container_tools import cla, deprecated_exceptions, safe_textio, secrets_manager
from dataeng_container_tools.modules.db import DB
from dataeng_container_tools.modules.gcs import GCSFileIO

warnings.warn(
    "The 'dataEng_container_tools' module is deprecated. Use 'dataeng_container_tools' instead. "
    "If you wish to continue using the old package, please pin the library version to some v0 version "
    "(latest v0 is v0.6.4)",
    DeprecationWarning,
    stacklevel=2,
)

# Virtual Modules
class VirtualModule(types.ModuleType):
    """A virtual module that redirects imports to their new locations."""

    def __init__(self, name, mappings, is_package=False):
        """Initialize the virtual module.

        Args:
            name: The name of the virtual module
            mappings: Dict mapping attribute names to their actual objects
            is_package: Whether this module should behave like a package

        """
        super().__init__(name)
        self.__mappings = mappings

        # Set package attributes if needed
        if is_package:
            self.__path__ = []  # Empty list indicates it's a namespace package

        # Add all the mapped attributes directly to the module
        for attr_name, attr_value in mappings.items():
            setattr(self, attr_name, attr_value)

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401
        """Handle attribute lookup for explicitly mapped attributes."""
        # Package-specific attributes that may be requested by import system
        if name == "__path__":
            return []

        # Only check if the attribute is in our mappings
        if name in self.__mappings:
            return self.__mappings[name]

        # No fallback - if it's not explicitly mapped, it doesn't exist
        msg = f"cannot import name '{name}' from '{self.__name__}'"
        raise ImportError(msg)


# Deprecation - cla.py
# Define the mappings
command_line_arguments = cla.CommandLineArguments

custom_command_line_argument = cla.CustomCommandLineArgument
command_line_argument_type = cla.CommandLineArgumentType
command_line_secret = cla.SecretLocations

# command_line_arguments.get_secrets was moved to SecretManager.secrets
command_line_arguments.get_secrets = lambda: secrets_manager.SecretManager.secrets  # type: ignore  # noqa: PGH003

cla_mappings = {
    "command_line_arguments": command_line_arguments,
    "command_line_argument_type": command_line_argument_type,
    "command_line_secret": command_line_secret,
    "custom_command_line_argument": custom_command_line_argument,
}

# Register the module
cla_module = VirtualModule(
    "cla",
    cla_mappings,
)
sys.modules["dataEng_container_tools.cla"] = cla_module


# Deprecation - safe_stdout.py
# Define mappings
class safe_stdout(safe_textio.SafeTextIO):  # noqa: N801
    """Deprecated wrapper for SafeTextIO class."""

    def __init__(self, bad_words):
        """Pass to SafeTextIO class."""
        if bad_words is None:
            bad_words = []
        super().__init__(textio=sys.stdout, bad_words=bad_words)


setup_stdout = secrets_manager.SecretManager.process_secret_folder
setup_default_stdout = safe_textio.setup_default_stdio

safe_stdout_mappings = {
    "safe_stdout": safe_stdout,
    "setup_stdout": setup_stdout,
    "setup_default_stdout": setup_default_stdout,
}

# Register the module
safe_stdout_module = VirtualModule(
    "safe_stdout",
    safe_stdout_mappings,
)
sys.modules["dataEng_container_tools.safe_stdout"] = safe_stdout_module


# Deprecation - exceptions.py
sys.modules["dataEng_container_tools.exceptions"] = deprecated_exceptions


# Deprecation - gcs.py
class gcs_file_io(GCSFileIO):  # noqa: N801
    """Deprecated wrapper for GCSFileIO class."""

    def __init__(self, gcs_secret_location, local=False):
        """Pass to GCSFileIO class."""
        super().__init__(
            gcs_secret_location=gcs_secret_location,
            local=local,
        )

gcs_mappings = {
    "gcs_file_io": gcs_file_io,
}

# Register the module
gcs_module = VirtualModule(
    "gcs",
    gcs_mappings,
)
sys.modules["dataEng_container_tools.gcs"] = gcs_module


# Deprecation - db.py
class Db(DB):
    """Deprecated wrapper for DB class."""

    def __init__(self, task_kind):
        """Pass to DB class."""
        self.task_kind = task_kind

    def get_data_store_client(self, PATH):
        """Pass to DB class."""
        super().__init__(task_kind=self.task_kind, gcp_secret_location=PATH)

    def get_task_entry(self, client, filter_map, kind, order_task_entries_params=None):
        """Wrapper for the updated `get_task_entry` method without requiring `client`."""
        return super().get_task_entry(
            filter_map=filter_map,
            kind=kind,
            order_task_entries_params=order_task_entries_params,
        )

    def put_snapshot_task_entry(self, client, task_entry, params):
        """Pass to DB class."""
        return super().put_snapshot_task_entry(
            task_entry=task_entry,
            params=params,
        )

    def handle_task(self, client, params, order_task_entries_params=None):
        """Pass to DB class."""
        return super().handle_task(
            params=params,
            order_task_entries_params=order_task_entries_params,
        )

db_mappings = {
    "Db": Db,
}

# Register the module
db_module = VirtualModule(
    "db",
    db_mappings,
)
sys.modules["dataEng_container_tools.db"] = db_module
