"""Package dataEng_container_tools to dataeng_container_tools redirector.

The old package namespace is 'dataEng_container_tools' and uses many old
function names. This makes backwards compatability and deprecation difficult
from within the new package namespace.
This module-level script serves to get around that.
"""

import sys
import types
import warnings
from collections.abc import Iterable
from typing import Any

from dataeng_container_tools import cla, safe_textio, secrets_manager

warnings.warn(
    "The 'dataEng_container_tools' module is deprecated. Use 'dataeng_container_tools' instead."
    "If you wish to continue using the old package, please pin the library version to some v0 version"
    "(latest v0 is v0.6.4)",
    DeprecationWarning,
    stacklevel=2,
)

# Virtual Modules
class VirtualModule(types.ModuleType):
    """A virtual module that redirects imports to their new locations."""

    def __init__(self, name: str, mappings: dict[str, Any], *, is_package: bool=False) -> None:
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
command_line_secret = cla.CommandLineSecret

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

    def __init__(self, bad_words: Iterable) -> None:
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
