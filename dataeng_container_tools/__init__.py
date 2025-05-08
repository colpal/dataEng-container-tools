"""Data Engineering Container Tools."""

__version__ = "1.0.0"

from .cla import (
    CommandLineArguments,
    CommandLineArgumentType,
    CustomCommandLineArgument,
)
from .log_utils import configure_logger
from .modules import DB, GCS, GCSFileIO
from .safe_textio import SafeTextIO, setup_default_stdio
from .secrets_manager import SecretLocations, SecretManager

__all__ = [
    "DB",
    "GCS",
    "CommandLineArgumentType",
    "CommandLineArguments",
    "CustomCommandLineArgument",
    "GCSFileIO",
    "SafeTextIO",
    "SecretLocations",
    "SecretManager",
    "configure_logger",
    "setup_default_stdio",
]

# Set up the logger
logger = configure_logger("Container Tools")

# Initialize secrets and stdout/stderr bad words output
setup_default_stdio()
SecretManager.initialize_secret_paths()
SecretManager.process_secret_folder()
