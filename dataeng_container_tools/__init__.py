"""Data Engineering Container Tools."""

__version__ = "1.0.0"

from .cla import (
    CommandLineArguments,
    CommandLineArgumentType,
    CommandLineSecret,
    CustomCommandLineArgument,
)
from .log_utils import configure_logger
from .modules import GCS, GCSFileIO
from .safe_textio import SafeTextIO, setup_default_stdio
from .secrets_manager import SecretManager

__all__ = [
    "GCS",
    "CommandLineArgumentType",
    "CommandLineArguments",
    "CommandLineSecret",
    "CustomCommandLineArgument",
    "GCSFileIO",
    "SafeTextIO",
    "SecretManager",
    "configure_logger",
    "setup_default_stdio",
]

# Set up the logger
logger = configure_logger("Container Tools")

# Initialize secrets and stdout/stderr bad words output
setup_default_stdio()
SecretManager.process_secret_folder()
