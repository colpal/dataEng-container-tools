"""Data Engineering Container Tools."""
from .log_utils import configure_logger
from .safe_textio import setup_default_stdio
from .secrets_manager import SecretManager

__version__ = "1.0.0"

# Set up the logger
logger = configure_logger("Container Tools")

# Initialize secrets and stdout/stderr bad words output
setup_default_stdio()
SecretManager.process_secret_folder()
