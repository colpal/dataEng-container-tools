"""Data Engineering Container Tools."""
from .log_utils import configure_logger
from .safe_textio import setup_default_stdio

# Set up the logger
logger = configure_logger("Container Tools")

setup_default_stdio()
