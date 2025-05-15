#!/usr/bin/env python3
"""Load environment variables from .env file.

This script loads environment variables from a .env file in the current directory.
It's used by the documentation build process to load Confluence credentials.
"""

import os
from pathlib import Path


def load_env_file(env_file=None):
    """Load environment variables from .env file.

    Args:
        env_file: Path to the .env file. If None, looks for .env in the current directory.

    Returns:
        dict: Dictionary of loaded environment variables
    """
    if env_file is None:
        env_file = Path(__file__).parent / ".env"

    env_vars = {}

    try:
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if not line or line.startswith("#"):
                    continue

                # Parse key=value lines
                if "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()

                    # Remove quotes if present
                    if (value.startswith('"') and value.endswith('"')) or (
                        value.startswith("'") and value.endswith("'")
                    ):
                        value = value[1:-1]

                    # Set environment variable
                    os.environ[key] = value
                    env_vars[key] = value

        return env_vars
    except FileNotFoundError:
        print(f"Warning: Environment file {env_file} not found.")
        return {}


if __name__ == "__main__":
    # When run directly, load and display the environment variables
    env_vars = load_env_file()

    if env_vars:
        print("Loaded environment variables:")
        for key, value in env_vars.items():
            # Mask API keys and passwords for security
            if "KEY" in key or "PASSWORD" in key or "TOKEN" in key or "SECRET" in key:
                masked_value = value[:4] + "****" + value[-4:] if len(value) > 8 else "********"
                print(f"  {key}={masked_value}")
            else:
                print(f"  {key}={value}")
    else:
        print("No environment variables loaded.")
        print("To use this script, create a .env file with your Confluence credentials.")
        print("You can copy the .env.example file as a starting point.")
