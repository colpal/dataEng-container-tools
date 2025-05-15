Secrets Handling
===============

The DE Container Tools package provides robust tools for handling secrets securely with the `SecretManager` and `SafeTextIO` modules.

Basic Secret Management
---------------------

Here's a basic example of using the ``SecretManager`` class:

.. code-block:: python

    from dataeng_container_tools import SecretManager, SecretLocations
    
    # Process secrets in default folder (/vault/secrets/)
    SecretManager.process_secret_folder()
    
    # Access secrets
    all_secrets = SecretManager.secrets
    
    # Get default paths from registered modules
    default_paths = SecretManager.get_module_secret_paths()
    print(f"Default secret paths: {default_paths}")
    
    # Use SecretLocations singleton to access common secret paths
    secret_locations = SecretLocations()
    gcs_secret_path = secret_locations.GCS
    db_secret_path = secret_locations.DB

Safe Handling of Output with Secrets
----------------------------------

Here's how to use ``SafeTextIO`` to prevent secrets from being accidentally printed:

.. code-block:: python

    from dataeng_container_tools import SafeTextIO, setup_default_stdio
    import sys
    
    # Setup stdout and stderr to censor secrets
    setup_default_stdio()
    
    # Define additional sensitive values to censor
    SafeTextIO.add_words(["secret_api_key", "my_password"])
    
    # Now when you print sensitive information, it will be censored
    api_key = "secret_api_key"
    password = "my_password"
    
    print(f"API Key: {api_key}")  # Output: API Key: *************
    print(f"Password: {password}")  # Output: Password: ***********
    
    # Access the original stdout if needed
    original_stdout = sys.stdout._SafeTextIO__old_textio
    
    # Create a custom SafeTextIO for a specific file
    with open("log.txt", "w") as log_file:
        safe_logger = SafeTextIO(textio=log_file)
        safe_logger.write("Sensitive info: secret_api_key")  # Writes censored text to file

Parsing Secret Files
------------------

You can parse secret files using the ``SecretManager``:

.. code-block:: python

    from dataeng_container_tools import SecretManager
    from pathlib import Path
    
    # Parse a specific secret file
    gcs_secret = SecretManager.parse_secret("/path/to/gcs-credentials.json")
    
    # If the secret is a JSON file, you can access its contents as a dictionary
    if isinstance(gcs_secret, dict):
        project_id = gcs_secret.get("project_id")
        print(f"Project ID: {project_id}")
    
    # Process all secrets in a directory
    SecretManager.process_secret_folder(Path("/custom/secrets/path"))
    
    # Access all parsed secrets
    all_secrets = SecretManager.secrets
    
    # Use parsed secrets in your application
    for secret_path, secret_content in all_secrets.items():
        print(f"Secret path: {secret_path}")
        # Content will be masked in output due to SafeTextIO

Module Integration for Secrets
----------------------------

The ``BaseModule`` class automatically registers with ``SecretManager`` to centralize secret handling:

.. code-block:: python

    from dataeng_container_tools.modules.base_module import BaseModule
    
    # Create a custom module that automatically registers with SecretManager
    class MyCustomModule(BaseModule):
        MODULE_NAME = "CUSTOM"
        DEFAULT_SECRET_PATHS = {
            "api_key": "/path/to/api_key.json",
            "config": "/path/to/config.json"
        }
        
        def __init__(self):
            super().__init__()
            # Module-specific initialization
    
    # The module's secret paths are now registered with SecretManager
    my_module = MyCustomModule()
    
    # Access all registered secret paths
    all_paths = SecretManager.get_module_secret_paths()
    print(f"All registered paths: {all_paths}")
    # Should include the CUSTOM module paths