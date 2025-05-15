Command Line Arguments Usage
=========================

The Command Line Arguments (CLA) module provides tools for parsing command line inputs in a standardized way.

Basic Usage
----------

Here's a basic example of using the ``CommandLineArguments`` class:

.. code-block:: python

    from dataeng_container_tools import CommandLineArguments, CommandLineArgumentType

    # Create command line arguments parser
    args = CommandLineArguments(
        input_files=CommandLineArgumentType.REQUIRED,      # Require input file arguments
        output_files=CommandLineArgumentType.REQUIRED,     # Require output file arguments
        secret_locations=CommandLineArgumentType.OPTIONAL, # Make secret locations optional
    )

    # Get parsed arguments
    input_uris = args.get_input_uris()
    output_uris = args.get_output_uris()

    # Print retrieved arguments
    print(f"Input URIs: {input_uris}")
    print(f"Output URIs: {output_uris}")

When you run this script, you'll need to provide the required command line arguments:

.. code-block:: bash

    python my_script.py \
        --input_bucket_names input-bucket \
        --input_paths path/to/inputs \
        --input_filenames input_file.csv \
        --output_bucket_names output-bucket \
        --output_paths path/to/outputs \
        --output_filenames output_file.csv

Custom Arguments
--------------

You can add custom command line arguments using the ``CustomCommandLineArgument`` class:

.. code-block:: python

    from dataeng_container_tools import (
        CommandLineArguments, 
        CommandLineArgumentType,
        CustomCommandLineArgument
    )

    # Define custom arguments
    custom_args = [
        CustomCommandLineArgument(
            name="batch_size",
            data_type=int,
            default=100,
            required=False,
            help_message="Batch size for processing"
        ),
        CustomCommandLineArgument(
            name="verbose",
            action="store_true",
            required=False,
            help_message="Enable verbose output"
        )
    ]

    # Create command line arguments parser with custom arguments
    args = CommandLineArguments(
        custom_inputs=custom_args,
        description="My data processing script"
    )

    # Get the parsed arguments namespace
    parsed_args = args.get_arguments()

    # Access custom arguments
    batch_size = parsed_args.batch_size
    verbose = parsed_args.verbose

    if verbose:
        print(f"Using batch size: {batch_size}")

When you run this script, you can provide the custom arguments:

.. code-block:: bash

    python my_script.py --batch_size 200 --verbose

Working with Secret Locations
---------------------------

The ``CommandLineArguments`` class also handles secret locations:

.. code-block:: python

    from dataeng_container_tools import (
        CommandLineArguments, 
        CommandLineArgumentType,
        SecretLocations
    )

    # Create command line arguments parser with secret locations
    args = CommandLineArguments(
        secret_locations=CommandLineArgumentType.REQUIRED
    )
    
    # When SecretLocations is returned as a dictionary with specified secret paths
    secret_paths = SecretLocations()
    
    # Access secret paths
    gcs_secret = secret_paths.GCS
    db_secret = secret_paths.DB
    
    print(f"GCS Secret Path: {gcs_secret}")
    print(f"DB Secret Path: {db_secret}")

When using this script, you would provide the secret locations as a JSON dictionary:

.. code-block:: bash

    python my_script.py --secret_locations '{"GCS": "/path/to/gcs_secret.json", "DB": "/path/to/db_secret.json"}'