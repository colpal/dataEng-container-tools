Command Line Arguments Usage
============================

The Command Line Arguments (CLA) module provides tools for parsing command line inputs in a standardized way.

Most of CLA acts as a wrapper for `argparse <https://docs.python.org/3/library/argparse.html>`_. The main benefit of this class comes from various presets and automation with other modules in this library.

.. note::

    In the following examples, Python script argument values will be passed through the commandline. In most actual use cases, this will be through an Airflow KubernetesPodOperator.

Basic Usage
-----------

The ``CommandLineArguments`` class has multiple parameters initialied as ``CommandLineArgumentType.UNUSED``. When set otherwise, a preset list of arguments will be added to the Python script.

The two options each CLA parameter can be set is ``OPTIONAL`` or ``REQUIRED``. ``OPTIONAL`` will cause the added list of Python script arguments to not be required on use, potentially leaving them as ``None`` in code. ``REQUIRED`` will cause the program to crash if the added arguments are not provided and ensures their values will not be ``None``.

.. code-block:: python

    from dataeng_container_tools import CommandLineArguments, CommandLineArgumentType

    # Create command line arguments parser
    cla = CommandLineArguments(
        input_files=CommandLineArgumentType.REQUIRED,      # Require input file arguments
        output_files=CommandLineArgumentType.REQUIRED,     # Require output file arguments
        secret_locations=CommandLineArgumentType.OPTIONAL, # Make secret locations optional
    )

    # Get parsed arguments
    input_uris = cla.get_input_uris()
    output_uris = cla.get_output_uris()

    # Print retrieved arguments
    print(f"Input URIs: {input_uris}")
    print(f"Output URIs: {output_uris}")

The arguments can be passed through the commandline as so follows. Note that secret_location args were ``OPTIONAL`` so we can exclude it safely.

.. code-block:: bash

    python my_script.py \
        --input_bucket_names input-bucket \
        --input_paths path/to/inputs \
        --input_filenames input_file.csv \
        --output_bucket_names output-bucket \
        --output_paths path/to/outputs \
        --output_filenames output_file.csv

The output will be:

.. code-block:: text

    Input URIs: ['gs://input-bucket/path/to/inputs/input_file.csv']
    Output URIs: ['gs://output-bucket/path/to/outputs/output_file.csv']

Custom Arguments
----------------

To add additional arguments, use the ``CustomCommandLineArgument`` class which fully wraps around `argparse's add_argument function <https://docs.python.org/3/library/argparse.html#the-add-argument-method>`_. There might be slight differences in parameter naming.

In short, define a ``name`` at minimum which will be accessed as ``--name`` in the commandline. Additional parameters such as `data_type`, `default`, and `required` may be useful in restricting what gets inputted.

Of the ``argparse`` args, ``type`` or ``data_type`` here is the most interesting since it allows functions. This can be to force additional useful types such as ``json`` or add custom restrictions.

.. code-block:: python

    import ast
    import json
    from argparse import ArgumentTypeError

    from dataeng_container_tools import CommandLineArguments, CustomCommandLineArgument


    def positive_int(value: str) -> int:
        """Argparse type to ensure only positive integers."""
        ivalue = int(value)
        if ivalue < 1:
            msg = f"{value} is an invalid positive int value"
            raise ArgumentTypeError(msg)
        return ivalue


    # Define custom arguments
    custom_args = [
        CustomCommandLineArgument(
            name="some_number",
            data_type=int,
            default=0,
            required=True,
        ),
        CustomCommandLineArgument(
            name="some_json",
            data_type=json.loads,  # Loads a string as a JSON dict
            default={},
            required=False,
        ),
        CustomCommandLineArgument(
            name="some_list",
            data_type=ast.literal_eval,  # Evaluates most Python ASTs including lists
            default=[],
            required=False,
        ),
        CustomCommandLineArgument(
            name="batch_size",
            data_type=positive_int,  # Custom type
            default=100,
            required=False,
        ),
        CustomCommandLineArgument(
            name="batch_size",
            data_type=positive_int,  # Custom type
            default=100,
            required=True,
        ),
        CustomCommandLineArgument(
            name="verbose",
            action="store_true",  # If flag is present, value becomes True
            required=False,
            help_message="Enable verbose output",
        ),
    ]

    # Create command line arguments parser with custom arguments
    cla = CommandLineArguments(custom_inputs=custom_args, description="My data processing script")

    # Get the parsed arguments namespace
    args = cla.get_arguments()

    # Access custom arguments, note that type hinting needs to be provided manually
    some_number: int = args.some_number
    some_json: dict = args.some_json
    some_list: list = args.some_list
    batch_size: int = args.batch_size
    verbose: bool = args.verbose

    if verbose:
        print(f"Values: some_number = {some_number}, some_json = {some_json}, some_list = {some_list}")
        print(f"Using batch size: {batch_size}")

When you run this script, you can provide the custom arguments:

.. code-block:: bash

    python my_script.py --some_number 3 --some_json '{"key": "val"}' --some_list '["one", 2, "THREE"]' --batch_size 200 --verbose

Preet Arguments
---------------

This section will go over each preset argument when set as not ``UNUSED``

Argument: Secret Locations
~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``secret_locations`` parameter adds the ``--secret_locations`` arg which accepts JSON. Secrets processed this way are automatically used by SecretManager and SecretLocations as seen here :doc:`secrets_handling`.

.. code-block:: python

    from dataeng_container_tools import (
        CommandLineArguments, 
        CommandLineArgumentType,
        SecretLocations
    )

    # Create command line arguments parser with secret locations
    cla = CommandLineArguments(
        secret_locations=CommandLineArgumentType.REQUIRED
    )

    # When SecretLocations is returned as a dictionary with specified secret paths
    secret_locations = SecretLocations()

    # Access secret paths
    gcs_secret = secret_locations.GCS
    db_secret = secret_locations.DB
    custom_secret = secret_locations.CUSTOM

    print(f"GCS Secret Path: {gcs_secret}")
    print(f"DB Secret Path: {db_secret}")

When using this script, you would provide the secret locations as a JSON dictionary:

.. code-block:: bash

    python my_script.py --secret_locations '{"GCS": "/path/to/gcs_secret.json", "DB": "/path/to/db_secret.json"}'

Argument: Input Output
~~~~~~~~~~~~~~~~~~~~~~
.. warning::
   This documentation is currently under construction (TBD).

