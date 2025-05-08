"""Tools for retrieving command line inputs.

Deals with receiving input from the command line. Has three
classes: `CustomCommandLineArgument`, `CommandLineArgumentType`,
and `CommandLineArguments`. `CommandLineArguments` contains most
of the functionality. `CommandLineArgumentType` is an enumeration.
`CustomCommandLineArgument` is a wrapper for `parser.add_argument()`.

Typical usage example:

    my_inputs = CommandLineArguments(secret_locations=CommandLineArgumentType.OPTIONAL,
                                    input_files=CommandLineArgumentType.REQUIRED,
                                    output_files=CommandLineArgumentType.REQUIRED)

    input_uris = my_inputs.get_input_uris()
    output_uris = my_inputs.get_output_uris()
    secret_locations = my_inputs.get_secret_locations()
    file_io = gcs_file_io(gcs_secret_location = secret_locations.GCS)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from enum import Enum
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from . import __version__
from .secrets_manager import SecretManager

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger("Container Tools")


class CustomCommandLineArgument:
    """Class for creating custom command line arguments.

    Wrapper of `argparse.add_argument`. <br />
    Source: https://github.com/python/typeshed/blob/30b16c168d428f2690473e8d317c5eb240e7000e/stdlib/argparse.pyi

    This class is used for creating custom command line arguments. A
    list of these objects can be passed into `CommandLineArguments` which
    will add them as command line arguments, parse the inputs, and return the results.

    """

    def __init__(
        self,
        name: str,
        *,
        # str covers predefined actions ("store_true", "count", etc.)
        action: str | type[argparse.Action] = ...,
        # more precisely, Literal["?", "*", "+", "...", "A...", "==SUPPRESS=="]
        nargs: int | str | None = None,
        const: Any = ...,  # noqa: ANN401
        default: Any = ...,  # noqa: ANN401
        data_type: argparse._ActionType = ...,
        choices: Iterable[argparse._T] | None = ...,
        required: bool = ...,
        help_message: str | None = ...,
        metavar: str | tuple[str, ...] | None = ...,
        dest: str | None = ...,
        version: str = ...,
        **kwargs: Any,  # noqa: ANN401
    ) -> None:
        """Initialize CustomCommandLineArgument with desired configuration.

        See: https://docs.python.org/3.9/library/argparse.html

        Args:
            name (str): Argument name.
            action (str | type[argparse.Action]): Indicates the basic type of action to be taken when this
                argument is encountered at the command line.
            nargs (int | str | None): Indicates the number of command-line arguments that should be consumed.
            const (Any): A constant value required by some action and nargs selections.
            default (Any): The value produced if the argument is absent from the command line and if it is
                absent from the namespace object.
            data_type (argparse._ActionType): The type to which the command-line argument should be converted.
            choices (Iterable[argparse._T] | None): A container of the allowable values for the argument.
            required (bool): Indicates whether or not the command-line option may be omitted (optionals only).
            help_message (str | None): A brief description of what the argument does.
            metavar (str | tuple[str, ...] | None): The name for the argument in usage messages.
            dest (str | None): The name of the attribute to be added to the object returned by parse_args().
            version (str): Version of the argument.
            kwargs (Any): Additional keyword arguments.

        """
        self.name = name
        self.action = action
        self.nargs = nargs
        self.const = const
        self.default = default
        self.data_type = data_type
        self.choices = choices or []
        self.required = required
        self.help_message = help_message
        self.metavar = metavar
        self.dest = dest
        self.version = version
        self.kwargs = kwargs

    def __str__(self) -> str:
        """Convert argument to a string."""
        attributes = [
            f"name: {self.name}",
            f"action: {self.action}",
            f"nargs: {self.nargs}",
            f"const: {self.const}",
            f"default: {self.default}",
            f"data_type: {self.data_type}",
            f"choices: {self.choices}",
            f"required: {self.required}",
            f"help_message: {self.help_message}",
            f"metavar: {self.metavar}",
            f"dest: {self.dest}",
            f"version: {self.version}",
            f"kwargs: {self.kwargs}",
        ]
        return ", ".join(attributes)


class CommandLineArgumentType(Enum):
    """Enumeration class for use with CommandLineArguments.

    Attributes:
        UNUSED: For when a command line argument should not be used.
        OPTIONAL: For when a command line argument should be optional.
        REQUIRED: For when a command line argument should be required.

    """

    UNUSED = None
    OPTIONAL = False
    REQUIRED = True


class CommandLineSecret:
    """Takes in a dictionary of secret names and locations and adds the keys and values to the class attributes.

    This allows secrets to be called by name but not in a dictionary fashion.

    Attributes:
        GCS (str): Default location for GCP storage secret.
        SF (str): Default location for Snowflake secret.
        DB (str): Default location for Datastore secret.
        Others: Additional keys from registered modules or input of the init function.
    """

    # Add type hints for common attributes
    GCS: str
    SF: str
    DB: str

    def __init__(self, kwargs: dict) -> None:
        """Initialize secret locations dict.

        Args:
            kwargs: Dictionary of secret names and their locations.
        """
        # First get default paths from SecretManager
        default_paths = SecretManager.get_all_secret_paths()

        # Create attributes from default paths
        for key, value in default_paths.items():
            setattr(self, key, value)

        # Override with any provided paths from kwargs
        self.__dict__.update(**kwargs)


class CommandLineArguments:
    """Creates, parses, and retrieves command line inputs.

    This class creates command line arguments that are typically
    used in Airflow containers. It will handle much of the backend
    boilerplate code involved with creating, parsing, and storing
    the relevant command line arguments using Python's argparse.
    Includes helper functions for using the command line inputs.
    """

    # Singleton instance
    instance: Self | None = None

    def __new__(cls, *_args: ..., **_kwargs: ...) -> Self:
        """Create a new instance of CommandLineArguments or return the existing one.

        Implements the singleton pattern to ensure only one instance exists.

        Returns:
            CommandLineArguments: The singleton instance
        """
        if cls.instance is None:
            cls.instance = super().__new__(cls)
        return cls.instance

    def __init__(
        self,
        input_files: CommandLineArgumentType = CommandLineArgumentType.UNUSED,
        output_files: CommandLineArgumentType = CommandLineArgumentType.UNUSED,
        secret_locations: CommandLineArgumentType = CommandLineArgumentType.UNUSED,
        default_file_type: CommandLineArgumentType = CommandLineArgumentType.UNUSED,
        custom_inputs: list[CustomCommandLineArgument] | None = None,
        description: str | None = None,
        input_dtypes: CommandLineArgumentType = CommandLineArgumentType.UNUSED,
        running_local: CommandLineArgumentType = CommandLineArgumentType.UNUSED,
        identifying_tags: CommandLineArgumentType = CommandLineArgumentType.UNUSED,
        parser: argparse.ArgumentParser | None = None,
        *,
        parse_known_args: bool = True,
    ) -> None:
        """Initialize CommandLineArguments with desired configuration.

        Args:
            input_files (CommandLineArgumentType): Determines if input files are required, optional, or unused.
            output_files (CommandLineArgumentType): Determines if output files are required, optional, or unused.
            secret_locations (CommandLineArgumentType): Determines if secret locations are required, optional,
                or unused.
            default_file_type (CommandLineArgumentType): Specifies the default file type (e.g., parquet, csv).
            custom_inputs (list[CustomCommandLineArgument] | None): List of custom command line arguments.
            description (str | None): Description for the command line parser.
            input_dtypes (CommandLineArgumentType): Determines if input data types are required, optional, or unused.
            running_local (CommandLineArgumentType): Flag to indicate if the script is running locally.
            identifying_tags (CommandLineArgumentType): Determines if identifying tags are required, optional,
                or unused.
            parser (argparse.ArgumentParser | None): Custom parser for command line arguments.
            parse_known_args (bool): Whether to parse known arguments only.

        """
        if custom_inputs is None:
            custom_inputs = []

        self.__input_files = input_files
        self.__output_files = output_files
        self.__secret_locations = secret_locations
        self.__default_file_type = default_file_type
        self.__custom_inputs = custom_inputs
        self.__description = description
        self.__input_dtypes = input_dtypes
        self.__running_local = running_local
        self.__identifying_tags = identifying_tags
        parser = parser if parser else argparse.ArgumentParser(description=description)

        self.__add_container_args(parser)

        if custom_inputs:
            for item in custom_inputs:
                if item.action:
                    parser.add_argument(
                        "--" + item.name,
                        required=item.required,
                        action=item.action,
                        help=item.help_message,
                    )
                else:
                    parser.add_argument(
                        "--" + item.name,
                        action=item.action,
                        nargs=item.nargs,
                        const=item.const,
                        default=item.default,
                        type=item.data_type,
                        choices=item.choices,
                        required=item.required,
                        help=item.help_message,
                        metavar=item.metavar,
                        dest=item.dest,
                        version=item.version,
                        kwargs=item.kwargs,
                    )
        try:
            if parse_known_args:
                self.__args, _ = parser.parse_known_args()  # Discard extra args
            else:
                self.__args = parser.parse_args()
        except Exception:
            logger.exception(
                "ARGUMENT ERROR: Reference the dataEng_container_tools README at "
                "https://github.com/colpal/dataEng-container-tools/blob/v%(version)s/README.md "
                "for examples of new updates from v%(version)s.",
                extra={"version": __version__},
            )
            raise
        logger.info("CLA Input: %s", self)

        if identifying_tags.value is not None:
            os.environ["DAG_ID"] = self.__args.dag_id
            os.environ["RUN_ID"] = self.__args.run_id
            os.environ["NAMESPACE"] = self.__args.namespace
            os.environ["POD_NAME"] = self.__args.pod_name

    def __add_container_args(self, parser: argparse.ArgumentParser) -> None:
        if self.__input_files.value is not None:
            parser.add_argument(
                "--input_bucket_names",
                type=str,
                required=self.__input_files.value,
                nargs="+",
                help="GCS Buckets to read from.",
            )
            parser.add_argument(
                "--input_paths",
                type=str,
                required=self.__input_files.value,
                nargs="+",
                help="GCS folders in bucket to read file from.",
            )
            parser.add_argument(
                "--input_filenames",
                type=str,
                required=self.__input_files.value,
                nargs="+",
                help="Filenames to read file from.",
            )
            parser.add_argument(
                "--input_delimiters",
                type=str,
                required=False,
                nargs="+",
                help="Delimiters for input files",
            )

            if self.__input_dtypes.value is not None:
                parser.add_argument(
                    "--input_dtypes",
                    type=json.loads,
                    required=self.__input_dtypes.value,
                    nargs="+",
                    help="JSON dictionaries of (column: type) pairs to cast columns to",
                )

        if self.__output_files.value is not None:
            parser.add_argument(
                "--output_bucket_names",
                type=str,
                required=self.__output_files.value,
                nargs="+",
                help="GCS Bucket to write to.",
            )
            parser.add_argument(
                "--output_paths",
                type=str,
                required=self.__output_files.value,
                nargs="+",
                help="GCS folder in bucket to write file to.",
            )
            parser.add_argument(
                "--output_filenames",
                type=str,
                required=self.__output_files.value,
                nargs="+",
                help="Filename to write file to.",
            )
            parser.add_argument(
                "--output_delimiters",
                type=str,
                required=False,
                nargs="+",
                help="Delimiters for output files",
            )

        if self.__secret_locations.value is not None:
            parser.add_argument(
                "--secret_locations",
                type=json.loads,
                required=self.__secret_locations.value,
                default=SecretManager.get_all_secret_paths(),
                help="Dictionary of the locations of secrets injected by Vault. Default: '"
                + str(SecretManager.get_all_secret_paths())
                + "'.",
            )

        if self.__default_file_type.value is not None:
            parser.add_argument(
                "--default_file_type",
                type=str,
                required=self.__default_file_type.value,
                choices=["parquet", "csv", "pkl", "json"],
                default="parquet",
                help=(
                    "How to handle input/output files if no file extension found."
                    "Choice of 'parquet', 'csv', 'pkl', and 'json'. Default 'parquet'."
                ),
            )

        if self.__running_local.value is not None:
            parser.add_argument(
                "--running_local",
                type=bool,
                required=self.__running_local.value,
                default=False,
                help="If the container is running locally (no contact with GCP).",
            )

        if self.__identifying_tags.value is not None:
            parser.add_argument("--dag_id", type=str, required=self.__identifying_tags.value, help="The DAG ID")
            parser.add_argument("--run_id", type=str, required=self.__identifying_tags.value, help="The run ID")
            parser.add_argument("--namespace", type=str, required=self.__identifying_tags.value, help="The namespace")
            parser.add_argument("--pod_name", type=str, required=self.__identifying_tags.value, help="The pod name")

    def __str__(self) -> str:
        """Print the string value of the argparse args."""
        return self.__args.__str__()

    def get_arguments(self) -> argparse.Namespace:
        """Retrieve the arguments passed in through the command line.

        Returns:
            argparse.Namespace: A Namespace object with all of the command line arguments.

        """
        return self.__args

    def get_input_dtypes(self) -> ...:
        """Retrieve the input dtypes passed in through the command line.

        Returns:
            None if dtypes were not asked for in initialization or the loaded JSON
            object passed to input_dtypes through the command line otherwise.

        """
        return self.__args.input_dtypes

    def get_input_uris(self) -> list[str]:
        """Retrieve the input URIs passed in through the command line.

        Returns:
            list[str]: A list of all input URIs passed in through the command line. URIs
            are of the format 'gs://bucket_name/input_path/filename'.

        """
        if not self.__input_files:
            return []
        constant_bucket = False
        bucket_name = ""
        output: list[str] = []
        if len(self.__args.input_bucket_names) == 1:
            constant_bucket = True
            bucket_name = self.__args.input_bucket_names[0]
        for pos, filename in enumerate(self.__args.input_filenames):
            if not constant_bucket:
                bucket_name = self.__args.input_bucket_names[pos]
            prefix = r"gs://"
            uri_body = (
                f"{bucket_name}/{self.__args.input_paths[pos]}/{filename}".replace("/ /", "/")
                .replace("/./", "/")
                .replace("//", "/")
            )
            output.append(prefix + uri_body)
        return output

    def get_output_uris(self) -> list[str]:
        """Retrieve the output URIs passed in through the command line.

        Returns:
            list[str]: A list of all output URIs passed in through the command line. URIs
            are of the format 'gs://bucket_name/output_path/filename'.

        """
        if not self.__output_files:
            return []
        constant_bucket = False
        bucket_name = ""
        output: list[str] = []
        if len(self.__args.output_bucket_names) == 1:
            constant_bucket = True
            bucket_name = self.__args.output_bucket_names[0]
        for pos, filename in enumerate(self.__args.output_filenames):
            if not constant_bucket:
                bucket_name = self.__args.output_bucket_names[pos]
            prefix = r"gs://"
            uri_body = (
                f"{bucket_name}/{self.__args.output_paths[pos]}/{filename}".replace("/ /", "/")
                .replace("/./", "/")
                .replace("//", "/")
            )
            output.append(prefix + uri_body)
        return output

    def get_secret_locations(self) -> CommandLineSecret | list | None:
        """Retrieve the secret file locations passed in through the command line.

        Returns:
            CommandLineSecret | list | None: A list of all the secret file locations passed in through the command line.
                If secret_locations was not asked for during initialization, returns a list of secrets
                found automatically.

        """
        if self.__secret_locations:
            return CommandLineSecret(self.__args.secret_locations)
        if len(SecretManager.files) > 0:
            return SecretManager.files
        return None
