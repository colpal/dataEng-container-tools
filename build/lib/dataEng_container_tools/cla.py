"""Tools for retrieving command line inputs.

Deals with receiving input from the command line. Has three
classes: `custom_command_line_argument`, `command_line_argument_type`,
and `command_line_arguments`. `commnad_line_arguments` contains most
of the functionality. `command_line_argument_type` is an enumeration.
`custom_command_line_argument` is a wrapper for `parser.add_argument()`.

Typical usage example:

    my_inputs = command_line_arguments(secret_locations=command_line_argument_type.OPTIONAL,
                                    input_files=command_line_argument_type.REQUIRED,
                                    output_files=command_line_argument_type.REQUIRED)

    input_uris = my_inputs.get_input_uris()
    output_uris = my_inputs.get_output_uris()
    secret_locations = my_inputs.get_secret_locations()                              
    file_io = gcs_file_io(gcs_secret_location = secret_locations[0])
"""

import argparse
import json
import sys
from enum import Enum
from .safe_stdout import setup_stdout, default_gcs_secret_locations, secrets_files
import os


class custom_command_line_argument:
    """Class for creating custom command line arguments.  

    This class is used for creating custom command line arguments. A
    list of these objects can be passed into `command_line_arguments` which
    will add them as command line arguments, parse the inputs, and return the results.
    Objects of this class have all the same attributes available to `parser.add_argument()`.  

    Attributes:
        name: A string to Argument name.  
        action: A string indicating the basic type of action to be taken when this argument is encountered at the command line.
        nargs: An integer indicating the number of command-line arguments that should be consumed.
        const: A constant value required by some action and nargs selections.
        default: The value produced if the argument is absent from the command line and if it is absent from the namespace object.
        data_type: The type to which the command-line argument should be converted.
        choices: A container of the allowable values for the argument.
        required: A boolean indicating whether or not the command-line option may be omitted (optionals only).
        help_message: A string providing a brief description of what the argument does.
        metavar: A string indicating the name for the argument in usage messages.
        dest: A string indicating the name of the attribute to be added to the object returned by parse_args().
    """

    def __init__(self,
                 name,
                 action=None,
                 nargs=None,
                 const=None,
                 default=None,
                 data_type=None,
                 choices=None,
                 required=None,
                 help_message=None,
                 metavar=None,
                 dest=None):
        """Initializes custom_command_line_arguments with desired configuration.

        Args:
            name: A string to Argument name.  
            action: A string indicating the basic type of action to be taken when this argument is encountered at the command line.  
            nargs: An integer indicating the number of command-line arguments that should be consumed.  
            const: A constant value required by some action and nargs selections.  
            default: The value produced if the argument is absent from the command line and if it is absent from the namespace object.  
            data_type: The type to which the command-line argument should be converted.  
            choices: A container of the allowable values for the argument.  
            required: A boolean indicating whether or not the command-line option may be omitted (optionals only).  
            help_message: A string providing a brief description of what the argument does.  
            metavar: A string indicating the name for the argument in usage messages.  
            dest: A string indicating the name of the attribute to be added to the object returned by parse_args().
        """
        self.name = name
        self.action = action
        self.nargs = nargs
        self.const = const
        self.default = default
        self.data_type = data_type
        self.choices = choices
        self.required = required
        self.help_message = help_message
        self.metavar = metavar
        self.dest = dest

    def __str__(self):
        return ("name: " + self.name + ", " + "action: " + self.action + ", " +
                "nargs: " + self.nargs + ", " + "const: " + self.const + ", " +
                "default: " + self.default + ", " + "data_type: " +
                self.data_type + ", " + "choices: " + self.choices + ", " +
                "required: " + self.required + ", " + "help_message: " +
                self.help_message + ", " + "metavar: " + self.metavar + ", " +
                "dest: " + self.dest)


class command_line_argument_type(Enum):
    """Enumeration class for use with command_line_arguments.
    
    Attributes:
        OPTIONAL: For when a command line argument should be optional.
        REQUIRED: For when a command line argument should be required.
    """
    OPTIONAL = False
    REQUIRED = True


class command_line_arguments:
    """Creates, parses, and retrieves command line inputs.

    This class creates command line arguments that are typically
    used in Airflow containers. It will handle much of the backend
    boilerplate code involved with creating, parsing, and storing
    the relevant command line arguments using Python's argparse.
    Includes helper functions for using the command line inputs.
    """

    __default_secret_locations = default_gcs_secret_locations

    def __init__(self,
                 input_files=None,
                 output_files=None,
                 secret_locations=None,
                 default_file_type=None,
                 custom_inputs=None,
                 description=None,
                 input_dtypes=None,
                 running_local=None,
                 identifying_tags=None,
                 input_pandas_kwargs=None,
                 output_pandas_kwargs=None,
                 parser=None):
        """Initializes command_line_arguments with desired configuration.

        Args:
            input_files: Optional command_line_argument_type. Defaults to None. If REQUIRED,
                will add --input_bucket_names, --input_paths, and --input_filenames as required
                command line inputs. If OPTIONAL, will add them as optional. If None they will
                not be added.
            output_files: Optional command_line_argument_type. Defaults to None. If REQUIRED,
                will add --output_bucket_names, --output_paths, and --output_filenames as required
                command line inputs. If command_line_argument_type.OPTIONAL, will add them as
                optional. If None they will not be added.
            secret_locations: Optional command_line_argument_type. Defaults to None. If REQUIRED,
                will add --gcs_secret_locations as required command line input. If OPTIONAL,
                will add it as optional. If None, it will not be added.
            default_file_type: Optional command_line_argument_type. Defaults to None. If REQUIRED,
                will add --default_file_type as required command line argument. If OPTIONAL,
                will add it as optional. Input can be one of parquet, csv, pkl, or json, with
                the default being parquet. If None, the command line argument will not be added.
            custom_inputs: Optional list of custom_command_line_arguments. Defaults to None.
                All items in list will be added to the command line arguments.
            description : Optional string. Defaults to None. A description to be printed when the
                command line argument --help is used.
            input_dtypes: Optional command_line_argument_type. Defaults to None. If input_files
                is None, then this does nothing. If input_files is not None and input_dtypes
                is REQUIRED, will add --input_dtypes as a required command line input. If OPTIONAL,
                will add it as optional. Input is a JSON dictionary of (column: type) pairs.
            parser: Optional argparse.ArgumentParser. Defaults to None. A parser on which to
                add the command line arguments and parse. If None one will be created.
            running_local: Optional boolean. Defaults to False. A flag for determining whether
                or not the script is running locally and so should not attempt to contact GCP.
                Defaults to False.
        """
        self.__input_files = input_files
        self.__output_files = output_files
        self.__secret_locations = secret_locations
        self.__default_file_type = default_file_type
        self.__custom_inputs = custom_inputs
        self.__description = description
        self.__input_dtypes = input_dtypes
        self.__running_local = running_local
        self.__input_pandas_kwargs = input_pandas_kwargs
        self.__output_pandas_kwargs = output_pandas_kwargs
        parser = parser if parser else argparse.ArgumentParser(
            description=description)
        if input_files:
            parser.add_argument("--input_bucket_names",
                                type=str,
                                required=input_files.value,
                                nargs='+',
                                help="GCS Buckets to read from.")

            parser.add_argument("--input_paths",
                                type=str,
                                required=input_files.value,
                                nargs='+',
                                help="GCS folders in bucket to read file from.")

            parser.add_argument("--input_filenames",
                                type=str,
                                required=input_files.value,
                                nargs='+',
                                help="Filenames to read file from.")
            parser.add_argument("--input_delimiters",
                                type=str,
                                required=False,
                                nargs='+',
                                help="Delimiters for input files") 
            if input_dtypes:
                parser.add_argument(
                    "--input_dtypes",
                    type=json.loads,
                    required=input_dtypes.value,
                    nargs='+',
                    help=
                    "JSON dictionaries of (column: type) pairs to cast columns to"
                )
            if input_pandas_kwargs:
                parser.add_argument("--input_pandas_kwargs",
                    type=json.loads,
                    required=input_pandas_kwargs.value,
                    help="JSON dictionary of additional arguments for reading a file to a pandas dataframe")
           
        if output_files:
            parser.add_argument("--output_bucket_names",
                                type=str,
                                required=output_files.value,
                                nargs='+',
                                help="GCS Bucket to write to.")

            parser.add_argument("--output_paths",
                                type=str,
                                required=output_files.value,
                                nargs='+',
                                help="GCS folder in bucket to write file to.")

            parser.add_argument("--output_filenames",
                                type=str,
                                required=output_files.value,
                                nargs='+',
                                help="Filename to write file to.")
            parser.add_argument("--output_delimiters",
                                type=str,
                                required=False,
                                nargs='+',
                                help="Delimiters for output files")
            if output_pandas_kwargs:
                parser.add_argument("--output_pandas_kwargs",
                    type=json.loads,
                    required=output_pandas_kwargs.value,
                    help="JSON dictionary of additional arguments for reading a file to a pandas dataframe")
        if secret_locations:
            parser.add_argument(
                "--secret_locations",
                type=str,
                required=secret_locations.value,
                default=self.__default_secret_locations,
                nargs='+',
                help="Locations of secrets injected by Vault. Default: '" +
                str(self.__default_secret_locations) + "'.")
        if default_file_type:
            parser.add_argument(
                "--default_file_type",
                type=str,
                required=default_file_type.value,
                choices=["parquet", "csv", "pkl", "json"],
                default="parquet",
                help=
                "How to handle input/output files if no file extension found. Choice of 'parquet', 'csv', 'pkl', and 'json'. Default 'parquet'."
            )
        if running_local:
            parser.add_argument(
                "--running_local",
                type=bool,
                required=running_local.value,
                default=False,
                help="If the container is running locally (no contact with GCP)."
            )
        if identifying_tags:
            parser.add_argument("--dag_id",
                                type=str,
                                required=identifying_tags.value,
                                help="The DAG ID")
            parser.add_argument("--run_id",
                                type=str,
                                required=identifying_tags.value,
                                help="The run ID")
            parser.add_argument("--namespace",
                                type=str,
                                required=identifying_tags.value,
                                help="The namespace")
            parser.add_argument("--pod_name",
                                type=str,
                                required=identifying_tags.value,
                                help="The pod name")
        if custom_inputs:
            for item in custom_inputs:
                parser.add_argument("--" + item.name,
                                    action=item.action,
                                    nargs=item.nargs,
                                    const=item.const,
                                    default=item.default,
                                    type=item.data_type,
                                    choices=item.choices,
                                    required=item.required,
                                    help=item.help_message,
                                    metavar=item.metavar,
                                    dest=item.dest)
        self.__args = parser.parse_args()
        print("CLA Input:", self)
        if identifying_tags:
            os.environ["DAG_ID"] = self.__args.dag_id
            os.environ["RUN_ID"] = self.__args.run_id
            os.environ["NAMESPACE"] = self.__args.namespace
            os.environ["POD_NAME"] = self.__args.pod_name
        self.check_args()
        if self.__secret_locations:
            setup_stdout(self.__args.secret_locations)

    def __str__(self):
        return self.__args.__str__()

    def get_arguments(self):
        """Retrieves the arguments passed in through the command line.
        
        Returns:
            A Namespace object with all of the command line arguments.
        """
        return self.__args

    def get_input_dtypes(self):
        """Retrieves the input dtypes passed in through the command line.
        
        Returns:
            None if dtypes were not asked for in intialisation or the loaded JSON
            object passed to input_dtypes through the command line otherwise.
        """
        if not self.__input_dtypes:
            return None
        return self.__args.input_dtypes

    def get_input_uris(self):
        """Retrieves the input URIs passed in through the command line.
        
        Returns:
            A list of all input URIs passed in through the command line. URIs
            are of the format 'gs://bucket_name/input_path/filename'.
        """
        if not self.__input_files:
            return []
        constant_bucket = False
        bucket_name = ''
        output = []
        if len(self.__args.input_bucket_names) == 1:
            constant_bucket = True
            bucket_name = self.__args.input_bucket_names[0]
        for pos, filename in enumerate(self.__args.input_filenames):
            if not constant_bucket:
                bucket_name = self.__args.input_bucket_names[pos]
            prefix = r"gs://"
            uri_body = f"{bucket_name}/{self.__args.input_paths[pos]}/{filename}".replace("/ /","/").replace("/./","/").replace("//","/")
            output.append(prefix + uri_body)
        return output

    def get_output_uris(self):
        """Retrieves the output URIs passed in through the command line.
        
        Returns:
            A list of all output URIs passed in through the command line. URIs
            are of the format 'gs://bucket_name/output_path/filename'.
        """
        if not self.__output_files:
            return []
        constant_bucket = False
        bucket_name = ''
        output = []
        if len(self.__args.output_bucket_names) == 1:
            constant_bucket = True
            bucket_name = self.__args.output_bucket_names[0]
        for pos, filename in enumerate(self.__args.output_filenames):
            if not constant_bucket:
                bucket_name = self.__args.output_bucket_names[pos]
            prefix = r"gs://"
            uri_body = f"{bucket_name}/{self.__args.output_paths[pos]}/{filename}".replace("/ /","/").replace("/./","/").replace("//","/")
            output.append(prefix + uri_body)
        return output

    def get_secret_locations(self):
        """Retrieves the secret file locations passed in through the command line.

        Returns:
            A list of all the secret file locations passed in through the command line. If
            secret_locations was not asked for during initialization, returns a list of secrets
            found automatically.
        """
        if self.__secret_locations:
            return self.__args.secret_locations
        if len(secrets_files) > 0:
            return secrets_files
        return None

    def get_secrets(self):
        """Retrieves the secrets loaded from files specified through the command line.

        Returns:
            A dictionary of all the secrets specified through the command line. If
            secret_locations was not asked for during initialization, returns a dictionary
            of secrets found automatically. The key is the name of the file with '.json' removed
            and the value is the loaded secret file.
        """
        return_list = {}
        secret_list = None
        if self.__secret_locations:
            secret_list = self.__args.secret_locations
        elif len(secrets_files) > 0:
            secret_list = secrets_files
        else:
            return None
        for item in secret_list:
            try:
                return_list[item.strip('.json').split('/')[-1]] = json.load(
                    open(item, 'r'))
            except ValueError:
                print(item, "is not a properly formatted json file.")
        return return_list
    
    def get_pandas_kwargs(self):
        kwargs = (self.__args.input_pandas_kwargs,self.__args.output_pandas_kwargs)
        return kwargs
    
    def check_args(self):
        """Ensures arguments are present and valid.
        """
        #TODO: Implement this
        return
