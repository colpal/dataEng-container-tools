import argparse
import json
import sys
from enum import Enum
from .safe_stdout import setup_stdout, default_secret_folder
import os

default_gcs_secret_locations = [default_secret_folder + '/gcp-sa-storage.json']

class custom_command_line_argument:
    """Class for creating custom command line arguments.
       Takes in all arguments needed for 'command_line_arguments'
       to construct an additional custom argument."""
    name = None
    action = None
    nargs = None
    const = None
    default = None
    data_type = None
    choices = None
    required = None
    help_message = None
    metavar = None
    dest = None
    def __init__(self, name, action = None, nargs = None, const = None, default = None,
                data_type = None, choices = None, required = None, help_message = None,
                metavar = None, dest = None):
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
        return ("name: " + self.name + ", " +
        "action: " + self.action + ", " +
        "nargs: " + self.nargs + ", " +
        "const: " + self.const + ", " +
        "default: " + self.default + ", " +
        "data_type: " + self.data_type + ", " +
        "choices: " + self.choices + ", " +
        "required: " + self.required + ", " +
        "help_message: " + self.help_message + ", " +
        "metavar: " + self.metavar + ", " +
        "dest: " + self.dest)

class command_line_argument_type(Enum):
    """Enum type for dictating whether fields in 'command_line_arguments' class are OPTIONAL or REQUIRED."""
    OPTIONAL = False
    REQUIRED = True

class command_line_arguments:
    """Simplified process for creating command line arguments. Allows for custom CLAs."""
    __args = None
    __input_files = None
    __output_files = None
    __secret_locations = None
    __default_file_type = None
    __custom_inputs = None
    __description = None
    __input_dtypes = None
    __running_local = None
    __default_secret_locations = default_gcs_secret_locations
    def __init__(self, input_files = None, output_files = None, secret_locations = None,
                default_file_type = None, custom_inputs = None, description = None,
                input_dtypes = None, running_local = None, identifying_tags = None, parser = None):
        self.__input_files = input_files
        self.__output_files = output_files
        self.__secret_locations = secret_locations
        self.__default_file_type = default_file_type
        self.__custom_inputs = custom_inputs
        self.__description = description
        self.__input_dtypes = input_dtypes
        self.__running_local = running_local
        parser = parser if parser else argparse.ArgumentParser(description=description)
        if input_files:
            parser.add_argument("--input_bucket_names", type=str, required=input_files.value,
                                nargs = '+', help="GCS Buckets to read from.")

            parser.add_argument("--input_paths", type=str, required=input_files.value,
                                nargs = '+', help="GCS folders in bucket to read file from.")

            parser.add_argument("--input_filenames", type=str, required=input_files.value,
                                nargs = '+', help="Filenames to read file from.")
            if input_dtypes:
                parser.add_argument("--input_dtypes", type=json.loads, required=input_dtypes.value,
                                nargs ='+', help = "JSON dictionaries of (column: type) pairs to cast columns to")

        if output_files:
            parser.add_argument("--output_bucket_names", type=str, required=output_files.value,
                                nargs = '+', help="GCS Bucket to write to.")

            parser.add_argument("--output_paths", type=str, required=output_files.value,
                                nargs = '+', help="GCS folder in bucket to write file to.")

            parser.add_argument("--output_filenames", type=str, required=output_files.value,
                                nargs = '+', help="Filename to write file to.")
        if secret_locations:
            parser.add_argument("--gcs_secret_locations", type = str, required=secret_locations.value,
                                default = self.__default_secret_locations, nargs = '+', 
                                help = "Locations of GCS secrets injected by Vault. Default: '" + str(self.__default_secret_locations) + "'.")
        if default_file_type:
            parser.add_argument("--default_file_type", type = str,required=default_file_type.value,
                                choices = ["parquet", "csv", "pkl", "json"], default = "parquet",
                                help = "How to handle input/output files if no file extension found. Choice of 'parquet', 'csv', 'pkl', and 'json'. Default 'parquet'.")
        if running_local:
            parser.add_argument("--running_local", type = bool, required=running_local.value,
                                default = False, help = "If the container is running locally (no contact with GCP).")
        if identifying_tags:
            parser.add_argument("--dag_id", type = str, required = identifying_tags.value, help = "The DAG ID")
            parser.add_argument("--run_id", type = str, requiired = identifying_tags.value, help = "The run ID")
            parser.add_argument("--namespace", type = str, requiired = identifying_tags.value, help = "The namespace")
            parser.add_argument("--pod_name", type = str, requiired = identifying_tags.value, help = "The pod name")
        if custom_inputs:
            for item in custom_inputs:
                parser.add_argument(name = "--"+item.name, action = item.action, nargs = item.nargs,
                                        const = item.const, default = item.default, type = item.data_type,
                                        choices = item.choices, required = item.required,
                                        help = item.help_message, metavar = item.metavar,
                                        dest = item.dest)
        self.__args = parser.parse_args()
        print("CLA Input:", self)
        if identifying_tags:
            os.environ["DAG_ID"] = self.__args.dag_id
            os.environ["RUN_ID"] = self.__args.run_id
            os.environ["NAMESPACE"] = self.__args.namespace
            os.environ["POD_NAME"] = self.__args.pod_name
        self.check_args()
        if self.__secret_locations:
            setup_stdout(self.__args.gcs_secret_locations)

    def __str__(self):
        return self.__args.__str__()

    def get_arguments(self):
        return self.__args

    def get_input_dtypes(self):
        if not self.__input_dtypes:
            return None
        return self.__args.input_dtypes

    def get_input_uris(self):
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
            output.append("gs://"+bucket_name+"/"+self.__args.input_paths[pos]+"/"+filename)
        return output

    def get_output_uris(self):
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
            output.append("gs://"+bucket_name+"/"+self.__args.output_paths[pos]+"/"+filename)
        return output

    def get_secret_locations(self):
        if not self.__secret_locations:
            return None
        return self.__args.gcs_secret_locations

    def check_args(self):
        #TODO: Implement this
        return
