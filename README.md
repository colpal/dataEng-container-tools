# Data Engineering Container Tools

This packages is split into three parts: **CLA**, **GCS**, and **safe_stdout**.

## CLA:
Deals with receiving input from the command line. Has three classes: **custom_command_line_argument**, **command_line_argument_type**, and **command_line_arguments**.
* custom_command_line_arguments: The purpose of this class is to act as a container for custom command line arguments. All of the attributes available when creating command line arguments through `parser.add_argument()` method from the `argparse` library are available in this class. Has the following methods:
  * Initialization: Creates the class with the following inputs:
    * name: Required.
    * action: Optional. Defaults to `None`.
    * nargs: Optional. Defaults to `None`.
    * const: Optional. Defaults to `None`.
    * default: Optional. Defaults to `None`.
    * data_type: Optional. Defaults to `None`.
    * choices: Optional. Defaults to `None`.
    * required: Optional. Defaults to `None`.
    * help_message: Optional. Defaults to `None`.
    * metavar: Optional. Defaults to `None`.
    * dest: Optional. Defaults to `None`.
    
* command_line_argument_type: Enumeration type. Used for populating initialization fields in **command_line_arguments**. Has the following types:
  * OPTIONAL: Indicates the given command line argument should be created as optional.
  * REQUIRED: Indicates the given command line argument should be created as required.
  
* command_line_arguments: Creates command line arguments. Includes helper functions for using the command line inputs. Has the following methods:
  * Initialization: Created the class with the following inputs:
    * input_files: Optional. Defaults to `None`.
    * output_files: Optional. Defaults to `None`.
    * secret_locations: Optional. Defaults to `None`.
    * default_file_type: Optional. Defaults to `None`.
    * custom_inputs: Optional. Defaults to `None`.
    * description : Optional. Defaults to `None`.
    * input_dtypes: Optional. Defaults to `None`.
    * parser: Optional. Defaults to `None`.
  * get_arguments: Returns the arguments passed in through the command line.
  * get_input_dtypes: 
  * get_input_uris:
  * get_output_uris: 
  * get_secret_locations:
  * check_args: 
  
## GCS:
Deals with uploading and downloading files to/from GCS. Has one class `gcs_file_io` with the following methods:
* Initialization: Creates the class with the following inputs:
  * gcs_secret_location: Required.
* download_file_to_object: Downloads a file from GCS to an object in memory:
  * gcs_uri: Required.
  * default_file_type: Optional. Defaults to `None`.
  * dtype: Optional. Defaults to `None`.
* download_files_to_objects: Downloads files from GCS to objects in memory:
  * gcs_uris: Required.
  * default_file_types: Optional. Defaults to `None`.
  * dtypes: Optional. Defaults to `None`.
* download_file_to_disk: Downloads a file from GCS to the container's hard drive:
  * gcs_uri: Required.
  * local_location: Optional. Defaults to `None`.
* download_files_to_disk: Downloads files from GCS to the container's hard drive:
  * gcs_uris: Required.
  * local_locations: Optional. Defaults to `None`.
* upload_file_to_object: Uploads a file from GCS to an object in memory:
  * gcs_uri: Required.
  * default_file_type: Optional. Defaults to `None`.
  * dtype: Optional. Defaults to `None`.
* upload_files_to_objects: Uploads files from GCS to objects in memory:
  * gcs_uris: Required.
  * default_file_types: Optional. Defaults to `None`.
  * dtypes: Optional. Defaults to `None`.
* upload_file_to_disk: Uploads a file from GCS to the container's hard drive:
  * gcs_uri: Required.
  * local_location: Optional. Defaults to `None`.
* upload_files_to_disk: Uploads files from GCS to the container's hard drive:
  * gcs_uris: Required.
  * local_locations: Optional. Defaults to `None`.
