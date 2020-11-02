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

## safe_stdout:
Ensures that secrets are not accidentally printed using stdout. Has one class `safe_stdout`, two helper methods, `setup_stdout` and `setup_default_stdout`, and one global variable `default_secret_folder`:
* safe_stdout: The output class in charge of outputting to the command line. Replaces stdout. Has the following methods:
  * init: Creates the class with the following inputs:
    * bad_words: Required. A list of words to censor from output.
  * write: Writes a message to the command line. Usually called through Python's built in `print()` function. Has the following inputs:
    * message: Required. The message to write.
  * add_words: Adds a list of words to the list of words being censored. Has the following inputs:
    * bad_words: Required. A list of wors to censor from output.
* setup_stdout: Censors all the values in a list of secret files from stdout. Takes the following input:
  * secret_locations: Required. A list of secret file locations.
* setup_default_stdout: Censors all values from secret files contained in folder. Takes the following input:
  * folder: Optional. Defaults to `default_secret_folder`. The path of the folder containing the secret files.
* default_secret_folder: Variable containing the folder in which secrets are injected by default. Currently set to `'/vault/secrets/'`.


