"""Simplest way to interact with CLA and GCS.

A simple way to get input from the command line, and download and upload documents to/from GCS.
Fewer options than the classes above but also fewer lines of code to write. The class will
automatically add CLA arguments based on the types of arguments passed in to the function. The goal of the
method is for an end user to be able to use command line input without ever having to know what the command
line input actually is. 

Typical usage example:

    simple = simple_setup(['input_left', 'input_right', 'output_inner', 'output_outer', 'secret_location', 'example_flag'])
    objects = simple.get_input_objects()
    #
    # Edit the objects in some way here.
    #
    return_objs = {'output_outer': objects['input_left'], 'output_inner': objects['input_right']}
    upload = simp.upload_objects(return_objs)
"""

from .gcs import gcs_file_io
from .safe_stdout import default_gcs_secret_locations, default_secret_folder
import argparse
import json
import os


class simple_setup:
    """Simplifies GCS and CLA usage.
    
    Takes in a list of arguments and automatically groups them by type. It then creates the appropriate
    Command Line Arguments for each parameter, and parses the command line input. A single method call
    can be used to download all files that were passed in as 'input file' command line arguments, and 
    another single method call can be used to upload a list of objects to all of the inputs corresponding
    to output GCS locations. Parameters are grouped into the following categories:
        input_args: any parameter containing the word 'input' is in this category. For each parameter in this
            category, bucket, path, filename, and URI CLA arguments are created. Either the URI or the bucket,
            path, and filename must be populated.
        output_args: any parameter containing the word 'output' is in this category. For each parameter in this
            category, bucket, path, filename, and URI CLA arguments are created. Either the URI or the bucket,
            path, and filename must be populated.
        secret_location_args: any parameter containing the words 'secret' and 'location' is in this category.
            For each parameter in this category, one CLA is created.
        other_args: any parameter not belonging to the previous three categories is included in this category.
            One CLA is created for each parameter.
        found_secrets: any secret files found in the default secrets location belong to this category. The keys
            are the names of the files with the file extension removed and the values are the filepaths.
    """

    def __init__(self, argument_names):
        """Initializes simple_setup with desired configuration.

        Args:
            argument_names: An iterable of strings containing the names of arguments that should be used
                to create CLAs.
        """
        input_args = {}
        output_args = {}
        other_args = {}
        found_secrets = {}
        secret_location_args = {}
        gcs_secret_location = default_gcs_secret_locations[0]
        parser = argparse.ArgumentParser()
        for name in argument_names:
            if "input" in name:
                print("Input:", name)
                help = f'Location of {name} file. Either URI or bucket, path, and filename must be provided.'
                parser.add_argument('--' + name + '_uri', required=False, help=help)
                parser.add_argument('--' + name + '_bucket', required=False, help=help)
                parser.add_argument('--' + name + '_path', required=False, help=help)
                parser.add_argument('--' + name + '_filename', required=False, help=help)
                input_args[name] = None
            elif "output" in name:
                print("Output:", name)
                help = f'Location of {name} file. Either URI or bucket, path, and filename must be provided.'
                parser.add_argument('--' + name + '_uri', required=False, help=help)
                parser.add_argument('--' + name + '_bucket', required=False, help=help)
                parser.add_argument('--' + name + '_path', required=False, help=help)
                parser.add_argument('--' + name + '_filename', required=False, help=help)
                output_args[name] = None
            elif 'secret' in name and 'location' in name:
                print("Secret:", name)
                parser.add_argument('--' + name, required=True)
                secret_location_args[name] = None
            else:
                print("Other:", name)
                parser.add_argument('--' + name, required=True)
                other_args[name] = None
        parser.add_argument(
            "--running_local",
            type=bool,
            required=False,
            default=False,
            help="If the container is running locally (no contact with GCP).")
        other_args['running_local'] = None
        parser.add_argument('--csv_delimiter',
                            required=False,
                            default=',',
                            help='Delimiter for CSV files. Default is ",".')
        other_args['csv_delimiter'] = None
        args = parser.parse_args()
        for arg in input_args:
            if args.__dict__[arg] is not None:
                input_args[arg] = args.__dict__[arg]
            else:
                input_args[arg] = args.__dict__[
                    arg + 'bucket'] + '/' + args.__dict__[
                        arg + 'path'] + '/' + args.__dict__[arg + 'filename']
        for arg in output_args:
            if args.__dict__[arg] is not None:
                output_args[arg] = args.__dict__[arg]
            else:
                output_args[arg] = args.__dict__[
                    arg + 'bucket'] + '/' + args.__dict__[
                        arg + 'path'] + '/' + args.__dict__[arg + 'filename']
        for arg in other_args:
            other_args[arg] = args.__dict__[arg]
        for arg in secret_location_args:
            secret_location_args[arg] = args.__dict__[arg]
            if self.__is_storage_secret(arg, secret_location_args[arg]):
                gcs_secret_location = secret_location_args[arg]
        for secret in self.__find_secrets():
            name = secret.split('/')[-1].strip('.json')
            found_secrets[name] = secret
        self.__input_args = input_args
        self.__output_args = output_args
        self.__secret_location_args = secret_location_args
        self.__other_args = other_args
        self.__found_secrets = found_secrets
        print(self.get_args())
        self.__gcs_io = gcs_file_io(gcs_secret_location=gcs_secret_location,
                                    local=other_args['running_local'])

    def __is_storage_secret(self, word1, word2):
        words = ['gcs', 'storage', 'GCS', 'STORAGE']
        for word in words:
            if (word in word1) or (word in word2):
                return True
        return False

    def __find_secrets(self):
        if (not os.path.exists(default_secret_folder)):
            print("No secret files found in default directory")
            return
        files = [
            os.path.join(dp, f)
            for dp, dn, fn in os.walk(default_secret_folder)
            for f in fn
        ]
        print("Found these secret files:", files)
        return files

    def get_input_objects(self):
        """Returns a dictionary of input objects.
        
        For each object in the 'input_args' category of CLAs, downloads the object
        from GCS and attempts to convert it to a dataframe based on the file extension.
        If it cannot convert, returns a file-like object instead of a dataframe for that object.
        The keys in the dictionary are the names of the input_args, and the values are the dataframes.
        For example, if 'input_left' was passed in during init, then the key would be 'input_left' and
        the value would be the object located in the GCS URI passed into the 'input_left' CLA.
        """
        return_dict = {}
        for item in self.__input_args:
            return_dict[item] = self.__gcs_io.download_file_to_object(
                self.__input_args[item],
                delimiter=self.__other_args['csv_delimiter'])
        return return_dict

    def upload_objects(self, objects):
        """Uploads a dictionary of objects to GCS.

        This method uploads all the objects in the dictionary to GCS. It
        does this by taking each key in the dictionary and using the 
        command line input associated with that key as the GCS URI for
        uploading. The object will be converted a file type based off the
        file extension in the URI. For example, if 'output_outer' was passed
        in during init, the object whose key is 'output_outer' will be uploaded
        to whatever URI was passed in to the 'output_outer' CLA.

        Args:
          objects: A dictionary containing the objects to upload to GCS.
            The keys should be strings in the 'output_args' group, and the
            values should be dataframe objects.

        Returns: A dictionary where the keys are the same as the input
            dictionary and the values are the results from blob.upload()
        """
        return_dict = {}
        for item in objects:
            return_dict[item] = self.__gcs_io.upload_file_from_object(
                self.__output_args[item], objects[item])
            print('Successfully uploaded', item, 'to', self.__output_args[item])
        return return_dict

    def get_input_args(self):
        """Returns the CLA in the input_args group.
        
        Returns a dictionary of input_args. The keys of the dictionary are the names
        of the input_args, and the values are the values passed into the CLA corresponding
        to that argument. For example, if 'input_left' were passed in during init, then
        'input_left' would be a key in the return dictionary and the value for that key would
        be the value that was passed in through the command line. Any parameter passed in
        during init containing the word 'input' is in this category. For each parameter in this
        category, bucket, path, filename, and URI CLA arguments are created. Either the URI or the bucket,
        path, and filename must be populated.
        """
        return self.__input_args

    def get_output_args(self):
        """Returns the CLA in the output_args group.
        
        Returns a dictionary of output_args. The keys of the dictionary are the names
        of the output_args, and the values are the values passed into the CLA corresponding
        to that argument. For example, if 'output_left' were passed in during init, then
        'output_left' would be a key in the return dictionary and the value for that key would
        be the value that was passed in through the command line. Any parameter passed in
        during init containing the word 'output' is in this category. For each parameter in this
        category, bucket, path, filename, and URI CLA arguments are created. Either the URI or the bucket,
        path, and filename must be populated.
        """
        return self.__output_args

    def get_secret_location_args(self):
        """Returns the CLA in the secret_location_args group.
        
        Returns a dictionary of secret_location_args. The keys of the dictionary are the names
        of the secret_location_args, and the values are the values passed into the CLA corresponding
        to that argument. For example, if 'sap_secret_location' were passed in during init, then
        'sap_secret_location' would be a key in the return dictionary and the value for that key would
        be the value that was passed in through the command line. Any parameter containing the words
        'secret' and 'location' is in this category. For each parameter in this category, one CLA is created.
        """
        return self.__secret_location_args

    def get_found_secrets(self):
        """Returns a dictionary of secret files found automatically.

        Any secret files found in the default secrets location belong to this category. The keys
        are the names of the files with the file extension removed and the values are the filepaths.
        """
        return self.__found_secrets

    def get_secret_location_args_objects(self):
        """Returns the JSON loaded secrets from the secret_location_args group.
        
        Returns a dictionary of JSON loaded secrets specified in secret_location_args. The keys
        of the dictionary are the names of the secret_location_args, and the values are the
        JSON loaded files passed into the CLA corresponding to that argument. For example, if
        'sap_secret_location' were passed in during init, then 'sap_secret_location' would be a key in
        the return dictionary and the value for that key would be the JSON loaded file specified
        through the command line. Any parameter containing the words 'secret' and 'location' is
        in this category. For each parameter in this category, one CLA is created.
        """
        return_dict = {}
        for item in self.__secret_location_args:
            try:
                return_dict[item] = json.load(self.__secret_location_args[item])
            except ValueError:
                print(self.__secret_location_args[item],
                      'is not a properly formatted JSON.')
        return return_dict
    
    def get_found_secret_objects(self):
        """Returns the JSON loaded secrets from secret files found automatically.
        
        Returns a dictionary of JSON loaded secrets specified in the default secrets location. The keys
        are the names of the files with the file extension removed, and the values are the
        JSON loaded files. For example, if 'sap_secret_location.json' is found, then 'sap_secret_location'
        would be a key in the return dictionary and the value for that key would be the JSON loaded file.
        Any secret files found in the default secrets location belong to this category. The keys
        are the names of the files with the file extension removed and the values are the filepaths.
        """
        return_dict = {}
        for item in self.__found_secrets:
            try:
                return_dict[item] = json.load(self.__found_secrets[item])
            except ValueError:
                print(self.__found_secrets[item],
                      'is not a properly formatted JSON.')
        return return_dict

    def get_other_args(self):
        """Returns a dictionary of arguments in the other_args category.
        
        The keys of the dictionary are the names of the parameters passed in through init, and the
        values are the arguments passed in through the command line that correspond to those parameters.
        Any parameter not belonging to the input_args, output_args, or secret_location_args is included
        in this category. One CLA is created for each parameter.
        """
        return self.__other_args

    def get_args(self):
        """Returns a list dictionary of all arguments, broken into groups.
        
        The keys of the dictionary are 'input', 'output', 'secret_location_args',
        'other', and 'found_secrets'. The values of these keys are the dictionaries
        associated with each input grouping.
        """
        return {
            'input': self.__input_args,
            'output': self.__output_args,
            'secret_location_args': self.__secret_location_args,
            'other': self.__other_args,
            'found_secrets': self.__found_secrets
        }

    def __str__(self):
        str(self.get_args())
