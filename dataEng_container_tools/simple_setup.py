from .gcs import gcs_file_io
from .safe_stdout import default_gcs_secret_locations, default_secret_folder
import argparse
import json
import os


class simple_setup:
    """ """

    def __init__(self, argument_names):
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
                parser.add_argument('--' + name, required=False)
                parser.add_argument('--' + name + 'bucket', required=False)
                parser.add_argument('--' + name + 'path', required=False)
                parser.add_argument('--' + name + 'filename', required=False)
                input_args[name] = None
            elif "output" in name:
                print("Output:", name)
                parser.add_argument('--' + name, required=False)
                parser.add_argument('--' + name + 'bucket', required=False)
                parser.add_argument('--' + name + 'path', required=False)
                parser.add_argument('--' + name + 'filename', required=False)
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
        """ """
        return_dict = {}
        for item in self.__input_args:
            return_dict[item] = self.__gcs_io.download_file_to_object(
                self.__input_args[item],
                delimiter=self.__other_args['csv_delimiter'])
        return return_dict

    def upload_objects(self, objects):
        """

        Args:
          objects: 

        Returns:

        """
        return_dict = {}
        for item in objects:
            return_dict[item] = self.__gcs_io.upload_file_from_object(
                self.__output_args[item], objects[item])
            print('Successfully uploaded', item, 'to', self.__output_args[item])
        return return_dict

    def get_input_args(self):
        """ """
        return self.__input_args

    def get_output_args(self):
        """ """
        return self.__output_args

    def get_secret_location_args(self):
        """ """
        return self.__secret_location_args

    def get_found_secrets(self):
        """ """
        return self.__found_secrets

    def get_secrets_objects(self):
        """ """
        return_dict = {}
        for item in self.__secret_location_args:
            try:
                return_dict[item] = json.load(self.__secret_location_args[item])
            except ValueError:
                print(self.__secret_location_args[item],
                      'is not a properly formatted JSON.')
        return return_dict

    def get_other_args(self):
        """ """
        return self.__other_args

    def get_args(self):
        """ """
        return {
            'input': self.__input_args,
            'output': self.__output_args,
            'secret_location': self.__secret_location_args,
            'other': self.__other_args,
            'found_secrets': self.__found_secrets
        }

    def __str__(self):
        str(self.get_args())
