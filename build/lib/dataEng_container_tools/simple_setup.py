from .gcs import gcs_file_io
from .cla import default_gcs_secret_locations
import argparse

class simple_setup:
    def __init__(self, argument_names):
        input_args = {}
        output_args = {}
        other_args = {}
        secret_args = {}
        gcs_secret_location = default_gcs_secret_locations[0]
        parser = argparse.ArgumentParser()
        for name in argument_names:
            if "input" in name:
                print("Input:", name)
                parser.add_argument('--'+name, required=False)
                parser.add_argument('--'+name+'bucket', required=False)
                parser.add_argument('--'+name+'path', required=False)
                parser.add_argument('--'+name+'filename', required=False)
                input_args[name] = None
            elif "output" in name:
                print("Output:", name)
                parser.add_argument('--'+name, required=False)
                parser.add_argument('--'+name+'bucket', required=False)
                parser.add_argument('--'+name+'path', required=False)
                parser.add_argument('--'+name+'filename', required=False)
                output_args[name] = None
            elif 'secret' in name and 'location' in name:
                print("Secret:", name)
                parser.add_argument('--'+name, required=True)
                secret_args[name] = None
            else:
                print("Other:", name)
                parser.add_argument('--'+name, required=True)
                other_args[name] = None
        args = parser.parse_args()
        for arg in input_args:
            if args.__dict__[arg] is not None:
                input_args[arg] = args.__dict__[arg]
            else:
                input_args[arg] = args.__dict__[arg+'bucket'] + '/' + args.__dict__[arg+'path'] + '/' + args.__dict__[arg+'filename']
        for arg in output_args:
            if args.__dict__[arg] is not None:
                output_args[arg] = args.__dict__[arg]
            else:
                output_args[arg] = args.__dict__[arg+'bucket'] + '/' + args.__dict__[arg+'path'] + '/' + args.__dict__[arg+'filename']
        for arg in other_args:
            other_args[arg] = args.__dict__[arg]
        for arg in secret_args:
            secret_args[arg] = args.__dict__[arg]
            if 'gcs' in arg or 'gcs' in secret_args[arg]:
                gcs_secret_location = secret_args[arg]
        self.__input_args = input_args
        self.__output_args = output_args
        self.__secret_args = secret_args
        self.__other_args = other_args
        print(self.get_args())
        self.__gcs_io = gcs_file_io(gcs_secret_location = gcs_secret_location)

    def get_input_objects(self):
        return_dict = {}
        for item in self.__input_args:
            return_dict[item] = self.__gcs_io.download_file_to_object(item)
        return return_dict

    def upload_objects(self, objects):
        return_dict = {}
        for item in objects:
            return_dict[item] = self.__gcs_io.upload_file_from_object(self.__output_args[item], objects[item])
        return return_dict

    def get_input_args(self):
        return self.__input_args
    
    def get_output_args(self):
        return self.__output_args

    def get_secret_args(self):
        return self.__secret_args
    
    def get_other_args(self):
        return self.__other_args

    def get_args(self):
        return {'input': self.__input_args, 'output': self.__output_args, 'secret': self.__secret_args, 'other': self.__other_args}


# simple = simple_setup(['input_left', 'input_right', 'output_inner', 'output_outer', 'secret_location'])

# print(simple.get_args())
