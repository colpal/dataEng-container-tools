import json
import pandas as pd
import io
from google.cloud import storage
import pickle

class gcs_file_io:
    gcs_client = None
    gcs_secret_location = None

    def __init__(self, gcs_secret_location, local = False):
        self.gcs_secret_location = gcs_secret_location
        self.local = local
        with open(gcs_secret_location,'r') as f:
            gcs_sa = json.load(f)
        with open('gcs-sa.json', 'w') as json_file:
            json.dump(gcs_sa, json_file)
        self.gcs_client = storage.Client.from_service_account_json('gcs-sa.json')

    def __get_parts(self, gcs_uri):
        if gcs_uri.startswith('gs://'):
            uri = gcs_uri[5:]
        bucket = uri[:uri.find("/")]
        file_path = uri[uri.find("/")+1:]
        return bucket, file_path

    def download_file_to_object(self, gcs_uri, default_file_type = None, dtype = None):
        file_path = None
        file_like_object = None
        if self.local:
            file_path = gcs_uri
            file_like_object = open(gcs_uri)
        else:
            bucket_name, file_path = self.__get_parts(gcs_uri)
            bucket = self.gcs_client.bucket(bucket_name)
            binary_object = bucket.get_blob(file_path).download_as_string()
            file_like_object = io.BytesIO(binary_object)
        hasEnding = file_path.endswith('.parquet') or file_path.endswith('.csv') or file_path.endswith('.pkl')
        if file_path.endswith('.parquet') or ((not hasEnding) and (default_file_type == 'parquet')):
            return pd.read_parquet(file_like_object, dtype = dtype) if dtype else pd.read_parquet(file_like_object)
        if file_path.endswith('.csv') or ((not hasEnding) and (default_file_type == 'csv')):
            return pd.read_csv(file_like_object, dtype = dtype) if dtype else pd.read_csv(file_like_object)
        if file_path.endswith('.pkl') or ((not hasEnding) and (default_file_type == 'pkl')):
            return pd.read_pickle(file_like_object) if dtype else pd.read_pickle(file_like_object)#, dtype = dtype
        if file_path.endswith('.json') or ((not hasEnding) and (default_file_type == 'json')):
            return json.load(file_like_object)
        return file_like_object

    def download_files_to_objects(self, gcs_uris, default_file_types = None, dtypes = None):
        dtypes_status = 0
        if dtypes:
            dtypes_status +=1
            if len(dtypes)>1:
                dtypes_status +=1
        return_objects = []
        for pos, gcs_uri in enumerate(gcs_uris):
            if dtypes_status == 0:
                return_objects.append(self.download_file_to_object(gcs_uri, default_file_type= default_file_types[pos]))
            elif dtypes_status == 1:
                return_objects.append(self.download_file_to_object(gcs_uri,
                                                                    default_file_type= default_file_types[pos],
                                                                    dtype = dtypes[0]))
            else:
                return_objects.append(self.download_file_to_object(gcs_uri,
                                                                    default_file_type= default_file_types[pos],
                                                                    dtype = dtypes[pos]))
        return return_objects

    def download_file_to_disk(self, gcs_uri, local_location = None):
        if self.local:
            if not local_location:
                return gcs_uri
            open(local_location, 'wb').write(open(gcs_uri, 'rb').read())
            return local_location
        bucket_name, file_path = self.__get_parts(gcs_uri)
        # print("Bucket:", bucket_name, "File:", file_path)
        # print(self.gcs_client)
        bucket = self.gcs_client.bucket(bucket_name)
        local_location = local_location if local_location else file_path
        bucket.get_blob(file_path).download_to_filename(local_location)
        return local_location

    def download_files_to_disk(self, gcs_uris, local_locations = None):
        return_locations = []
        for pos, gcs_uri in enumerate(gcs_uris):
            return_locations.append(self.download_file_to_disk(gcs_uri = gcs_uri, local_location = local_locations[pos]))
        return return_locations

    def upload_file_from_disk(self, gcs_uri, local_location):
        if self.local:
            open(gcs_uri, 'wb').write(open(local_location, 'rb').read())
            return gcs_uri
        bucket_name, file_path = self.__get_parts(gcs_uri)
        bucket = self.gcs_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        return blob.upload_from_filename(local_location)

    def upload_files_from_disk(self, gcs_uris, local_locations):
        return_objects = []
        for pos, gcs_uri in enumerate(gcs_uris):
            return_objects.append(self.upload_file_from_disk(gcs_uri, local_locations[pos]))
        return return_objects

    def upload_file_from_object(self, gcs_uri, object_to_upload, default_file_type = None):
        file_path = None
        blob = None
        if not self.local:
            bucket_name, file_path = self.__get_parts(gcs_uri)
            bucket = self.gcs_client.bucket(bucket_name)
            blob = bucket.blob(file_path)
        hasEnding = file_path.endswith('.parquet') or file_path.endswith('.csv') or file_path.endswith('.pkl')
        if file_path.endswith('.parquet') or ((not hasEnding) and (default_file_type == 'parquet')):
            if self.local:
                return object_to_upload.to_parquet(gcs_uri)
            fileObject = io.BytesIO()
            object_to_upload.to_parquet(fileObject)
            fileObject.seek(0)
            return blob.upload_from_file(fileObject)
        if file_path.endswith('.csv') or ((not hasEnding) and (default_file_type == 'csv')):
            if self.local:
                return object_to_upload.to_csv(gcs_uri)
            csv_string = object_to_upload.to_csv(encoding='utf-8')
            return blob.upload_from_string(csv_string)
        if file_path.endswith('.pkl') or ((not hasEnding) and (default_file_type == 'pkl')):
            if self.local:
                return pickle.dump(gcs_uri, open(gcs_uri,'wb'))
            fileObject = io.BytesIO(pickle.dumps(object_to_upload))
            fileObject.seek(0)
            return blob.upload_from_file(fileObject)
        if file_path.endswith('.json') or ((not hasEnding) and (default_file_type == 'json')):
            if self.local:
                return json.dump(gcs_uri, open(gcs_uri,'w'))
            json_string = json.dumps(fileObject)
            return blob.upload_from_string(json_string)
        if self.local:
            return pickle.dump(gcs_uri, open(gcs_uri,'wb'))
        fileObject = io.BytesIO(pickle.dumps(object_to_upload))
        fileObject.seek(0)
        return blob.upload_from_file(fileObject)

    def upload_files_from_objects(self, gcs_uris, objects_to_upload, default_file_type = None):
        return_objects = []
        for pos, gcs_uri in enumerate(gcs_uris):
            return_objects.append(self.upload_file_from_object(gcs_uri, objects_to_upload[pos], default_file_type = default_file_type))
        return return_objects