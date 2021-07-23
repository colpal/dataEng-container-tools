"""Tools for working with GCP.

Deals with receiving downloading and uploading files from/to GCP. Has one
class: `gcs_file_io`.

Typical usage example:

    file_io = gcs_file_io(gcs_secret_location = secret_locations[0])
    pqt_obj = file_io.download_file_to_object(input_uris[0])
    #
    # Edit the object in some way here.
    #
    result = file_io.upload_file_from_object(gcs_uri=output_uris[0], object_to_upload=pqt_obj)
"""

import json
import pandas as pd
import io
from google.cloud import storage
import pickle
import os
from dataEng_container_tools.db import get_secrets
from google.cloud import bigquery as GBQ

from dataEng_container_tools.exceptions import StorageCredentialNotFound


class gcs_file_io:
    """Uploads and downloads files to/from GCS.

    This uploads and downloads files to/from GCS. It will handle
    much of the backend boilerplate code involved with downloading,
    to object of file and uploading from object or file.
    Includes helper functions for using GCS.

    Attributes:
        gcs_client: The GCS Client
        gcs_secret_location: The location of the secret file
            associated with the the GCS upload or download location.
        local: A boolean flag indicating whether or not the library
            is running in local only mode and should not attempt to
            contact GCP. If True, will look for the files locally.
    """
    gcs_client = None
    gcs_secret_location = None
    local = None

    def __init__(self, gcs_secret_location, local=False):
        """Initializes gcs_file_io with desired configuration.

        Args:
            gcs_secret_location: Required. The location of the secret file
                needed for GCS.
            local: Optional. Defaults to False. If True, no contact
                will be made with GCS.
        """
        self.local = local
        if not local:
            self.gcs_secret_location = gcs_secret_location
            with open(gcs_secret_location, 'r') as f:
                gcs_sa = json.load(f)
            with open('gcs-sa.json', 'w') as json_file:
                json.dump(gcs_sa, json_file)
            self.gcs_client = storage.Client.from_service_account_json(
                'gcs-sa.json')

    def get_bq_client(self, PATH):
        """Returns client object to perform CRUD operations on a google.cloud library
        library = google.cloud library
        :type PATH: object
        """

        try:
            cred = get_secrets(PATH)
            key = cred["key"]
        except KeyError as ke:
            raise StorageCredentialNotFound("Storage credentials not"
                                            " mounted for gcs ")
        bq_sa = json.loads(key)
        with open('bq-sa.json', 'w') as json_file:
            json.dump(bq_sa, json_file)
        return GBQ.Client.from_service_account_json('bq-sa.json')

    def __get_parts(self, gcs_uri):
        if gcs_uri.startswith('gs://'):
            gcs_uri = gcs_uri[5:]
        bucket = gcs_uri[:gcs_uri.find("/")]
        file_path = gcs_uri[gcs_uri.find("/") + 1:]
        return bucket, file_path

    def __wildcard_download(self, gcs_uri, default_file_type, dtype):
        return_dict = {}
        bucket_name, file_path = self.__get_parts(gcs_uri)
        bucket = self.gcs_client.bucket(bucket_name)
        prefix = file_path.strip('*')
        blobs = list(bucket.list_blobs(prefix=prefix))
        for blob in blobs:
            return_dict[blob.name] = self.download_file_to_object(
                blob.name, default_file_type=default_file_type, dtype=dtype)
        return return_dict

    def download_file_to_object(self,
                                gcs_uri,
                                default_file_type=None,
                                dtype=None,
                                delimiter=None,
                                header=0,
                                encoding='utf-8'):
        """Downloads a file from GCS to an object in memory.

        Args:
            gcs_uri: Required. The uri of the object in GCS to download. If local is
                True, it is the path to a local file that will be read into an object.
            default_file_type: Optional. Defaults to None. If the uri the object does not have a
                 file type ending, it will be assumed to be this type.
            dtype: Optional. Defaults to None. A dictionary of (column: type) pairs.
            delimiter: Optional. Defaults to None. delimiter of the file
            header: Optional, Default to 0. If set to None it will not read first row as header for
            xls and csv files, if set to 0 or any int or List[int] it will read those rows to build
            header/columns
            encoding: Optional. Defaults to utf-8. encoding of the file

        Returns:
            A dataframe if the object format can be inferred, otherwise a file-like object.
        """
        if self.local:
            file_path = gcs_uri
            file_like_object = open(gcs_uri)
        elif '*' in gcs_uri:
            return self.__wildcard_download(gcs_uri, default_file_type, dtype)
        else:
            bucket_name, file_path = self.__get_parts(gcs_uri)
            bucket = self.gcs_client.bucket(bucket_name)
            binary_object = bucket.blob(file_path).download_as_string()
            file_like_object = io.BytesIO(binary_object)
        hasEnding = file_path.endswith('.parquet') or file_path.endswith(
            '.csv') or file_path.endswith('.pkl') or file_path.endswith('.xlsx')
        if file_path.endswith('.parquet') or ((not hasEnding) and
                                              (default_file_type == 'parquet')):
            return pd.read_parquet(
                file_like_object,
                dtype=dtype) if dtype else pd.read_parquet(file_like_object)
        if file_path.endswith('.csv') or ((not hasEnding) and
                                          (default_file_type == 'csv')):
            return pd.read_csv(file_like_object,
                               dtype=dtype, header=header,
                               delimiter=delimiter, encoding=encoding) if dtype else pd.read_csv(
                file_like_object, header=header, delimiter=delimiter, encoding=encoding)
        if file_path.endswith('.xlsx') or ((not hasEnding) and
                                           (default_file_type == 'xlsx')):
            if self.local:
                return pd.read_excel(file_path, dtype=dtype, header=header, engine='openpyxl') if dtype else \
                    pd.read_excel(file_path, header=header,
                                                                                                                           engine='openpyxl')
            else:
                return pd.read_excel(file_like_object, dtype=dtype, header=header, engine='openpyxl') if dtype else\
                    pd.read_excel(file_like_object, header=header, engine='openpyxl')

        if file_path.endswith('.pkl') or ((not hasEnding) and
                                          (default_file_type == 'pkl')):
            return pd.read_pickle(
                file_like_object) if dtype else pd.read_pickle(
                file_like_object)  # , dtype = dtype
        if file_path.endswith('.json') or ((not hasEnding) and
                                           (default_file_type == 'json')):
            return json.load(file_like_object)
        return file_like_object

    def download_files_to_objects(self,
                                  gcs_uris,
                                  default_file_type=None,
                                  dtypes=[],
                                  delimiters=[],
                                  encodings=[]):
        """Downloads files from GCS to an objects in memory.

        Args:
            gcs_uris: Required. The uris of the objects in GCS to download. If local
                is True, it is the paths to local files that will be read into objects.
            default_file_type: Optional. Defaults to None. A string. If the uri an object does
                not have a file type ending, it will be assumed to be this type.
            dtypes: Optional. Defaults to empty list. A list of dictionaries of (column: type) pairs.
            delimiters: Optional. Defaults to empty list. A list of delimiters of the files
            encodings: Optional. Defaults to empty list. A list of encodings of the file

        Returns:
            A list of dataframes and/or file-like objects. If the object format can be inferred it will
                be returned as a dataframe, otherwise a file-like object.
        """
        return_objects = []
        for pos, gcs_uri in enumerate(gcs_uris):
            dt = None
            delim = None
            encod = 'utf-8'
            if len(dtypes) == 1:
                dt = dtypes[0]
            elif len(dtypes) > 1:
                dt = dtypes[pos]
            if len(delimiters) == 1:
                delim = delimiters[0]
            elif len(delimiters) > 1:
                delim = delimiters[pos]
            if len(encodings) == 1:
                encod = encodings[0]
            elif len(encodings) > 1:
                encod = encodings[pos]
            return_objects.append(
                self.download_file_to_object(
                    gcs_uri,
                    default_file_type=default_file_type,
                    dtype=dt,
                    delimiter=delim,
                    encoding=encod))
        return return_objects

    def download_file_to_disk(self, gcs_uri, local_location=None):
        """Downloads a file from GCS to the container's disk.

        Args:
        gcs_uri: Required. The uri of the object in GCS to download. If
            local is True, it is the path to a local file that will be
            copied to local_location.
        local_location: Optional. Defaults to None. Where to save the object.
            If None, saves to same path as the the GCS URI.

        Returns:
            A string representation of the path to the downloaded file.
        """
        if self.local:
            if not local_location:
                return gcs_uri
            if gcs_uri != local_location:
                open(local_location, 'wb').write(open(gcs_uri, 'rb').read())
            return local_location
        bucket_name, file_path = self.__get_parts(gcs_uri)
        bucket = self.gcs_client.bucket(bucket_name)
        local_location = local_location if local_location else file_path
        bucket.get_blob(file_path).download_to_filename(local_location)
        return local_location

    def download_files_to_disk(self, gcs_uris, local_locations=[]):
        """Downloads files from GCS to the container's disk.

        Args:
            gcs_uris: Required. The uris of the objects in GCS to download. If
                local is True, it is the paths to local files that will be
                copied to local_locations.
            local_locations: Optional. Defaults to empty list. The locations to save
                the objects. If empty, saves to same paths as the the GCS URIs.

        Returns:
            A list of string representations of the paths to the downloaded files.
        """
        return_locations = []
        for pos, gcs_uri in enumerate(gcs_uris):
            if len(local_locations) > 0:
                return_locations.append(
                    self.download_file_to_disk(
                        gcs_uri=gcs_uri, local_location=local_locations[pos]))
            else:
                return_locations.append(
                    self.download_file_to_disk(gcs_uri=gcs_uri))
        return return_locations

    def upload_file_from_disk(self, gcs_uri, local_location, metadata={}):
        """Uploads a file to GCS from the container's hard drive.

        Args:
            gcs_uri: Required. The uri to which the object will be uploaded. If local is
                True, it is the path to a local file that will be copied from local_location.
            local_location: Optional. Defaults to None. The location of the object. If None, assumes
                the same path as the the GCS URI.
            metadata: Optional dictionary. Defaults to an empty dictionary. The metadata to add to the
                object. Git hash is added automatically if GITHUB_SHA is set as an environment variable.

        Returns:
            The result of blob.upload().
        """
        if 'DAG_ID' in os.environ.keys():
            metadata['DAG_ID'] = os.environ['DAG_ID']
        if 'RUN_ID' in os.environ.keys():
            metadata['RUN_ID'] = os.environ['RUN_ID']
        if 'NAMESPACE' in os.environ.keys():
            metadata['NAMESPACE'] = os.environ['NAMESPACE']
        if 'POD_NAME' in os.environ.keys():
            metadata['POD_NAME'] = os.environ['POD_NAME']
        if 'GITHUB_SHA' in os.environ.keys():
            metadata['git_hash'] = os.environ['GITHUB_SHA']
        if self.local:
            if gcs_uri != local_location:
                open(gcs_uri, 'wb').write(open(local_location, 'rb').read())
            return gcs_uri
        bucket_name, file_path = self.__get_parts(gcs_uri)
        bucket = self.gcs_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        blob.metadata = metadata
        return blob.upload_from_filename(local_location)

    def upload_files_from_disk(self, gcs_uris, local_locations, metadata=[]):
        """Uploads files to GCS from the container's hard drive.

        Args:
            gcs_uris: Required. The uris to which the objects will be uploaded. If local
                is True, it is the paths to local files that will be copied from local_locations.
            local_locations: Optional. Defaults to None. The locations of the objects. If None,
                assumes the same paths as the the GCS URIs.
            metadata: Optional list of dictionaries. Defaults to empty. The metadata to add to
                the objects. Git hash is added automatically if GITHUB_SHA is set as an environment variable.

        Returns:
            A list of the results of blob.upload().
        """
        return_objects = []
        for pos, gcs_uri in enumerate(gcs_uris):
            if len(metadata) == 0:
                return_objects.append(
                    self.upload_file_from_disk(gcs_uri, local_locations[pos]))
            elif len(metadata) == 1:
                return_objects.append(
                    self.upload_file_from_disk(gcs_uri,
                                               local_locations[pos],
                                               metadata=metadata[0]))
            else:
                return_objects.append(
                    self.upload_file_from_disk(gcs_uri,
                                               local_locations[pos],
                                               metadata=metadata[pos]))
        return return_objects

    def upload_file_from_object(self,
                                gcs_uri,
                                object_to_upload,
                                default_file_type=None,
                                header=True,
                                metadata={}):
        """Uploads a file to GCS from an object in memory.

        Args:
            gcs_uri: Required. The uri to which the object will be uploaded. If local
                is True, it is the path to a local file where the object will be written.
            default_file_type: Optional. Defaults to None. If the uri does not have a file type
                ending, it will be assumed to be this type.
            header: Optional. Defaults to True, if False the columns will
            not be written (for csv and excel)
            dtype: Optional. Defaults to None. A dictionary of (column: type) pairs.
            metadata: Optional dictionary. Defaults to an empty dictionary. The metadata to add to
                the object. Git hash is added automatically if GITHUB_SHA is set as an environment variable.

        Returns:
            The result from blob.upload().
        """
        if 'DAG_ID' in os.environ.keys():
            metadata['DAG_ID'] = os.environ['DAG_ID']
        if 'RUN_ID' in os.environ.keys():
            metadata['RUN_ID'] = os.environ['RUN_ID']
        if 'NAMESPACE' in os.environ.keys():
            metadata['NAMESPACE'] = os.environ['NAMESPACE']
        if 'POD_NAME' in os.environ.keys():
            metadata['POD_NAME'] = os.environ['POD_NAME']
        if 'GITHUB_SHA' in os.environ.keys():
            metadata['git_hash'] = os.environ['GITHUB_SHA']
        blob = None
        if not self.local:
            bucket_name, file_path = self.__get_parts(gcs_uri)
            bucket = self.gcs_client.bucket(bucket_name)
            blob = bucket.blob(file_path)
            blob.metadata = metadata
        else:
            file_path = gcs_uri
        hasEnding = file_path.endswith('.parquet') or file_path.endswith(
            '.csv') or file_path.endswith('.pkl') or file_path.endswith('.xlsx')
        if file_path.endswith('.parquet') or ((not hasEnding) and
                                              (default_file_type == 'parquet')):
            if self.local:
                return object_to_upload.to_parquet(gcs_uri)
            fileObject = io.BytesIO()
            object_to_upload.to_parquet(fileObject)
            fileObject.seek(0)
            return blob.upload_from_file(fileObject)
        if file_path.endswith('.csv') or ((not hasEnding) and
                                          (default_file_type == 'csv')):
            if self.local:
                return object_to_upload.to_csv(gcs_uri, header=header, index=False)
            csv_string = object_to_upload.to_csv(encoding='utf-8', header=header, index=False)
            return blob.upload_from_string(csv_string)
        if file_path.endswith('.xlsx') or ((not hasEnding) and
                                           (default_file_type == 'xlsx')):
            if self.local:
                return object_to_upload.to_excel(gcs_uri, header=header, index=False)
            fileObject = io.BytesIO()
            object_to_upload.to_excel(fileObject, header=header, index=False)
            fileObject.seek(0)
            return blob.upload_from_file(fileObject)
        if file_path.endswith('.pkl') or ((not hasEnding) and
                                          (default_file_type == 'pkl')):
            if self.local:
                return pickle.dump(gcs_uri, open(gcs_uri, 'wb'))
            fileObject = io.BytesIO(pickle.dumps(object_to_upload))
            fileObject.seek(0)
            return blob.upload_from_file(fileObject)
        if file_path.endswith('.json') or ((not hasEnding) and
                                           (default_file_type == 'json')):
            if self.local:
                return json.dump(gcs_uri, open(gcs_uri, 'w'))
            json_string = json.dumps(fileObject)
            return blob.upload_from_string(json_string)
        if self.local:
            return pickle.dump(gcs_uri, open(gcs_uri, 'wb'))
        fileObject = io.BytesIO(pickle.dumps(object_to_upload))
        fileObject.seek(0)
        return blob.upload_from_file(fileObject)

    def upload_files_from_objects(self,
                                  gcs_uris,
                                  objects_to_upload,
                                  default_file_type=None,
                                  metadata=[]):
        """Uploads files to GCS from objects in memory.

        Args:
            gcs_uris: Required. The uris to which the objects will be uploaded. If
                local is True, it is the paths to local files where the objects will be written.
            default_file_type: Optional. Defaults to None. A sting. If the uri an object does not
                have a file type ending, it will be assumed to be this type.
            dtypes: Optional. Defaults to None. A list of dictionary of (column: type) pairs.
            metadata: Optional list of dictionaries. Defaults to an empty list. The metadata to add to
                each object. Git hash is added automatically if GITHUB_SHA is set as an environment variable.

        Returns:
            A list of the results from blob.upload()
        """
        return_objects = []
        for pos, gcs_uri in enumerate(gcs_uris):
            if len(metadata) == 0:
                return_objects.append(
                    self.upload_file_from_object(
                        gcs_uri,
                        objects_to_upload[pos],
                        default_file_type=default_file_type))
            elif len(metadata) == 1:
                return_objects.append(
                    self.upload_file_from_object(
                        gcs_uri,
                        objects_to_upload[pos],
                        default_file_type=default_file_type,
                        metadata=metadata[0]))
            else:
                return_objects.append(
                    self.upload_file_from_object(
                        gcs_uri,
                        objects_to_upload[pos],
                        default_file_type=default_file_type,
                        metadata=metadata[pos]))
        return return_objects
