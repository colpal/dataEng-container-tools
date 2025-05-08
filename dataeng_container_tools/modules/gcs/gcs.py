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

from __future__ import annotations

import io
import json
import os
import pickle
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

import pandas as pd
from google.cloud import storage

from dataeng_container_tools.modules import BaseModule, BaseModuleUtilities

if TYPE_CHECKING:
    from google.cloud.storage.blob import Blob


class GCSFileIO(BaseModule):
    """Uploads and downloads files to/from GCS.

    This uploads and downloads files to/from GCS. It will handle
    much of the backend boilerplate code involved with downloading,
    to object of file and uploading from object or file.
    Includes helper functions for using GCS.

    Attributes:
        gcs_client: The GCS Client
            associated with the the GCS upload or download location.
        local: A boolean flag indicating whether or not the library
            is running in local only mode and should not attempt to
            contact GCP. If True, will look for the files locally.
    """

    MODULE_NAME: ClassVar[str] = "GCS"
    DEFAULT_SECRET_PATHS: ClassVar[dict[str, str]] = {"GCS": "/vault/secrets/gcp-sa-storage.json"}

    KNOWN_EXTENSIONS: Final = {".parquet", ".csv", ".pkl", ".xlsx", ".json"}

    def __init__(
        self,
        gcs_secret_location: str | Path | None = None,
        *,
        local: bool = False,
        use_cla_fallback: bool = True,
        use_file_fallback: bool = True,
    ) -> None:
        """Initializes gcs_file_io with desired configuration.

        Args:
            gcs_secret_location (str | Path | None): The location of the secret file needed for GCS.
            local (bool): If True, no contact will be made with GCS.
            use_cla_fallback (bool): If True, attempts to use command-line arguments
                as a fallback source for secrets when the primary source fails.
            use_file_fallback (bool): If True, attempts to use the default secret file
                as a fallback source when both primary and command-line sources fail.
        """
        self.local = local

        if not self.local:
            gcs_sa = BaseModuleUtilities.parse_secret_with_fallback(
                gcs_secret_location,
                self.MODULE_NAME if use_cla_fallback else None,
                self.DEFAULT_SECRET_PATHS[self.MODULE_NAME] if use_file_fallback else None,
            )

            if not gcs_sa:
                msg = "GCS credentials not found"
                raise FileNotFoundError(msg)

            self.client: storage.Client = storage.Client.from_service_account_info(gcs_sa)
        else:
            self.client: storage.Client = storage.Client()  # For typing, unused

    @staticmethod
    def __get_parts(gcs_uri: str) -> tuple[str, str]:
        gcs_uri = gcs_uri.removeprefix("gs://")
        bucket = gcs_uri[: gcs_uri.find("/")]
        file_path = gcs_uri[gcs_uri.find("/") + 1 :]
        return bucket, file_path

    def __wildcard_download(
        self,
        gcs_uri: str,
        default_file_type: str | None,
        dtype: dict | None,
        pandas_kwargs: dict,
    ) -> dict[str, Any]:
        return_dict: dict[str, Any] = {}
        bucket_name, file_path = self.__get_parts(gcs_uri)
        bucket = self.client.bucket(bucket_name)
        prefix = file_path.strip("*")
        blobs: list[Blob] = list(bucket.list_blobs(prefix=prefix))
        for blob in blobs:
            name = cast("str", blob.name)
            return_dict[name] = self.download_file_to_object(
                name,
                default_file_type=default_file_type,
                dtype=dtype,
                pandas_kwargs=pandas_kwargs,
            )
        return return_dict

    def download_file_to_object(
        self,
        gcs_uri: str | Path,
        default_file_type: str | None = None,
        dtype: dict | None = None,
        delimiter: str | None = None,
        header: int | list[int] | None = 0,
        pandas_kwargs: dict | None = None,
        encoding: str = "utf-8",
    ) -> dict | pd.DataFrame | io.BytesIO | io.TextIOWrapper:
        """Downloads a file from GCS to an object in memory.

        https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html
        https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html

        Args:
            gcs_uri: Required. The uri of the object in GCS to download. If local is
                True, it is the path to a local file that will be read into an object.
            default_file_type: Optional. Defaults to None. If the uri the object does not have a
                file type ending, it will be assumed to be this type.
            dtype: Optional. Defaults to None. A dictionary of (column: type) pairs.
            delimiter: Optional. Defaults to None. delimiter of the file.
            header: Optional. Default to 0. If set to None it will not read first row as header.
                For xls and csv files, if set to 0 or any int or List[int] it will read those
                rows to build header/columns.
            pandas_kwargs: Optional. Defaults to None. Additional keyword arguments to pass to pandas.
            encoding: Optional. Defaults to utf-8. encoding of the file.

        Returns:
            A dataframe if the object format can be inferred, otherwise a file-like object.
        """
        pandas_kwargs = pandas_kwargs or {}

        # Get the file object
        if self.local or isinstance(gcs_uri, Path):
            file_path = str(gcs_uri)
            file_obj = io.BytesIO(Path(gcs_uri).read_bytes())
        elif "*" in gcs_uri and not self.local:  # Handle wildcard downloads
            return self.__wildcard_download(gcs_uri, default_file_type, dtype, pandas_kwargs)
        else:
            bucket_name, file_path = self.__get_parts(gcs_uri)
            bucket = self.client.bucket(bucket_name)
            binary_object = bucket.blob(file_path).download_as_string()
            file_obj = io.BytesIO(binary_object)

        # Determine file type and read accordingly
        file_extension = next((ext.lstrip(".") for ext in self.KNOWN_EXTENSIONS if file_path.endswith(ext)), None)

        # Use default_file_type if no extension found
        if file_extension is None and default_file_type:
            file_extension = default_file_type

        # Process file based on its type
        if file_extension == "parquet":
            parquet_obj = pd.read_parquet(file_obj, **pandas_kwargs)
            if dtype:
                parquet_obj = parquet_obj.astype(dtype)
            return parquet_obj

        if file_extension == "csv":
            csv_kwargs = {"header": header, "delimiter": delimiter, "encoding": encoding}
            if dtype:
                return pd.read_csv(file_obj, dtype=dtype, **csv_kwargs)
            return pd.read_csv(file_obj, **{**csv_kwargs, **pandas_kwargs})

        if file_extension == "xlsx":
            excel_kwargs = {"header": header, "engine": "openpyxl"}
            if self.local:
                if dtype:
                    return pd.read_excel(file_path, dtype=dtype, **excel_kwargs)
                return pd.read_excel(file_path, **excel_kwargs)
            if dtype:
                return pd.read_excel(file_obj, dtype=dtype, **excel_kwargs)
            return pd.read_excel(file_obj, **{**excel_kwargs, **pandas_kwargs})

        if file_extension == "pkl":
            if dtype:
                return pd.read_pickle(file_obj)
            return pd.read_pickle(file_obj, **pandas_kwargs)

        if file_extension == "json":
            return pd.read_json(file_obj, lines=True, **pandas_kwargs)

        # If no recognized format, return the file object itself
        return file_obj

    def download_files_to_objects(
        self,
        gcs_uris: list[str | Path],
        default_file_type: str | None = None,
        dtypes: list[dict] | dict | None = None,
        delimiters: list[str] | str | None = None,
        encodings: list[str] | str = "utf-8",
        pandas_kwargs: dict | None = None,
        headers: list[int | list[int] | None] | int | list[int] | None = 0,
    ) -> list[dict | pd.DataFrame | io.BytesIO | io.TextIOWrapper]:
        """Downloads multiple files from GCS to objects in memory.

        Args:
            gcs_uris: The URIs of the objects in GCS to download. If local
                is True, these are paths to local files to read into objects.
            default_file_type: If a URI doesn't have a file extension,
                it will be assumed to be this type.
            dtypes: A dictionary or list of dictionaries with column:type pairs.
                If a single dictionary is provided, it will be used for all files.
            delimiters: Delimiter(s) for CSV files.
                If a single string is provided, it will be used for all files.
            encodings: Encoding(s) for the files. Default is "utf-8".
                If a single string is provided, it will be used for all files.
            pandas_kwargs: Additional keyword arguments to pass to pandas.
            headers: Header row(s) specification.
                If a single value is provided, it will be used for all files.

        Returns:
            A list of dataframes and/or file-like objects depending on file formats.
        """
        # Convert single values to lists
        dtypes_list = self._normalize_parameter(dtypes, len(gcs_uris))
        delimiters_list = self._normalize_parameter(delimiters, len(gcs_uris))
        encodings_list = self._normalize_parameter(encodings, len(gcs_uris))
        headers_list = self._normalize_parameter(headers, len(gcs_uris))

        # Process each URI
        return [
            self.download_file_to_object(
                gcs_uri,
                default_file_type=default_file_type,
                dtype=dtypes_list[i],
                delimiter=delimiters_list[i],
                encoding=encodings_list[i],
                pandas_kwargs=pandas_kwargs,
                header=headers_list[i],
            )
            for i, gcs_uri in enumerate(gcs_uris)
        ]

    def _normalize_parameter(
        self,
        param: object | list[Any],
        length: int,
    ) -> list[Any]:
        """Normalize a parameter to a list of the specified length.

        Args:
            param: The parameter to normalize. Can be a single value or a list.
            length: The desired length of the resulting list.

        Returns:
            A list of the parameter values.
        """
        if param is None:
            return [None] * length
        if isinstance(param, list):
            if len(param) == 0:
                return [None] * length
            if len(param) == 1:
                return param * length
            return param
        return [param] * length

    def download_file_to_disk(
        self,
        gcs_uri: str | Path,
        local_location: str | Path | None = None,
    ) -> Path | None:
        """Downloads a file from GCS to the container's disk.

        Args:
            gcs_uri: The URI of the object in GCS to download. If local is
            True, it is the path to a local file that will be
            copied to local_location.
            local_location: Where to save the object. If None, saves to
            same path as the GCS URI.

        Returns:
            Path to the downloaded file or None if failed.
        """
        gcs_uri = Path(gcs_uri)
        local_location = Path(local_location) if local_location else None

        if self.local:
            if not local_location:
                return None

            # Download from local
            if gcs_uri != local_location:
                local_location.write_bytes(gcs_uri.read_bytes())
                return local_location
            return None

        bucket_name, file_path = self.__get_parts(str(gcs_uri))
        bucket = self.client.bucket(bucket_name)
        local_location = local_location if local_location else Path(file_path)

        # Create directory structure if it doesn't exist
        Path(local_location).parent.mkdir(parents=True, exist_ok=True)

        # Download from GCS
        blob = bucket.get_blob(file_path)
        if blob:
            blob.download_to_filename(local_location)
            return local_location
        return None

    def download_files_to_disk(
        self,
        gcs_uris: list[str | Path],
        local_locations: list[str | Path] | None = None,
    ) -> list[Path | None]:
        """Downloads files from GCS to the container's disk.

        Args:
            gcs_uris: The uris of the objects in GCS to download. If
                local is True, it is the paths to local files that will be
                copied to local_locations.
            local_locations: Optional. The locations to save the objects.
                If None, saves to same paths as the GCS URIs.

        Returns:
            A list of paths to the downloaded files.
        """
        # Normalize local_locations to match gcs_uris length
        if local_locations is None:
            local_locations = []

        # Generate resulting locations list
        return [
            self.download_file_to_disk(
                gcs_uri=gcs_uri,
                local_location=local_locations[i] if i < len(local_locations) else None,
            )
            for i, gcs_uri in enumerate(gcs_uris)
        ]

    def upload_file_from_disk(
        self,
        gcs_uri: str | Path,
        local_location: str | Path,
        metadata: dict | None = None,
    ) -> None | Path:
        """Uploads a file to GCS from the container's hard drive.

        Args:
            gcs_uri: The URI to which the object will be uploaded. If local is
                True, it is the path to a local file that will be copied from local_location.
            local_location: The location of the object to upload.
            metadata: Optional metadata to add to the object. Git hash is added
                automatically if GITHUB_SHA is set as an environment variable.

        Returns:
            The result of blob.upload() or path string if local mode.
        """
        metadata = metadata or {}

        # Add environment variables to metadata
        env_vars = ["DAG_ID", "RUN_ID", "NAMESPACE", "POD_NAME", "GITHUB_SHA"]
        metadata.update({var: os.environ[var] for var in env_vars if var in os.environ})

        if self.local:
            # Convert to Path objects
            src_path = Path(local_location)
            dest_path = Path(gcs_uri)

            if src_path != dest_path:
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                dest_path.write_bytes(src_path.read_bytes())
            return dest_path

        # Upload to GCS
        bucket_name, file_path = self.__get_parts(str(gcs_uri))
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        blob.metadata = metadata
        return blob.upload_from_filename(str(local_location))

    def upload_files_from_disk(
        self,
        gcs_uris: list[str | Path],
        local_locations: list[str | Path],
        metadata: list[dict] | dict | None = None,
    ) -> list[Any]:
        """Uploads files to GCS from the container's hard drive.

        Args:
            gcs_uris: The URIs to which the objects will be uploaded. If local
                is True, these are paths to local files that will be copied from local_locations.
            local_locations: The locations of the objects to upload.
            metadata: Optional metadata to add to the objects. Can be a single dict
                applied to all files or a list of dicts. Git hash and other environment
                variables are added automatically when available.

        Returns:
            A list of the results of blob.upload() or path strings if local mode.
        """
        # Normalize metadata to a list matching the length of gcs_uris
        metadata_list = self._normalize_parameter(metadata, len(gcs_uris))

        # Upload each file with its corresponding metadata
        return [
            self.upload_file_from_disk(gcs_uri=gcs_uri, local_location=local_locations[i], metadata=metadata_list[i])
            for i, gcs_uri in enumerate(gcs_uris)
        ]

    def upload_file_from_object(
        self,
        gcs_uri: str | Path,
        object_to_upload: pd.DataFrame | object,
        default_file_type: str | None = None,
        metadata: dict | None = None,
        pandas_kwargs: dict | None = None,
        *,
        header: bool = True,
        index: bool = False,
    ) -> str | None:
        """Uploads a file to GCS from an object in memory.

        Args:
            gcs_uri: The URI to which the object will be uploaded. If local
                is True, it is the path to a local file where the object will be written.
            object_to_upload: Object to upload, typically a pandas DataFrame
                (required for parquet, csv, xlsx. Otherwise is a pickle).
            default_file_type: If the URI does not have a file type
                ending, it will be assumed to be this type.
            header: Whether to write out the column names (for CSV and Excel).
                Defaults to True.
            index: Whether to write the index or not (for CSV and Excel).
                Defaults to False.
            pandas_kwargs: Additional keyword arguments to pass to pandas methods.
            metadata: Metadata to add to the object. Environment variables like
                Git hash are added automatically when available.

        Returns:
            The result from blob.upload() or file path if in local mode.
        """
        pandas_kwargs = pandas_kwargs or {}
        metadata = metadata or {}

        # Add environment variables to metadata
        env_vars = ["DAG_ID", "RUN_ID", "NAMESPACE", "POD_NAME", "GITHUB_SHA"]
        metadata.update({var: os.environ[var] for var in env_vars if var in os.environ})

        # Setup GCS or local path
        gcs_uri_str = str(gcs_uri)
        if not self.local:
            bucket_name, file_path = self.__get_parts(gcs_uri_str)
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(file_path)
            blob.metadata = metadata
        else:
            file_path = gcs_uri_str

            # Fix type hinting for non local cases
            blob = None
            blob = cast("Blob", blob)

        # Determine file type
        file_extension = next(
            (ext.lstrip(".") for ext in self.KNOWN_EXTENSIONS if file_path.endswith(ext)),
            default_file_type,
        )

        # Handle based on file type
        if file_extension == "parquet" and isinstance(object_to_upload, pd.DataFrame):
            if self.local:
                object_to_upload.to_parquet(gcs_uri_str, **pandas_kwargs)
                return gcs_uri_str
            file_obj = io.BytesIO()
            object_to_upload.to_parquet(file_obj, **pandas_kwargs)
            file_obj.seek(0)
            return blob.upload_from_file(file_obj)

        if file_extension == "csv" and isinstance(object_to_upload, pd.DataFrame):
            if self.local:
                object_to_upload.to_csv(gcs_uri_str, header=header, index=index, **pandas_kwargs)
                return gcs_uri_str
            csv_string = object_to_upload.to_csv(encoding="utf-8", header=header, index=index, **pandas_kwargs)
            return blob.upload_from_string(csv_string)

        if file_extension == "xlsx" and isinstance(object_to_upload, pd.DataFrame):
            if self.local:
                object_to_upload.to_excel(gcs_uri_str, header=header, index=index, **pandas_kwargs)
                return gcs_uri_str
            file_obj = io.BytesIO()
            object_to_upload.to_excel(file_obj, header=header, index=index, **pandas_kwargs)
            file_obj.seek(0)
            return blob.upload_from_file(file_obj)

        if file_extension == "pkl":
            if self.local:
                Path(gcs_uri).write_bytes(pickle.dumps(object_to_upload))
                return gcs_uri_str
            file_obj = io.BytesIO(pickle.dumps(object_to_upload))
            file_obj.seek(0)
            return blob.upload_from_file(file_obj)

        if file_extension == "json":
            if self.local:
                Path(gcs_uri).write_text(json.dumps(object_to_upload))
                return gcs_uri_str
            json_string = json.dumps(object_to_upload)
            return blob.upload_from_string(json_string)

        # Default to pickle for unknown types
        if self.local:
            Path(gcs_uri).write_bytes(pickle.dumps(object_to_upload))
            return gcs_uri_str
        file_obj = io.BytesIO(pickle.dumps(object_to_upload))
        file_obj.seek(0)
        return blob.upload_from_file(file_obj)

    def upload_files_from_objects(
        self,
        gcs_uris: list[str | Path],
        objects_to_upload: list[pd.DataFrame | Any],
        default_file_type: str | None = None,
        metadata: list[dict] | dict | None = None,
        pandas_kwargs: dict | None = None,
        *,
        headers: list[bool] | bool = True,
        indices: list[bool] | bool = False,
    ) -> list[str | None]:
        """Uploads files to GCS from objects in memory.

        Args:
            gcs_uris: The URIs to which the objects will be uploaded. If
                local is True, these are paths to local files where the objects will be written.
            objects_to_upload: Objects to upload, typically pandas DataFrames.
            default_file_type: If URIs don't have file type endings,
                they will be assumed to be this type.
            metadata: Optional metadata to add to each object. Can be a single dict
                applied to all files or a list of dicts. Environment variables are
                added automatically when available.
            headers: Whether to write out column names (for CSV and Excel).
                Can be a single boolean or a list of booleans.
            indices: Whether to write the index or not (for CSV and Excel).
                Can be a single boolean or a list of booleans.
            pandas_kwargs: Additional keyword arguments to pass to pandas methods.

        Returns:
            A list of results from blob.upload() or file paths if in local mode.
        """
        # Normalize parameters to match length of gcs_uris
        metadata_list = self._normalize_parameter(metadata, len(gcs_uris))
        headers_list = self._normalize_parameter(headers, len(gcs_uris))
        indices_list = self._normalize_parameter(indices, len(gcs_uris))

        # Upload each object with its corresponding parameters
        return [
            self.upload_file_from_object(
                gcs_uri=gcs_uri,
                object_to_upload=objects_to_upload[i],
                default_file_type=default_file_type,
                metadata=metadata_list[i],
                header=headers_list[i],
                index=indices_list[i],
                pandas_kwargs=pandas_kwargs,
            )
            for i, gcs_uri in enumerate(gcs_uris)
        ]


GCS = GCSFileIO
