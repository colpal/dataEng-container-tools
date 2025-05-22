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
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast, overload

from dataeng_container_tools.modules import BaseModule, BaseModuleUtilities

if TYPE_CHECKING:
    from collections.abc import Iterator

    import pandas as pd
    from google.cloud import storage
    from google.cloud.storage.blob import Blob


class GCSUriUtils:
    """Utility class for handling GCS URIs.

    Provides static methods to resolve and parse GCS URIs.
    """

    PREFIX: Final = "gs://"

    @staticmethod
    def resolve_uri(gcs_uri: str) -> str:
        """Resolves a GCS URI to its absolute path.

        Removes the "gs://" prefix, resolves the path, and re-adds the prefix.

        Args:
            gcs_uri (str): The GCS URI string to resolve.

        Returns:
            str: The resolved GCS URI string.
        """
        gcs_uri = gcs_uri.removeprefix(GCSUriUtils.PREFIX)
        return GCSUriUtils.PREFIX + Path(gcs_uri.lstrip(GCSUriUtils.PREFIX)).resolve().as_posix()

    @staticmethod
    def get_components(gcs_uri: str) -> tuple[str, str]:
        """Extracts the bucket name and file path from a GCS URI.

        Args:
            gcs_uri (str): The GCS URI string.

        Returns:
            tuple[str, str]: A tuple containing the bucket name and the file path.
        """
        gcs_uri = gcs_uri.removeprefix("gs://")
        bucket = gcs_uri[: gcs_uri.find("/")]
        file_path = gcs_uri[gcs_uri.find("/") + 1 :]
        return bucket, file_path


class GCSFileIO(BaseModule):
    """Uploads and downloads files to/from Google Cloud Storage (GCS).

    This class handles the boilerplate code for interacting with GCS,
    allowing for downloading files to objects or local files, and uploading
    objects or local files to GCS. It also includes helper functions for
    common GCS operations.

    Attributes:
        client (storage.Client): The Google Cloud Storage client instance.
        local (bool): A boolean indicating if the module is in local-only mode.
               If True, no actual GCS operations are performed.
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
        """Initializes GCSFileIO with desired configuration.

        Args:
            gcs_secret_location (str | Path | None): Path to the GCS service account JSON key file.
            local (bool): If True, operates in local mode without GCS interaction. Should be used
                with a GCS local emulator.
            use_cla_fallback (bool): If True, attempts to use command-line arguments
                as a fallback for secret location if `gcs_secret_location` is not found.
            use_file_fallback (bool): If True, attempts to use the default secret file path
                as a fallback if other sources fail.

        Raises:
            FileNotFoundError: If GCS credentials are not found and not in local mode.
        """
        from google.cloud import storage

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
            from google.auth.credentials import AnonymousCredentials

            self.client: storage.Client = storage.Client(credentials=AnonymousCredentials())

    def uri_to_blobs(self, gcs_uri: str) -> Iterator[Blob]:
        """Converts a GCS URI to an iterator of Blob objects.

        Supports glob patterns in the GCS URI for matching multiple files.
        See `https://cloud.google.com/storage/docs/json_api/v1/objects/list#list-objects-and-prefixes-using-glob`
        for more information on glob matching.

        Args:
            gcs_uri (str): The GCS URI, which can include glob patterns.

        Returns:
            Iterator[Blob]: An iterator yielding `google.cloud.storage.blob.Blob` objects
            matching the URI.
        """
        bucket_name, file_path = GCSUriUtils.get_components(gcs_uri)
        bucket = self.client.bucket(bucket_name)
        return bucket.list_blobs(match_glob=file_path)

    @overload
    def download(
        self,
        *,
        gcs_uris: str | list[str],
        local_files: str | list[str] | Path | list[Path],
    ) -> None: ...

    @overload
    def download(
        self,
        *,
        gcs_uris: str | list[str],
        dtype: dict | None = None,
        **kwargs: Any,  # Use ParamSpec in future  # noqa: ANN401
    ) -> dict[str, Any]: ... # TODO: Returning type dict[str, pd.DataFrame | io.BytesIO] might be too ambiguous for user

    def download(
        self,
        *,
        gcs_uris: str | list[str],
        **kwargs: Any,  # Use ParamSpec in future
    ) -> ...:
        """Downloads files from GCS to local file paths or Python objects.

        This method dispatches to `download_to_file` if `local_files` is provided in `**kwargs`,
        or to `download_to_object` otherwise.

        When downloading to files:
        - The number of GCS URIs must match the number of local file paths.

        When downloading to objects:
        - Supports various file types like Parquet, CSV, XLSX, and JSON.
        - If the file extension is not recognized, it returns an `io.BytesIO` object.
        - For CSV files, keyword arguments like `header`, `delimiter`, `encoding` can be passed via `**kwargs`.
        - For XLSX files, keyword arguments like `header` can be passed via `**kwargs`.

        Args:
            gcs_uris (str | list[str]): A single GCS URI or a list of GCS URIs to download.
            local_files (str | list[str] | Path | list[Path], optional):
                A single local file path or a list of local file paths. If provided (via `**kwargs`),
                files are downloaded to these paths. Defaults to None.
            dtype (dict | None, optional): Optional dictionary specifying data types for columns,
                primarily for Pandas DataFrames when downloading to objects (e.g.,
                when reading CSV or Parquet). If provided (via `**kwargs`). Defaults to None.
            **kwargs (Any): Additional keyword arguments. These are passed to the underlying
                file reading functions (e.g., `pd.read_parquet`, `pd.read_csv`)
                when downloading to objects.

        Returns:
            None | dict[str, pd.DataFrame | io.BytesIO] | pd.DataFrame | io.BytesIO:
            - `None` if `local_files` is provided (i.e., downloading to file).
            - If downloading to objects:
                - A dictionary mapping blob names to downloaded objects if multiple
                  URIs result in multiple objects.
                - The type of object depends on the file extension.

        Raises:
            ValueError: If downloading to file and the number of `gcs_uris` and
                `local_files` do not match (raised by `download_to_file`).
            Other exceptions may be raised by GCS client or Pandas during file operations.
        """
        if "local_files" in kwargs:
            return self.download_to_file(gcs_uris, kwargs["local_files"])
        return self.download_to_object(gcs_uris, **kwargs)

    def download_to_file(
        self,
        gcs_uris: str | list[str],
        local_files: str | list[str] | Path | list[Path],
    ) -> None:
        """Downloads files from GCS to local file paths.

        The number of GCS URIs must match the number of local file paths.

        Args:
            gcs_uris (str | list[str]): A single GCS URI or a list of GCS URIs to download.
            local_files (str | list[str] | Path | list[Path]): A single local file path (str or Path)
                or a list of local file paths where the files will be downloaded.

        Raises:
            ValueError: If the number of `gcs_uris` and `local_files` do not match.
        """
        if not isinstance(gcs_uris, list):
            gcs_uris = [gcs_uris]
        local_files = (
            [str(local_files)] if isinstance(local_files, (str, Path)) else [str(f) for f in local_files]
        )

        if len(gcs_uris) != len(local_files):
            msg = f"'gcs_uris' ({len(gcs_uris)}) and 'local_files' ({len(local_files)}) must be of equal length"
            raise ValueError(msg)

        for gcs_uri, local_file_path in zip(gcs_uris, local_files):
            bucket_name, file_path = GCSUriUtils.get_components(str(gcs_uri))
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(file_path)
            if blob.exists():
                blob.download_to_filename(str(local_file_path))
            else:
                msg = f"Blob {file_path} does not exist in bucket {bucket_name}"
                raise FileNotFoundError(msg)

    def download_to_object(
        self,
        gcs_uris: str | list[str],
        dtype: dict | None = None,
        **kwargs: Any,  # Use ParamSpec in future  # noqa: ANN401
    ) -> dict[str, pd.DataFrame | io.BytesIO]:
        """Downloads file(s) from GCS into Python objects.

        Supports various file types like Parquet, CSV, XLSX, and JSON.
        If the file extension is not recognized, it returns an `io.BytesIO` object.

        For CSV files, keyword arguments like `header`, `delimiter`, `encoding` can be passed.
        For XLSX files, keyword arguments like `header` can be passed.

        Args:
            gcs_uris (str | list[str]): A single GCS URI or a list of GCS URIs to download.
            dtype (dict | None): Optional dictionary specifying data types for columns, primarily for
                Pandas DataFrames (e.g., when reading CSV or Parquet).
            **kwargs (Any): Additional keyword arguments passed to the underlying file reading
                functions (e.g., `pd.read_parquet`, `pd.read_csv`).

        Returns:
            dict[pd.DataFrame | io.BytesIO]: A dictionary mapping blob names to the downloaded objects.
                The type of object depends on the file extension.
        """
        import pandas as pd

        if not isinstance(gcs_uris, list):
            gcs_uris = [gcs_uris]

        data_dict = {}
        for blob in (blob for uri in gcs_uris for blob in self.uri_to_blobs(uri)):
            data = io.BytesIO(blob.download_as_bytes())

            file_name = cast("str", blob.name)
            file_extension = next((ext.lstrip(".") for ext in self.KNOWN_EXTENSIONS if file_name.endswith(ext)), None)

            if file_extension == "parquet":
                parquet_obj = pd.read_parquet(data, **kwargs)
                if dtype:
                    parquet_obj = parquet_obj.astype(dtype)

            elif file_extension == "csv":
                kwargs.setdefault("encoding", "utf-8")
                file_obj = pd.read_csv(data, dtype=dtype, **kwargs) if dtype else pd.read_csv(data, **kwargs)

            elif file_extension == "xlsx":
                kwargs.setdefault("engine", "openpyxl")
                file_obj = pd.read_excel(data, dtype=dtype, **kwargs) if dtype else pd.read_excel(data, **kwargs)

            elif file_extension == "json":
                file_obj = pd.read_json(data, lines=True, **kwargs)

            else:
                file_obj = data

            # If no recognized format, return the file object itself
            data_dict[blob.name] = file_obj

        return data_dict

    @overload
    def upload(
        self,
        *,
        gcs_uris: str | list[str],
        files: str | list[str] | Path | list[Path],
        metadata: dict | None = None,
        **kwargs: Any,  # Use ParamSpec in future  # noqa: ANN401
    ) -> None: ...

    @overload
    def upload(
        self,
        *,
        gcs_uris: str | list[str],
        objects_to_upload: object | list[object],
        metadata: dict | None = None,
        **kwargs: Any,  # Use ParamSpec in future  # noqa: ANN401
    ) -> None: ...

    def upload(
        self,
        *,
        gcs_uris: str | list[str],
        metadata: dict | None = None,
        **kwargs: Any,  # Use ParamSpec in future
    ) -> None:
        """Uploads local files or in-memory Python objects to GCS.

        This method serves as a dispatcher for uploading either files from the
        local filesystem or Python objects directly to GCS. You must provide
        either `files` or `objects_to_upload`, but not both.

        The number of `gcs_uris` must match the number of `files` or
        `objects_to_upload`.

        Metadata can be provided for the uploaded objects. Environment variables
        like `DAG_ID`, `RUN_ID`, `NAMESPACE`, `POD_NAME`, `GITHUB_SHA` are
        automatically added to the metadata if present.

        For object uploads, the method attempts to infer the file type from the
        GCS URI's extension (e.g., .parquet, .csv, .xlsx, .json) and uses
        appropriate serialization methods (e.g., `to_parquet` for Pandas DataFrames).

        Args:
            gcs_uris (str | list[str]): A single GCS URI or a list of GCS URIs where the
                files/objects will be uploaded.
            files (str | list[str] | Path | list[Path] | None): A single file path (str or Path)
                or a list of file paths to upload from the local filesystem.
            objects_to_upload (object | list[object] | None): A single Python object or a list of Python
                objects to upload. Supported object types depend on the
                file extension of the `gcs_uri` (e.g., `pd.DataFrame` for
                .parquet, .csv, .xlsx; `str` for .json).
            metadata (dict | None): Optional dictionary of metadata to associate with the
                uploaded GCS object(s).
            **kwargs (Any): Additional keyword arguments passed to the underlying
                upload or serialization functions (e.g., `pd.DataFrame.to_parquet`,
                `pd.DataFrame.to_csv`).

        Raises:
            ValueError: If both `files` and `objects_to_upload` are provided,
                or if neither is provided.
            ValueError: If the number of `gcs_uris` does not match the number
                of `files` or `objects_to_upload`.
            ValueError: If uploading an object and no compatible file extension
                is found in the `gcs_uri`.
        """
        if "files" in kwargs and "objects_to_upload" in kwargs:
            msg = "Invalid GCS upload args, only have one of files or objects_to_upload"
            raise ValueError(msg)

        if "files" in kwargs:
            self.upload_file(gcs_uris=gcs_uris, metadata=metadata, **kwargs)
        elif "objects_to_upload" in kwargs:
            self.upload_object(gcs_uris=gcs_uris, metadata=metadata, **kwargs)
        else:
            msg = "Invalid GCS upload args"
            raise ValueError(msg)

    def upload_file(
        self,
        gcs_uris: str | list[str],
        files: str | list[str] | Path | list[Path],
        metadata: dict | None = None,
    ) -> None:
        """Uploads local file(s) to GCS.

        The number of GCS URIs must match the number of files.
        Metadata can be provided, and common environment variables are
        automatically included.

        Args:
            gcs_uris (str | list[str]): A single GCS URI or a list of GCS URIs for the destination.
            files (str | list[str] | Path | list[Path]): A single file path (str or Path) or a list of file paths
                from the local filesystem.
            metadata (dict | None): Optional dictionary of metadata for the GCS object(s).

        Raises:
            ValueError: If the number of `gcs_uris` and `files` do not match.
        """
        metadata = metadata or {}

        # Add environment variables to metadata
        env_vars = ["DAG_ID", "RUN_ID", "NAMESPACE", "POD_NAME", "GITHUB_SHA"]
        for var in env_vars:
            if var in os.environ:
                metadata.setdefault(var, os.environ[var])

        if not isinstance(gcs_uris, list):
            gcs_uris = [gcs_uris]
        files = [str(files)] if isinstance(files, (str, Path)) else [str(f) for f in files]

        if len(gcs_uris) != len(files):
            msg = f"'gcs_uris' ({len(gcs_uris)}) and 'files' ({len(files)}) must be of equal length"
            raise ValueError(msg)

        for gcs_uri, file in zip(gcs_uris, files):
            bucket_name, file_path = GCSUriUtils.get_components(str(gcs_uri))
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(file_path)
            blob.metadata = metadata
            blob.upload_from_filename(file)

    def upload_object(
        self,
        gcs_uris: str | list[str],
        objects_to_upload: object | list[object],
        metadata: dict | None = None,
        **kwargs: Any,  # Use ParamSpec in future  # noqa: ANN401
    ) -> None:
        """Uploads Python object(s) to GCS.

        The method attempts to serialize objects based on the GCS URI's file
        extension (e.g., .parquet, .csv, .xlsx, .json).
        The number of GCS URIs must match the number of objects.
        Metadata can be provided, and common environment variables are
        automatically included.

        Args:
            gcs_uris (str | list[str]): A single GCS URI or a list of GCS URIs for the destination.
            objects_to_upload (object | list[object]): A single Python object or a list of Python objects.
                Supported types include `pd.DataFrame` (for .parquet, .csv, .xlsx)
                and `str` (for .json).
            metadata (dict | None): Optional dictionary of metadata for the GCS object(s).
            **kwargs (Any): Additional keyword arguments passed to the serialization
                functions (e.g., `to_parquet`, `to_csv`).

        Raises:
            ValueError: If the number of `gcs_uris` and `objects_to_upload`
                do not match.
            ValueError: If no compatible file extension is found in the `gcs_uri`
                for serializing the object.
        """
        import pandas as pd

        metadata = metadata or {}

        # Add environment variables to metadata
        env_vars = ["DAG_ID", "RUN_ID", "NAMESPACE", "POD_NAME", "GITHUB_SHA"]
        for var in env_vars:
            if var in os.environ:
                metadata.setdefault(var, os.environ[var])

        if not isinstance(gcs_uris, list):
            gcs_uris = [gcs_uris]
        if not isinstance(objects_to_upload, list):
            objects_to_upload = [objects_to_upload]

        if len(gcs_uris) != len(objects_to_upload):
            msg = (
                f"'gcs_uris' ({len(gcs_uris)}) and 'objects_to_upload' "
                f"({len(objects_to_upload)}) must be of equal length"
            )
            raise ValueError(msg)

        for gcs_uri, object_to_upload in zip(gcs_uris, objects_to_upload):
            bucket_name, file_path = GCSUriUtils.get_components(str(gcs_uri))
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(file_path)
            blob.metadata = metadata

            # Determine file type
            file_extension = next((ext.lstrip(".") for ext in self.KNOWN_EXTENSIONS if file_path.endswith(ext)), None)

            # Handle based on file type
            if file_extension == "parquet" and isinstance(object_to_upload, pd.DataFrame):
                file_obj = io.BytesIO()
                object_to_upload.to_parquet(file_obj, **kwargs)
                file_obj.seek(0)
                blob.upload_from_file(file_obj)

            elif file_extension == "csv" and isinstance(object_to_upload, pd.DataFrame):
                kwargs.setdefault("encoding", "utf-8")
                csv_string = object_to_upload.to_csv(**kwargs)
                blob.upload_from_string(csv_string)

            elif file_extension == "xlsx" and isinstance(object_to_upload, pd.DataFrame):
                file_obj = io.BytesIO()
                object_to_upload.to_excel(file_obj, **kwargs)
                file_obj.seek(0)
                blob.upload_from_file(file_obj)

            elif file_extension == "json" and isinstance(object_to_upload, str):
                json_string = json.dumps(object_to_upload)
                blob.upload_from_string(json_string)

            else:
                msg = (
                    f"No compatible file extension '{file_extension}' found for object type "
                    f"'{type(object_to_upload)!s}'."
                )
                raise ValueError(msg)
