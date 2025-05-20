GCS Operations
==============

The Google Cloud Storage (GCS) module provides tools for interacting with Google Cloud Storage, allowing you to upload and download files in various formats.

Credentials
-----------

Credentials are checked in 3 locations by default. The first of which is the positional argument ``gcs_secret_location`` which takes a ``str`` or ``Path`` to any particular file.

.. code-block:: python

    gcs = GCSFileIO(gcs_secret_location="/path/to/gcs-credentials.json")

There are two additional boolean positional parameters, ``use_cla_fallback`` and ``use_file_fallback`` both defaulted to ``True``.

If no ``gcs_secret_location`` is provided, the ``SecretLocations`` file path for ``GCS`` will be checked instead. See :ref:`command-line-secret-locations` and :doc:`secrets-handling` for more information.

Finally if ``SecretLocations`` does not provide any secret, then ``GCSFileIO.DEFAULT_SECRET_PATHS`` or ``/vault/secrets/gcp-sa-storage.json`` will be used instead.

Note that invalid files may cause exceptions, the only time the next location is checked is when the file is empty or does not exist.

Downloading and Uploading Files
-------------------------------

The ``GCSFileIO`` class can be summarized with 6 main operations:
    | ``download_file_to_object`` (single)
    | ``download_files_to_objects`` (batch)
    | ``upload_file_from_disk`` (single)
    | ``upload_files_from_disk`` (batch)
    | ``upload_file_from_object`` (single)
    | ``upload_files_from_objects`` (batch)

As their name suggests, each operation performs a download or upload using disk files or objects. They take a GCS URI which is in the general format ``gs://bucket/path/file.extension``. See :ref:`command-line-input-output` which help builds these URIs.

Here is what these operations might look like:

.. code-block:: python

    from dataeng_container_tools import GCSFileIO
    import pandas as pd

    # Initialize the GCS client with your credentials
    gcs = GCSFileIO()

    # Download a file from GCS to a DataFrame
    df = gcs.download_file_to_object("gs://my-bucket/path/to/data.csv")

    # Process the data
    df['new_column'] = df['existing_column'] * 2

    # Upload the modified DataFrame back to GCS
    gcs.upload_file_from_object(
        gcs_uri="gs://my-bucket/path/to/processed_data.csv",
        object_to_upload=df,
    )

Working with Different File Formats
-----------------------------------

The GCS module supports various file formats including ``parquet``, ``csv``, ``xlsx``, and ``json``. If an unrecognized file type is downloaded, it will be a ``BytesIO`` object (similar to ``open`` files). However, for upload the operation will fail.

By default the URI determines the file extension with ``default_file_type`` being a fallback.

.. margin::

    .. note::
        Since ``v1.0``, ``pickle``/``pkl`` has been removed from support due to security concerns.

.. code-block:: python

    from dataeng_container_tools import GCSFileIO
    import pandas as pd

    gcs = GCSFileIO()

    # Download files in different formats
    parquet_df = gcs.download_file_to_object("gs://my-bucket/data.parquet")
    csv_df = gcs.download_file_to_object("gs://my-bucket/data.csv")
    excel_df = gcs.download_file_to_object("gs://my-bucket/data.xlsx")
    json_df = gcs.download_file_to_object("gs://my-bucket/data.json")
    
    # For files without extensions, specify the format
    df = gcs.download_file_to_object(
        "gs://my-bucket/data_file", 
        default_file_type="parquet",
    )

    # Process data
    result_df = pd.concat([parquet_df, csv_df])
    
    # Upload in different formats
    gcs.upload_file_from_object(
        gcs_uri="gs://my-bucket/output.parquet",
        object_to_upload=result_df,
    )
    
    gcs.upload_file_from_object(
        gcs_uri="gs://my-bucket/output.csv",
        object_to_upload=result_df,
        header=True,
        index=False,
    )

Batch Operations
----------------

To use batch operations, a list of GCS URIs will need to be provided. Additional parameters will either need to be single (and be applied to all URIs) match the length of the list (and be applied with matching indices respectively).

.. code-block:: python

    from dataeng_container_tools import GCSFileIO

    gcs = GCSFileIO()

    # Download multiple files
    files = gcs.download_files_to_objects([
        "gs://my-bucket/file1.csv",
        "gs://my-bucket/file2.csv",
        "gs://my-bucket/file3.csv",
    ])

    # Process the files
    processed_files = []
    for df in files:
        # Perform operations on each DataFrame
        df['processed'] = True
        processed_files.append(df)

    # Upload the processed files
    gcs.upload_files_from_objects(
        gcs_uris=[
            "gs://my-bucket/processed/file1.csv",
            "gs://my-bucket/processed/file2.csv",
            "gs://my-bucket/processed/file3.csv",
        ],
        objects_to_upload=processed_files,
        headers=0,
    )

Wildcards
---------

Wildcard download is supported for ``download_file_to_object``. If the URI ends with ``*`` then all files will be downloaded and the returned object is a dict with the keys being the path of the file.

.. code-block:: python

    from dataeng_container_tools import GCSFileIO

    # Initialize the GCS client
    gcs = GCSFileIO()

    # Download all files from a directory in GCS
    # For example, if the bucket 'my-bucket' has:
    # - data/subdir/file1.csv
    # - data/subdir/file2.csv
    # - data/other/file3.txt
    # Files file1.csv and file2.csv will be downloaded
    downloaded_files = gcs.download_file_to_object("gs://my-bucket/data/subdir/*")

    # downloaded_files will be a dict:
    # {
    #   "data/subdir/file1.csv": df_file1,
    #   "data/subdir/file2.csv": df_file2,
    # }

    for file_path, df_object in downloaded_files.items():
        print(f"Processing file: {file_path}")
        print(df_object.head())

Working with Local Files
------------------------

You can use the GCS module to work with local files. Paths and locations such as ``gcs_uri`` will instead use the local file paths and no connection to GCS will be required.

.. code-block:: python

    from dataeng_container_tools import GCSFileIO

    # Initialize in local mode
    gcs = GCSFileIO(local=True)

    # Download a file to local disk
    gcs.download_file_to_disk(
        gcs_uri="/path/to/source/file.csv",
        local_location="/path/to/destination/file.csv",
    )

    # Upload a local file
    gcs.upload_file_from_disk(
        gcs_uri="/path/to/destination/file.csv",
        local_location="/path/to/source/file.csv",
    )
