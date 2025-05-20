GCS Operations
==============

The Google Cloud Storage (GCS) module provides tools for interacting with Google Cloud Storage, allowing you to upload and download files in various formats.

Basic Usage
-----------

Here's a basic example of using the ``GCSFileIO`` class:

.. code-block:: python

    from dataeng_container_tools import GCSFileIO
    import pandas as pd

    # Initialize the GCS client with your credentials
    gcs = GCSFileIO(gcs_secret_location="/path/to/gcs-credentials.json")

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

The GCS module supports various file formats:

.. code-block:: python

    from dataeng_container_tools import GCSFileIO
    import pandas as pd

    gcs = GCSFileIO(gcs_secret_location="/path/to/gcs-credentials.json")

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

You can process multiple files at once:

.. code-block:: python

    from dataeng_container_tools import GCSFileIO
    
    gcs = GCSFileIO(gcs_secret_location="/path/to/gcs-credentials.json")
    
    # Download multiple files
    files = gcs.download_files_to_objects([
        "gs://my-bucket/file1.csv",
        "gs://my-bucket/file2.csv",
        "gs://my-bucket/file3.csv"
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
        headers=True,
        indices=False,
    )

Working with Local Files
------------------------

You can use the GCS module to work with local files:

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
