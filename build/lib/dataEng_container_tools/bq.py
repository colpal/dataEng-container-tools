"""Tools for working with GCP.

Deals with receiving, editing, downloading and uploading tables from/to GCP. Has one
class: `BQ`.

Typical usage example:

    bq = BQ(bq_secret_location = secret_locations.BQ)
    #
    # Include Job_config here
    #
    result = bq.LoadJob(args)
"""
import json
import pandas as pd
import os
import string
import random

from datetime import datetime
from google.cloud import bigquery as GBQ
from google.cloud.bigquery.job import QueryJob, QueryJobConfig
from google.cloud.bigquery.job import ExtractJob, ExtractJobConfig
from google.cloud.bigquery.job import LoadJob, LoadJobConfig
from google.cloud.bigquery.job import CopyJob, CopyJobConfig
from google.cloud.bigquery.job import WriteDisposition
from google.cloud.bigquery.enums import SourceFormat
from google.cloud.bigquery import DatasetReference, TableReference


class BQ:
    """Interacts with BigQuery.

    It will handle much of the backend boilerplate code involved with interfacing
    with big query.

    Includes helper functions for using BQ.

    Attributes:
        bq_client: The BigQuery Client
        bq_secret_location: The location of the secret file
            associated with the the BQ interaction location.
        local: A boolean flag indicating whether or not the library
            is running in local only mode and should not attempt to
            contact GCP. If True, will look for the files locally.
    """
    bq_client = None
    bq_secret_location = None
    local = None

    def __init__(self, bq_secret_location):
        """Initializes BQ with desired configuration.

            Args:
                bq_secret_location: Required. The location of the secret file
                    needed for BigQuery.
        """
        self.bq_secret_location = bq_secret_location
        with open(self.bq_secret_location, 'r') as f:
            bq_sa = json.load(f)
        with open('bq-sa.json', 'w') as json_file:
            json.dump(bq_sa, json_file)
        self.bq_client = GBQ.Client.from_service_account_json(
            'bq-sa.json')

    def __create_job_id(self, project_id, job_type):
        chars = string.ascii_letters + string.digits
        random_string = ''.join(random.choice(chars) for i in range(10))
        return f'{project_id}-{job_type}-{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}-{random_string}'

    def __get_parts(self, gcs_uri):
        if gcs_uri.startswith('gs://'):
            gcs_uri = gcs_uri[5:]

        uri_parts = gcs_uri.split("/")

        bucket = uri_parts[0]
        path = "".join(uri_parts[1:-1])
        filename = uri_parts[-1]
        return bucket,  path, filename

    def __get_file_type(self, suffix):
        if suffix == "parquet":
            file_type = SourceFormat.PARQUET
        elif suffix == "json":
            file_type = SourceFormat.NEWLINE_DELIMITED_JSON
        elif suffix == "avro":
            file_type = SourceFormat.AVRO
        else:
            file_type = SourceFormat.CSV
        return file_type

    def __get_results(self, bq_job):
        try:
            bq_job_results = bq_job.result()
        except Exception as e:
            print(e)
        finally:
            job_result = {
                "start_time": bq_job.started.ctime(),
                "end_time": bq_job.ended.ctime(),
                "job_errors": bq_job.errors,
                "total_bytes_billed": bq_job.total_bytes_billed,
                "total_bytes_processed": bq_job.total_bytes_processed,
                "total_rows_returned": bq_job_results.total_rows,
                "job_results": bq_job_results
            }
            

        try:
            job_result["query_plan"] = bq_job.query_plan
        except:
            "No query to plan"
        
        print(job_result)
        
        return job_result

    def send_to_gcs(self, query, project_id, output_uri, delimiter=","):
        job_results = {}

        client = self.bq_client

        queryJob = QueryJob(self.__create_job_id(
            project_id, "queryJob"), query, client)
        job_results["queryJob"] = self.__get_results(queryJob)

        output_type = output_uri.split(".")[-1]
        dest_format = self.__get_file_type(output_type)

        if dest_format == SourceFormat.CSV:
            config = ExtractJobConfig(
                destination_format=dest_format, field_delimiter=delimiter)
        else:
            config = ExtractJobConfig(destination_format=dest_format)

        extractJob = ExtractJob(self.__create_job_id(project_id, "extractJob"),
                                job_results["queryJob"]["job_results"].destination, output_uri, client, job_config=config
                                )

        job_results["extractJob"] = self.__get_results(extractJob)

        return job_results

    def load_from_gcs(self, table_id, input_uri):
        job_results = {}

        client = self.bq_client

        project_id, ds_id, table_name = table_id.split(".")
        dataset = DatasetReference(project_id, ds_id)
        output_table = TableReference(dataset, table_name)

        ending = input_uri.split(".")[-1]
        file_type = self.__get_file_type(ending)

        config = LoadJobConfig(
            autodetect=True, write_disposition=WriteDisposition.WRITE_APPEND, source_format=file_type)
        loadJob = LoadJob(self.__create_job_id(
            project_id, "loadJob"), input_uri, output_table, client, job_config=config)

        job_results["loadJob"] = self.__get_results(loadJob)

        return job_results

    def copy_tables(self, destination_table, source_tables):
        job_results = {}

        # copyJob = CopyJob()

        # job_results["copyJob"] = self.__get_results(copyJob)

        # return job_results
