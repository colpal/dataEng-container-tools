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
import string
import random
import logging

from datetime import datetime
from google.cloud import bigquery as GBQ
from google.cloud.bigquery.enums import SourceFormat
from google.cloud.bigquery.job import (
    QueryJob, QueryJobConfig,
    ExtractJob, ExtractJobConfig,
    LoadJob, LoadJobConfig,
    CopyJob, CopyJobConfig,
    WriteDisposition
)
from google.cloud.bigquery.table import TableReference
from google.cloud.bigquery.dataset import DatasetReference
from google.cloud.bigquery.schema import SchemaField


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

    def __init__(self, bq_secret_location,custom_config_args=None):
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

        logging.basicConfig()
        self.logger = logging.getLogger("BQ")
        self.logger.setLevel(logging.INFO)

        self.custom_config_args = custom_config_args if custom_config_args else {}

    def __create_job_id(self, project_id, job_type):
        chars = string.ascii_letters + string.digits
        random_string = ''.join(random.choice(chars) for i in range(10))
        return f'{project_id}-{job_type}-{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}-{random_string}'
    

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
        bq_job_results = bq_job.result()

        job_result = {
            "start_time": None if not bq_job.started else bq_job.started.ctime(),
            "end_time": None if not bq_job.ended else bq_job.ended.ctime(),
            "job_errors": None if hasattr(bq_job, "errors") else bq_job.errors,
            "total_bytes_billed": None if not hasattr(bq_job, "total_bytes_billed") else bq_job.total_bytes_billed,
            "total_bytes_processed": None if not hasattr(bq_job, "total_bytes_processed") else bq_job.total_bytes_processed,
            "total_rows_returned": None if not hasattr(bq_job_results, "total_rows") else bq_job_results.total_rows,
            "query_plan": None if not hasattr(bq_job, "query_plan") else bq_job.query_plan,
            "job_results": bq_job_results,
        }

        self.logger.info(job_result)

        return job_result
    

    def send_to_gcs(self, query, project_id, output_uri, delimiter=","):
        job_results = {}

        client = self.bq_client

        queryJob = QueryJob(self.__create_job_id(
            project_id, "queryJob"), query, client)
        
        job_results["queryJob"] = self.__get_results(queryJob)

        output_type = output_uri.split(".")[-1]
        dest_format = self.__get_file_type(output_type)

        extract_config_args = {
            "destination_format" : dest_format
        }
        
        extract_config_args.update(self.custom_config_args.get("extract_config_args", {}))

        if dest_format == SourceFormat.CSV:
            extract_config_args["field_delimiter"] = delimiter

        config = ExtractJobConfig(**extract_config_args)

        extractJob = ExtractJob(
                        self.__create_job_id(project_id, "extractJob"),
                        queryJob.destination, output_uri, client, 
                        job_config=config
                    )
        
        job_results["extractJob"] = self.__get_results(extractJob)

        return job_results
    

    def load_from_gcs(self, table_id, input_uri):
        job_results = {}

        client = self.bq_client

        project_id, ds_id, table_name = table_id.split(".")
        dataset = DatasetReference(project_id, ds_id)
        output_table = TableReference(dataset, table_name)

        file_extenetion = input_uri.split(".")[-1]
        file_type = self.__get_file_type(file_extenetion)

        load_config_args = {
            "autodetect" : True, 
            "write_disposition" : WriteDisposition.WRITE_APPEND, 
            "source_format" : file_type
        }

        if passed_schema := self.custom_config_args.get("schema"):
            new_schema = []
            for field in passed_schema:
                new_schema.append(SchemaField(field["name"],field["type"],mode=field["mode"]))

            load_config_args.update({"schema": new_schema})

        load_config_args.update(self.custom_config_args.get("load_config_args", {}))

        config = LoadJobConfig(**load_config_args)
        
        loadJob = LoadJob(self.__create_job_id(project_id, "loadJob"), 
                    input_uri, output_table, client, job_config=config
                )

        job_results["loadJob"] = self.__get_results(loadJob)

        return job_results
    
    # TODO finish function
    def copy_tables(self, destination_table, source_tables):
        job_results = {}

        # copyJob = CopyJob()

        # job_results["copyJob"] = self.__get_results(copyJob)

        # return job_results
