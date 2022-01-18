"""Tools for working with GCP.

Deals with receiving, editing, downloading and uploading tables from/to GCP. Has one
class: `BQ`.

Typical usage example:

    bq = BQ(bq_secret_location = secret_locations[0])
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
from dataEng_container_tools.db import get_secrets
from google.cloud import bigquery as GBQ
from google.cloud.bigquery.job import QueryJob, QueryJobConfig
from google.cloud.bigquery.job import ExtractJob, ExtractJobConfig
from google.cloud.bigquery.job import LoadJob, LoadJobConfig
from google.cloud.bigquery.job import DestinationFormat
from google.cloud.bigquery.job import WriteDisposition



from dataEng_container_tools.exceptions import StorageCredentialNotFound


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
        with open(bq_secret_location, 'r') as f:
            gcs_sa = json.load(f)
        with open('gcs-sa.json', 'w') as json_file:
            json.dump(gcs_sa, json_file)
        self.bq_client = GBQ.Client.from_service_account_json(
            'bq-sa.json')
        
    def __create_job_id(self,project_id,job_type):
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

    def send_to_gcs(self,query,project_id,output_uri,delimiter = ","):
        job_results = {}
        
        client = self.bq_client
        
        queryJob = QueryJob(self.__create_job_id(project_id,"queryJob"), query, client)
        queryJob_results = queryJob.result()
        
        job_results["queryJob"] = {
            "start_time" : queryJob.started.ctime(),
            "end_time" : queryJob.ended.ctime(),
            "query_errors" : queryJob.errors,
            "total_bytes_billed" : queryJob.total_bytes_billed,
            "total_bytes_processed" : queryJob.total_bytes_processed,
            "query_plan" : queryJob.query_plan,
            "total_rows_returned" : queryJob_results.total_rows
        }
        
        filename = self.__get_parts(output_uri)[-1]

        output_type = filename.split(".")[-1]

        if output_type == "avro":
            dest_format = DestinationFormat().AVRO    
        elif output_type == "parquet":
            dest_format = DestinationFormat().PARQUET
        elif output_type == "json":
            dest_format = DestinationFormat().NEWLINE_DELIMITED_JSON
        else:
            dest_format = DestinationFormat().CSV
        
        if dest_format == DestinationFormat().CSV:
            config = ExtractJobConfig(destination_format = dest_format, field_delimiter = delimiter)
        else:
            config = ExtractJobConfig(destination_format = dest_format)

        extractJob = ExtractJob(self.__create_job_id(project_id,"extractJob"),
            queryJob_results.destination, output_uri, client, job_config=config
        )
            
        extractJob_results = extractJob.result()
        
        job_results["extractJob"] = {
            "start_time" : extractJob.started.ctime(),
            "end_time" : extractJob.ended.ctime(),
            "query_errors" : extractJob.errors,
            "total_bytes_billed" : extractJob.total_bytes_billed,
            "total_bytes_processed" : extractJob.total_bytes_processed,
            "query_plan" : extractJob.query_plan,
            "total_rows_returned" : extractJob_results.total_rows
        }
        
        return job_results
        
    def load_from_gcs(self,):
        pass