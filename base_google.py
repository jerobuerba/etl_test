from google.oauth2 import service_account, credentials
from google.cloud import bigquery, client
from google.cloud.bigquery.job import LoadJobConfig, LoadJob
from google.cloud.bigquery.client import Table
import pandas as pd
import os
#from dotenv import dotenv_values
from google.cloud import storage
from datetime import datetime

GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("sa_cred")

class GoogleJobs:
    def __init__(self):
        try:
            self.google_credentials:  credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
        except Exception as e:
            print(f"Error getting GCloud credentials with SA, exception is {str(e)}")
            self.google_credentials = None
    def get_gcs_client(self) -> client:
        gcs_client = storage.Client(project=self.google_credentials.project_id,
                                                credentials=self.google_credentials)
        return gcs_client
    
    def retrieve_gcs_file(self,file_name, bucket_name = 'etl_pipeline_test'):
        gcs = self.get_gcs_client()
        bucket = gcs.get_bucket(bucket_name)
        blob = bucket.get_blob(file_name)
        #downloaded_file = blob.download_as_text(encoding="utf-8")
        gcs.close()
        return blob
    
    def move_gcs_file(self,file_name, dest_folder,bucket_name = 'etl_pipeline_test'):
        gcs = self.get_gcs_client()
        bucket = gcs.get_bucket(bucket_name)
        blob = bucket.get_blob(file_name)
        date = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        dest_file_name = f"{dest_folder}{date}_{file_name.split('/')[-1]}"
        blob_copy = bucket.copy_blob(blob, bucket, dest_file_name)
        generation_match_precondition = blob.generation

        blob.delete(if_generation_match=generation_match_precondition)
        gcs.close()
        return "File moved"

    def get_bq_client(self) -> client:
        """
        CALL: self.get_bigquery_client()
        DESCRIPTION: This method constructs a BigQuery client object.
        RESULT: client
        """
        try:
            # Construct a BigQuery client object.
            bq_client: client = bigquery.Client(project=self.google_credentials.project_id,
                                                credentials=self.google_credentials)

            return bq_client
        except Exception as err:
            print(f"Error getting BigQuery client object, exception is {str(err)}")
            raise
        
        
    def write_data_to_bq(self, df: pd, schema:list, destination_table, mode_insert: str = "WRITE_APPEND") -> None:
        """
        CALL: self write_data_to_bq(self, df: pd, schema: list, mode_insert: str = "WRITE_TRUNCATE")
        DESCRIPTION: This method executes a query using a BigQuery client object.
        Args:
            df: pandas dataframe.
            schema: specify a schema, is used to assist in data type definitions.
            mode_insert: set the write disposition, with WRITE_TRUNCATE write disposition it replaces the table
            with the loaded data.
        RESULT: None
        """
        try:
            job_config: LoadJobConfig = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=mode_insert
            )
            bq_client = self.get_bq_client()
            #bq_client: client = self.get_bigquery_client()

            dest_table = f"{self.google_credentials.project_id}.{destination_table}"
            job: LoadJob = bq_client.load_table_from_dataframe(dataframe= df,
                                                               destination= dest_table,
                                                               job_config= job_config)
            # Wait for the job to complete and then show the job results
            job.result()

            # Read back the properties of the table
            table: Table = bq_client.get_table(table= dest_table)
            print("Job result - Table", table.project, table.dataset_id, table.table_id, "has", table.num_rows, "rows and", len(table.schema), "columns")
            bq_client.close()
        except Exception as err:
            print(f"Error sending dataframe to BQ, exception is {str(err)}")
            raise

    def read_data_from_bq(self, query) -> list:
        """
        CALL: self read_data_from_bq(self, query: str = "")
        DESCRIPTION: This method executes a query using a BigQuery client object and expects a df as return.
        Args:
            query: string sql
        RESULT: pd
        """
        try:
            bq_client = self.get_bq_client()
            result = bq_client.query(query)
            bq_client.close()
        except Exception as err:
            print(f"""
                Error running query, the error is: {err}
                  """)
            raise
        return [dict(row) for row in result]