import json
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests


class RestApiJsonToGCSOperator(BaseOperator):
    """
    Operator to download data from a REST API that returns JSON and store it in Google Cloud Storage.
    """

    def __init__(self, 
                 rest_api_endpoint: str,
                 gcs_bucket: str,
                 gcs_destination_path: str,
                 gcp_conn_id: str = "google_cloud_default",
                 *args, **kwargs):
        """
        Initializes the operator.

        :param rest_api_endpoint: The REST API endpoint to download data from.
        :type rest_api_endpoint: str
        :param gcs_bucket: The name of the Google Cloud Storage bucket to store the data in.
        :type gcs_bucket: str
        :param gcs_destination_path: The destination path within the bucket to store the data at.
        :type gcs_destination_path: str
        :param gcp_conn_id: The Airflow connection ID to use for Google Cloud Storage.
        :type gcp_conn_id: str
        """
        super().__init__(*args, **kwargs)
        self.rest_api_endpoint = rest_api_endpoint
        self.gcs_bucket = gcs_bucket
        self.gcs_destination_path = gcs_destination_path
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        """
        Downloads data from the REST API and stores it in Google Cloud Storage.
        """
        try:
            response = requests.get(self.rest_api_endpoint)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.log.error(f"Error fetching data from {self.rest_api_endpoint}: {e}")
            raise

        try:
            data = json.loads(response.text)
        except ValueError as e:
            self.log.error(f"Error parsing JSON from {self.rest_api_endpoint}: {e}")
            raise

        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        try:
            gcs_hook.upload(
                bucket_name=self.gcs_bucket,
                object_name=self.gcs_destination_path,
                data=json.dumps(data),
                mime_type='application/json'
            )
        except Exception as e:
            self.log.error(f"Error writing data to gs://{self.gcs_bucket}/{self.gcs_destination_path}: {e}")
            raise

        self.log.info(f"Stored data from {self.rest_api_endpoint} to gs://{self.gcs_bucket}/{self.gcs_destination_path}")
