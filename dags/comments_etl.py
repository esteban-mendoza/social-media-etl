# -*- coding: utf-8 -*-

"""Airflow DAG that downloads comments from 
the example API https://jsonplaceholder.typicode.com/,
uploads it to Google Cloud Storage, 
and then loads it to a Postgres database hosted in Google Cloud Platform.

Notice that the DAG is not scheduled to run.
"""

# --------------------------------------------------------------------------------
# Load The Dependencies
# --------------------------------------------------------------------------------

import datetime
import logging
import json

from airflow import DAG, models
from airflow.operators import dummy_operator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from operators.rest_api_json_to_gcs_operator import RestApiJsonToGCSOperator
from utils.utils import flatten_dict

# --------------------------------------------------------------------------------
# Set default arguments
# --------------------------------------------------------------------------------

default_args = {
    "owner": "Jorge Mendoza",
    "depends_on_past": False,
    "start_date": datetime.datetime.today(),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

# --------------------------------------------------------------------------------
# Set logging
# --------------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------------
# Set variables
# --------------------------------------------------------------------------------

# API URL to fetch data from
api_url = models.Variable.get("api_url")
comments_endpoint = f"{api_url}/comments"

# Bucket for storing raw data
raw_bucket = models.Variable.get("gcs_raw_bucket")

# Postgres table names
comments_table = models.Variable.get("comments_table")

# --------------------------------------------------------------------------------
# Custom functions
# --------------------------------------------------------------------------------


def _download_json_from_gcs():
    """
    This function downloads the JSON file from GCS,
    and returns it as a list of dictionaries.
    """

    # Instantiate a GCSHook
    gcs_hook = GCSHook()

    # Get the raw data from GCS
    raw_json = gcs_hook.download(bucket_name=raw_bucket, object_name="comments.json")

    # Transform the raw data into a list of dictionaries
    json_data = json.loads(raw_json)
    return json_data


def _transform_json(json_data):
    """
    This function takes a list of dictionaries as input,
    and returns a processed list of dictionaries for the
    comments data
    """

    for comment in json_data:
        comment["post_id"] = comment.pop("postId")

    return json_data


def _transform_and_load():
    """
    This function reads the JSON comments file from GCS,
    transforms a list of dictionaries
    and then loads it into its respective table in Postgres.
    """
    try:
        # Download the JSON file from GCS
        json_data = _download_json_from_gcs()

        # Transform the JSON data
        comments = _transform_json(json_data)

        # Get a connection to Postgres
        pg_hook = PostgresHook(postgres_conn_id="postgres_social_media")
        conn = pg_hook.get_conn()

        # Load the comments into Postgres
        cursor = conn.cursor()

        cursor.executemany(
            f"""INSERT INTO {comments_table} ({", ".join(comments[0].keys())}) VALUES ({", ".join(["%s"] * len(comments[0]))})""",
            [tuple(comment.values()) for comment in comments],
        )

        conn.commit()
        cursor.close()
        conn.close()

        logging.info("Data successfully loaded into Postgres tables.")
    except Exception as e:
        logging.error("Error in _transform_and_load function: " + str(e))
        raise


# --------------------------------------------------------------------------------
# Define the DAG
# --------------------------------------------------------------------------------

with DAG(
    "comments_etl",
    default_args=default_args,
    schedule_interval=None,
) as dag:
    start = dummy_operator.DummyOperator(task_id="start")

    end = dummy_operator.DummyOperator(task_id="end")

    api_is_available = HttpSensor(
        task_id="api_is_available",
        http_conn_id="jsonplaceholder_comments_api",
        endpoint="",
        response_check=lambda response: response.status_code == 200,
    )

    ingest_comments_to_gcs = RestApiJsonToGCSOperator(
        task_id="ingest_comments_to_gcs",
        rest_api_endpoint=comments_endpoint,
        gcs_bucket=raw_bucket,
        gcs_destination_path="comments.json",
    )

    empty_table = PostgresOperator(
        task_id="empty_table",
        postgres_conn_id="postgres_social_media",
        sql=f"DELETE FROM {comments_table}",
    )

    transform_and_load = PythonOperator(
        task_id="transform_and_load", python_callable=_transform_and_load
    )

    (
        start
        >> api_is_available
        >> ingest_comments_to_gcs
        >> empty_table
        >> transform_and_load
        >> end
    )
