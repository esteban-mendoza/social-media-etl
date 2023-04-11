# -*- coding: utf-8 -*-

"""Airflow DAG that downloads users from 
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
import uuid
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
users_endpoint = f"{api_url}/users"

# Bucket for storing raw data
raw_bucket = models.Variable.get("gcs_raw_bucket")

# Postgres table names
users_table = models.Variable.get("users_table")
addresses_table = models.Variable.get("addresses_table")
companies_table = models.Variable.get("companies_table")

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
    raw_json = gcs_hook.download(bucket_name=raw_bucket, object_name="users.json")

    # Transform the raw data into a list of dictionaries
    json_data = json.loads(raw_json)
    return json_data


def _transform_json(json_data):
    """
    This function takes a list of dictionaries as input,
    and returns 3 lists of dictionaries – one for users,
    one for companies and one for addresses –
    """

    users = []
    companies = []
    addresses = []

    for row in json_data:
        company = row.pop("company")
        address = row.pop("address")

        company_uuid = uuid.uuid4()
        company["uuid"] = company_uuid
        companies.append(company)

        address_uuid = uuid.uuid4()
        address["uuid"] = address_uuid
        address = flatten_dict(address)
        address["geo_lat"] = float(address["geo_lat"])
        address["geo_lng"] = float(address["geo_lng"])
        addresses.append(address)

        row["company_uuid"] = company_uuid
        row["address_uuid"] = address_uuid
        users.append(row)

    return users, companies, addresses


def _transform_and_load():
    """
    This function reads the JSON users file from GCS,
    transforms it into three separate lists of dictionaries
    – one for users, one for companies and one for addresses –
    and then loads each of them into their respective tables in Postgres.
    """
    try:
        # Download the JSON file from GCS
        json_data = _download_json_from_gcs()

        # Transform the JSON file into three separate lists of dictionaries
        users, companies, addresses = _transform_json(json_data)

        # Get a connection to Postgres
        pg_hook = PostgresHook(postgres_conn_id="postgres_social_media")
        conn = pg_hook.get_conn()

        # Load the users, companies and addresses into Postgres
        cursor = conn.cursor()

        cursor.executemany(
            f"""INSERT INTO {addresses_table} ({", ".join(addresses[0].keys())}) VALUES ({", ".join(["%s"] * len(addresses[0]))})""",
            [tuple(address.values()) for address in addresses],
        )

        cursor.executemany(
            f"""INSERT INTO {companies_table} ({", ".join(companies[0].keys())}) VALUES ({", ".join(["%s"] * len(companies[0]))})""",
            [tuple(company.values()) for company in companies],
        )

        cursor.executemany(
            f"""INSERT INTO {users_table} ({", ".join(users[0].keys())}) VALUES ({", ".join(["%s"] * len(users[0]))})""",
            [tuple(user.values()) for user in users],
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
    "users_etl",
    default_args=default_args,
    schedule_interval=None,
) as dag:
    start = dummy_operator.DummyOperator(task_id="start")

    end = dummy_operator.DummyOperator(task_id="end")

    api_is_available = HttpSensor(
        task_id="api_is_available",
        http_conn_id="jsonplaceholder_users_api",
        endpoint="",
        response_check=lambda response: response.status_code == 200,
    )

    ingest_users_to_gcs = RestApiJsonToGCSOperator(
        task_id="ingest_users_to_gcs",
        rest_api_endpoint=users_endpoint,
        gcs_bucket=raw_bucket,
        gcs_destination_path="users.json",
    )

    empty_users_table = PostgresOperator(
        task_id="empty_users_table",
        postgres_conn_id="postgres_social_media",
        sql=f"DELETE FROM {users_table}",
    )

    empty_addresses_table = PostgresOperator(
        task_id="empty_addresses_table",
        postgres_conn_id="postgres_social_media",
        sql=f"DELETE FROM {addresses_table}",
    )

    empty_companies_table = PostgresOperator(
        task_id="empty_companies_table",
        postgres_conn_id="postgres_social_media",
        sql=f"DELETE FROM {companies_table}",
    )

    transform_and_load = PythonOperator(
        task_id="transform_and_load", python_callable=_transform_and_load
    )

    (
        start
        >> api_is_available
        >> ingest_users_to_gcs
        >> [empty_users_table, empty_addresses_table, empty_companies_table]
        >> transform_and_load
        >> end
    )
