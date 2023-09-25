"""
Retrieve data for the Spreaker podcasts and upload to Big Query table.
"""

from getStatistics import connect
from google.cloud import bigquery
from google.oauth2 import service_account
import argparse
import datetime
import json

# TODO: add consolidation date parameter to function to avoid non-standard data


def main():
    """
    Description
    -----------
    Retrieve relevant statistics from Spreaker podcasts in a certain date
    range and upload to Big Query table.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Obtain latest podcast statistics and upload to BigQuery. Uses "
            "JSON authentication. Date is strict, meaning if "
            "`date = 2020-01-01`, it will upload data for podcasts from day 02 "
            "onwards, not from 01."
        )
    )

    # Define arguments
    parser.add_argument(
        "-date",
        default="2020-01-01",
        help=(
            "Date to start the upload at. From that date onwards, upload ALL "
            "podcasts for the specified show. Should be a date before the "
            "first podcast was uploaded to update all podcast data."
        ),
    )
    parser.add_argument(
        "-show",
        required=True,
        help="Show ID for the podcast.",
    )
    parser.add_argument(
        "-credentials",
        default="./credentials.json",
        help="Local credentials file for BQ, defaults to credentials.json",
    )
    parser.add_argument(
        "-auth",
        default="./auth.json",
        help="Local credentials file for Spreaker, defaults to auth.json",
    )
    parser.add_argument(
        "-project",
        required=True,
        help="BQ project name.",
    )
    parser.add_argument(
        "-dataset",
        required=True,
        help="BQ dataset.",
    )
    parser.add_argument(
        "-table",
        default="spreaker_english",
        required=True,
        help=(
            "BQ table, required. For English podcast, use spreaker_english; "
            "for Spanish one, use spreaker_spanish."
        ),
    )

    # Parse the input args
    args = parser.parse_args()

    # Init required arguments
    selected_date = args.date
    credentials_file = service_account.Credentials.from_service_account_file(
        args.credentials
    )
    project_id = args.project
    dataset = args.dataset
    table = args.table
    show_ID = args.show

    with open(args.auth) as f:
        SECRET = json.load(f)["TOKEN"]

    # From the selected date onwards, select all podcasts
    new_podcasts = connect.podcast_list(show_id=show_ID, date=selected_date)

    print(
        (
            f"Found {new_podcasts.__len__()} new podcasts; current datetime "
            f"is {datetime.datetime.now()}. Now uploading..."
        )
    )

    # Add all data to DataFrame
    aggregated_data = connect.parse_to_df(
        json_list=[
            connect.statistics(episode, auth_token=SECRET) for episode in new_podcasts
        ]
    )

    # Set schema and variables
    aggregated_data = aggregated_data.astype(
        {
            "episode_title": "str",
            "plays_count": "Int64",
            "plays_ondemand_count": "Int64",
            "plays_live_count": "Int64",
            "chapters_count": "Int64",
            "messages_count": "Int64",
            "likes_count": "Int64",
            "downloads_count": "Int64",
            "published_at": "datetime64[ns]",
            "date": "datetime64[ns]",
        }
    )

    client = bigquery.Client(credentials=credentials_file, project=project_id)

    table_id = project_id + "." + dataset + "." + table

    # Define schema for BigQuery, changing data types
    bq_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(name="episode_title", field_type="STRING"),
            bigquery.SchemaField(name="plays_count", field_type="INT64"),
            bigquery.SchemaField(name="plays_ondemand_count", field_type="INT64"),
            bigquery.SchemaField(name="plays_live_count", field_type="INT64"),
            bigquery.SchemaField(name="chapters_count", field_type="INT64"),
            bigquery.SchemaField(name="messages_count", field_type="INT64"),
            bigquery.SchemaField(name="likes_count", field_type="INT64"),
            bigquery.SchemaField(name="downloads_count", field_type="INT64"),
            bigquery.SchemaField(name="published_at", field_type="DATE"),
            bigquery.SchemaField(name="date", field_type="DATE"),
        ],
        write_disposition="WRITE_APPEND",
    )
    # Run the upload to BQ
    client.load_table_from_dataframe(aggregated_data, table_id, job_config=bq_config)

    print("Upload finished.")


if __name__ == "__main__":
    # Entrypoint for GCP function
    main()
