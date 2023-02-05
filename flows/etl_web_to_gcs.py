from pathlib import Path 

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, retries=3)


@flow(log_prints=True)
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "green"
    year = 2020
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"


    df = fetch(dataset_url)
    # df_clean = clean(df)
    # path = write_local(df_clean, color, dataset_file)
    # write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
