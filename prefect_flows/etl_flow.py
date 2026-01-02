from prefect import flow, get_run_logger

# Import your task modules
from prefect_flows.tasks.extract import extract
from prefect_flows.tasks.transform import transform
from prefect_flows.tasks.load import load

@flow(name="NY Taxi ETL Flow")
def ny_taxi_etl_flow(
    filename: str="yellow_tripdata_2021-01.parquet",
    table_name: str="yellow_taxi_data_2021_01"
):
    """
    Orchestrates the Extract -> Transform -> Load pipeline
    for NYC Taxi data using Prefect
    """
    logger = get_run_logger()
    logger.info(f"Starting the ETL flow for file: {filename}")

    # ---- Extract ----
    df_raw = extract(filename)
    logger.info(f"Extracted {len(df_raw)} records")

    # ---- Transform ----
    df_transformed = transform(df_raw)
    logger.infor(f"Transformed {len(df_transformed)} records")

    # ---- Load ---- 
    load(df_transformed, table_name=table_name)
    logger.info(f"Data successfully loaded into table {table_name}")

    logger.info("ETL flow completed successfully!!!")


if __name__ == "__main__":
    ny_taxi_etl_flow(
        filename="yellow_tripdata_2021-01.parquet",
        table_name="yellow_taxi_data_2021_01"
    )
