import pandas as pd
import os 
from prefect import task, get_run_logger
from dotenv import load_dotenv

# Global 2 decimal settings in pandas
pd.set_option("display.float_format", '{:.2f}'.format)

# Load environment variables
load_dotenv()

@task(name="Transform the data")
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforming the NYC Taxi data and save it as a csv file
    """

    logger = get_run_logger()
    logger.info("Starting the data transformation...")

    transformed_df = df.copy()

    # Renaming columns
    transformed_df.rename(columns={
        'VendorID': 'vendor_id',
        'tpep_pickup_datetime': 'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime',
        'trip_distance': 'trip_distance_miles',
        'RatecodeID': 'rate_code_id',
        'store_and_fwd_flag': 'store_and_forward_flag',
        'PULocationID': 'pickup_location_id',
        'DOLocationID': 'dropoff_location_id'
    }, inplace=True)


    # Dropping the unneccessary columns in our dataset
    transformed_df.drop('airport_fee', axis=1, errors="ignore", inplace=True)

    # Dropping the null values in our dataset
    transformed_df.drop(transformed_df.loc[
        (transformed_df['passenger_count'].isnull()) &
        (transformed_df['rate_code_id'].isnull()) &
        (transformed_df['congestion_surcharge'].isnull())
    ].index, inplace=True)

    # Negative values were found in the dataset that needed to be removed
    transformed_df.drop(transformed_df[
        transformed_df['fare_amount'] <= 0
    ].index, inplace=True)

    # Let's create a new column

    transformed_df['trip_duration_minutes'] = (
        (transformed_df['dropoff_datetime'] - transformed_df['pickup_datetime'])
        .dt.total_seconds() / 60
    )

    # After creating a new column we need to drop any zero or negative values
    transformed_df.drop(
        transformed_df[transformed_df['trip_duration_minutes'] <= 0].index, inplace=True
    )

    # After more investigations we can drop any outliers in our dataset
    transformed_df.drop(transformed_df[
        (transformed_df['total_amount'] >= 500) & \
        (transformed_df['trip_distance_miles'] < 50)
    ].index, inplace=True)

    # Adding another duration column in the format of HH:MM:SS
    duration_components = (transformed_df['dropoff_datetime'] - transformed_df['pickup_datetime']).dt.components[['hours', 'minutes', 'seconds']]
    transformed_df['trip_duration_HHMMSS'] = duration_components.apply(
        lambda row: f"{row['hours']:02d}:{row['minutes']:02d}:{row['seconds']:02d}", axis=1
    )

    # Changing the data types of some columns

    transformed_df['rate_code_id'] = transformed_df['rate_code_id'].astype(int)

    transformed_df['passenger_count'] = transformed_df['passenger_count'].astype(int)

    # Mapping categorical values from the data dictionary

    transformed_df['vendor_id'] = transformed_df['vendor_id'].replace({
        1: 'Creative Mobile Technologies, LLC',
        2: 'Curb Mobility, LLC'
    })

    transformed_df['rate_code_id'] = transformed_df['rate_code_id'].replace({
        1: 'Standard rate',
        2: 'JFK',
        3: 'Newark',
        4: 'Nassau or Westchester',
        5: 'Negotiated fare',
        6: 'Group ride'
    })

    transformed_df['store_and_forward_flag'] = transformed_df['store_and_forward_flag'].replace({
        'N': 'No',
        'Y': 'Yes'
    })

    transformed_df['payment_type'] = transformed_df['payment_type'].replace({
        1: 'Credit card',
        2: 'Cash',
        3: 'No charge',
        4: 'Dispute'
    })

    # ---- Save the transformed data ----
    output_dir = os.getenv("TRANSFORMED_PATH")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "transformed_taxi_data_2021_01.csv")

    transformed_df.to_csv(output_path, index=False)
    logger.info(f"Transformed data saved to {output_path}")
    logger.info(f"Transformed complete - {len(transformed_df)} rows processed")

    return transformed_df