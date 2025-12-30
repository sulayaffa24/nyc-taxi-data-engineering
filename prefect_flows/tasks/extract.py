import pandas as pd
import os 
from dotenv import load_dotenv
from prefect import task, get_run_logger

# Configure pandas display
pd.set_option("display.float_format", "{:.2f}".format)

# Load environment
load_dotenv()

@task(name="Extracting Data....")
def extract(filename: str = "yellow_tripdata_2021-01.parquet") -> pd.DataFrame:
    """
    Extracts NYC taxi data from a local parquet file and returns a DataFrame.

    Parameters
    ----------

    filename: str, optional
        Name of the Parquet file to load from the raw data directory.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the extracted taxi data.
    """
    logger = get_run_logger()

    data_path = os.getenv("DATA_PATH")
    file_path = os.path.join(data_path, filename)

    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"Data file not found at {file_path}")
    
    logger.info(f"Extracting data from {file_path}")
    df = pd.read_parquet(file_path, engine="fastparquet")

    logger.info(f"Successfully extracted {len(df)} records")
    return df