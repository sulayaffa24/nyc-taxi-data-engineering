import pandas as pd
import os
from prefect import task, get_run_logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables
load_dotenv()

@task(name="Get Postgres Engine")
def get_engine():
    """
    Creates a SQLAlchemy engine for the Dockerized Postgres database,
    Automatically reads the credentials from the environment variables
    """
    logger = get_run_logger()

    PG_USER = os.getenv("POSTGRES_USER")
    PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    PG_HOST = os.getenv("POSTGRES_HOST")
    PG_PORT = os.getenv("POSTGRES_PORT")
    PG_DB = os.getenv("POSTGRES_DB")

    if not all([PG_USER, PG_PASSWORD, PG_DB]):
        logger.error("Database credentials are missing. Please check your .env file.")
        raise ValueError("Missing database credentials")
    
    connection_string = (
        f"postgres+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )

    logger.info(f"Creating SQLAlchemy engine for {PG_DB} on {PG_HOST}:{PG_PORT}")
    engine = create_engine(connection_string)
    return engine

@task(name="Creating NYC Taxi Schema for the Database")
def create_table_if_not_exists(engine, schema_sql_path: str):

    logger = get_run_logger()

    if not os.path.exists(schema_sql_path):
        logger.error("Schema file path not found")
        raise FileNotFoundError(f"Schema file not found: {schema_sql_path}")
    
    with open(schema_sql_path, "r") as f:
        create_table_sql = f.read()

    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()

    logger.info(f"Database schema created successfully!")

@task(name="Load data to Postgres", retries=3, retry_delay_seconds=10)
def load(dataframe: pd.DataFrame, table_name: str, if_exists: str="replace"):
    """
    Loads a pandas DataFrame into a PostgreSQL database table.
    Works well with Dockerized PostgreSQL
    """
    logger = get_run_logger()
    engine = get_engine.fn()

    try:
        # Ensure table schema exists
        schema_file = f"sql/create_{table_name}_schema.sql"
        default_schema_file = "sql/create_yellow_taxi_schema.sql"

        if os.path.exists(schema_file):
            logger.info(f"Found table-specific schema: {schema_file}")
            create_table_if_not_exists(engine, schema_file)
        elif os.path.exists(default_schema_file):
            logger.info("using default yellow taxi schema...")
            create_table_if_not_exists(engine, default_schema_file)
        else:
            logger.warning("No Schema file found - loading without explicit schema creation")

        # Load the data
        logger.info(f"Starting to load data into the table: {table_name}")
        dataframe.to_sql(table_name, engine, index=False, if_exists=if_exists)
        logger.info(f"Successfully loaded {len(dataframe)} rows into '{table_name}'.")

    except SQLAlchemyError as e:
        logger.error(f"Failed to load data into Postgres: {e}")
        raise
    finally:
        engine.dispose()
        logger.info("Database connection closed!")


