from prefect import flow
from prefect.deployments import Deployment 
from prefect.infrastructure import Process
from prefect_flows.etl_flow import ny_taxi_etl_flow

process_infra = Process(
    env={"PREFECT_LOGGING_LEVEL": "INFO"}
)

deployment = Deployment.build_from_flow(
    flow=ny_taxi_etl_flow,
    name="local-nyc-taxi-etl",
    version="1.0",
    work_queue_name="default",
    infrastructure=process_infra,
    tags=["local", "etl", "nyc_taxi"]
)

if __name__ == "__main__":
    deployment.apply()
    print("ETL flow successfully registered with Prefect!")