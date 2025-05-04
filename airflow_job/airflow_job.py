from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from datetime import datetime, timedelta
from airflow.models import Variable
import uuid

default_args={
    'owner':'airflow',
    'depends_on_past':False,
    # 'email_on_failure':False,
    # 'email_on_retry':False,
    'retries':0,
    #'retry_delay':timedelta(minutes=5),
    'start_date':datetime(2025,5,4),
}

with DAG(
    dag_id="Flight_Booking_CICD",
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
) as dag:
    
    #Fetching env variables
    env=Variable.get("env",default_var="dev")
    gcs_bucket= Variable.get("gcs_bucket",default_var="spark_ex_airflow")
    bq_project=Variable.get("bq_project",default_var="fit-legacy-454720-g4")
    bq_dataset=Variable.get("bq_dataset",default_var=f"flight_data_{env}")
    tables= Variable.get("tables", deserialize_json=True)


    transformed_table=tables["transformed_table"]
    route_insights_table=tables["route_insights_table"]
    orgin_insights_table=tables["orgin_insights_table"]

    batch_id= f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"

    file_sensor=GCSObjectExistenceSensor(
        task_id="Check_file_arrival",
        bucket=gcs_bucket,
        object=f"flight_cicd/source-{env}/flight_booking.csv",
        google_cloud_conn_id="google_cloud_default",
        timeout=300,
        poke_interval=30,
        mode="poke",
    )

    batch_details={
        "pyspark_batch":{
            "main_python_file_uri": f"gs://{gcs_bucket}/flight_cicd/spark_job/spark_job.py",
            #"python_file_uri":["gs://spark-lib/bigquery/spark-bigquery-latest.jar"],
            "jar_files_uris":["gs://spark-lib/bigquery/spark-3.3-bigquery-0.35.0.jar"],
            "args":[
                f"--env={env}",
                f"--bq_project={bq_project}",
                f"--bq_dataset={bq_dataset}",
                f"--transformed_table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={orgin_insights_table}"
            ]
        },
        "runtime_config":{
            "version":"2.2",
        },
        "environment_config":{
            "execution_conifg":{
                "service_account":"863324801108-compute@developer.gserviceaccount.com",
                "network_uri":"projects/fit-legacy-454720-g4/global/networks/default",
                "subnetwork_uri":"projects/fit-legacy-454720-g4/regions/us-central1/subnetworks/default",
            }
        },
    }

    pyspark_job=DataprocCreateBatchOperator(
        task_id="run_spark_job_on_batches_on_dataproc_serverless",
        batch=batch_details,
        batch_id=batch_id,
        project_id="fit-legacy-454720-g4",
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )

    file_sensor>>pyspark_job
