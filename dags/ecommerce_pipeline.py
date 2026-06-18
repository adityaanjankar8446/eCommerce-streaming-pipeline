from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False
}

SPARK_HOME   = "/home/airflow/.local/lib/python3.8/site-packages/pyspark"
SPARK_SUBMIT = f"{SPARK_HOME}/bin/spark-submit"
DELTA_PACKAGES = "io.delta:delta-spark_2.12:3.1.0"
DELTA_CONF = (
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
)

task_env = {
    "JAVA_HOME":             "/usr/lib/jvm/java-11-openjdk-amd64",
    "SPARK_HOME":            SPARK_HOME,
    "PATH":                  "/usr/lib/jvm/java-11-openjdk-amd64/bin:/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
    "PYSPARK_PYTHON":        "python3",
    "PYSPARK_DRIVER_PYTHON": "python3",
    "PYTHONPATH":            "/home/airflow/.local/lib/python3.8/site-packages"  # ← add this
}

with DAG(
    dag_id="ecommerce_streaming_pipeline_v6",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Kolkata"),
    schedule="0 * * * *",
    catchup=False,
    tags=["ecommerce", "streaming", "delta"]
) as dag:

    api_ingest = BashOperator(
        task_id="api_ingest",
        bash_command="python3 /opt/scripts/api_ingest.py",
        env=task_env
    )

    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"--packages {DELTA_PACKAGES} "
            f"{DELTA_CONF} "
            f"/opt/scripts/01_bronze.py"
        ),
        env=task_env
    )

    silver = BashOperator(
        task_id="silver_transformation",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"--packages {DELTA_PACKAGES} "
            f"{DELTA_CONF} "
            f"/opt/scripts/02_silver.py"
        ),
        env=task_env
    )

    gold = BashOperator(
        task_id="gold_aggregations",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"--packages {DELTA_PACKAGES} "
            f"{DELTA_CONF} "
            f"/opt/scripts/03_gold.py"
        ),
        env=task_env
    )

    api_ingest >> bronze >> silver >> gold