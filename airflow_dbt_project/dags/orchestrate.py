from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import os
import pendulum
import time

@dag(
        dag_id="orchestrate",
        schedule="0 11 * * *",
        catchup=False,
        start_date=pendulum.datetime(year=2026, month=10, day=3, tz="UTC"),
)
def orchestrate():

    @task
    def ingest_cdc():
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")
        job_id_str = os.getenv("DATABRICKS_JOB_ID")

        if not job_id_str:
            raise ValueError("Environment variable DATABRICKS_JOB_ID is required to trigger CDC ingestion.")

        try:
            job_id = int(job_id_str)
        except ValueError:
            raise ValueError(f"DATABRICKS_JOB_ID must be a numeric integer, got: {job_id_str}")

        ws = WorkspaceClient(
            host=host,
            token=token,
        )
        jobs_trigger = ws.jobs.run_now(job_id=job_id)

        while True:
            job_run = ws.jobs.get_run(jobs_trigger.run_id)
            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully.")
                    break
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")
            time.sleep(5)

        return "CDC ingestion completed successfully."

    @task.bash
    def source_freshness():
        return "cd /opt/airflow/walmart_db && dbt source freshness"

    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd="/opt/airflow/walmart_db",
        bash_command="dbt run --select silver_tech"
    )

    silver_technical_test = BashOperator(
        task_id="silver_technical_test",
        cwd="/opt/airflow/walmart_db",
        bash_command="dbt test --select silver_tech"
    )

    silver_business = BashOperator(
        task_id="silver_business",
        cwd="/opt/airflow/walmart_db",
        bash_command="dbt run --select silver_b"
    )

    silver_business_test = BashOperator(
        task_id="silver_business_test",
        cwd="/opt/airflow/walmart_db",
        bash_command="dbt test --select silver_b"
    )

    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        cwd="/opt/airflow/walmart_db",
        bash_command="dbt run --select gold/ephemeral"
    )

    gold_dimensional = BashOperator(
        task_id="gold_dimensional",
        cwd="/opt/airflow/walmart_db",
        bash_command="dbt snapshot" 
    )

    gold_fact = BashOperator(
        task_id="gold_fact",
        cwd="/opt/airflow/walmart_db",
        bash_command="dbt run --select gold/fact"
    )


    ingest_cdc() >> source_freshness() >> silver_technical >> silver_technical_test >> silver_business >> silver_business_test >> gold_ephemeral >> gold_dimensional >> gold_fact

orchestrate_dag = orchestrate()