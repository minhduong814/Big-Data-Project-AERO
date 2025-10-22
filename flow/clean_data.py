from prefect import flow, task
from prefect_shell import shell_run_command

@task
def create_dataproc_cluster():
    command = """
    gcloud dataproc clusters create flight-clean-data \
        --region asia-east2 \
        --master-machine-type n2-standard-2 \
        --master-boot-disk-size 130GB \
        --num-workers 2 \
        --worker-machine-type n2-standard-2 \
        --worker-boot-disk-size 130GB \
        --project double-arbor-475907-s5
    """
    shell_run_command(command)

@task
def submit_spark_cleaning_job():
    command = """
    gcloud dataproc jobs submit pyspark gs://bk9999airline/scripts/clean_data.py \
        --cluster=flight-clean-data \
        --region=asia-east2 \
        --project=double-arbor-475907-s5
    """
    shell_run_command(command)

@task
def delete_dataproc_cluster():
    command = """
    gcloud dataproc clusters delete flight-clean-data \
        --region=asia-east2 \
        --quiet \
        --project=double-arbor-475907-s5
    """
    shell_run_command(command)

@flow
def clean_flight_data_workflow():
    create_dataproc_cluster()
    submit_spark_cleaning_job()
    delete_dataproc_cluster()

if __name__ == "__main__":
    clean_flight_data_workflow()
