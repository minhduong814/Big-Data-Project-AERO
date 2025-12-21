from prefect import flow, task, get_run_logger
from prefect_shell import shell_run_command
import asyncio

@task
async def upload_spark_script():
    logger = get_run_logger()
    logger.info("Uploading Spark script to GCS...")
    # Upload the SPARK script, not this Prefect file!
    command = """
    gsutil cp ./flow/clean_data.py gs://aero_data/scripts/flow/
    """
    result = await shell_run_command(command, return_all=True)
    logger.info(f"Upload result: {result}")

@task
async def create_dataproc_cluster():
    logger = get_run_logger()
    logger.info("Creating Dataproc cluster...")
    command = """
    gcloud dataproc clusters create flight-clean-data \
        --region asia-east2 \
        --zone asia-east2-a \
        --master-machine-type n2-standard-2 \
        --master-boot-disk-size 130GB \
        --num-workers 2 \
        --worker-machine-type n2-standard-2 \
        --worker-boot-disk-size 130GB \
        --project double-arbor-475907-s5
    """
    result = await shell_run_command(command, return_all=True)
    logger.info(f"Cluster created: {result}")

@task
async def submit_spark_cleaning_job():
    logger = get_run_logger()
    logger.info("Submitting PySpark job...")
    # Submit the SPARK script (not the Prefect workflow)
    command = """
    gcloud dataproc jobs submit pyspark gs://aero_data/scripts/flow/clean_data.py \
        --cluster=flight-clean-data \
        --region=asia-east2 \
        --project=double-arbor-475907-s5
    """
    result = await shell_run_command(command, return_all=True)
    logger.info(f"Job output: {result}")

@task
async def delete_dataproc_cluster():
    logger = get_run_logger()
    logger.info("Deleting Dataproc cluster...")
    command = """
    gcloud dataproc clusters delete flight-clean-data \
        --region=asia-east2 \
        --quiet \
        --project=double-arbor-475907-s5
    """
    result = await shell_run_command(command, return_all=True)
    logger.info(f"Cluster deleted: {result}")

@flow
async def clean_flight_data_workflow():
    logger = get_run_logger()
    logger.info("Starting Flight Data Cleaning Workflow")
    
    try:
        await upload_spark_script()
        await create_dataproc_cluster()
        await submit_spark_cleaning_job()
        logger.info("Workflow completed successfully!")
    except Exception as e:
        logger.error(f"Workflow failed: {e}")
        raise
    finally:
        try:
            await delete_dataproc_cluster()
        except Exception as cleanup_error:
            logger.warning(f"Cluster deletion failed: {cleanup_error}")

if __name__ == "__main__":
    asyncio.run(clean_flight_data_workflow())