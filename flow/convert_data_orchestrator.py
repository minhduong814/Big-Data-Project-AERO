from prefect import flow, task, get_run_logger
from prefect_shell import shell_run_command
import asyncio

@task
async def upload_conversion_script():
    logger = get_run_logger()
    logger.info("Uploading CSV to Parquet conversion script to GCS...")
    command = """
    gsutil cp /home/quangminh/Documents/code/Python/Big-Data-Project-AERO/scripts/create_parquet.py gs://aero_data/scripts/
    """
    result = await shell_run_command(command, return_all=True)
    logger.info(f"Upload completed. Verifying file exists...")
    
    # Verify the upload
    verify_command = "gsutil ls -l gs://aero_data/scripts/create_parquet.py"
    verify_result = await shell_run_command(verify_command, return_all=True)
    logger.info(f"Verification result: {verify_result}")

@task
async def create_converter_cluster():
    logger = get_run_logger()
    logger.info("Creating Dataproc cluster for parquet conversion...")
    command = """
    gcloud dataproc clusters create parquet-converter \
        --region=asia-east2 \
        --zone=asia-east2-a \
        --master-machine-type=n2-standard-2 \
        --master-boot-disk-size=50GB \
        --num-workers=2 \
        --worker-machine-type=n2-standard-2 \
        --worker-boot-disk-size=50GB \
        --project=double-arbor-475907-s5
    """
    result = await shell_run_command(command, return_all=True)
    logger.info(f"Cluster creation completed successfully")
    
    # Verify cluster exists
    verify_command = """
    gcloud dataproc clusters describe parquet-converter \
        --region=asia-east2 \
        --project=double-arbor-475907-s5 \
        --format="value(status.state)"
    """
    verify_result = await shell_run_command(verify_command, return_all=True)
    logger.info(f"Cluster status: {verify_result}")

@task
async def submit_conversion_job():
    logger = get_run_logger()
    logger.info("Submitting CSV to Parquet conversion job...")
    command = """
    gcloud dataproc jobs submit pyspark flow/convert_csv_to_parquet.py \
        --cluster=parquet-converter \
        --region=asia-east2 \
        --project=double-arbor-475907-s5
    """
    result = await shell_run_command(command, return_all=True)
    logger.info(f"Job submitted successfully")

@task
async def verify_parquet_created():
    logger = get_run_logger()
    logger.info("Verifying parquet file was created...")
    command = "gsutil ls -lh gs://aero_data/parquet/"
    result = await shell_run_command(command, return_all=True)
    logger.info(f"Parquet files created: {result}")

@task
async def delete_converter_cluster():
    logger = get_run_logger()
    logger.info("Deleting conversion cluster...")
    command = """
    gcloud dataproc clusters delete parquet-converter \
        --region=asia-east2 \
        --quiet \
        --project=double-arbor-475907-s5
    """
    result = await shell_run_command(command, return_all=True)
    logger.info(f"Cluster deleted successfully")

@flow
async def convert_csv_to_parquet_workflow():
    logger = get_run_logger()
    logger.info("=" * 50)
    logger.info("Starting CSV to Parquet Conversion Workflow")
    logger.info("=" * 50)
    
    try:
        # await upload_conversion_script()
        await create_converter_cluster()
        await submit_conversion_job()
        await verify_parquet_created()
        logger.info("=" * 50)
        logger.info("Conversion workflow completed successfully!")
        logger.info("=" * 50)
    except Exception as e:
        logger.error(f"Conversion workflow failed: {e}")
        raise
    finally:
        try:
            await delete_converter_cluster()
        except Exception as cleanup_error:
            logger.warning(f"Cluster deletion failed: {cleanup_error}")

if __name__ == "__main__":
    asyncio.run(convert_csv_to_parquet_workflow())