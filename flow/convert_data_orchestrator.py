from prefect import flow, task, get_run_logger
from prefect_shell import shell_run_command
import asyncio


@task
async def create_converter_cluster():
    logger = get_run_logger()
    logger.info("Creating Dataproc cluster for parquet conversion...")
    command = """
    gcloud dataproc clusters create parquet-converter \
        --region=asia-east2 \
        --zone=asia-east2-a \
        --master-machine-type=n2-standard-4 \
        --master-boot-disk-size=50GB \
        --num-workers=2 \
        --worker-machine-type=n2-standard-4 \
        --worker-boot-disk-size=50GB \
        --properties=spark:spark.executor.memory=4g,spark:spark.driver.memory=4g,spark:spark.executor.memoryOverhead=1g,spark:spark.sql.shuffle.partitions=200 \
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
        --project=double-arbor-475907-s5 \
        --format="value(reference.jobId)"
    """
    result = await shell_run_command(command, return_all=True)
    # result is a list: [stdout, stderr, return_code]
    job_id = result[0].strip() if result[0] else ""
    logger.info(f"Job submitted successfully. Job ID: {job_id}")
    
    # Wait for job and stream logs
    logger.info("Waiting for job to complete and streaming logs...")
    wait_command = f"""
    gcloud dataproc jobs wait {job_id} \
        --region=asia-east2 \
        --project=double-arbor-475907-s5
    """
    wait_result = await shell_run_command(wait_command, return_all=True)
    logger.info(f"Job output:\n{wait_result[0]}")
    
    if wait_result[1]:
        logger.warning(f"Job stderr:\n{wait_result[1]}")
    
    return job_id

@task
async def submit_cleaning_job():
    logger = get_run_logger()
    logger.info("Submitting data cleaning job...")
    command = """
    gcloud dataproc jobs submit pyspark flow/clean_data.py \
        --cluster=parquet-converter \
        --region=asia-east2 \
        --project=double-arbor-475907-s5 \
        --format="value(reference.jobId)"
    """
    result = await shell_run_command(command, return_all=True)
    # result is a list: [stdout, stderr, return_code]
    job_id = result[0].strip() if result[0] else ""
    logger.info(f"Cleaning job submitted successfully. Job ID: {job_id}")
    
    # Wait for job and stream logs
    logger.info("Waiting for cleaning job to complete and streaming logs...")
    wait_command = f"""
    gcloud dataproc jobs wait {job_id} \
        --region=asia-east2 \
        --project=double-arbor-475907-s5
    """
    wait_result = await shell_run_command(wait_command, return_all=True)
    logger.info(f"Cleaning job output:\n{wait_result[0]}")
    
    if wait_result[1]:
        logger.warning(f"Cleaning job stderr:\n{wait_result[1]}")
    
    return job_id

@task
async def verify_parquet_created():
    logger = get_run_logger()
    logger.info("Verifying parquet file was created...")
    command = "gsutil ls -lh gs://aero_data/parquet/"
    result = await shell_run_command(command, return_all=True)
    logger.info(f"Parquet files created:\n{result[0]}")

@task
async def verify_cleaned_data():
    logger = get_run_logger()
    logger.info("Verifying cleaned data was created...")
    command = "gsutil ls -lh gs://aero_data/cleaned_data/"
    result = await shell_run_command(command, return_all=True)
    logger.info(f"Cleaned data files:\n{result[0]}")

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
        await create_converter_cluster()
        conversion_job_id = await submit_conversion_job()
        await verify_parquet_created()
        
        # Run cleaning job on the converted data
        cleaning_job_id = await submit_cleaning_job()
        await verify_cleaned_data()
        
        logger.info("=" * 50)
        logger.info(f"Workflow completed successfully!")
        logger.info(f"Conversion Job ID: {conversion_job_id}")
        logger.info(f"Cleaning Job ID: {cleaning_job_id}")
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