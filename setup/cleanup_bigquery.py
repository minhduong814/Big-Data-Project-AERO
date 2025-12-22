#!/usr/bin/env python3
"""
Cleanup BigQuery Dataset and Tables
Deletes the AERO dataset and all its tables
"""

import os
import logging
from google.cloud import bigquery
from google.api_core import exceptions

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def delete_dataset(project_id: str, dataset_id: str, delete_contents: bool = True):
    """
    Delete BigQuery dataset and all its contents
    
    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID to delete
        delete_contents: If True, delete all tables in the dataset first
    """
    client = bigquery.Client(project=project_id)
    dataset_ref = f"{project_id}.{dataset_id}"
    
    try:
        # Check if dataset exists
        dataset = client.get_dataset(dataset_ref)
        logger.info(f"Found dataset: {dataset_ref}")
        
        # List all tables in the dataset
        tables = list(client.list_tables(dataset_ref))
        if tables:
            logger.info(f"\nTables in dataset:")
            for table in tables:
                logger.info(f"  - {table.table_id}")
        
        # Ask for confirmation
        logger.info(f"\n{'='*60}")
        logger.warning(f"⚠️  WARNING: This will DELETE the dataset '{dataset_id}' and ALL its contents!")
        logger.info(f"{'='*60}")
        
        response = input("\nAre you sure you want to continue? (yes/no): ").strip().lower()
        
        if response != 'yes':
            logger.info("❌ Operation cancelled by user")
            return False
        
        # Delete the dataset
        logger.info(f"\nDeleting dataset {dataset_ref}...")
        client.delete_dataset(
            dataset_ref,
            delete_contents=delete_contents,
            not_found_ok=True
        )
        
        logger.info(f"✅ Successfully deleted dataset: {dataset_ref}")
        return True
        
    except exceptions.NotFound:
        logger.warning(f"⚠️  Dataset {dataset_ref} not found (already deleted?)")
        return False
    except Exception as e:
        logger.error(f"❌ Error deleting dataset: {e}")
        raise


def delete_table(project_id: str, dataset_id: str, table_id: str):
    """
    Delete a specific BigQuery table
    
    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_id: Table ID to delete
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    try:
        # Check if table exists
        table = client.get_table(table_ref)
        logger.info(f"Found table: {table_ref}")
        logger.info(f"  Rows: {table.num_rows:,}")
        logger.info(f"  Size: {table.num_bytes / (1024**3):.2f} GB")
        
        # Ask for confirmation
        response = input(f"\nDelete table '{table_id}'? (yes/no): ").strip().lower()
        
        if response != 'yes':
            logger.info("❌ Operation cancelled by user")
            return False
        
        # Delete the table
        logger.info(f"Deleting table {table_ref}...")
        client.delete_table(table_ref, not_found_ok=True)
        
        logger.info(f"✅ Successfully deleted table: {table_ref}")
        return True
        
    except exceptions.NotFound:
        logger.warning(f"⚠️  Table {table_ref} not found (already deleted?)")
        return False
    except Exception as e:
        logger.error(f"❌ Error deleting table: {e}")
        raise


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Delete BigQuery dataset and tables')
    parser.add_argument('--project-id', default=os.environ.get("GCP_PROJECT_ID", "double-arbor-475907-s5"),
                        help='GCP project ID')
    parser.add_argument('--dataset-id', default='aero_dataset',
                        help='BigQuery dataset ID to delete')
    parser.add_argument('--table-id', default=None,
                        help='Specific table ID to delete (if not specified, entire dataset will be deleted)')
    parser.add_argument('--force', action='store_true',
                        help='Skip confirmation prompt')
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("BigQuery Cleanup for AERO Project")
    logger.info("="*60)
    logger.info(f"Project: {args.project_id}")
    logger.info(f"Dataset: {args.dataset_id}")
    if args.table_id:
        logger.info(f"Table: {args.table_id}")
    logger.info("="*60 + "\n")
    
    if args.table_id:
        # Delete specific table
        delete_table(args.project_id, args.dataset_id, args.table_id)
    else:
        # Delete entire dataset
        if args.force:
            logger.warning("⚠️  Force mode enabled - skipping confirmation")
        delete_dataset(args.project_id, args.dataset_id, delete_contents=True)
    
    logger.info("\n✅ Cleanup completed!")


if __name__ == "__main__":
    main()
