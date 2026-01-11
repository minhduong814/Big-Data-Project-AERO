"""
Main entry point for AERO Data Pipeline
Orchestrates the complete Extract-Load-Transform-Visualize workflow
"""
import logging
import argparse
import sys
import os
from typing import Optional

# Add src to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extract.kafka_producer import AeroDataProducer, FlightDataExtractor
from src.load.kafka_consumer import AeroDataLoader
from src.transform.spark_streaming import AeroSparkProcessor
from src.visualize.dashboard import FlightDashboard
from src.visualize.looker_connector import LookerConnector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_extraction(config: dict) -> None:
    """
    Run data extraction phase
    
    Args:
        config: Configuration dictionary
    """
    logger.info("Starting data extraction...")
    
    kafka_config = config.get('kafka', {})
    bootstrap_servers = kafka_config.get('bootstrap_servers', 'localhost:9092')
    topic = kafka_config.get('topics', {}).get('raw', 'flights-raw')
    
    producer = AeroDataProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        config_path='config/pipeline_config.yaml'
    )
    
    extractor = FlightDataExtractor(producer)
    
    try:
        # Example: Extract from test file
        test_file = 'data/test.json'
        if os.path.exists(test_file):
            extractor.extract_from_file(test_file)
            logger.info("Extraction completed successfully")
        else:
            logger.warning(f"Test file {test_file} not found")
    finally:
        producer.close()


def run_loading(config: dict, max_messages: Optional[int] = None) -> None:
    """
    Run data loading phase
    
    Args:
        config: Configuration dictionary
        max_messages: Maximum messages to process
    """
    logger.info("Starting data loading...")
    
    kafka_config = config.get('kafka', {})
    gcp_config = config.get('gcp', {})
    pipeline_config = config.get('pipeline', {})
    
    loader = AeroDataLoader(
        bootstrap_servers=kafka_config.get('bootstrap_servers', 'localhost:9092'),
        topic=kafka_config.get('topics', {}).get('processed', 'flights-processed'),
        group_id='aero-main-consumer',
        project_id=gcp_config.get('project_id'),
        dataset_id=gcp_config.get('dataset_id'),
        table_id=gcp_config.get('table_id'),
        config_path='config/pipeline_config.yaml'
    )
    
    try:
        batch_size = pipeline_config.get('batch_size', 100)
        count = loader.consume_and_load(
            batch_size=batch_size,
            max_messages=max_messages
        )
        logger.info(f"Loading completed: {count} messages processed")
    finally:
        loader.close()


def run_transformation(config: dict) -> None:
    """
    Run data transformation phase
    
    Args:
        config: Configuration dictionary
    """
    logger.info("Starting data transformation...")
    
    kafka_config = config.get('kafka', {})
    topics = kafka_config.get('topics', {})
    
    processor = AeroSparkProcessor(
        app_name='AERO-Main-Transform',
        config_path='config/pipeline_config.yaml'
    )
    
    try:
        processor.process_stream(
            kafka_bootstrap_servers=kafka_config.get('bootstrap_servers', 'localhost:9092'),
            input_topic=topics.get('raw', 'flights-raw'),
            output_topic=topics.get('processed', 'flights-processed')
        )
    finally:
        processor.stop()


def run_visualization(config: dict) -> None:
    """
    Run data visualization phase
    
    Args:
        config: Configuration dictionary
    """
    logger.info("Starting data visualization...")
    
    gcp_config = config.get('gcp', {})
    
    # Create Looker views
    looker = LookerConnector(
        project_id=gcp_config.get('project_id'),
        dataset_id=gcp_config.get('dataset_id')
    )
    looker.create_standard_views()
    
    # Generate dashboards
    dashboard = FlightDashboard(
        project_id=gcp_config.get('project_id'),
        dataset_id=gcp_config.get('dataset_id')
    )
    summary = dashboard.create_full_dashboard(output_dir='dashboards')
    
    logger.info(f"Visualization completed. Summary: {summary}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='AERO Data Pipeline')
    parser.add_argument(
        '--mode',
        choices=['extract', 'load', 'transform', 'visualize', 'all'],
        default='all',
        help='Pipeline mode to run'
    )
    parser.add_argument(
        '--config',
        default='config/pipeline_config.yaml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--max-messages',
        type=int,
        help='Maximum messages to process (load mode only)'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    import yaml
    try:
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {args.config}")
        sys.exit(1)
    
    try:
        if args.mode == 'extract' or args.mode == 'all':
            run_extraction(config)
        
        if args.mode == 'transform' or args.mode == 'all':
            run_transformation(config)
        
        if args.mode == 'load' or args.mode == 'all':
            run_loading(config, args.max_messages)
        
        if args.mode == 'visualize' or args.mode == 'all':
            run_visualization(config)
        
        logger.info("Pipeline execution completed successfully")
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
