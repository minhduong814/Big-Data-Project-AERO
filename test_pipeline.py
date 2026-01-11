#!/usr/bin/env python3
"""
Test script for AERO Data Pipeline
Tests each component of the Extract-Load-Transform-Visualize workflow
"""
import logging
import sys
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_extract():
    """Test data extraction component"""
    logger.info("Testing Extract Layer...")
    
    try:
        from src.extract.kafka_producer import AeroDataProducer
        from src.extract.data_collector import DataCollector
        
        # Test data collector
        collector = DataCollector()
        test_file = "data/test.json"
        
        if os.path.exists(test_file):
            data = collector.collect_from_json(test_file)
            assert len(data) > 0, "No data collected from test file"
            logger.info(f"✓ Data Collector: Collected {len(data)} records")
        else:
            logger.warning("Test file not found, skipping data collector test")
        
        # Test producer initialization
        producer = AeroDataProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic"
        )
        logger.info("✓ Kafka Producer: Initialized successfully")
        producer.close()
        
        logger.info("Extract Layer: PASSED")
        return True
        
    except Exception as e:
        logger.error(f"Extract Layer: FAILED - {e}")
        return False


def test_load():
    """Test data loading component"""
    logger.info("Testing Load Layer...")
    
    try:
        from src.load.data_ingestion import DataIngestion
        
        # Test data ingestion initialization
        project_id = os.environ.get("GCP_PROJECT_ID", "double-arbor-475907-s5")
        
        ingestion = DataIngestion(project_id=project_id)
        logger.info("✓ Data Ingestion: Initialized successfully")
        
        # Test local ingestion
        sample_data = [{
            "timestamp": "2024-01-10T10:00:00Z",
            "flight_id": "TEST001",
            "airline": "Test Airlines"
        }]
        
        os.makedirs("data/output", exist_ok=True)
        result = ingestion.ingest_to_local(
            sample_data,
            "data/output/test.json",
            format="json"
        )
        assert result, "Local ingestion failed"
        logger.info("✓ Local Ingestion: Working")
        
        logger.info("Load Layer: PASSED")
        return True
        
    except Exception as e:
        logger.error(f"Load Layer: FAILED - {e}")
        return False


def test_transform():
    """Test data transformation component"""
    logger.info("Testing Transform Layer...")
    
    try:
        from src.transform.spark_streaming import AeroSparkProcessor
        from src.transform.spark_batch import AeroSparkBatchProcessor
        
        # Test Spark processor initialization
        processor = AeroSparkProcessor(app_name="Test-Spark")
        logger.info("✓ Spark Streaming Processor: Initialized successfully")
        
        # Test schema definition
        schema = processor.get_flight_schema()
        assert schema is not None, "Schema is None"
        logger.info(f"✓ Schema Definition: {len(schema.fields)} fields")
        
        processor.stop()
        
        # Test batch processor
        batch_processor = AeroSparkBatchProcessor(app_name="Test-Batch")
        logger.info("✓ Spark Batch Processor: Initialized successfully")
        batch_processor.stop()
        
        logger.info("Transform Layer: PASSED")
        return True
        
    except Exception as e:
        logger.error(f"Transform Layer: FAILED - {e}")
        return False


def test_visualize():
    """Test visualization component"""
    logger.info("Testing Visualize Layer...")
    
    try:
        from src.visualize.looker_connector import LookerConnector
        
        # Test Looker connector initialization
        project_id = os.environ.get("GCP_PROJECT_ID", "double-arbor-475907-s5")
        
        connector = LookerConnector(
            project_id=project_id,
            dataset_id="aero_dataset"
        )
        logger.info("✓ Looker Connector: Initialized successfully")
        
        # Test view SQL generation
        metrics_view = connector.get_flight_metrics_view()
        assert "SELECT" in metrics_view, "Invalid SQL"
        logger.info("✓ View SQL Generation: Working")
        
        logger.info("Visualize Layer: PASSED")
        return True
        
    except Exception as e:
        logger.error(f"Visualize Layer: FAILED - {e}")
        return False


def test_config():
    """Test configuration loading"""
    logger.info("Testing Configuration...")
    
    try:
        import yaml
        
        config_path = "config/pipeline_config.yaml"
        if not os.path.exists(config_path):
            logger.error(f"Configuration file not found: {config_path}")
            return False
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Validate required sections
        required_sections = ['kafka', 'spark', 'gcp', 'pipeline']
        for section in required_sections:
            assert section in config, f"Missing section: {section}"
            logger.info(f"✓ Config Section: {section}")
        
        logger.info("Configuration: PASSED")
        return True
        
    except Exception as e:
        logger.error(f"Configuration: FAILED - {e}")
        return False


def main():
    """Run all tests"""
    logger.info("=" * 60)
    logger.info("AERO Data Pipeline - Component Tests")
    logger.info("=" * 60)
    
    results = {
        "Configuration": test_config(),
        "Extract Layer": test_extract(),
        "Load Layer": test_load(),
        "Transform Layer": test_transform(),
        "Visualize Layer": test_visualize()
    }
    
    logger.info("=" * 60)
    logger.info("Test Summary")
    logger.info("=" * 60)
    
    for component, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        logger.info(f"{component:20s} {status}")
    
    total_passed = sum(results.values())
    total_tests = len(results)
    
    logger.info("=" * 60)
    logger.info(f"Total: {total_passed}/{total_tests} tests passed")
    logger.info("=" * 60)
    
    if total_passed == total_tests:
        logger.info("All tests passed! ✓")
        return 0
    else:
        logger.error("Some tests failed! ✗")
        return 1


if __name__ == "__main__":
    sys.exit(main())
