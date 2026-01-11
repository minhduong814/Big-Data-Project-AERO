"""
Looker connector for AERO data visualization
Connects to Looker/Looker Studio for dashboard creation
"""
import logging
from typing import Dict, Any, Optional, List
from google.cloud import bigquery
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LookerConnector:
    """Connector for Looker/Looker Studio integration"""
    
    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        credentials_path: Optional[str] = None
    ):
        """
        Initialize Looker connector
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            credentials_path: Path to credentials
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.logger = logging.getLogger(__name__)
        
        # Initialize BigQuery client
        self.bq_client = bigquery.Client(project=project_id)
        
        self.logger.info(f"Looker connector initialized for {project_id}.{dataset_id}")
    
    def create_view_for_dashboard(
        self,
        view_name: str,
        query: str
    ) -> bool:
        """
        Create a BigQuery view for Looker dashboard
        
        Args:
            view_name: Name of the view
            query: SQL query for the view
            
        Returns:
            True if successful
        """
        try:
            view_ref = f"{self.project_id}.{self.dataset_id}.{view_name}"
            view = bigquery.Table(view_ref)
            view.view_query = query
            
            # Create or replace view
            self.bq_client.delete_table(view_ref, not_found_ok=True)
            self.bq_client.create_table(view)
            
            self.logger.info(f"Created view: {view_ref}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating view: {e}")
            return False
    
    def get_flight_metrics_view(self) -> str:
        """
        Get SQL for flight metrics view
        
        Returns:
            SQL query string
        """
        return f"""
        SELECT
            DATE(timestamp) as flight_date,
            airline,
            COUNT(*) as total_flights,
            AVG(departure_delay_minutes) as avg_departure_delay,
            AVG(arrival_delay_minutes) as avg_arrival_delay,
            SUM(CASE WHEN departure_delay_minutes > 15 THEN 1 ELSE 0 END) as delayed_flights,
            ROUND(SUM(CASE WHEN departure_delay_minutes > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as delay_percentage
        FROM
            `{self.project_id}.{self.dataset_id}.flights`
        GROUP BY
            flight_date, airline
        ORDER BY
            flight_date DESC, airline
        """
    
    def get_route_analysis_view(self) -> str:
        """
        Get SQL for route analysis view
        
        Returns:
            SQL query string
        """
        return f"""
        SELECT
            origin,
            destination,
            COUNT(*) as flight_count,
            AVG(flight_duration_minutes) as avg_duration,
            AVG(departure_delay_minutes) as avg_delay,
            airline
        FROM
            `{self.project_id}.{self.dataset_id}.flights`
        WHERE
            origin IS NOT NULL
            AND destination IS NOT NULL
        GROUP BY
            origin, destination, airline
        HAVING
            flight_count > 10
        ORDER BY
            flight_count DESC
        """
    
    def get_hourly_trends_view(self) -> str:
        """
        Get SQL for hourly trends view
        
        Returns:
            SQL query string
        """
        return f"""
        SELECT
            EXTRACT(HOUR FROM timestamp) as hour_of_day,
            COUNT(*) as flight_count,
            AVG(departure_delay_minutes) as avg_delay,
            delay_category
        FROM
            `{self.project_id}.{self.dataset_id}.flights`
        GROUP BY
            hour_of_day, delay_category
        ORDER BY
            hour_of_day, delay_category
        """
    
    def create_standard_views(self) -> None:
        """Create all standard views for Looker dashboards"""
        views = {
            'flight_metrics': self.get_flight_metrics_view(),
            'route_analysis': self.get_route_analysis_view(),
            'hourly_trends': self.get_hourly_trends_view()
        }
        
        for view_name, query in views.items():
            self.create_view_for_dashboard(view_name, query)
    
    def export_data_for_looker_studio(
        self,
        query: str,
        output_path: str
    ) -> bool:
        """
        Export data for Looker Studio
        
        Args:
            query: SQL query
            output_path: Output file path
            
        Returns:
            True if successful
        """
        try:
            df = self.bq_client.query(query).to_dataframe()
            df.to_csv(output_path, index=False)
            
            self.logger.info(f"Exported {len(df)} rows to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting data: {e}")
            return False


if __name__ == "__main__":
    connector = LookerConnector(
        project_id="double-arbor-475907-s5",
        dataset_id="aero_dataset"
    )
    
    # Create standard views
    connector.create_standard_views()
    print("Standard views created for Looker dashboards")
