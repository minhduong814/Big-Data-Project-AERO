"""
Dashboard module for AERO flight data visualization
Creates analytical dashboards and reports
"""
import logging
from typing import Dict, Any, Optional, List
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from google.cloud import bigquery
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FlightDashboard:
    """Dashboard generator for flight analytics"""
    
    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        credentials_path: Optional[str] = None
    ):
        """
        Initialize dashboard
        
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
        
        # Set visualization style
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = (12, 6)
        
        self.logger.info(f"Dashboard initialized for {project_id}.{dataset_id}")
    
    def get_delay_statistics(self, days: int = 7) -> pd.DataFrame:
        """
        Get delay statistics for the last N days
        
        Args:
            days: Number of days to analyze
            
        Returns:
            DataFrame with delay statistics
        """
        query = f"""
        SELECT
            DATE(timestamp) as date,
            airline,
            COUNT(*) as total_flights,
            AVG(departure_delay_minutes) as avg_departure_delay,
            AVG(arrival_delay_minutes) as avg_arrival_delay,
            STDDEV(departure_delay_minutes) as std_departure_delay,
            MAX(departure_delay_minutes) as max_departure_delay,
            MIN(departure_delay_minutes) as min_departure_delay
        FROM
            `{self.project_id}.{self.dataset_id}.flights`
        WHERE
            timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        GROUP BY
            date, airline
        ORDER BY
            date DESC, airline
        """
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            self.logger.info(f"Retrieved delay statistics: {len(df)} rows")
            return df
        except Exception as e:
            self.logger.error(f"Error getting delay statistics: {e}")
            return pd.DataFrame()
    
    def plot_delay_trends(self, days: int = 30, output_path: str = "delay_trends.png"):
        """
        Plot delay trends over time
        
        Args:
            days: Number of days to plot
            output_path: Output file path
        """
        df = self.get_delay_statistics(days)
        
        if df.empty:
            self.logger.warning("No data to plot")
            return
        
        plt.figure(figsize=(14, 7))
        
        # Plot average delay by airline
        for airline in df['airline'].unique():
            airline_data = df[df['airline'] == airline]
            plt.plot(
                airline_data['date'],
                airline_data['avg_departure_delay'],
                marker='o',
                label=airline
            )
        
        plt.title('Flight Departure Delays by Airline Over Time', fontsize=16)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Average Delay (minutes)', fontsize=12)
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        self.logger.info(f"Delay trends plot saved to {output_path}")
    
    def plot_delay_distribution(self, output_path: str = "delay_distribution.png"):
        """
        Plot distribution of flight delays
        
        Args:
            output_path: Output file path
        """
        query = f"""
        SELECT
            delay_category,
            COUNT(*) as count
        FROM
            `{self.project_id}.{self.dataset_id}.flights`
        WHERE
            delay_category IS NOT NULL
        GROUP BY
            delay_category
        """
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            
            plt.figure(figsize=(10, 6))
            plt.bar(df['delay_category'], df['count'], color='skyblue', edgecolor='navy')
            plt.title('Distribution of Flight Delays', fontsize=16)
            plt.xlabel('Delay Category', fontsize=12)
            plt.ylabel('Number of Flights', fontsize=12)
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            self.logger.info(f"Delay distribution plot saved to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Error plotting delay distribution: {e}")
    
    def plot_route_heatmap(self, output_path: str = "route_heatmap.png"):
        """
        Plot heatmap of popular routes
        
        Args:
            output_path: Output file path
        """
        query = f"""
        SELECT
            origin,
            destination,
            COUNT(*) as flight_count
        FROM
            `{self.project_id}.{self.dataset_id}.flights`
        WHERE
            origin IS NOT NULL
            AND destination IS NOT NULL
        GROUP BY
            origin, destination
        HAVING
            flight_count > 5
        ORDER BY
            flight_count DESC
        LIMIT 50
        """
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            
            # Create pivot table for heatmap
            pivot_df = df.pivot_table(
                index='origin',
                columns='destination',
                values='flight_count',
                fill_value=0
            )
            
            plt.figure(figsize=(14, 10))
            sns.heatmap(pivot_df, annot=True, fmt='g', cmap='YlOrRd', cbar_kws={'label': 'Flight Count'})
            plt.title('Flight Route Heatmap', fontsize=16)
            plt.xlabel('Destination', fontsize=12)
            plt.ylabel('Origin', fontsize=12)
            plt.tight_layout()

            
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            self.logger.info(f"Route heatmap saved to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Error plotting route heatmap: {e}")
    
    def generate_summary_report(self) -> Dict[str, Any]:
        """
        Generate summary statistics report
        
        Returns:
            Dictionary with summary statistics
        """
        query = f"""
        SELECT
            COUNT(*) as total_flights,
            COUNT(DISTINCT airline) as total_airlines,
            COUNT(DISTINCT origin) as total_origins,
            COUNT(DISTINCT destination) as total_destinations,
            AVG(departure_delay_minutes) as avg_delay,
            MAX(departure_delay_minutes) as max_delay,
            SUM(CASE WHEN departure_delay_minutes > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as delay_percentage
        FROM
            `{self.project_id}.{self.dataset_id}.flights`
        """
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            report = df.to_dict('records')[0]
            
            self.logger.info("Summary report generated")
            return report
            
        except Exception as e:
            self.logger.error(f"Error generating summary report: {e}")
            return {}
    
    def create_full_dashboard(self, output_dir: str = "dashboards"):
        """
        Create all dashboard visualizations
        
        Args:
            output_dir: Output directory for dashboard files
        """
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        self.logger.info("Creating full dashboard...")
        
        # Generate all plots
        self.plot_delay_trends(days=30, output_path=f"{output_dir}/delay_trends.png")
        self.plot_delay_distribution(output_path=f"{output_dir}/delay_distribution.png")
        self.plot_route_heatmap(output_path=f"{output_dir}/route_heatmap.png")
        
        # Generate summary report
        summary = self.generate_summary_report()
        
        # Save summary as JSON
        import json
        with open(f"{output_dir}/summary_report.json", 'w') as f:
            json.dump(summary, f, indent=2)
        
        self.logger.info(f"Full dashboard created in {output_dir}")
        return summary


if __name__ == "__main__":
    dashboard = FlightDashboard(
        project_id="double-arbor-475907-s5",
        dataset_id="aero_dataset"
    )
    
    # Create full dashboard
    summary = dashboard.create_full_dashboard()
    print("Dashboard Summary:")
    print(summary)
