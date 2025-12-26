#!/usr/bin/env python3
"""
Flight Analytics and Visualization Script
Based on load_historical_data.py structure

This script runs analytics queries on the flights_raw table and creates visualizations.
"""

import os
import logging
import argparse
from typing import Dict, Any, Optional
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from google.cloud import bigquery
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FlightAnalytics:
    """Analytics and visualization for historical flight data"""
    
    def __init__(
        self,
        project_id: str,
        dataset_id: str = "aero_dataset",
        table_id: str = "flights_raw"
    ):
        """
        Initialize analytics engine
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID (flights_raw)
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"
        
        # Initialize BigQuery client
        self.bq_client = bigquery.Client(project=project_id)
        
        # Set visualization style
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = (14, 8)
        plt.rcParams['font.size'] = 10
        
        logger.info(f"Analytics initialized for {self.table_ref}")
    
    def get_table_info(self) -> Dict[str, Any]:
        """
        Get basic table information
        
        Returns:
            Dictionary with table statistics
        """
        query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT Year) as unique_years,
            MIN(Year) as min_year,
            MAX(Year) as max_year,
            COUNT(DISTINCT Reporting_Airline) as unique_airlines,
            COUNT(DISTINCT Origin) as unique_origins,
            COUNT(DISTINCT Dest) as unique_destinations
        FROM `{self.table_ref}`
        """
        
        try:
            result = self.bq_client.query(query).to_dataframe()
            info = result.to_dict('records')[0]
            logger.info(f"Table info: {info['total_rows']:,} rows, {info['min_year']}-{info['max_year']}")
            return info
        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            return {}
    
    def query_delays_by_year(self) -> pd.DataFrame:
        """
        Query: Average delays by year
        
        Returns:
            DataFrame with delay statistics by year
        """
        query = f"""
        SELECT
            Year,
            COUNT(*) as total_flights,
            AVG(DepDelay) as avg_departure_delay,
            AVG(ArrDelay) as avg_arrival_delay,
            AVG(CarrierDelay) as avg_carrier_delay,
            AVG(WeatherDelay) as avg_weather_delay,
            AVG(NASDelay) as avg_nas_delay,
            AVG(SecurityDelay) as avg_security_delay,
            AVG(LateAircraftDelay) as avg_late_aircraft_delay,
            SUM(CASE WHEN Cancelled = 1 THEN 1 ELSE 0 END) as cancelled_flights,
            SUM(CASE WHEN DepDel15 = 1 THEN 1 ELSE 0 END) as delayed_flights_15min
        FROM `{self.table_ref}`
        WHERE Year IS NOT NULL
        GROUP BY Year
        ORDER BY Year
        """
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            logger.info(f"Retrieved delay data by year: {len(df)} years")
            return df
        except Exception as e:
            logger.error(f"Error querying delays by year: {e}")
            return pd.DataFrame()
    
    def query_top_airlines_by_delays(self, limit: int = 20) -> pd.DataFrame:
        """
        Query: Top airlines by average delays and flight volume
        
        Args:
            limit: Number of airlines to return
            
        Returns:
            DataFrame with airline delay statistics
        """
        query = f"""
        SELECT
            Reporting_Airline,
            COUNT(*) as total_flights,
            AVG(DepDelay) as avg_departure_delay,
            AVG(ArrDelay) as avg_arrival_delay,
            SUM(CASE WHEN Cancelled = 1 THEN 1 ELSE 0 END) as cancelled_flights,
            ROUND(SUM(CASE WHEN Cancelled = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as cancellation_rate,
            ROUND(SUM(CASE WHEN DepDel15 = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as delay_rate_15min
        FROM `{self.table_ref}`
        WHERE Reporting_Airline IS NOT NULL
        GROUP BY Reporting_Airline
        HAVING total_flights > 1000
        ORDER BY total_flights DESC
        LIMIT {limit}
        """
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            logger.info(f"Retrieved top {len(df)} airlines")
            return df
        except Exception as e:
            logger.error(f"Error querying top airlines: {e}")
            return pd.DataFrame()
    
    def query_top_routes(self, limit: int = 30) -> pd.DataFrame:
        """
        Query: Top routes by flight volume and delays
        
        Args:
            limit: Number of routes to return
            
        Returns:
            DataFrame with route statistics
        """
        query = f"""
        SELECT
            CONCAT(Origin, ' → ', Dest) as route,
            Origin,
            Dest,
            COUNT(*) as total_flights,
            AVG(DepDelay) as avg_departure_delay,
            AVG(ArrDelay) as avg_arrival_delay,
            AVG(Distance) as avg_distance,
            SUM(CASE WHEN Cancelled = 1 THEN 1 ELSE 0 END) as cancelled_flights
        FROM `{self.table_ref}`
        WHERE Origin IS NOT NULL AND Dest IS NOT NULL
        GROUP BY Origin, Dest
        HAVING total_flights > 100
        ORDER BY total_flights DESC
        LIMIT {limit}
        """
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            logger.info(f"Retrieved top {len(df)} routes")
            return df
        except Exception as e:
            logger.error(f"Error querying top routes: {e}")
            return pd.DataFrame()
    
    def query_monthly_trends(self) -> pd.DataFrame:
        """
        Query: Flight trends by month across all years
        
        Returns:
            DataFrame with monthly statistics
        """
        query = f"""
        SELECT
            Month,
            COUNT(*) as total_flights,
            AVG(DepDelay) as avg_departure_delay,
            AVG(ArrDelay) as avg_arrival_delay,
            SUM(CASE WHEN Cancelled = 1 THEN 1 ELSE 0 END) as cancelled_flights,
            AVG(Distance) as avg_distance
        FROM `{self.table_ref}`
        WHERE Month IS NOT NULL
        GROUP BY Month
        ORDER BY Month
        """
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            logger.info(f"Retrieved monthly trends: {len(df)} months")
            return df
        except Exception as e:
            logger.error(f"Error querying monthly trends: {e}")
            return pd.DataFrame()
    
    def query_day_of_week_analysis(self) -> pd.DataFrame:
        """
        Query: Analysis by day of week
        
        Returns:
            DataFrame with day of week statistics
        """
        query = f"""
        SELECT
            DayOfWeek,
            CASE DayOfWeek
                WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'
                WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
                WHEN 7 THEN 'Sunday'
            END as day_name,
            COUNT(*) as total_flights,
            AVG(DepDelay) as avg_departure_delay,
            AVG(ArrDelay) as avg_arrival_delay,
            SUM(CASE WHEN Cancelled = 1 THEN 1 ELSE 0 END) as cancelled_flights
        FROM `{self.table_ref}`
        WHERE DayOfWeek IS NOT NULL
        GROUP BY DayOfWeek
        ORDER BY DayOfWeek
        """
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            logger.info(f"Retrieved day of week analysis: {len(df)} days")
            return df
        except Exception as e:
            logger.error(f"Error querying day of week analysis: {e}")
            return pd.DataFrame()
    
    def query_cancellation_analysis(self) -> pd.DataFrame:
        """
        Query: Cancellation analysis by reason
        
        Returns:
            DataFrame with cancellation statistics
        """
        query = f"""
        SELECT
            CancellationCode,
            CASE CancellationCode
                WHEN 'A' THEN 'Carrier'
                WHEN 'B' THEN 'Weather'
                WHEN 'C' THEN 'National Air System'
                WHEN 'D' THEN 'Security'
                ELSE 'Unknown'
            END as cancellation_reason,
            COUNT(*) as cancellation_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM `{self.table_ref}`
        WHERE Cancelled = 1 AND CancellationCode IS NOT NULL
        GROUP BY CancellationCode
        ORDER BY cancellation_count DESC
        """
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            logger.info(f"Retrieved cancellation analysis: {len(df)} reasons")
            return df
        except Exception as e:
            logger.error(f"Error querying cancellation analysis: {e}")
            return pd.DataFrame()
    
    def plot_delays_by_year(self, df: pd.DataFrame, output_path: str = "delays_by_year.png"):
        """Plot delay trends by year"""
        if df.empty:
            logger.warning("No data to plot for delays by year")
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # Average delays over time
        ax1 = axes[0, 0]
        ax1.plot(df['Year'], df['avg_departure_delay'], marker='o', label='Departure Delay', linewidth=2)
        ax1.plot(df['Year'], df['avg_arrival_delay'], marker='s', label='Arrival Delay', linewidth=2)
        ax1.set_title('Average Flight Delays by Year', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Year', fontsize=12)
        ax1.set_ylabel('Average Delay (minutes)', fontsize=12)
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Delay causes over time
        ax2 = axes[0, 1]
        ax2.plot(df['Year'], df['avg_carrier_delay'], label='Carrier', marker='o')
        ax2.plot(df['Year'], df['avg_weather_delay'], label='Weather', marker='s')
        ax2.plot(df['Year'], df['avg_nas_delay'], label='NAS', marker='^')
        ax2.plot(df['Year'], df['avg_late_aircraft_delay'], label='Late Aircraft', marker='d')
        ax2.set_title('Delay Causes by Year', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Year', fontsize=12)
        ax2.set_ylabel('Average Delay (minutes)', fontsize=12)
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Total flights over time
        ax3 = axes[1, 0]
        ax3.bar(df['Year'], df['total_flights'], color='steelblue', alpha=0.7)
        ax3.set_title('Total Flights by Year', fontsize=14, fontweight='bold')
        ax3.set_xlabel('Year', fontsize=12)
        ax3.set_ylabel('Total Flights', fontsize=12)
        ax3.grid(True, alpha=0.3, axis='y')
        
        # Cancellation rate over time
        ax4 = axes[1, 1]
        cancellation_rate = (df['cancelled_flights'] / df['total_flights'] * 100)
        ax4.plot(df['Year'], cancellation_rate, marker='o', color='red', linewidth=2)
        ax4.set_title('Cancellation Rate by Year', fontsize=14, fontweight='bold')
        ax4.set_xlabel('Year', fontsize=12)
        ax4.set_ylabel('Cancellation Rate (%)', fontsize=12)
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Delays by year plot saved to {output_path}")
    
    def plot_top_airlines(self, df: pd.DataFrame, output_path: str = "top_airlines.png"):
        """Plot top airlines analysis"""
        if df.empty:
            logger.warning("No data to plot for top airlines")
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # Top airlines by flight volume
        ax1 = axes[0, 0]
        top_10 = df.head(10)
        ax1.barh(top_10['Reporting_Airline'], top_10['total_flights'], color='steelblue')
        ax1.set_title('Top 10 Airlines by Flight Volume', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Total Flights', fontsize=12)
        ax1.invert_yaxis()
        ax1.grid(True, alpha=0.3, axis='x')
        
        # Average delays by airline
        ax2 = axes[0, 1]
        ax2.barh(top_10['Reporting_Airline'], top_10['avg_departure_delay'], color='coral')
        ax2.set_title('Average Departure Delay by Airline (Top 10)', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Average Delay (minutes)', fontsize=12)
        ax2.invert_yaxis()
        ax2.grid(True, alpha=0.3, axis='x')
        
        # Cancellation rate
        ax3 = axes[1, 0]
        ax3.barh(top_10['Reporting_Airline'], top_10['cancellation_rate'], color='lightcoral')
        ax3.set_title('Cancellation Rate by Airline (Top 10)', fontsize=14, fontweight='bold')
        ax3.set_xlabel('Cancellation Rate (%)', fontsize=12)
        ax3.invert_yaxis()
        ax3.grid(True, alpha=0.3, axis='x')
        
        # Delay rate (15+ min)
        ax4 = axes[1, 1]
        ax4.barh(top_10['Reporting_Airline'], top_10['delay_rate_15min'], color='orange')
        ax4.set_title('Delay Rate (15+ min) by Airline (Top 10)', fontsize=14, fontweight='bold')
        ax4.set_xlabel('Delay Rate (%)', fontsize=12)
        ax4.invert_yaxis()
        ax4.grid(True, alpha=0.3, axis='x')
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Top airlines plot saved to {output_path}")
    
    def plot_top_routes(self, df: pd.DataFrame, output_path: str = "top_routes.png"):
        """Plot top routes analysis"""
        if df.empty:
            logger.warning("No data to plot for top routes")
            return
        
        fig, axes = plt.subplots(2, 1, figsize=(16, 12))
        
        # Top routes by volume
        ax1 = axes[0]
        top_15 = df.head(15)
        ax1.barh(top_15['route'], top_15['total_flights'], color='teal', alpha=0.8)
        ax1.set_title('Top 15 Routes by Flight Volume', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Total Flights', fontsize=12)
        ax1.invert_yaxis()
        ax1.grid(True, alpha=0.3, axis='x')
        
        # Average delays by route
        ax2 = axes[1]
        ax2.barh(top_15['route'], top_15['avg_departure_delay'], color='salmon', alpha=0.8)
        ax2.set_title('Average Departure Delay by Route (Top 15)', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Average Delay (minutes)', fontsize=12)
        ax2.invert_yaxis()
        ax2.grid(True, alpha=0.3, axis='x')
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Top routes plot saved to {output_path}")
    
    def plot_monthly_trends(self, df: pd.DataFrame, output_path: str = "monthly_trends.png"):
        """Plot monthly trends"""
        if df.empty:
            logger.warning("No data to plot for monthly trends")
            return
        
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # Flight volume by month
        ax1 = axes[0, 0]
        ax1.bar(df['Month'], df['total_flights'], color='skyblue', alpha=0.8)
        ax1.set_xticks(range(1, 13))
        ax1.set_xticklabels(month_names, rotation=45)
        ax1.set_title('Total Flights by Month', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Total Flights', fontsize=12)
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Average delays by month
        ax2 = axes[0, 1]
        ax2.plot(df['Month'], df['avg_departure_delay'], marker='o', label='Departure', linewidth=2)
        ax2.plot(df['Month'], df['avg_arrival_delay'], marker='s', label='Arrival', linewidth=2)
        ax2.set_xticks(range(1, 13))
        ax2.set_xticklabels(month_names, rotation=45)
        ax2.set_title('Average Delays by Month', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Average Delay (minutes)', fontsize=12)
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Cancellations by month
        ax3 = axes[1, 0]
        cancellation_rate = (df['cancelled_flights'] / df['total_flights'] * 100)
        ax3.bar(df['Month'], cancellation_rate, color='lightcoral', alpha=0.8)
        ax3.set_xticks(range(1, 13))
        ax3.set_xticklabels(month_names, rotation=45)
        ax3.set_title('Cancellation Rate by Month', fontsize=14, fontweight='bold')
        ax3.set_ylabel('Cancellation Rate (%)', fontsize=12)
        ax3.grid(True, alpha=0.3, axis='y')
        
        # Average distance by month
        ax4 = axes[1, 1]
        ax4.bar(df['Month'], df['avg_distance'], color='lightgreen', alpha=0.8)
        ax4.set_xticks(range(1, 13))
        ax4.set_xticklabels(month_names, rotation=45)
        ax4.set_title('Average Flight Distance by Month', fontsize=14, fontweight='bold')
        ax4.set_ylabel('Average Distance (miles)', fontsize=12)
        ax4.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Monthly trends plot saved to {output_path}")
    
    def plot_day_of_week_analysis(self, df: pd.DataFrame, output_path: str = "day_of_week.png"):
        """Plot day of week analysis"""
        if df.empty:
            logger.warning("No data to plot for day of week")
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # Flight volume by day
        ax1 = axes[0, 0]
        ax1.bar(df['day_name'], df['total_flights'], color='steelblue', alpha=0.8)
        ax1.set_title('Total Flights by Day of Week', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Total Flights', fontsize=12)
        ax1.tick_params(axis='x', rotation=45)
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Average delays by day
        ax2 = axes[0, 1]
        ax2.plot(df['day_name'], df['avg_departure_delay'], marker='o', label='Departure', linewidth=2)
        ax2.plot(df['day_name'], df['avg_arrival_delay'], marker='s', label='Arrival', linewidth=2)
        ax2.set_title('Average Delays by Day of Week', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Average Delay (minutes)', fontsize=12)
        ax2.tick_params(axis='x', rotation=45)
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Cancellations by day
        ax3 = axes[1, 0]
        cancellation_rate = (df['cancelled_flights'] / df['total_flights'] * 100)
        ax3.bar(df['day_name'], cancellation_rate, color='lightcoral', alpha=0.8)
        ax3.set_title('Cancellation Rate by Day of Week', fontsize=14, fontweight='bold')
        ax3.set_ylabel('Cancellation Rate (%)', fontsize=12)
        ax3.tick_params(axis='x', rotation=45)
        ax3.grid(True, alpha=0.3, axis='y')
        
        # Flight volume comparison (bar chart with trend)
        ax4 = axes[1, 1]
        colors = ['lightblue' if x < df['total_flights'].mean() else 'darkblue' for x in df['total_flights']]
        ax4.bar(df['day_name'], df['total_flights'], color=colors, alpha=0.8)
        ax4.axhline(y=df['total_flights'].mean(), color='red', linestyle='--', label='Average')
        ax4.set_title('Flight Volume by Day (vs Average)', fontsize=14, fontweight='bold')
        ax4.set_ylabel('Total Flights', fontsize=12)
        ax4.tick_params(axis='x', rotation=45)
        ax4.legend()
        ax4.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Day of week plot saved to {output_path}")
    
    def plot_cancellation_analysis(self, df: pd.DataFrame, output_path: str = "cancellation_analysis.png"):
        """Plot cancellation analysis"""
        if df.empty:
            logger.warning("No data to plot for cancellation analysis")
            return
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Pie chart of cancellation reasons
        ax1.pie(df['cancellation_count'], labels=df['cancellation_reason'], autopct='%1.1f%%',
               startangle=90, colors=sns.color_palette("Set3"))
        ax1.set_title('Cancellation Reasons Distribution', fontsize=14, fontweight='bold')
        
        # Bar chart of cancellation counts
        ax2.bar(df['cancellation_reason'], df['cancellation_count'], color='coral', alpha=0.8)
        ax2.set_title('Cancellation Counts by Reason', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Cancellation Count', fontsize=12)
        ax2.tick_params(axis='x', rotation=45)
        ax2.grid(True, alpha=0.3, axis='y')
        
        # Add percentage labels on bars
        for i, (reason, count, pct) in enumerate(zip(df['cancellation_reason'], 
                                                      df['cancellation_count'], 
                                                      df['percentage'])):
            ax2.text(i, count, f'{pct}%', ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Cancellation analysis plot saved to {output_path}")
    
    def generate_all_visualizations(self, output_dir: str = "analytics/output") -> Dict[str, Any]:
        """
        Generate all analytics queries and visualizations
        
        Args:
            output_dir: Output directory for visualizations
            
        Returns:
            Dictionary with summary statistics
        """
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info("="*60)
        logger.info("Starting Flight Analytics and Visualization")
        logger.info("="*60)
        
        # Get table info
        table_info = self.get_table_info()
        
        # Run queries
        logger.info("\n1. Querying delays by year...")
        delays_by_year = self.query_delays_by_year()
        
        logger.info("2. Querying top airlines...")
        top_airlines = self.query_top_airlines_by_delays()
        
        logger.info("3. Querying top routes...")
        top_routes = self.query_top_routes()
        
        logger.info("4. Querying monthly trends...")
        monthly_trends = self.query_monthly_trends()
        
        logger.info("5. Querying day of week analysis...")
        day_of_week = self.query_day_of_week_analysis()
        
        logger.info("6. Querying cancellation analysis...")
        cancellation_analysis = self.query_cancellation_analysis()
        
        # Generate visualizations
        logger.info("\nGenerating visualizations...")
        self.plot_delays_by_year(delays_by_year, f"{output_dir}/delays_by_year.png")
        self.plot_top_airlines(top_airlines, f"{output_dir}/top_airlines.png")
        self.plot_top_routes(top_routes, f"{output_dir}/top_routes.png")
        self.plot_monthly_trends(monthly_trends, f"{output_dir}/monthly_trends.png")
        self.plot_day_of_week_analysis(day_of_week, f"{output_dir}/day_of_week.png")
        self.plot_cancellation_analysis(cancellation_analysis, f"{output_dir}/cancellation_analysis.png")
        
        # Save data to CSV for further analysis
        logger.info("\nSaving query results to CSV...")
        delays_by_year.to_csv(f"{output_dir}/delays_by_year.csv", index=False)
        top_airlines.to_csv(f"{output_dir}/top_airlines.csv", index=False)
        top_routes.to_csv(f"{output_dir}/top_routes.csv", index=False)
        monthly_trends.to_csv(f"{output_dir}/monthly_trends.csv", index=False)
        day_of_week.to_csv(f"{output_dir}/day_of_week.csv", index=False)
        cancellation_analysis.to_csv(f"{output_dir}/cancellation_analysis.csv", index=False)
        
        # Create summary
        summary = {
            "table_info": table_info,
            "total_visualizations": 6,
            "output_directory": output_dir,
            "generation_time": datetime.now().isoformat()
        }
        
        logger.info("\n" + "="*60)
        logger.info("Analytics and Visualization Complete!")
        logger.info("="*60)
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Total visualizations: 6")
        logger.info(f"Table rows analyzed: {table_info.get('total_rows', 'N/A'):,}")
        logger.info("="*60)
        
        return summary


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Flight Analytics and Visualization')
    parser.add_argument('--project-id', default=os.environ.get("GCP_PROJECT_ID", "double-arbor-475907-s5"),
                        help='GCP project ID')
    parser.add_argument('--dataset-id', default='aero_dataset',
                        help='BigQuery dataset ID')
    parser.add_argument('--table-id', default='flights_raw',
                        help='BigQuery table ID')
    parser.add_argument('--output-dir', default='analytics/output',
                        help='Output directory for visualizations')
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("Flight Analytics and Visualization Script")
    logger.info("Based on load_historical_data.py")
    logger.info("="*60)
    logger.info(f"Project: {args.project_id}")
    logger.info(f"Dataset: {args.dataset_id}")
    logger.info(f"Table: {args.table_id}")
    logger.info(f"Output Directory: {args.output_dir}")
    logger.info("="*60 + "\n")
    
    # Initialize analytics
    analytics = FlightAnalytics(
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        table_id=args.table_id
    )
    
    # Generate all visualizations
    summary = analytics.generate_all_visualizations(output_dir=args.output_dir)
    
    logger.info("\nSummary:")
    logger.info(f"  Table Info: {summary.get('table_info', {})}")
    logger.info(f"  Visualizations Created: {summary.get('total_visualizations', 0)}")
    logger.info(f"  Output Directory: {summary.get('output_directory', '')}")


if __name__ == "__main__":
    main()
