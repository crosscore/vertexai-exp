from typing import Dict, List, Union, Optional
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import os
from datetime import datetime
from google.cloud import bigquery

class QueryExecutor:
    """
    A class for executing SQL queries in BigQuery
    """
    def __init__(self, project_id: str):
        """
        Initialize the QueryExecutor

        Args:
            project_id: GCP project ID
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def execute_query(self, sql_query: str) -> Optional[List[Dict]]:
        """
        Execute a SQL query and return the results

        Args:
            sql_query: SQL query to execute

        Returns:
            List of dictionaries containing query results if successful, None otherwise
        """
        try:
            query_job = self.client.query(sql_query)
            results = query_job.result()
            return [dict(row) for row in results]
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            return None

    def get_table_schema(self, dataset_id: str, table_id: str) -> Optional[List[Dict]]:
        """
        Get schema information for a specific table

        Args:
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID

        Returns:
            List of dictionaries containing schema information if successful, None otherwise
        """
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            table = self.client.get_table(table_ref)

            schema_info = []
            for field in table.schema:
                schema_info.append({
                    'name': field.name,
                    'type': field.field_type,
                    'mode': field.mode,
                    'description': field.description
                })
            return schema_info
        except Exception as e:
            print(f"Error getting table schema: {str(e)}")
            return None

class GraphGenerator:
    """
    A class for generating various types of graphs
    """
    def __init__(self, output_dir: str = "./img"):
        """
        Initialize the GraphGenerator

        Args:
            output_dir: Directory to save generated graphs
        """
        self.output_dir = output_dir
        self._ensure_output_dir()

    def _ensure_output_dir(self):
        """Ensure the output directory exists"""
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

    def _generate_filename(self, base_name: str) -> str:
        """Generate a unique filename with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{base_name}_{timestamp}.png"

    def generate_time_series(self,
                            data: List[Dict],
                            time_column: str,
                            value_column: str,
                            title: str = None,
                            figsize: tuple = (12, 6)) -> Optional[str]:
        """
        Generate a time series line plot

        Args:
            data: List of dictionaries containing the data
            time_column: Name of the time column
            value_column: Name of the value column to plot
            title: Title of the graph (optional)
            figsize: Figure size tuple (width, height)

        Returns:
            Path to the saved graph file if successful, None otherwise
        """
        try:
            df = pd.DataFrame(data)
            if time_column not in df.columns or value_column not in df.columns:
                return None

            plt.figure(figsize=figsize)
            plt.plot(df[time_column], df[value_column], marker='o')

            if title:
                plt.title(title)
            plt.xlabel(time_column)
            plt.ylabel(value_column)
            plt.grid(True)
            plt.xticks(rotation=45)
            plt.tight_layout()

            # Save the graph
            filename = self._generate_filename("time_series")
            filepath = os.path.join(self.output_dir, filename)
            plt.savefig(filepath)
            plt.close()

            return filepath
        except Exception as e:
            print(f"Error in generate_time_series: {str(e)}")
            return None

    def generate_bar_chart(self,
                            data: List[Dict],
                            category_column: str,
                            value_column: str,
                            title: str = None,
                            figsize: tuple = (12, 6)) -> Optional[str]:
        """
        Generate a bar chart

        Args:
            data: List of dictionaries containing the data
            category_column: Name of the category column
            value_column: Name of the value column to plot
            title: Title of the graph (optional)
            figsize: Figure size tuple (width, height)

        Returns:
            Path to the saved graph file if successful, None otherwise
        """
        try:
            df = pd.DataFrame(data)
            if category_column not in df.columns or value_column not in df.columns:
                return None

            plt.figure(figsize=figsize)
            plt.bar(df[category_column], df[value_column])

            if title:
                plt.title(title)
            plt.xlabel(category_column)
            plt.ylabel(value_column)
            plt.grid(True, axis='y')
            plt.xticks(rotation=45)
            plt.tight_layout()

            # Save the graph
            filename = self._generate_filename("bar_chart")
            filepath = os.path.join(self.output_dir, filename)
            plt.savefig(filepath)
            plt.close()

            return filepath
        except Exception as e:
            print(f"Error in generate_bar_chart: {str(e)}")
            return None

class DataAnalyzer:
    """
    A class for analyzing data with various statistical methods
    """
    @staticmethod
    def get_max_value(data: List[Dict], column_name: str) -> Optional[float]:
        """
        Get the maximum value from a specific column

        Args:
            data: List of dictionaries containing the data
            column_name: Name of the column to analyze

        Returns:
            Maximum value if successful, None if column not found
        """
        try:
            df = pd.DataFrame(data)
            if column_name in df.columns:
                return float(df[column_name].max())
            return None
        except Exception as e:
            print(f"Error in get_max_value: {str(e)}")
            return None

    @staticmethod
    def get_min_value(data: List[Dict], column_name: str) -> Optional[float]:
        """
        Get the minimum value from a specific column

        Args:
            data: List of dictionaries containing the data
            column_name: Name of the column to analyze

        Returns:
            Minimum value if successful, None if column not found
        """
        try:
            df = pd.DataFrame(data)
            if column_name in df.columns:
                return float(df[column_name].min())
            return None
        except Exception as e:
            print(f"Error in get_min_value: {str(e)}")
            return None

    @staticmethod
    def get_average_value(data: List[Dict], column_name: str) -> Optional[float]:
        """
        Get the average value from a specific column

        Args:
            data: List of dictionaries containing the data
            column_name: Name of the column to analyze

        Returns:
            Average value if successful, None if column not found
        """
        try:
            df = pd.DataFrame(data)
            if column_name in df.columns:
                return float(df[column_name].mean())
            return None
        except Exception as e:
            print(f"Error in get_average_value: {str(e)}")
            return None

    @staticmethod
    def detect_outliers(data: List[Dict], column_name: str, threshold: float = 2.0) -> List[Dict]:
        """
        Detect outliers in a specific column using z-score method

        Args:
            data: List of dictionaries containing the data
            column_name: Name of the column to analyze
            threshold: Z-score threshold for outlier detection (default: 2.0)

        Returns:
            List of records containing outliers
        """
        try:
            df = pd.DataFrame(data)
            if column_name not in df.columns:
                return []

            z_scores = np.abs((df[column_name] - df[column_name].mean()) / df[column_name].std())
            outliers = df[z_scores > threshold]
            return outliers.to_dict('records')
        except Exception as e:
            print(f"Error in detect_outliers: {str(e)}")
            return []
