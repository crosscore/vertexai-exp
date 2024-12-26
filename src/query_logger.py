import os
import csv
from typing import List, Dict
from pathlib import Path

class QueryLogger:
    """
    A class to log query generation results to CSV file
    """
    def __init__(self, csv_path: str = "./csv/query_logs.csv"):
        """
        Initialize QueryLogger with CSV file path
        
        Args:
            csv_path: Path to the CSV file where logs will be saved
        """
        self.csv_path = csv_path
        self._ensure_directory_exists()

    def _ensure_directory_exists(self):
        """Create the directory for the CSV file if it doesn't exist"""
        directory = os.path.dirname(self.csv_path)
        if directory:
            Path(directory).mkdir(parents=True, exist_ok=True)

    def _format_search_results(self, search_results: List[Dict]) -> str:
        """
        Format search results exactly as they appear in the prompt
        
        Args:
            search_results: List of search results from search_sample function
            
        Returns:
            Formatted string with search results
        """
        formatted_results = []
        for result in search_results:
            formatted_results.append(f"{result['content']}\n---")
        return '\n'.join(formatted_results).strip()

    def log_query(self, 
                  question: str, 
                  search_results: List[Dict], 
                  selected_table: str, 
                  generated_sql: str):
        """
        Log the query generation results to CSV file
        
        Args:
            question: Original user question
            search_results: Results from search_sample function
            selected_table: Table selected by LLM
            generated_sql: Generated SQL query
        """
        # Format search results as they appear in the prompt
        formatted_results = self._format_search_results(search_results)
        
        # Prepare the row to be written
        row = [
            question,
            formatted_results,
            selected_table,
            generated_sql
        ]
        
        # Check if file exists to determine if headers need to be written
        file_exists = os.path.isfile(self.csv_path)
        
        # Open file in append mode with UTF-8 encoding
        with open(self.csv_path, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            
            # Write headers if file is new
            if not file_exists:
                writer.writerow([
                    'ユーザーの質問',
                    '検索結果',
                    'テーブル選択結果',
                    '生成されたSQLクエリ'
                ])
            
            # Write the data row
            writer.writerow(row)

def main():
    """
    Example usage of QueryLogger
    """
    # Sample data
    sample_data = {
        'question': 'テーブルの全期間の商品の在庫数の推移のグラフを作成して下さい',
        'search_results': [
            {
                'content': 'tableId: test-table-005\ncategory: sales_order\ndescription: 商品の販売データと在庫データを記録しています。...'
            },
            {
                'content': 'tableId: test-table-001\ncategory: environment_monitoring\ndescription: 特定の環境における温度、湿度、気圧...'
            }
        ],
        'selected_table': 'test-table-005',
        'generated_sql': 'SELECT\n    Timestamp,\n    ProductName,\n    StockQuantity\nFROM\n    `test-table-005`\nORDER BY\n    Timestamp,\n    ProductName'
    }

    # Create logger instance
    logger = QueryLogger()
    
    # Log the sample data
    logger.log_query(
        sample_data['question'],
        sample_data['search_results'],
        sample_data['selected_table'],
        sample_data['generated_sql']
    )
    
    print(f"Query logged successfully to {logger.csv_path}")

if __name__ == "__main__":
    main()