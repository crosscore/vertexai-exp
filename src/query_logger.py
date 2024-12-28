import os
import csv
from typing import List, Dict
from pathlib import Path
from datetime import datetime

class QueryLogger:
    def __init__(self, csv_path: str = "./csv/query_logs.csv"):
        """
        Initialize QueryLogger

        Args:
            csv_path: Path to the CSV log file
        """
        self.csv_path = csv_path
        self._ensure_directory_exists()

    def _ensure_directory_exists(self):
        """Ensure the output directory exists"""
        directory = os.path.dirname(self.csv_path)
        if directory:
            Path(directory).mkdir(parents=True, exist_ok=True)

    def _format_search_results(self, search_results: List[Dict]) -> str:
        """
        Format search results for CSV logging

        Args:
            search_results: List of search result dictionaries

        Returns:
            Formatted string of search results
        """
        formatted_results = []
        for result in search_results:
            formatted_results.append(f"{result['content']}\n---")
        return '\n'.join(formatted_results).strip()

    def log_query(self,
                    question: str,
                    search_results: List[Dict],
                    selected_table: str,
                    generated_sql: str,
                    expected_table: str = None,
                    is_correct: bool = None):
        """
        Log query information to CSV file

        Args:
            question: User's question
            search_results: Search results from the engine
            selected_table: Selected table ID
            generated_sql: Generated SQL query
            expected_table: Expected table ID (for testing)
            is_correct: Whether table selection was correct (for testing)
        """
        try:
            formatted_results = self._format_search_results(search_results)

            row = [
                question,
                formatted_results,
                selected_table,
                generated_sql,
                datetime.now().isoformat()
            ]

            # Add test results if provided
            if expected_table is not None:
                row.extend([expected_table, str(is_correct)])

            file_exists = os.path.isfile(self.csv_path)

            with open(self.csv_path, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)

                if not file_exists:
                    headers = [
                        'ユーザーの質問',
                        '検索結果',
                        'テーブル選択結果',
                        '生成されたSQLクエリ',
                        'タイムスタンプ'
                    ]
                    if expected_table is not None:
                        headers.extend(['期待されるテーブル', 'テーブル選択の正誤'])
                    writer.writerow(headers)

                writer.writerow(row)

        except Exception as e:
            print(f"Error in log_query: {str(e)}")
            # Continue execution even if logging fails

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
