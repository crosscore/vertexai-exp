from typing import Dict, List, Optional
from datetime import datetime
import time
from tqdm import tqdm
from vertexai.preview.generative_models import GenerativeModel
from agent_search_from_engine import search_sample
from result_generator import ResultGenerator
from table_selector import TableSelector
from sql_query_generator_tester import SQLQueryGeneratorTester
from utils.json_utils import JSONOutputManager
from query_logger import QueryLogger
from test_questions import questions_dict
import os
from dotenv import load_dotenv

load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION")
ENGINE_ID = os.getenv("ENGINE_ID")
MODEL_NAME = os.getenv("MODEL_NAME")

class QueryProcessor:
    def __init__(self,
                project_id: str = PROJECT_ID,
                batch_size: int = 5,
                batch_wait: int = 60,
                request_wait: int = 3):
        """
        Initialize the QueryProcessor with both single and batch processing capabilities

        Args:
            project_id: GCP project ID
            batch_size: Number of questions to process in each batch
            batch_wait: Wait time between batches (seconds)
            request_wait: Wait time between API requests (seconds)
        """
        # Core components
        self.model = GenerativeModel(MODEL_NAME)
        self.project_id = project_id
        self.dataset_id = "test_dataset"
        self.result_generator = ResultGenerator(project_id)
        self.table_selector = TableSelector()
        self.query_generator = SQLQueryGeneratorTester()

        # Batch processing settings
        self.batch_size = batch_size
        self.batch_wait = batch_wait
        self.request_wait = request_wait

        # Output managers
        self.json_manager = JSONOutputManager()
        self.logger = QueryLogger()

    def process_question(self, question: str, expected_table: Optional[str] = None) -> Dict:
        """
        Process a single question

        Args:
            question: Question to process
            expected_table: Expected table ID for testing purposes

        Returns:
            Dictionary containing processing results
        """
        try:
            # Get search results
            search_results = search_sample(
                self.project_id,
                LOCATION,
                ENGINE_ID,
                question
            )

            if not search_results:
                return self._create_error_result(question, "検索結果が取得できませんでした")

            # Select table
            selected_table = self.table_selector.select_table(question, search_results)
            if not selected_table:
                return self._create_error_result(question, "テーブル選択に失敗しました")

            # Generate SQL query
            table_info = self._get_table_info(search_results, selected_table)
            if not table_info:
                return self._create_error_result(question, "テーブル情報の取得に失敗しました")

            # Qualify the table with dataset ID
            qualified_table = f"{self.dataset_id}.{selected_table}"
            table_info = table_info.replace(selected_table, qualified_table)

            generated_sql = self.query_generator.generate_query(question, table_info)
            if not generated_sql:
                return self._create_error_result(question, "SQLクエリの生成に失敗しました")

            # Process results using ResultGenerator
            result = self.result_generator.process_question(
                question=question,
                search_results=search_results,
                selected_table=qualified_table,
                generated_sql=generated_sql,
                expected_table=expected_table,
                table_selection_is_correct=(selected_table == expected_table if expected_table else None)
            )

            # Add additional information to the result
            result.update({
                'question': question,
                'search_results': search_results,
                'selected_table': selected_table,
                'generated_sql': generated_sql,
                'expected_table': expected_table,
                'timestamp': datetime.now().isoformat()
            })

            # Log successful query
            if result.get("success", False):
                self._log_result(result)

            return result

        except Exception as e:
            return self._create_error_result(question, str(e))

    def process_questions(self, questions: List[str],
                        expected_tables: Optional[Dict[str, str]] = None,
                        show_progress: bool = True) -> Dict:
        """
        Process multiple questions with batch control

        Args:
            questions: List of questions to process
            expected_tables: Dictionary mapping questions to expected table IDs
            show_progress: Whether to show progress information

        Returns:
            Dictionary containing processing results and statistics
        """
        results = {
            "total_questions": len(questions),
            "successful_queries": 0,
            "failed_questions": [],
            "details": [],
            "timestamp": datetime.now().isoformat()
        }

        if show_progress:
            self._print_batch_info(len(questions))

        # Process questions in batches
        for i in range(0, len(questions), self.batch_size):
            batch = questions[i:i + self.batch_size]
            batch_start_time = datetime.now()

            if show_progress:
                self._print_batch_start(i, len(questions))

            # Process each question in the batch
            for j, question in enumerate(batch, 1):
                if show_progress:
                    print(f"\n質問 {j}/{len(batch)} 処理中:")
                    print(f"質問: {question}")

                expected_table = expected_tables.get(question) if expected_tables else None
                result = self.process_question(question, expected_table)
                time.sleep(self.request_wait)

                if result["success"]:
                    results["successful_queries"] += 1
                    results["details"].append(result)
                    if show_progress:
                        print(f"✓ 処理成功")
                else:
                    results["failed_questions"].append(result)
                    if show_progress:
                        print(f"× 処理失敗: {result.get('error', '不明なエラー')}")

            # Wait before next batch (except for the last batch)
            if i + self.batch_size < len(questions):
                self._handle_batch_wait(batch_start_time, show_progress)

        # Save and display results
        self._save_results(results)
        if show_progress:
            self._print_summary(results)
        return results

    def process_test_questions(self) -> Dict:
        """
        Process all questions from test_questions.py with correctness validation

        Returns:
            Dictionary containing processing results and statistics
        """
        test_questions = []
        expected_tables = {}

        # Create mapping of questions to expected tables
        for table_id, questions in questions_dict.items():
            for question in questions:
                test_questions.append(question)
                expected_tables[question] = table_id

        return self.process_questions(test_questions, expected_tables)

    def _create_error_result(self, question: str, error: str) -> Dict:
        """Create an error result dictionary"""
        return {
            "success": False,
            "question": question,
            "error": error,
            "timestamp": datetime.now().isoformat()
        }

    def _get_table_info(self, search_results: List[Dict], table_id: str) -> Optional[str]:
        """Get table information from search results"""
        for result in search_results:
            if f"tableId: {table_id}" in result['content']:
                return result['content']
        return None

    def _log_result(self, result: Dict):
        """Log processing result to CSV"""
        self.logger.log_query(
            question=result['question'],
            search_results=result.get('search_results', []),
            selected_table=result.get('selected_table', ''),
            generated_sql=result.get('generated_sql', ''),
            expected_table=result.get('expected_table'),
            is_correct=result.get('table_selection_is_correct')
        )

    def _save_results(self, results: Dict):
        """Save results to JSON file"""
        self.json_manager.save_json(results, prefix="query_results")

    def _print_batch_info(self, total_questions: int):
        """Print initial batch processing information"""
        print("\n" + "="*50)
        print(f"処理開始: 全{total_questions}件")
        print(f"バッチサイズ: {self.batch_size}件")
        print(f"バッチ間待機時間: {self.batch_wait}秒")
        print(f"リクエスト間待機時間: {self.request_wait}秒")
        print("="*50)

    def _print_batch_start(self, current_index: int, total_questions: int):
        """Print batch start information"""
        print(f"\nバッチ {current_index//self.batch_size + 1}/" +
              f"{(total_questions + self.batch_size - 1)//self.batch_size} 開始")
        print(f"処理時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-"*30)

    def _handle_batch_wait(self, batch_start_time: datetime, show_progress: bool):
        """Handle waiting between batches"""
        batch_duration = (datetime.now() - batch_start_time).total_seconds()
        adjusted_wait_time = max(0, self.batch_wait - batch_duration)

        if show_progress and adjusted_wait_time > 0:
            print(f"\nバッチ処理時間: {batch_duration:.1f}秒")
            print(f"次のバッチまで {adjusted_wait_time:.1f}秒 待機します...")

        if adjusted_wait_time > 0:
            time.sleep(adjusted_wait_time)

    def _print_summary(self, results: Dict):
        """Print processing summary"""
        print("\n" + "="*50)
        print("処理完了サマリー")
        print("="*50)
        print(f"総質問数: {results['total_questions']}")
        print(f"成功したクエリ: {results['successful_queries']}/{results['total_questions']} " +
              f"({(results['successful_queries']/results['total_questions']*100):.1f}%)")

        if "correct_table_selections" in results:
            print(f"正しいテーブル選択: {results['correct_table_selections']}/{results['total_questions']} " +
                  f"({results['table_selection_accuracy']:.1f}%)")

        if results["failed_questions"]:
            print("\n失敗した質問の詳細:")
            print("-"*50)
            for failure in results["failed_questions"]:
                print(f"質問: {failure['question']}")
                print(f"エラー: {failure['error']}")
                print("-"*30)

        print("\n出力ファイル:")
        print(f"1. 処理結果サマリー: ./json/query_results_*.json")
        print(f"2. 詳細ログ: ./csv/query_logs.csv")
        if results.get("successful_queries", 0) > 0:
            print(f"3. 生成されたグラフ: ./img/*")


def main():
    """Example usage of QueryProcessor"""
    try:
        # Initialize processor
        processor = QueryProcessor()

        # テスト質問を処理する場合
        test_questions = []
        expected_tables = {}

        # questions_dictから質問とテーブルIDの対応を作成
        for table_id, questions in questions_dict.items():
            for question in questions:
                test_questions.append(question)
                expected_tables[question] = table_id

        # テスト質問を実行（テーブルIDの対応付きで）
        results = processor.process_questions(test_questions, expected_tables)

        # もしくは、process_test_questions()を使用する場合は
        # results = processor.process_test_questions()
        # でもOK

    except Exception as e:
        print(f"\nエラーが発生しました: {str(e)}")
        raise

if __name__ == "__main__":
    main()
