from typing import Dict, List, Optional
from datetime import datetime
import time
from tqdm import tqdm

from query_processor import QueryProcessor
from test_questions import questions_dict
from utils.json_utils import JSONOutputManager
from query_logger import QueryLogger

class BatchProcessor:
    def __init__(self,
                batch_size: int = 5,
                batch_wait: int = 60,
                request_wait: int = 3):
        """
        Initialize the BatchProcessor

        Args:
            batch_size: Number of questions to process in each batch
            batch_wait: Wait time between batches (seconds)
            request_wait: Wait time between requests (seconds)
        """
        self.batch_size = batch_size
        self.batch_wait = batch_wait
        self.request_wait = request_wait
        self.processor = QueryProcessor()
        self.json_manager = JSONOutputManager()
        self.logger = QueryLogger()

    def process_batch(self, questions: List[str]) -> Dict:
        """
        Process a batch of questions with rate limiting

        Args:
            questions: List of questions to process

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

        print("\n" + "="*50)
        print(f"処理開始: 全{len(questions)}件")
        print(f"バッチサイズ: {self.batch_size}件")
        print(f"バッチ間待機時間: {self.batch_wait}秒")
        print(f"リクエスト間待機時間: {self.request_wait}秒")
        print("="*50)

        # Process questions in batches
        for i in range(0, len(questions), self.batch_size):
            batch = questions[i:i + self.batch_size]
            batch_start_time = datetime.now()

            print(f"\nバッチ {i//self.batch_size + 1}/{(len(questions) + self.batch_size - 1)//self.batch_size} 開始")
            print(f"処理時刻: {batch_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("-"*30)

            # Process each question in the batch
            for j, question in enumerate(batch, 1):
                print(f"\n質問 {j}/{len(batch)} 処理中:")
                print(f"質問: {question}")

                # Process question with wait time
                result = self.processor.process_question(question)
                time.sleep(self.request_wait)

                if result["success"]:
                    results["successful_queries"] += 1
                    results["details"].append(result)
                    print(f"✓ 処理成功")

                    # Log successful query to CSV
                    self.logger.log_query(
                        question=question,
                        search_results=result.get('search_results', []),
                        selected_table=result.get('selected_table', ''),
                        generated_sql=result.get('generated_sql', ''),
                        executed_function=result.get('executed_function'),
                        function_result=result.get('function_result'),
                        final_answer=result.get('final_answer'),
                        output_file=result.get('output_file')
                    )
                else:
                    results["failed_questions"].append(result)
                    print(f"× 処理失敗: {result.get('error', '不明なエラー')}")

            # Wait before next batch (except for the last batch)
            if i + self.batch_size < len(questions):
                batch_end_time = datetime.now()
                batch_duration = (batch_end_time - batch_start_time).total_seconds()
                adjusted_wait_time = max(0, self.batch_wait - batch_duration)

                print(f"\nバッチ処理時間: {batch_duration:.1f}秒")
                if adjusted_wait_time > 0:
                    print(f"次のバッチまで {adjusted_wait_time:.1f}秒 待機します...")
                    time.sleep(adjusted_wait_time)

        # Save results
        self._save_results(results)
        self._print_summary(results)
        return results

    def process_test_questions(self) -> Dict:
        """
        Process all test questions from questions_dict

        Returns:
            Dictionary containing processing results and statistics
        """
        test_questions = []
        # Extract questions from questions_dict
        for table_id, questions in questions_dict.items():
            test_questions.extend(questions)

        return self.process_batch(test_questions)

    def _print_summary(self, results: Dict):
        """Print summary of processing results"""
        print("\n" + "="*50)
        print("処理完了サマリー")
        print("="*50)
        print(f"総質問数: {results['total_questions']}")
        print(f"成功したクエリ: {results['successful_queries']}/{results['total_questions']} " +
              f"({(results['successful_queries']/results['total_questions']*100):.1f}%)")

        if results["failed_questions"]:
            print("\n失敗した質問の詳細:")
            print("-"*50)
            for failure in results["failed_questions"]:
                print(f"質問: {failure['question']}")
                print(f"エラー: {failure['error']}")
                print("-"*30)

        print("\n出力ファイル:")
        print(f"1. 処理結果サマリー: ./json/batch_results_*.json")
        print(f"2. 詳細ログ: ./csv/query_logs.csv")
        if results.get("successful_queries", 0) > 0:
            print(f"3. 生成されたグラフ: ./img/*")

    def _save_results(self, results: Dict):
        """Save results to JSON file"""
        self.json_manager.save_json(results, prefix="batch_results")


def main():
    """
    バッチ処理の実行例
    """
    try:
        # Initialize and run processor
        processor = BatchProcessor(
            batch_size=5,
            batch_wait=60,
            request_wait=3
        )

        # Process test questions
        results = processor.process_test_questions()

    except Exception as e:
        print(f"\nエラーが発生しました: {str(e)}")
        raise

if __name__ == "__main__":
    main()
