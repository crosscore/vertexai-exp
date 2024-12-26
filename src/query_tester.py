from typing import Dict, List, Optional
import json
from query_generator import QueryGenerator, get_table_info
from table_selector import TableSelector
from agent_search_from_engine import search_sample
from query_logger import QueryLogger
from test_questions import questions_dict

class QueryTester:
    """
    A class to test query generation against expected tables
    """
    def __init__(self, project_id: str = "business-test-001",
                    location: str = "global",
                    engine_id: str = "test-agent-app_1735199159695"):
        self.project_id = project_id
        self.location = location
        self.engine_id = engine_id
        self.table_selector = TableSelector()
        self.query_generator = QueryGenerator()
        self.logger = QueryLogger()

    def test_queries(self, questions_dict: Dict[str, List[str]]) -> Dict:
        """
        Test query generation for all questions and validate table selection

        Args:
            questions_dict: Dictionary mapping table IDs to lists of test questions

        Returns:
            Dictionary containing test results and statistics
        """
        results = {
            "total_questions": 0,
            "correct_table_selections": 0,
            "successful_queries": 0,
            "failed_questions": [],
            "details": []
        }

        # Flatten questions for processing while keeping track of expected tables
        test_cases = []
        for table_id, questions in questions_dict.items():
            for question in questions:
                test_cases.append((question, table_id))
                results["total_questions"] += 1

        try:
            from tqdm import tqdm
            test_iterator = tqdm(test_cases, desc="Processing test cases")
        except ImportError:
            print("tqdm not installed. Using standard iteration.")
            test_iterator = test_cases

        for question, expected_table in test_iterator:
            try:
                print(f"\n{'='*50}")
                print(f"Testing question: {question}")
                print(f"Expected table: {expected_table}")
                print(f"{'='*50}\n")

                # Get search results
                search_results = search_sample(
                    self.project_id,
                    self.location,
                    self.engine_id,
                    question
                )

                # Select table
                selected_table = self.table_selector.select_table(question, search_results)

                if not selected_table:
                    results["failed_questions"].append({
                        "question": question,
                        "expected_table": expected_table,
                        "error": "テーブル選択失敗"
                    })
                    continue

                # Check table selection accuracy
                table_correct = selected_table == expected_table
                if table_correct:
                    results["correct_table_selections"] += 1

                # Get table info and generate query
                table_info = get_table_info(search_results, selected_table)
                if not table_info:
                    results["failed_questions"].append({
                        "question": question,
                        "expected_table": expected_table,
                        "selected_table": selected_table,
                        "error": "テーブル情報取得失敗"
                    })
                    continue

                sql_query = self.query_generator.generate_query(question, table_info)

                if sql_query:
                    results["successful_queries"] += 1
                    # Log successful query
                    self.logger.log_query(
                        question,
                        search_results,
                        selected_table,
                        sql_query
                    )
                else:
                    results["failed_questions"].append({
                        "question": question,
                        "expected_table": expected_table,
                        "selected_table": selected_table,
                        "error": "SQLクエリ生成失敗"
                    })
                    continue

                # Store detailed results
                results["details"].append({
                    "question": question,
                    "expected_table": expected_table,
                    "selected_table": selected_table,
                    "table_selection_correct": table_correct,
                    "generated_query": sql_query
                })

            except Exception as e:
                results["failed_questions"].append({
                    "question": question,
                    "expected_table": expected_table,
                    "error": f"エラー: {str(e)}"
                })

        return results

def main():
    """
    Run tests and display results
    """
    tester = QueryTester()
    results = tester.test_queries(questions_dict)

    # Display summary
    print("\n" + "="*50)
    print("テスト結果サマリー")
    print("="*50)
    print(f"総質問数: {results['total_questions']}")
    print(f"正しいテーブル選択: {results['correct_table_selections']}/{results['total_questions']} " +
            f"({(results['correct_table_selections']/results['total_questions']*100):.1f}%)")
    print(f"成功したクエリ生成: {results['successful_queries']}/{results['total_questions']} " +
            f"({(results['successful_queries']/results['total_questions']*100):.1f}%)")
    print(f"失敗した質問数: {len(results['failed_questions'])}/{results['total_questions']} " +
            f"({(len(results['failed_questions'])/results['total_questions']*100):.1f}%)")

    # Display failed questions
    if results["failed_questions"]:
        print("\n失敗した質問の詳細:")
        print("-"*50)
        for failure in results["failed_questions"]:
            print(f"質問: {failure['question']}")
            print(f"期待されたテーブル: {failure['expected_table']}")
            if 'selected_table' in failure:
                print(f"選択されたテーブル: {failure['selected_table']}")
            print(f"エラー: {failure['error']}")
            print("-"*30)

    # Save detailed results to file
    with open("test_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
