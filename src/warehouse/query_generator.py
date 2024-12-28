from functools import lru_cache
from vertexai.preview.generative_models import GenerativeModel
from typing import Dict, List, Optional
from agent_search_from_engine import search_sample
from query_logger import QueryLogger
from test_questions import questions_dict
import json
from tqdm import tqdm

class OptimizedQueryProcessor:
    def __init__(self,
                    model_name: str = "gemini-2.0-flash-exp",
                    project_id: str = "business-test-001",
                    location: str = "global",
                    engine_id: str = "test-agent-app_1735199159695"):
        """
        Initialize the OptimizedQueryProcessor with a single Gemini model instance
        """
        self.model = GenerativeModel(model_name)
        self.logger = QueryLogger()
        self.project_id = project_id
        self.location = location
        self.engine_id = engine_id

    @lru_cache(maxsize=128)
    def get_search_results(self, question: str) -> List[Dict]:
        """
        Get and cache search results for a given question
        """
        return search_sample(self.project_id, self.location, self.engine_id, question)

    def select_table(self, question: str, search_results: List[Dict]) -> Optional[str]:
        """
        Select the most relevant table using the Gemini model
        """
        prompt = self._create_table_selection_prompt(question, search_results)
        try:
            response = self.model.generate_content(prompt)
            selected_table = response.text.strip()
            print(f"\nLLMによるテーブル選択結果: {selected_table}")
            return selected_table
        except Exception as e:
            print(f"Error in table selection: {str(e)}")
            return None

    def generate_query(self, question: str, table_info: str) -> Optional[str]:
        """
        Generate SQL query using the Gemini model
        """
        prompt = self._create_query_prompt(question, table_info)
        try:
            response = self.model.generate_content(prompt)
            return self._clean_sql_query(response.text)
        except Exception as e:
            print(f"Error in query generation: {str(e)}")
            return None

    def process_question(self, question: str, expected_table: str = None) -> Dict:
        """
        Process a single question through the pipeline
        """
        try:
            # Get cached search results
            search_results = self.get_search_results(question)

            # Select table
            selected_table = self.select_table(question, search_results)
            if not selected_table:
                return self._create_error_result(question, expected_table, "テーブル選択失敗")

            # Get table info
            table_info = self._get_table_info(search_results, selected_table)
            if not table_info:
                return self._create_error_result(question, expected_table, "テーブル情報取得失敗")

            # Generate SQL query
            sql_query = self.generate_query(question, table_info)
            if not sql_query:
                return self._create_error_result(question, expected_table, "SQLクエリ生成失敗")

            # Log the successful result
            self.logger.log_query(
                question,
                search_results,
                selected_table,
                sql_query,
                expected_table,
                selected_table == expected_table if expected_table else None
            )

            return {
                "success": True,
                "question": question,
                "selected_table": selected_table,
                "expected_table": expected_table,
                "is_correct": selected_table == expected_table if expected_table else None,
                "generated_query": sql_query
            }

        except Exception as e:
            return self._create_error_result(question, expected_table, str(e))

    def process_all_questions(self) -> Dict:
        """
        Process all questions from the test dictionary
        """
        results = {
            "total_questions": 0,
            "correct_table_selections": 0,
            "successful_queries": 0,
            "failed_questions": [],
            "details": []
        }

        # Prepare test cases
        test_cases = []
        for table_id, questions in questions_dict.items():
            for question in questions:
                test_cases.append((question, table_id))
                results["total_questions"] += 1

        # Process each test case
        for question, expected_table in tqdm(test_cases, desc="Processing questions"):
            print(f"\n処理開始: {question}")

            result = self.process_question(question, expected_table)

            if result["success"]:
                results["successful_queries"] += 1
                if result["is_correct"]:
                    results["correct_table_selections"] += 1
                results["details"].append(result)
            else:
                results["failed_questions"].append(result)

        return results

    def _create_table_selection_prompt(self, question: str, search_results: List[Dict]) -> str:
        """
        Create prompt for table selection
        """
        prompt = f"""
質問: {question}

以下の検索結果の中から、質問に最も関連性の高いテーブルのtableIdを1つ選んでください。
回答は tableId の名称の文字列のみを返してください。理由や説明は一切不要です。

検索結果:
"""
        for result in search_results:
            prompt += f"\n{result['content']}\n---"
        return prompt

    def _create_query_prompt(self, question: str, table_info: str) -> str:
        """
        Create prompt for SQL query generation
        """
        return f"""
あなたはSQLクエリを生成する専門家です。
以下の情報を基に、ユーザーの質問に答えるために必要なSQLクエリを生成して下さい。

ユーザーの質問:
{question}

対象テーブルの情報:
{table_info}

以下の要件に従ってSQLクエリを生成して下さい:
1. SELECT句には質問に関連するカラムのみを含める。
2. 回答には生のSQLクエリのみを含める。(説明や理由は一切不要です)
3. クエリは簡潔で効率的なものにする。
4. 必要に応じてWHERE句やJOIN句を使用する。
5. 集計が必要な場合はGROUP BY句を使用する。

SQLクエリ:
"""

    def _clean_sql_query(self, query: str) -> str:
        """
        Clean and format the generated SQL query
        """
        query = query.replace('```sql', '').replace('```', '')
        lines = query.split('\n')
        cleaned_lines = [line for line in lines if not line.strip().startswith('#')]
        return '\n'.join(cleaned_lines).strip()

    def _get_table_info(self, search_results: List[Dict], table_id: str) -> Optional[str]:
        """
        Get table information from search results
        """
        for result in search_results:
            if f"tableId: {table_id}" in result['content']:
                return result['content']
        return None

    def _create_error_result(self, question: str, expected_table: str, error: str) -> Dict:
        """
        Create a standardized error result dictionary
        """
        return {
            "success": False,
            "question": question,
            "expected_table": expected_table,
            "error": error
        }

def main():
    """
    Main execution function
    """
    processor = OptimizedQueryProcessor()
    results = processor.process_all_questions()

    # Display summary
    print("\n" + "="*50)
    print("処理完了サマリー")
    print("="*50)
    print(f"総質問数: {results['total_questions']}")
    print(f"正しいテーブル選択: {results['correct_table_selections']}/{results['total_questions']} " +
          f"({(results['correct_table_selections']/results['total_questions']*100):.1f}%)")
    print(f"成功したクエリ生成: {results['successful_queries']}/{results['total_questions']} " +
          f"({(results['successful_queries']/results['total_questions']*100):.1f}%)")

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

    # Save results to JSON
    with open("test_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
