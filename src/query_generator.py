from vertexai.preview.generative_models import GenerativeModel
from typing import Optional
from table_selector import TableSelector
from agent_search_from_engine import search_sample
from query_logger import QueryLogger

import json
from typing import Optional, Dict, List
from vertexai.preview.generative_models import GenerativeModel
from table_selector import TableSelector
from agent_search_from_engine import search_sample
from query_logger import QueryLogger
from test_questions import questions_dict

class QueryGenerator:
    def __init__(self, model_name: str = "gemini-2.0-flash-exp"):
        self.model = GenerativeModel(model_name)

    def _create_prompt(self, question: str, table_info: str) -> str:
        prompt = f"""
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
        return prompt

    def _clean_sql_query(self, query: str) -> str:
        query = query.replace('```sql', '').replace('```', '')
        lines = query.split('\n')
        cleaned_lines = [line for line in lines if not line.strip().startswith('#')]
        return '\n'.join(cleaned_lines).strip()

    def generate_query(self, question: str, table_info: str) -> Optional[str]:
        try:
            prompt = self._create_prompt(question, table_info)
            response = self.model.generate_content(prompt)
            raw_query = response.text.strip()
            return self._clean_sql_query(raw_query)
        except Exception as e:
            print(f"Error generating query: {str(e)}")
            return None

def get_table_info(search_results: list, table_id: str) -> Optional[str]:
    for result in search_results:
        if f"tableId: {table_id}" in result['content']:
            return result['content']
    return None

def process_questions(project_id: str = "business-test-001",
                        location: str = "global",
                        engine_id: str = "test-agent-app_1735199159695") -> Dict:

    results = {
        "total_questions": 0,
        "correct_table_selections": 0,
        "successful_queries": 0,
        "failed_questions": [],
        "details": []
    }

    selector = TableSelector()
    generator = QueryGenerator()
    logger = QueryLogger()

    test_cases = []
    for table_id, questions in questions_dict.items():
        for question in questions:
            test_cases.append((question, table_id))
            results["total_questions"] += 1

    try:
        from tqdm import tqdm
        test_iterator = tqdm(test_cases, desc="Processing questions")
    except ImportError:
        test_iterator = test_cases

    for question, expected_table in test_iterator:
        try:
            print(f"\n処理開始: {question}")

            results_entry = {
                "question": question,
                "expected_table": expected_table,
            }

            # 検索実行
            search_results = search_sample(project_id, location, engine_id, question)

            # テーブル選択
            selected_table = selector.select_table(question, search_results)

            if not selected_table:
                error_msg = "テーブル選択失敗"
                results["failed_questions"].append({**results_entry, "error": error_msg})
                continue

            results_entry["selected_table"] = selected_table
            is_correct = selected_table == expected_table

            if is_correct:
                results["correct_table_selections"] += 1

            # テーブル情報取得とクエリ生成
            table_info = get_table_info(search_results, selected_table)
            if not table_info:
                error_msg = "テーブル情報取得失敗"
                results["failed_questions"].append({**results_entry, "error": error_msg})
                continue

            sql_query = generator.generate_query(question, table_info)

            if sql_query:
                results["successful_queries"] += 1
                results_entry["generated_query"] = sql_query
                results["details"].append(results_entry)

                # CSVログ出力
                logger.log_query(
                    question,
                    search_results,
                    selected_table,
                    sql_query,
                    expected_table,
                    is_correct
                )
            else:
                error_msg = "SQLクエリ生成失敗"
                results["failed_questions"].append({**results_entry, "error": error_msg})

        except Exception as e:
            error_msg = f"エラー: {str(e)}"
            results["failed_questions"].append({
                "question": question,
                "expected_table": expected_table,
                "error": error_msg
            })

    return results

def main():
    results = process_questions()

    print("\n" + "="*50)
    print("処理完了サマリー")
    print("="*50)
    print(f"総質問数: {results['total_questions']}")
    print(f"正しいテーブル選択: {results['correct_table_selections']}/{results['total_questions']} " +
            f"({(results['correct_table_selections']/results['total_questions']*100):.1f}%)")
    print(f"成功したクエリ生成: {results['successful_queries']}/{results['total_questions']} " +
            f"({(results['successful_queries']/results['total_questions']*100):.1f}%)")

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

    # JSON出力
    with open("test_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
