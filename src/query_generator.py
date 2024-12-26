from vertexai.preview.generative_models import GenerativeModel
from typing import Optional
from table_selector import TableSelector
from agent_search_from_engine import search_sample
from query_logger import QueryLogger

class QueryGenerator:
    """
    A class to generate SQL queries using Gemini model based on the user question and table information
    """
    def __init__(self, model_name: str = "gemini-2.0-flash-exp"):
        """
        Initialize the QueryGenerator with specified model

        Args:
            model_name: Name of the Gemini model to use
        """
        self.model = GenerativeModel(model_name)

    def _create_prompt(self, question: str, table_info: str) -> str:
        """
        Create a prompt for Gemini model to generate SQL query

        Args:
            question: User's original question
            table_info: Information about the selected table including schema and time range

        Returns:
            Formatted prompt string
        """
        prompt = f"""
あなたはSQLクエリを生成する専門家です。
以下の情報を基に、ユーザーの質問に答えるために必要なSQLクエリを生成して下さい。

ユーザーの質問:
{question}

対象テーブルの情報:
{table_info}

以下の要件に従ってSQLクエリを生成して下さい:
1. SELECT句には質問に関連するカラムのみを含める。
2. 回答には生のSQLクエリのみを含める。 (説明や理由は一切不要です)
3. クエリは簡潔で効率的なものにする。
4. 必要に応じてWHERE句やJOIN句を使用する。
5. 集計が必要な場合はGROUP BY句を使用する。

SQLクエリ:
"""
        return prompt

    def _clean_sql_query(self, query: str) -> str:
        """
        Clean SQL query by removing markdown formatting and comments

        Args:
            query: Raw SQL query string that might contain markdown formatting

        Returns:
            Cleaned SQL query string
        """
        # Remove markdown SQL code block
        query = query.replace('```sql', '').replace('```', '')

        # Remove any "# SQL文" or similar comments
        lines = query.split('\n')
        cleaned_lines = [line for line in lines if not line.strip().startswith('#')]

        return '\n'.join(cleaned_lines).strip()

    def generate_query(self, question: str, table_info: str) -> Optional[str]:
        """
        Generate SQL query based on the question and table information

        Args:
            question: User's original question
            table_info: Information about the selected table

        Returns:
            Generated SQL query or None if generation fails
        """
        try:
            prompt = self._create_prompt(question, table_info)
            response = self.model.generate_content(prompt)
            raw_query = response.text.strip()
            return self._clean_sql_query(raw_query)
        except Exception as e:
            print(f"Error generating query: {str(e)}")
            return None

def get_table_info(search_results: list, table_id: str) -> Optional[str]:
    """
    Extract table information for the specified table_id from search results

    Args:
        search_results: List of search results from agent_search
        table_id: Selected table ID

    Returns:
        Table information string or None if not found
    """
    for result in search_results:
        if f"tableId: {table_id}" in result['content']:
            return result['content']
    return None

def main():
    """
    Process multiple questions and generate SQL queries
    """
    # Configuration
    project_id = "business-test-001"
    location = "global"
    engine_id = "test-agent-app_1735199159695"

    # List of questions to process
    questions = [
        "商品の在庫数を知りたい",
        "テーブルの全期間の商品の在庫数の推移のグラフを作成して下さい",
        "直近1週間の商品カテゴリー別の在庫数を教えて下さい",
        "在庫数が10個未満の商品を抽出して下さい",
        "商品別の在庫数の平均値を計算して下さい"
    ]

    # Initialize counters and error tracking
    total_questions = len(questions)
    successful_queries = 0
    failed_questions = []

    try:
        from tqdm import tqdm
        questions_iterator = tqdm(enumerate(questions, 1), total=total_questions, desc="Processing questions")
    except ImportError:
        print("tqdm not installed. Using standard iteration.")
        questions_iterator = enumerate(questions, 1)

    for i, question in questions_iterator:
        print(f"\n{'='*50}")
        print(f"処理開始: 質問 {i}/{total_questions}")
        print(f"質問内容: {question}")
        print(f"{'='*50}\n")

        try:
            # Execute search
            results = search_sample(project_id, location, engine_id, question)

            print("\n=== 検索結果 ===")
            for idx, result in enumerate(results, 1):
                print(f"\n[上位{idx}件目]")
                print(result['content'])
                print("-" * 30)

            # Get table recommendation
            selector = TableSelector()
            selected_table = selector.select_table(question, results)

            if not selected_table:
                print("\n適切なテーブルが見つかりませんでした。")
                failed_questions.append((i, question, "テーブル選択失敗"))
                continue

            # Get table information
            table_info = get_table_info(results, selected_table)
            if not table_info:
                print(f"\nテーブル情報が見つかりませんでした: {selected_table}")
                failed_questions.append((i, question, "テーブル情報取得失敗"))
                continue

            # Generate SQL query
            generator = QueryGenerator()
            sql_query = generator.generate_query(question, table_info)

            if sql_query:
                print("\n=== 生成されたSQLクエリ ===")
                print(sql_query)
                successful_queries += 1
            else:
                print("\nSQLクエリの生成に失敗しました。")
                failed_questions.append((i, question, "SQLクエリ生成失敗"))
                continue

            # Log the results
            logger = QueryLogger()
            logger.log_query(question, results, selected_table, sql_query)

            print(f"\n質問 {i} の処理が完了しました。")

        except Exception as e:
            print(f"\nエラーが発生しました: {str(e)}")
            failed_questions.append((i, question, f"エラー: {str(e)}"))

    # Print summary
    print("\n" + "="*50)
    print("処理完了サマリー")
    print("="*50)
    print(f"総質問数: {total_questions}")
    print(f"成功: {successful_queries}/{total_questions} " +
            f"({(successful_queries/total_questions*100):.1f}%)")
    print(f"失敗: {len(failed_questions)}/{total_questions} " +
            f"({(len(failed_questions)/total_questions*100):.1f}%)")

    if failed_questions:
        print("\n失敗した質問の詳細:")
        print("-"*50)
        for idx, question, reason in failed_questions:
            print(f"質問 {idx}: {question}")
            print(f"理由: {reason}")
            print("-"*30)

if __name__ == "__main__":
    main()