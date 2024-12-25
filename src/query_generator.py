from vertexai.preview.generative_models import GenerativeModel
from typing import Optional
from table_selector import TableSelector
from agent_search_from_engine import search_sample
from dotenv import load_dotenv
import os

load_dotenv()
LOCATION = os.getenv("LOCATION")
PROJECT_ID = os.getenv("PROJECT_ID")
ENGINE_ID = os.getenv("ENGINE_ID")


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
            table_info: Information about the selected table including schema

        Returns:
            Formatted prompt string
        """
        prompt = f"""
あなたはSQLクエリを生成する専門家です。
以下の情報を基に、ユーザーの質問に答えるために必要なSQLクエリを生成してください。

ユーザーの質問: {question}

対象テーブルの情報:
{table_info}

以下の要件に従ってSQLクエリを生成してください：
1. SELECT句には質問に関連するカラムのみを含めてください
2. 回答には生のSQLクエリのみを含めてください（説明や理由は不要です）
3. クエリは簡潔で効率的なものにしてください
4. 必要に応じてWHERE句やJOIN句を使用してください
5. 集計が必要な場合はGROUP BY句を使用してください

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
    Example usage of QueryGenerator
    """
    # Configuration
    project_id = PROJECT_ID
    location = LOCATION
    engine_id = ENGINE_ID
    search_query = "商品の在庫数を知りたい"

    try:
        # Execute search
        results = search_sample(project_id, location, engine_id, search_query)

        # Get table recommendation
        selector = TableSelector()
        selected_table = selector.select_table(search_query, results)

        if not selected_table:
            print("\n適切なテーブルが見つかりませんでした。")
            return

        # Get table information
        table_info = get_table_info(results, selected_table)
        if not table_info:
            print(f"\nテーブル情報が見つかりませんでした: {selected_table}")
            return

        # Generate SQL query
        generator = QueryGenerator()
        sql_query = generator.generate_query(search_query, table_info)

        if sql_query:
            print("\n=== 生成されたSQLクエリ ===")
            print(sql_query)
        else:
            print("\nSQLクエリの生成に失敗しました。")

    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")

if __name__ == "__main__":
    main()
