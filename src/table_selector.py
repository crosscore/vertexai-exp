from vertexai.preview.generative_models import GenerativeModel
from typing import List, Dict, Optional
from agent_search_from_engine import search_sample
from dotenv import load_dotenv
import os

load_dotenv()
LOCATION = os.getenv("LOCATION")
PROJECT_ID = os.getenv("PROJECT_ID")
ENGINE_ID = os.getenv("ENGINE_ID")

class TableSelector:
    """
    A class to select the most relevant table using Gemini model based on search results
    """
    def __init__(self, model_name: str = "gemini-2.0-flash-exp"):
        """
        Initialize the TableSelector with specified model

        Args:
            model_name: Name of the Gemini model to use
        """
        self.model = GenerativeModel(model_name)

    def _create_prompt(self, question: str, search_results: List[Dict]) -> str:
        """
        Create a prompt for Gemini model based on the question and search results

        Args:
            question: User's original question
            search_results: List of search results containing table information

        Returns:
            Formatted prompt string
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

    def select_table(self, question: str, search_results: List[Dict]) -> Optional[str]:
        """
        Select the most relevant table for the given question

        Args:
            question: User's original question
            search_results: List of search results from agent_search

        Returns:
            Selected table ID or None if no table is found
        """
        try:
            prompt = self._create_prompt(question, search_results)
            response = self.model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            print(f"Error selecting table: {str(e)}")
            return None

def main():
    """
    Example usage of TableSelector
    """
    # Configuration
    project_id = PROJECT_ID
    location = LOCATION
    engine_id = ENGINE_ID
    search_query = "商品の在庫数を知りたい"

    try:
        # Execute search
        results = search_sample(project_id, location, engine_id, search_query)

        # Initialize TableSelector and get recommendation
        selector = TableSelector()
        selected_table = selector.select_table(search_query, results)

        if selected_table:
            print(f"\nLLMによるテーブル選択の回答結果: {selected_table}")
        else:
            print("\n適切なテーブルが見つかりませんでした。")

    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")

if __name__ == "__main__":
    main()
