from vertexai.preview.generative_models import GenerativeModel
from typing import List, Dict, Optional
from agent_search_from_engine import search_sample
import os
from dotenv import load_dotenv

load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION")
ENGINE_ID = os.getenv("ENGINE_ID")
MODEL_NAME = os.getenv("MODEL_NAME")


class TableSelector:
    """
    A class to select the most relevant table using Gemini model based on search results
    """
    def __init__(self, model_name: str = MODEL_NAME):
        self.model = GenerativeModel(model_name)

    def _create_prompt(self, question: str, search_results: List[Dict]) -> str:
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
        try:
            prompt = self._create_prompt(question, search_results)
            response = self.model.generate_content(prompt)
            print("\nLLMによるテーブル選択結果:" + response.text.strip())
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
    question = "商品の在庫数を知りたい"

    try:
        # Execute search
        results = search_sample(project_id, location, engine_id, question)

        # Initialize TableSelector and get recommendation
        selector = TableSelector()
        selected_table = selector.select_table(question, results)

        if selected_table:
            print(f"\nLLMによるテーブル選択の回答結果: {selected_table}")
        else:
            print("\n適切なテーブルが見つかりませんでした。")

    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")

if __name__ == "__main__":
    main()
