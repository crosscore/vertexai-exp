from vertexai.preview.generative_models import GenerativeModel
from typing import Dict, List, Optional
from agent_search_from_engine import search_sample
from query_logger import QueryLogger
from test_questions import questions_dict
import json
from tqdm import tqdm
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION")
ENGINE_ID = os.getenv("ENGINE_ID")
MODEL_NAME = os.getenv("MODEL_NAME")

class SQLQueryGeneratorTester:
    def __init__(self,
                    model_name: str = MODEL_NAME,
                    project_id: str = PROJECT_ID,
                    location: str = LOCATION,
                    engine_id: str = ENGINE_ID,
                    cache_dir: str = "./cache"):
        """
        Initialize the unified test runner
        """
        self.model = GenerativeModel(model_name)
        self.logger = QueryLogger()
        self.project_id = project_id
        self.location = location
        self.engine_id = engine_id
        self.cache_dir = cache_dir
        self._ensure_cache_dir()

        # メモリ内キャッシュ
        self.search_results_cache = {}
        self.table_selection_cache = {}
        self.query_generation_cache = {}

    def _ensure_cache_dir(self):
        """キャッシュディレクトリの作成"""
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    def _get_cache_path(self, cache_type: str) -> str:
        """キャッシュファイルパスの生成"""
        return os.path.join(self.cache_dir, f"{cache_type}_cache.json")

    def _load_cache(self, cache_type: str) -> Dict:
        """キャッシュの読み込み"""
        cache_path = self._get_cache_path(cache_type)
        if os.path.exists(cache_path):
            with open(cache_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def _save_cache(self, cache_type: str, cache_data: Dict):
        """キャッシュの保存"""
        cache_path = self._get_cache_path(cache_type)
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)

    def get_search_results(self, question: str) -> List[Dict]:
        """
        検索結果の取得（キャッシュ対応）
        """
        # メモリキャッシュをチェック
        if question in self.search_results_cache:
            return self.search_results_cache[question]

        # ファイルキャッシュをチェック
        cache = self._load_cache('search_results')
        if question in cache:
            self.search_results_cache[question] = cache[question]
            return cache[question]

        # API呼び出し
        results = search_sample(self.project_id, self.location, self.engine_id, question)

        # キャッシュに保存
        self.search_results_cache[question] = results
        cache[question] = results
        self._save_cache('search_results', cache)

        return results

    def select_table(self, question: str, search_results: List[Dict]) -> Optional[str]:
        """
        テーブル選択（キャッシュ対応）
        """
        cache_key = f"{question}"

        # メモリキャッシュをチェック
        if cache_key in self.table_selection_cache:
            return self.table_selection_cache[cache_key]

        # ファイルキャッシュをチェック
        cache = self._load_cache('table_selection')
        if cache_key in cache:
            self.table_selection_cache[cache_key] = cache[cache_key]
            return cache[cache_key]

        # API呼び出し
        prompt = self._create_table_selection_prompt(question, search_results)
        try:
            response = self.model.generate_content(prompt)
            selected_table = response.text.strip()

            # キャッシュに保存
            self.table_selection_cache[cache_key] = selected_table
            cache[cache_key] = selected_table
            self._save_cache('table_selection', cache)

            return selected_table
        except Exception as e:
            print(f"Error in table selection: {str(e)}")
            return None

    def generate_query(self, question: str, table_info: str) -> Optional[str]:
        """
        SQLクエリ生成（キャッシュ対応）
        """
        cache_key = f"{question}_{table_info}"

        # メモリキャッシュをチェック
        if cache_key in self.query_generation_cache:
            return self.query_generation_cache[cache_key]

        # ファイルキャッシュをチェック
        cache = self._load_cache('query_generation')
        if cache_key in cache:
            self.query_generation_cache[cache_key] = cache[cache_key]
            return cache[cache_key]

        # API呼び出し
        prompt = self._create_query_prompt(question, table_info)
        try:
            response = self.model.generate_content(prompt)
            query = self._clean_sql_query(response.text)

            # キャッシュに保存
            self.query_generation_cache[cache_key] = query
            cache[cache_key] = query
            self._save_cache('query_generation', cache)

            return query
        except Exception as e:
            print(f"Error in query generation: {str(e)}")
            return None

    def process_questions(self) -> Dict:
        """
        全テストケースの処理実行
        """
        results = {
            "total_questions": 0,
            "correct_table_selections": 0,
            "successful_queries": 0,
            "failed_questions": [],
            "details": [],
            "timestamp": datetime.now().isoformat()
        }

        test_cases = []
        for table_id, questions in questions_dict.items():
            for question in questions:
                test_cases.append((question, table_id))
                results["total_questions"] += 1

        for question, expected_table in tqdm(test_cases, desc="Processing questions"):
            print(f"\n処理開始: {question}")
            result = self._process_single_question(question, expected_table)

            if result["success"]:
                results["successful_queries"] += 1
                if result["is_correct"]:
                    results["correct_table_selections"] += 1
                results["details"].append(result)
            else:
                results["failed_questions"].append(result)

        # 結果の保存
        self._save_results(results)
        return results

    def _process_single_question(self, question: str, expected_table: str) -> Dict:
        """
        単一の質問処理
        """
        try:
            search_results = self.get_search_results(question)
            selected_table = self.select_table(question, search_results)

            if not selected_table:
                return self._create_error_result(question, expected_table, "テーブル選択失敗")

            table_info = self._get_table_info(search_results, selected_table)
            if not table_info:
                return self._create_error_result(question, expected_table, "テーブル情報取得失敗")

            sql_query = self.generate_query(question, table_info)
            if not sql_query:
                return self._create_error_result(question, expected_table, "SQLクエリ生成失敗")

            # ログ出力
            self.logger.log_query(
                question,
                search_results,
                selected_table,
                sql_query,
                expected_table,
                selected_table == expected_table
            )

            return {
                "success": True,
                "question": question,
                "selected_table": selected_table,
                "expected_table": expected_table,
                "is_correct": selected_table == expected_table,
                "generated_query": sql_query,
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            return self._create_error_result(question, expected_table, str(e))

    def _create_table_selection_prompt(self, question: str, search_results: List[Dict]) -> str:
        """テーブル選択用プロンプト生成"""
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
        """SQLクエリ生成用プロンプト生成"""
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
        """SQLクエリのクリーニング"""
        query = query.replace('```sql', '').replace('```', '')
        lines = query.split('\n')
        cleaned_lines = [line for line in lines if not line.strip().startswith('#')]
        return '\n'.join(cleaned_lines).strip()

    def _get_table_info(self, search_results: List[Dict], table_id: str) -> Optional[str]:
        """テーブル情報の取得"""
        for result in search_results:
            if f"tableId: {table_id}" in result['content']:
                return result['content']
        return None

    def _create_error_result(self, question: str, expected_table: str, error: str) -> Dict:
        """エラー結果の生成"""
        return {
            "success": False,
            "question": question,
            "expected_table": expected_table,
            "error": error,
            "timestamp": datetime.now().isoformat()
        }

    def _save_results(self, results: Dict):
        """テスト結果の保存"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"test_results_{timestamp}.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

def main():
    import time
    from datetime import datetime

    BATCH_SIZE = 5        # 1バッチあたりの質問数
    BATCH_WAIT = 60       # バッチ間の待機時間（秒）
    REQUEST_WAIT = 3      # 各APIリクエスト間の待機時間（秒）

    # SQLQueryGeneratorTesterのインスタンス化
    runner = SQLQueryGeneratorTester()

    # APIリクエストの前に待機を入れる関数をオーバーライド
    original_search = runner.get_search_results
    original_select = runner.select_table
    original_generate = runner.generate_query

    def add_delay_decorator(func):
        def wrapper(*args, **kwargs):
            time.sleep(REQUEST_WAIT)
            return func(*args, **kwargs)
        return wrapper

    runner.get_search_results = add_delay_decorator(original_search)
    runner.select_table = add_delay_decorator(original_select)
    runner.generate_query = add_delay_decorator(original_generate)

    # テストケースの準備
    test_cases = []
    for table_id, questions in questions_dict.items():
        for question in questions:
            test_cases.append((question, table_id))

    # 結果格納用の辞書
    results = {
        "total_questions": len(test_cases),
        "correct_table_selections": 0,
        "successful_queries": 0,
        "failed_questions": [],
        "details": [],
        "timestamp": datetime.now().isoformat()
    }

    print("\n" + "="*50)
    print(f"テスト開始: 全{len(test_cases)}件")
    print(f"バッチサイズ: {BATCH_SIZE}件")
    print(f"バッチ間待機時間: {BATCH_WAIT}秒")
    print(f"リクエスト間待機時間: {REQUEST_WAIT}秒")
    print("="*50)

    # バッチ処理の実行
    for i in range(0, len(test_cases), BATCH_SIZE):
        batch = test_cases[i:i + BATCH_SIZE]
        batch_start_time = datetime.now()

        print(f"\nバッチ {i//BATCH_SIZE + 1}/{(len(test_cases) + BATCH_SIZE - 1)//BATCH_SIZE} 開始")
        print(f"処理時刻: {batch_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("-"*30)

        # バッチ内の各質問を処理
        for j, (question, expected_table) in enumerate(batch, 1):
            print(f"\n質問 {j}/{len(batch)} 処理中:")
            print(f"質問: {question}")

            result = runner._process_single_question(question, expected_table)

            if result["success"]:
                if result["is_correct"]:
                    results["correct_table_selections"] += 1
                results["successful_queries"] += 1
                results["details"].append(result)
                print(f"✓ 処理成功 (選択テーブル: {result['selected_table']})")
            else:
                results["failed_questions"].append(result)
                print("× 処理失敗:", result["error"])

        # 次のバッチの前に待機（最後のバッチを除く）
        if i + BATCH_SIZE < len(test_cases):
            batch_end_time = datetime.now()
            batch_duration = (batch_end_time - batch_start_time).total_seconds()
            adjusted_wait_time = max(0, BATCH_WAIT - batch_duration)

            print(f"\nバッチ処理時間: {batch_duration:.1f}秒")
            if adjusted_wait_time > 0:
                print(f"次のバッチまで {adjusted_wait_time:.1f}秒 待機します...")
                time.sleep(adjusted_wait_time)

    # 結果の表示
    print("\n" + "="*50)
    print("処理完了サマリー")
    print("="*50)
    print(f"総質問数: {results['total_questions']}")
    print(f"正しいテーブル選択: {results['correct_table_selections']}/{results['total_questions']} " +
            f"({(results['correct_table_selections']/results['total_questions']*100):.1f}%)")
    print(f"成功したクエリ生成: {results['successful_queries']}/{results['total_questions']} " +
            f"({(results['successful_queries']/results['total_questions']*100):.1f}%)")

    # 失敗した質問の詳細表示
    if results["failed_questions"]:
        print("\n失敗した質問の詳細:")
        print("-"*50)
        for failure in results["failed_questions"]:
            print(f"質問: {failure['question']}")
            print(f"期待されたテーブル: {failure['expected_table']}")
            print(f"エラー: {failure['error']}")
            print("-"*30)

    # 結果の保存
    runner._save_results(results)

if __name__ == "__main__":
    main()
