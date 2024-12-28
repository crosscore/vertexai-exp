from typing import Dict, List, Optional, Any
from vertexai.preview.generative_models import GenerativeModel, Tool, FunctionDeclaration
from datetime import datetime
import json
import time
from functions import DataAnalyzer, GraphGenerator, QueryExecutor
from query_logger import QueryLogger
import os
from dotenv import load_dotenv

load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION")
ENGINE_ID = os.getenv("ENGINE_ID")
MODEL_NAME = os.getenv("MODEL_NAME")

class ResultGenerator:
    def __init__(self,
                    project_id: str,
                    model_name: str = MODEL_NAME,
                    cache_dir: str = "./cache",
                    output_dir: str = "./img"):
        """
        Initialize the ResultGenerator

        Args:
            project_id: GCP project ID
            model_name: Name of the Gemini model to use
            cache_dir: Directory for cache files
            output_dir: Directory for output files
        """
        # Initialize components
        self.model = GenerativeModel(model_name)
        self.query_executor = QueryExecutor(project_id)
        self.graph_generator = GraphGenerator(output_dir)
        self.data_analyzer = DataAnalyzer()
        self.logger = QueryLogger()

        # Cache settings
        self.cache_dir = cache_dir
        self._ensure_cache_dir()

        # Memory cache
        self.function_results_cache = {}
        self.final_answer_cache = {}

        # Initialize function declarations for the model
        self.tools = self._initialize_tools()

    def _ensure_cache_dir(self):
        """Ensure cache directory exists"""
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    def _get_cache_path(self, cache_type: str) -> str:
        """Get cache file path"""
        return os.path.join(self.cache_dir, f"{cache_type}_cache.json")

    def _load_cache(self, cache_type: str) -> Dict:
        """Load cache from file"""
        cache_path = self._get_cache_path(cache_type)
        if os.path.exists(cache_path):
            with open(cache_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def _save_cache(self, cache_type: str, cache_data: Dict):
        """Save cache to file"""
        cache_path = self._get_cache_path(cache_type)
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)

    def _initialize_tools(self) -> Tool:
        """Initialize function declarations for the model"""
        function_declarations = [
            # SQL Query Execution
            FunctionDeclaration(
                name="execute_query",
                description="Execute a SQL query and return the results",
                parameters={
                    "type": "object",
                    "properties": {
                        "sql_query": {
                            "type": "string",
                            "description": "SQL query to execute",
                        }
                    },
                    "required": ["sql_query"]
                }
            ),
            # Statistical Analysis Functions
            FunctionDeclaration(
                name="analyze_data",
                description="Perform statistical analysis on data",
                parameters={
                    "type": "object",
                    "properties": {
                        "analysis_type": {
                            "type": "string",
                            "description": "Type of analysis (max, min, average, outliers)",
                        },
                        "column_name": {
                            "type": "string",
                            "description": "Column name to analyze",
                        }
                    },
                    "required": ["analysis_type", "column_name"]
                }
            ),
            # Graph Generation
            FunctionDeclaration(
                name="generate_graph",
                description="Generate a graph from the data",
                parameters={
                    "type": "object",
                    "properties": {
                        "graph_type": {
                            "type": "string",
                            "description": "Type of graph (time_series, bar_chart)",
                        },
                        "x_column": {
                            "type": "string",
                            "description": "Column name for x-axis",
                        },
                        "y_column": {
                            "type": "string",
                            "description": "Column name for y-axis",
                        },
                        "title": {
                            "type": "string",
                            "description": "Title of the graph",
                        }
                    },
                    "required": ["graph_type", "x_column", "y_column"]
                }
            )
        ]
        return Tool(function_declarations=function_declarations)

    def process_question(self,
                        question: str,
                        search_results: List[Dict],
                        selected_table: str,
                        generated_sql: str,
                        expected_table: str = None,
                        table_selection_is_correct: bool = None,
                        request_wait: int = 3) -> Dict:
        """
        Process a single question and generate the final result

        Args:
            question: User's question
            search_results: Search results for the question
            selected_table: Selected table ID
            generated_sql: Generated SQL query
            expected_table: Expected table ID (for testing)
            table_selection_is_correct: Whether table selection was correct (for testing)
            request_wait: Wait time between API requests

        Returns:
            Dictionary containing processing results
        """
        try:
            # Check cache for function results
            cache_key = f"{question}_{generated_sql}"

            if cache_key in self.function_results_cache:
                return self.function_results_cache[cache_key]

            # File cache check
            cache = self._load_cache('function_results')
            if cache_key in cache:
                self.function_results_cache[cache_key] = cache[cache_key]
                return cache[cache_key]

            # Execute query and get data
            query_result = self.query_executor.execute_query(generated_sql)
            if not query_result:
                raise Exception("Failed to execute query")

            time.sleep(request_wait)  # Wait between API calls

            # Generate prompt for function selection
            prompt = self._create_function_selection_prompt(question, query_result)
            response = self.model.generate_content(prompt, tools=[self.tools])

            # Process function calls and generate final answer
            result = self._process_function_calls(response, query_result)

            # Add processing results to result dictionary
            result.update({
                'question': question,
                'search_results': search_results,
                'selected_table': selected_table,
                'generated_sql': generated_sql,
                'expected_table': expected_table,
                'table_selection_is_correct': table_selection_is_correct,
                'timestamp': datetime.now().isoformat()
            })

            # Save to cache
            self.function_results_cache[cache_key] = result
            cache[cache_key] = result
            self._save_cache('function_results', cache)

            # Log results
            self._log_results(result)

            return result

        except Exception as e:
            error_result = {
                'success': False,
                'error': str(e),
                'question': question,
                'timestamp': datetime.now().isoformat()
            }
            print(f"Error in process_question: {str(e)}")
            return error_result

    def _create_function_selection_prompt(self, question: str, data: List[Dict]) -> str:
        """Create prompt for function selection"""
        return f"""
あなたは、ユーザーの質問に答えるためのアシスタントです。
以下の質問に対して、適切な関数を選択して実行し、日本語で回答を生成してください。

質問: {question}

利用可能なデータ:
{json.dumps(data[:3], ensure_ascii=False, indent=2)}  # 最初の3行のみ表示

以下の関数が利用可能です:
1. execute_query: SQLクエリを実行してデータを取得
2. analyze_data: データの統計分析（最大値、最小値、平均値、外れ値検出）
3. generate_graph: グラフの生成（時系列グラフ、棒グラフ）

質問の内容に応じて、適切な関数を選択し実行してください。
グラフを生成する場合は、適切なタイトルと軸ラベルを設定してください。
"""

    def _process_function_calls(self, response: Any, query_result: List[Dict]) -> Dict:
        """Process function calls and generate final answer"""
        try:
            result = {
                'success': True,
                'executed_function': None,
                'function_result': None,
                'output_file': None,
                'final_answer': None
            }

            # Check if function call exists in response
            if hasattr(response, 'candidates') and response.candidates[0].content.parts[0].function_call:
                function_call = response.candidates[0].content.parts[0].function_call
                result['executed_function'] = function_call.name

                # Process different function types
                if function_call.name == "analyze_data":
                    result.update(
                        self._handle_analysis_function(function_call.args, query_result)
                    )
                elif function_call.name == "generate_graph":
                    result.update(
                        self._handle_graph_function(function_call.args, query_result)
                    )

            # Generate final answer
            final_prompt = self._create_final_answer_prompt(result)
            final_response = self.model.generate_content(final_prompt)
            result['final_answer'] = final_response.text

            return result

        except Exception as e:
            raise Exception(f"Error in _process_function_calls: {str(e)}")

    def _handle_analysis_function(self, args: Dict, data: List[Dict]) -> Dict:
        """Handle statistical analysis functions"""
        analysis_type = args.get('analysis_type')
        column_name = args.get('column_name')

        if analysis_type == 'max':
            result = self.data_analyzer.get_max_value(data, column_name)
        elif analysis_type == 'min':
            result = self.data_analyzer.get_min_value(data, column_name)
        elif analysis_type == 'average':
            result = self.data_analyzer.get_average_value(data, column_name)
        elif analysis_type == 'outliers':
            result = self.data_analyzer.detect_outliers(data, column_name)
        else:
            raise ValueError(f"Unknown analysis type: {analysis_type}")

        return {
            'function_result': result,
            'output_file': None
        }

    def _handle_graph_function(self, args: Dict, data: List[Dict]) -> Dict:
        """Handle graph generation functions"""
        graph_type = args.get('graph_type')
        x_column = args.get('x_column')
        y_column = args.get('y_column')
        title = args.get('title')

        if graph_type == 'time_series':
            output_file = self.graph_generator.generate_time_series(
                data, x_column, y_column, title
            )
        elif graph_type == 'bar_chart':
            output_file = self.graph_generator.generate_bar_chart(
                data, x_column, y_column, title
            )
        else:
            raise ValueError(f"Unknown graph type: {graph_type}")

        return {
            'function_result': None,
            'output_file': output_file
        }

    def _create_final_answer_prompt(self, result: Dict) -> str:
        """Create prompt for generating final answer"""
        return f"""
以下の処理結果に基づいて、ユーザーの質問に対する回答を日本語で生成してください。

実行された関数: {result['executed_function']}
関数の実行結果: {result['function_result']}
出力ファイル: {result['output_file']}

回答は以下の点に注意して生成してください：
1. 数値結果がある場合は、適切な単位を付ける
2. グラフが生成された場合は、その場所を示す
3. 結果の解釈や示唆があれば含める

回答:
"""

    def _log_results(self, result: Dict):
        """Log processing results"""
        self.logger.log_query(
            question=result['question'],
            search_results=result['search_results'],
            selected_table=result['selected_table'],
            generated_sql=result['generated_sql'],
            expected_table=result.get('expected_table'),
            is_correct=result.get('table_selection_is_correct'),
            executed_function=result.get('executed_function'),
            function_result=result.get('function_result'),
            final_answer=result.get('final_answer'),
            output_file=result.get('output_file'),
            timestamp=result.get('timestamp')
        )
