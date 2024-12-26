import subprocess
import os
import json
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

project_id = "business-test-001"
dataset_name = "test_dataset"
output_folder = "./txt"
bucket_name = "test-bk-001"  # Cloud Storage バケット名

def get_table_time_range(client: bigquery.Client, table_ref: str, timestamp_column: str) -> dict:
    try:
        # タイムゾーンを日本時間（UTC+9）に調整するクエリ
        query = f"""
        SELECT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S+09', MIN({timestamp_column})) as start_time,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S+09', MAX({timestamp_column})) as end_time
        FROM `{table_ref}`
        """

        query_job = client.query(query)
        results = query_job.result()

        for row in results:
            return {
                'start_time': row.start_time if row.start_time else 'unknown',
                'end_time': row.end_time if row.end_time else 'unknown'
            }

    except Exception as e:
        print(f"Warning: テーブル {table_ref} の時間範囲取得に失敗しました: {str(e)}")
        return {
            'start_time': 'unknown',
            'end_time': 'unknown'
        }

# Cloud Storage クライアントの初期化
storage_client = storage.Client()
# BigQuery クライアントの初期化
bq_client = bigquery.Client()

# バケットが存在するか確認し、存在しない場合は作成
try:
    bucket = storage_client.get_bucket(bucket_name)
except NotFound:
    print(f"Bucket {bucket_name} not found. Creating it...")
    bucket = storage_client.create_bucket(bucket_name, location="us")
    print(f"Bucket {bucket_name} created successfully.")

# 出力フォルダが存在しない場合は作成
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# bq ls コマンドを実行してテーブルリストを取得
command = [
    "bq",
    "ls",
    "--format=json",
    f"--project_id={project_id}",
    dataset_name,
]
process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
stdout, stderr = process.communicate()

# エラーが発生した場合、エラーメッセージを表示して終了
if process.returncode != 0:
    print(f"Error executing command to list tables:")
    print(stderr.decode("utf-8"))
    exit()

# テーブルリストをJSONとしてパース
try:
    tables = json.loads(stdout.decode("utf-8"))
except json.JSONDecodeError as e:
    print(f"Error decoding JSON for table list:")
    print(e)
    exit()

# 各テーブルに対して処理を実行
for table in tables:
    table_id = table["tableReference"]["tableId"]
    table_name = table_id

    # bq show コマンドを実行
    command = [
        "bq",
        "show",
        "--format=prettyjson",
        f"--project_id={project_id}",
        f"{dataset_name}.{table_name}",
    ]

    # サブプロセスでコマンドを実行し、出力をキャプチャ
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = process.communicate()

    # エラーが発生した場合、エラーメッセージを表示してスキップ
    if process.returncode != 0:
        print(f"Error executing command for {table_name}:")
        print(stderr.decode("utf-8"))
        continue

    # 出力をJSONとしてパース
    try:
        data = json.loads(stdout.decode("utf-8"))
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON for {table_name}:")
        print(e)
        continue

    # 必要な情報を抽出
    try:
        category = data.get("labels", {}).get("category", "uncategorized")
        description = data.get("description", "")
        schema_fields = [
            {
                "name": field.get("name"),
                "type": field.get("type"),
                "mode": field.get("mode"),
            }
            for field in data.get("schema", {}).get("fields", [])
        ]

        # タイムスタンプ列を探す（最も左側のものを使用）
        timestamp_column = None
        for field in schema_fields:
            if field["type"].upper() in ["TIMESTAMP", "DATETIME"]:
                timestamp_column = field["name"]
                break

        # 時間範囲情報を取得
        if timestamp_column:
            table_ref = f"{project_id}.{dataset_name}.{table_name}"
            time_range = get_table_time_range(bq_client, table_ref, timestamp_column)
        else:
            print(f"Warning: テーブル {table_name} にタイムスタンプ列が見つかりませんでした。")
            time_range = {
                'start_time': 'unknown',
                'end_time': 'unknown'
            }

    except (AttributeError, TypeError) as e:
        print(f"Error processing data for {table_name}:")
        print(e)
        continue

    # TXTファイルの内容を作成
    txt_content = f"tableId: {table_name}\n"
    txt_content += f"description: {description}\n"
    txt_content += f"category: {category}\n"
    txt_content += "schema:\n"
    for field in schema_fields:
        txt_content += f"  - name: {field['name']}, type: {field['type']}, mode: {field['mode']}\n"
    # 時間範囲情報を追加
    txt_content += "time_range:\n"
    txt_content += f"  - start_time: {time_range['start_time']}\n"
    txt_content += f"  - end_time: {time_range['end_time']}\n"

    # TXTファイルとしてローカルに保存
    txt_file_name = f"{table_name}.txt"
    txt_local_path = os.path.join(output_folder, txt_file_name)
    with open(txt_local_path, "w", encoding="utf-8") as f:
        f.write(txt_content)

    # TXTファイルをCloud Storageにアップロード
    txt_blob = bucket.blob(txt_file_name)
    txt_blob.upload_from_filename(txt_local_path, content_type='text/plain; charset=utf-8')

    print(f"Successfully created and uploaded {txt_file_name}")
