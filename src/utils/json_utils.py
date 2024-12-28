# utils/json_utils.py
from pathlib import Path
import json
from datetime import datetime
import os
from typing import Dict

class JSONOutputManager:
    """JSONファイル出力を管理するクラス"""
    def __init__(self, base_dir: str = "./json"):
        self.base_dir = base_dir
        self._ensure_directory_exists()

    def _ensure_directory_exists(self):
        """出力ディレクトリの作成"""
        Path(self.base_dir).mkdir(parents=True, exist_ok=True)

    def save_json(self, data: Dict, prefix: str = "result") -> str:
        """
        JSONファイルを保存する

        Args:
            data: 保存するデータ
            prefix: ファイル名のプレフィックス

        Returns:
            保存したファイルのパス
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{timestamp}.json"
        filepath = os.path.join(self.base_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"JSONファイルを保存しました: {filepath}")
        return filepath

# 使用例
if __name__ == "__main__":
    # サンプルデータ
    sample_data = {
        "name": "test",
        "value": 123,
        "timestamp": datetime.now().isoformat()
    }

    # JSONOutputManagerのインスタンス化と使用
    json_manager = JSONOutputManager()
    filepath = json_manager.save_json(sample_data, prefix="sample")
    print(f"Sample JSON saved to: {filepath}")
