
from vertexai.language_models import TextGenerationModel

# Vertex AI のプロジェクトとリージョンを設定
# PROJECT_ID = "[YOUR_PROJECT_ID]"
# LOCATION = "[YOUR_REGION]"  # モデルをデプロイしたリージョン(ex: "us-central1")

# モデルのロード
model = TextGenerationModel.from_pretrained("text-bison@001")

def generate_response(question, system_prompt=None):
    prompt = question
    if system_prompt:
        prompt = f"{system_prompt}\n{question}"

    response = model.predict(
        prompt,
        temperature=0.2,  # 生成のランダム性を調整 (0.0 ~ 1.0)
        max_output_tokens=256,  # 生成されるトークン数の上限
        top_p=0.8,  # 生成されるトークンの多様性を調整 (0.0 ~ 1.0)
        top_k=40,  # 生成されるトークンの上位 k 個を選択
    )

    return response.text

system_prompt = "あなたは優秀なアシスタントです。質問に丁寧に答えてください。"
question = "今日の天気は？"
response = generate_response(question, system_prompt)
print(response)
