from vertexai.preview.generative_models import GenerativeModel

#PROJECT_ID = "business-test-001"
#REGION = "us-central1"
#vertexai.init(project=PROJECT_ID, location=REGION)

generative_multimodal_model = GenerativeModel("gemini-1.5-flash-002")

def answer_question(question):
    response = generative_multimodal_model.generate_content(question)
    return response.text

response = answer_question("あなたの名前は何ですか？")
print(response)
