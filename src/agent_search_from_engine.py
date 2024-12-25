import google.auth
from google.auth.transport.requests import AuthorizedSession
from typing import List, Dict
from dotenv import load_dotenv
import os

load_dotenv()
LOCATION = os.getenv("LOCATION")
PROJECT_ID = os.getenv("PROJECT_ID")
ENGINE_ID = os.getenv("ENGINE_ID")

def get_authorized_session() -> AuthorizedSession:
    credentials, project = google.auth.default()
    return AuthorizedSession(credentials)

def search_sample(
    project_id: str,
    location: str,
    engine_id: str,
    search_query: str,
) -> List[Dict]:
    """
    Search the Discovery Engine API and return formatted results

    Args:
        project_id: GCP project ID
        location: API location (e.g., 'global')
        engine_id: Discovery Engine ID
        search_query: Search query string

    Returns:
        List of dictionaries containing search results with title and content
    """
    session = get_authorized_session()

    # Construct the API endpoint URL
    base_url = "https://discoveryengine.googleapis.com/v1alpha"
    serving_config = f"projects/{PROJECT_ID}/locations/{LOCATION}/collections/default_collection/engines/{engine_id}/servingConfigs/default_search"

    # Prepare the request payload
    payload = {
        "query": search_query,
        "pageSize": 10,
        "queryExpansionSpec": {
            "condition": "AUTO"
        },
        "spellCorrectionSpec": {
            "mode": "AUTO"
        },
        "contentSearchSpec": {
            "extractiveContentSpec": {
                "maxExtractiveAnswerCount": 1
            }
        }
    }

    # Make the API request
    response = session.post(
        f"{base_url}/{serving_config}:search",
        json=payload
    )

    # Check for errors
    response.raise_for_status()
    response_data = response.json()

    results = []
    if response_data.get('results'):
        for result in response_data['results']:
            document = result.get('document', {})
            derived_data = document.get('derivedStructData', {})

            # Extract relevant information
            answer = derived_data.get('extractive_answers', [{}])[0].get('content', '')
            title = derived_data.get('title', 'Unknown Table')

            if answer:
                # Clean and format the content
                formatted_content = format_content(answer)

                results.append({
                    'table_name': title,
                    'content': formatted_content
                })

    return results

def format_content(content: str) -> str:
    """
    Format the content by cleaning up and structuring the text

    Args:
        content: Raw content string from the API response

    Returns:
        Formatted content string
    """
    # Remove HTML tags and clean up the text
    content = content.replace('<b>', '').replace('</b>', '')

    # Extract table information using string splitting
    parts = content.split('description:')
    if len(parts) != 2:
        return content

    table_id_part = parts[0].strip()
    description_category_part = parts[1]

    # Extract tableId
    table_id = table_id_part.replace('tableId:', '').strip()

    # Split description and category
    desc_cat_parts = description_category_part.split('category:')
    if len(desc_cat_parts) != 2:
        return content

    description = desc_cat_parts[0].strip()
    category_schema_parts = desc_cat_parts[1].split('schema:')

    if len(category_schema_parts) != 2:
        return content

    category = category_schema_parts[0].strip()
    schema = category_schema_parts[1].strip()

    # Format schema entries
    schema_entries = schema.split('-')
    formatted_schema = '\n'.join(f"- {entry.strip()}" for entry in schema_entries if entry.strip())

    # Construct the formatted output
    formatted_output = f"""tableId: {table_id}
description: {description}
category: {category}

schema:
{formatted_schema}"""

    print(formatted_output)
    print("-" * 30)
    return formatted_output

if __name__ == "__main__":
    # Configuration
    project_id = PROJECT_ID
    location = LOCATION
    engine_id = ENGINE_ID
    search_query = "商品の在庫数を知りたい"

    try:
        # Execute search
        results = search_sample(project_id, location, engine_id, search_query)

        print("\n=== 検索結果 ===")
        for i, result in enumerate(results, 1):
            print(f"\n[上位{i}件目]")
            print(result['content'])
            print("-" * 30)
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
