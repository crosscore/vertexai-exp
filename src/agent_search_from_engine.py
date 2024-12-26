import google.auth
from google.auth.transport.requests import AuthorizedSession
from typing import List, Dict

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
    serving_config = f"projects/{project_id}/locations/{location}/collections/default_collection/engines/{engine_id}/servingConfigs/default_search"

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
    #print(f"response_data: {response_data}")

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
    Format the content by parsing and restructuring the single-line text into a formatted structure
    
    Args:
        content: Raw content string from the API response (single line)
    
    Returns:
        Formatted content string with proper structure and line breaks
    """
    # Remove HTML tags and trailing period
    content = content.replace('<b>', '').replace('</b>', '')
    if content.endswith('.'):
        content = content[:-1]
    
    # Initialize sections dictionary
    sections = {
        'tableId': '',
        'description': '',
        'category': '',
        'schema': [],
        'time_range': {
            'start_time': '',
            'end_time': ''
        }
    }
    
    # Extract tableId and remaining content
    if 'tableId:' in content:
        parts = content.split('description:', 1)
        if len(parts) == 2:
            sections['tableId'] = parts[0].replace('tableId:', '').strip()
            content = parts[1].strip()
    
    # Extract description and category
    if 'category:' in content:
        parts = content.split('category:', 1)
        if len(parts) == 2:
            sections['description'] = parts[0].strip()
            content = parts[1].strip()
            
            # Split category and schema
            parts = content.split('schema:', 1)
            if len(parts) == 2:
                sections['category'] = parts[0].strip()
                schema_content = parts[1].strip()
                
                # Split schema and time_range
                if 'time_range:' in schema_content:
                    schema_parts = schema_content.split('time_range:', 1)
                    if len(schema_parts) == 2:
                        schema_text = schema_parts[0].strip()
                        time_range_text = schema_parts[1].strip()
                        
                        # Process schema entries
                        for line in schema_text.split('-'):
                            if 'name:' in line and 'type:' in line:
                                entry = line.strip()
                                if entry:
                                    sections['schema'].append(entry)
                        
                        # Process time_range entries
                        if 'start_time:' in time_range_text:
                            start_time_parts = time_range_text.split('start_time:', 1)
                            if len(start_time_parts) == 2:
                                remaining_text = start_time_parts[1].strip()
                                if 'end_time:' in remaining_text:
                                    time_parts = remaining_text.split('end_time:', 1)
                                    start_time = time_parts[0].strip()
                                    end_time = time_parts[1].strip()
                                    
                                    # Clean up the time values
                                    if start_time.endswith('-'):
                                        start_time = start_time[:-1].strip()
                                    sections['time_range']['start_time'] = start_time
                                    sections['time_range']['end_time'] = end_time
    
    # Construct the formatted output
    formatted_output = []
    formatted_output.append(f"tableId: {sections['tableId']}")
    formatted_output.append(f"category: {sections['category']}")
    formatted_output.append(f"description: {sections['description']}")
    
    # Add schema section
    formatted_output.append("schema:")
    for schema_entry in sections['schema']:
        formatted_output.append(f"  - {schema_entry}")
    
    # Add time_range section
    formatted_output.append("time_range:")
    formatted_output.append(f"  - start_time: {sections['time_range']['start_time']}")
    formatted_output.append(f"  - end_time: {sections['time_range']['end_time']}")
    
    return '\n'.join(formatted_output)

if __name__ == "__main__":
    # Configuration
    project_id = "business-test-001"
    location = "global"
    engine_id = "test-agent-app_1735199159695"
    question = "商品の在庫数を知りたい"

    try:
        # Execute search
        results = search_sample(project_id, location, engine_id, question)

        print("\n=== 検索結果 ===")
        for i, result in enumerate(results, 1):
            print(f"\n[上位{i}件目]")
            print(result['content'])
            print("-" * 30)
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")