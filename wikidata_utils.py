"""
Utilities for interacting with Wikidata API.
Used for checking images that might be pulled from Wikidata instead of being directly in wikitext.
"""

import requests
import json
from typing import Dict, List, Optional, Set, Generator, Tuple
from collections import defaultdict
from wiki_api_client import WikiAPIClient, WikiAPIError
from datetime import datetime

# API endpoints
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKI_API_TEMPLATE = "https://{wiki}/w/api.php"

P18_IMAGE = "P18"

def get_wikidata_item_for_page(wiki: str, page_title: str, user_agent: str) -> Optional[str]:
    """
    Get the Wikidata item ID for a Wikipedia page.
    
    Args:
        wiki: The wiki domain (e.g., 'en.wikipedia.org')
        page_title: The Wikipedia page title
        user_agent: User agent string for API requests
    
    Returns:
        Wikidata item ID (e.g., 'Q12345') or None if not found
    """
    client = WikiAPIClient(user_agent)
    api_url = WIKI_API_TEMPLATE.format(wiki=wiki)
    
    params = {
        "action": "query",
        "prop": "pageprops",
        "titles": page_title,
        "format": "json"
    }
    
    try:
        data = client.make_request(api_url, params)
        pages = data.get("query", {}).get("pages", {})
        
        for page in pages.values():
            if "pageprops" in page and "wikibase_item" in page["pageprops"]:
                return page["pageprops"]["wikibase_item"]
        
        return None
    except WikiAPIError as e:
        print(f"Error fetching Wikidata item: {e}")
        return None

def get_current_image_for_item(item_id: str, user_agent: str) -> Optional[str]:
    """
    Get the current P18 (image) value for a Wikidata item.
    
    Args:
        item_id: Wikidata item ID (e.g., 'Q12345')
        user_agent: User agent string for API requests
    
    Returns:
        Current image filename or None if not found
    """
    client = WikiAPIClient(user_agent)
    
    params = {
        "action": "wbgetclaims",
        "entity": item_id,
        "property": P18_IMAGE,
        "format": "json"
    }
    
    try:
        data = client.make_request(WIKIDATA_API, params)
        claims = data.get("claims", {}).get(P18_IMAGE, [])
        
        if not claims:
            return None
        
        mainsnak = claims[0].get("mainsnak", {})
        if mainsnak.get("snaktype") == "value" and "datavalue" in mainsnak:
            return mainsnak["datavalue"].get("value", "")
        
        return None
    except WikiAPIError as e:
        print(f"Error fetching Wikidata image: {e}")
        return None

def get_image_history_for_item(item_id: str, user_agent: str) -> List[Dict]:
    """
    Get the history of P18 (image) values for a Wikidata item.
    
    Args:
        item_id: Wikidata item ID (e.g., 'Q12345')
        user_agent: User agent string for API requests
    
    Returns:
        List of dicts with timestamp, image filename, revision_id for each change
        The list is ordered with the current/most recent image first.
        An empty list indicates no P18 property was found.
        If there's only one item in the list, this was the first P18 value.
    """
    client = WikiAPIClient(user_agent)
    
    # First check for a P18 claim
    current_image = get_current_image_for_item(item_id, user_agent)
    if not current_image:
        return []
    
    # Get revision history
    params = {
        "action": "query",
        "prop": "revisions",
        "titles": item_id,
        "rvprop": "ids|timestamp|content",
        "rvslots": "main",
        "rvlimit": "max",
        "format": "json",
        "rvdir": "older"
    }
    
    try:
        print(f"Fetching revision history for Wikidata item {item_id}")
        
        image_history = []
        current_p18_value = None
        
        # Paginate through revs
        while True:
            data = client.make_request(WIKIDATA_API, params)
            
            pages = data.get("query", {}).get("pages", {})
            for page_id, page_data in pages.items():
                revisions = page_data.get("revisions", [])
                
                for rev in revisions:
                    revision_id = rev.get("revid")
                    timestamp = rev.get("timestamp")
                    content_json = rev.get("slots", {}).get("main", {}).get("*", "{}")
                    
                    try:
                        content = json.loads(content_json)
                        
                        claims = content.get("claims", {}).get(P18_IMAGE, [])
                        
                        if claims:
                            mainsnak = claims[0].get("mainsnak", {})
                            if mainsnak.get("snaktype") == "value" and "datavalue" in mainsnak:
                                p18_value = mainsnak["datavalue"].get("value", "")
                                
                                # If this is a different value than we've seen before, record it
                                if p18_value != current_p18_value:
                                    formatted_ts = datetime.fromisoformat(
                                        timestamp.replace('Z', '+00:00')
                                    ).strftime('%Y-%m-%d %H:%M:%S UTC')
                                    
                                    image_history.append({
                                        "timestamp": formatted_ts,
                                        "image": p18_value,
                                        "revision_id": revision_id
                                    })
                                    current_p18_value = p18_value
                        elif current_p18_value is not None and not claims:
                            break
                            
                    except json.JSONDecodeError:
                        print(f"Error parsing revision JSON in revision {revision_id}")
                        continue
            
            continue_params = client.get_continue_params(data)
            if continue_params:
                params.update(continue_params)
            else:
                break
        
        if not image_history and current_image:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
            image_history.append({
                "timestamp": current_time,
                "image": current_image,
                "revision_id": 0
            })
        
        return image_history
        
    except WikiAPIError as e:
        print(f"Error fetching Wikidata history: {e}")
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
        return [{
            "timestamp": current_time,
            "image": current_image,
            "revision_id": 0
        }]

def check_if_file_matches_wikidata_image(file_name: str, wikidata_image: str) -> bool:
    """
    Check if a Commons file name matches a Wikidata image value.
    
    Args:
        file_name: Commons file name (e.g., "File:Example.jpg")
        wikidata_image: Wikidata image value (e.g., "Example.jpg")
    
    Returns:
        True if they match, False otherwise
    """
    # Remove "File:" prefix
    commons_name = file_name.replace("File:", "").lower()
    wikidata_name = wikidata_image.lower()
    
    commons_spaces = commons_name.replace("_", " ")
    commons_underscores = commons_name.replace(" ", "_")
    wikidata_spaces = wikidata_name.replace("_", " ")
    wikidata_underscores = wikidata_name.replace(" ", "_")
    
    return (commons_spaces == wikidata_spaces or 
            commons_underscores == wikidata_underscores or
            commons_name == wikidata_name) 