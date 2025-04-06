import json
from typing import Dict, Optional, Any
import requests

class WikiAPIError(Exception):
    """Custom exception for Wiki API errors."""
    pass

class WikiAPIClient:
    """Shared API client for making requests to Wikimedia APIs.
    
    Example:
        >>> client = WikiAPIClient("MyApp/1.0")
        >>> response = client.make_request(
        ...     "https://en.wikipedia.org/w/api.php",
        ...     {"action": "query", "format": "json"}
        ... )
    """
    
    def __init__(self, user_agent: str) -> None:
        """Initialize the API client with a user agent.
        
        Args:
            user_agent: The user agent string to use for API requests
        """
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
    
    def make_request(self, url: str, params: Dict[str, str], timeout: int = 30) -> Dict[str, Any]:
        """Make an API request with error handling and response validation.
        
        Args:
            url: The API endpoint URL
            params: The query parameters
            timeout: Request timeout in seconds (default: 30)
        
        Returns:
            Dict containing the JSON response
        
        Raises:
            WikiAPIError: If the API request fails or returns invalid data
            requests.RequestException: If the HTTP request fails
            json.JSONDecodeError: If the response is not valid JSON
        """
        try:
            response = self.session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            
            if 'error' in data:
                error_info = data['error'].get('info', 'Unknown error')
                error_code = data['error'].get('code', 'unknown')
                raise WikiAPIError(f"API error {error_code}: {error_info}")
                
            return data
        except requests.RequestException as e:
            raise WikiAPIError(f"HTTP request failed: {str(e)}")
        except json.JSONDecodeError as e:
            raise WikiAPIError(f"Invalid JSON response: {str(e)}")
    
    def get_continue_params(self, data: Dict) -> Dict[str, str]:
        """Extract continue parameters from API response.
        
        Args:
            data: The API response data
        
        Returns:
            Dictionary of continue parameters to use in the next request
        """
        continue_params = {}
        
        # Handle standard continue parameters
        if "continue" in data:
            continue_params.update(data["continue"])
            
        # Handle query-continue parameters
        if "query-continue" in data:
            for module, params in data["query-continue"].items():
                continue_params.update(params)
                
        return continue_params 