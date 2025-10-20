import requests
from typing import Dict, List, Any
import wmill
import json
from urllib.parse import urlparse, urlunparse


def _format_forms(forms: List[Dict], append: str) -> List[Dict[str, str]]:
    """Helper function to format forms into the desired output structure."""
    formatted_forms = []
    for form in forms:
        # Get the URL and remove query parameters
        url = form.get("url", "")
        if url:
            parsed_url = urlparse(url)
            # Rebuild URL without query parameters (remove query, fragment)
            clean_url = urlunparse((
                parsed_url.scheme,
                parsed_url.netloc,
                parsed_url.path,
                "",  # params (usually empty)
                "",  # query (remove this)
                ""   # fragment (remove this)
            ))
        else:
            clean_url = ""

        # Append the specified string to the clean URL
        final_url = clean_url + append if clean_url else ""

        formatted_forms.append({
            "name": form.get("name", ""),
            "endpoint": final_url
        })
    return formatted_forms


def main(endpoint: str, append: str = "") -> List[Dict[str, str]]:
    """
    Retrieve all available forms from a KoboToolbox endpoint with pagination support.

    Args:
        endpoint: The API endpoint URL (e.g., "https://kf.kobotoolbox.org/api/v2/assets/?format=json")
        append: String to append to each form endpoint URL (default: "")

    Returns:
        List of dictionaries, each containing:
        - name: The form name
        - endpoint: The form URL (with query parameters removed and append string added)
    """

    token = json.loads(wmill.get_variable("f/kobo/kobo_token"))

    # Extract token from the provided dictionary
    auth_token = token.get("token")
    if not auth_token:
        raise ValueError("Token is required in the format {'token': 'your_token'}")

    # Set up headers for authentication
    headers = {
        "Authorization": f"Token {auth_token}",
        "Content-Type": "application/json"
    }

    all_forms = []
    current_url = endpoint
    page_count = 0

    try:
        while current_url:
            page_count += 1
            print(f"Fetching page {page_count}: {current_url}")

            # Make the API request
            response = requests.get(current_url, headers=headers)
            response.raise_for_status()  # Raise an exception for bad status codes

            data = response.json()

            # Extract forms from current page
            forms = data.get("results", [])
            all_forms.extend(forms)

            # Get next page URL for pagination
            current_url = data.get("next")

            print(f"Retrieved {len(forms)} forms from page {page_count}")

        return _format_forms(all_forms, append)

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {str(e)}")
        # Return partial results if available
        return _format_forms(all_forms, append)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        # Return partial results if available
        return _format_forms(all_forms, append)

