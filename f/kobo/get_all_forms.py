import requests
from typing import Dict, List, Any
import wmill
import json
from urllib.parse import urlparse, urlunparse
import re


def _sanitize_name_for_postgres(name: str) -> str:
    """
    Sanitize a name string to be safe for PostgreSQL insertion.

    Args:
        name: The raw name string from the API

    Returns:
        Sanitized name safe for PostgreSQL text column
    """
    if not name or not isinstance(name, str):
        return ""

    # Remove null bytes (PostgreSQL doesn't allow these)
    name = name.replace('\x00', '')

    # Remove or replace control characters (except tab and newline which might be OK)
    # Keep printable characters, spaces, tabs, newlines, and non-ASCII characters
    name = re.sub(r'[\x01-\x08\x0B\x0C\x0E-\x1F\x7F]', '', name)

    # Normalize whitespace - replace multiple spaces/tabs with single space
    name = re.sub(r'\s+', ' ', name)

    # Strip leading and trailing whitespace
    name = name.strip()

    # Limit length to reasonable size (PostgreSQL text can be very long, but let's be reasonable)
    max_length = 255
    if len(name) > max_length:
        name = name[:max_length].strip()

    return name


def _format_forms(forms: List[Dict], append: str) -> List[Dict[str, str]]:
    """Helper function to format forms into the desired output structure.
    Only includes forms with asset_type 'survey'. Names are sanitized for PostgreSQL."""
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

        # Only include forms with asset_type 'survey'
        asset_type = form.get("asset_type", "")
        if asset_type == "survey":
            # Sanitize the name for safe PostgreSQL insertion
            raw_name = form.get("name", "")
            sanitized_name = _sanitize_name_for_postgres(raw_name)

            formatted_forms.append({
                "name": sanitized_name,
                "endpoint": final_url,
                "asset_type": asset_type
            })
    return formatted_forms


def main(endpoint: str, append: str = "") -> List[Dict[str, str]]:
    """
    Retrieve all survey forms from a KoboToolbox endpoint with pagination support.

    Args:
        endpoint: The API endpoint URL (e.g., "https://kf.kobotoolbox.org/api/v2/assets/?format=json")
        append: String to append to each form endpoint URL (default: "")

    Returns:
        List of dictionaries for forms with asset_type 'survey', each containing:
        - name: The form name (sanitized for PostgreSQL insertion)
        - endpoint: The form URL (with query parameters removed and append string added)
        - asset_type: The asset type (will always be 'survey')
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

