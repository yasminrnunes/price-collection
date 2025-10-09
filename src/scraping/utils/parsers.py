"""
Parse HTML and XML responses from requests.Response objects.
"""

import xml.etree.ElementTree as ET
from typing import Optional
from bs4 import BeautifulSoup
import requests


def parse_html(response: Optional[requests.Response]) -> Optional[BeautifulSoup]:
    """
    Parse HTML response from requests.Response object.

    This function takes a requests.Response object containing HTML content and converts
    it into a BeautifulSoup object for easier HTML parsing and data extraction.
    The function ensures proper encoding is used before parsing.

    Args:
        response: requests.Response object containing HTML content. Can be None.

    Returns:
        BeautifulSoup: BeautifulSoup object with parsed HTML ready for use,
                      or None if response is None or parsing fails.

    Example:
        >>> import requests
        >>> response = requests.get("https://example.com")
        >>> soup = parse_html(response)
        >>> if soup:
        ...     title = soup.find('title').text
    """

    if response is None:
        return None

    # Ensure proper encoding is used
    response.encoding = response.apparent_encoding or "utf-8"
    return BeautifulSoup(response.text, "html.parser")


def parse_xml(response: Optional[requests.Response]) -> Optional[ET.Element]:
    """
    Parse XML response from requests.Response object.

    Args:
        response: requests.Response object containing XML content

    Returns:
        ET.Element: Root element of the parsed XML, or None if parsing fails
    """
    if response is None:
        return None

    try:
        # Ensure proper encoding is used
        response.encoding = response.apparent_encoding or "utf-8"

        # Parse XML content
        root = ET.fromstring(response.text)
        return root

    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error parsing XML: {e}")
        return None
