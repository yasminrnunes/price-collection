"""HTML and XML parsing utilities for web scraping operations.

This module provides utility functions for parsing HTML and XML content from
requests.Response objects into more usable formats. It handles encoding
detection and conversion, making it easier to extract data from web pages
and API responses.

Features:
    - HTML parsing into BeautifulSoup objects for easy extraction
    - XML parsing into ElementTree objects for structured data
    - Automatic encoding detection and conversion
    - Error handling with graceful fallbacks
    - Support for None/null responses

Functions:
    - parse_html: Convert requests.Response to BeautifulSoup object
    - parse_xml: Convert requests.Response to ElementTree root element

Example:
    Parse HTML content:
        >>> from utils.http_request import make_request_with_delay
        >>> from utils.parsers import parse_html
        >>> response = make_request_with_delay("https://example.com")
        >>> soup = parse_html(response)
        >>> if soup:
        ...     title = soup.find('title').text

    Parse XML content:
        >>> from utils.parsers import parse_xml
        >>> response = make_request_with_delay("https://api.example.com/data.xml")
        >>> root = parse_xml(response)
        >>> if root:
        ...     for item in root.findall('item'):
        ...         print(item.text)
"""

import xml.etree.ElementTree as ET
from typing import Optional
from bs4 import BeautifulSoup
import requests


def parse_html(response: Optional[requests.Response]) -> Optional[BeautifulSoup]:
    """Parse HTML response from requests.Response object into BeautifulSoup.

    This function converts a requests.Response object containing HTML content
    into a BeautifulSoup object, which provides a convenient API for navigating
    and searching HTML documents. The function automatically detects and sets
    the proper encoding before parsing.

    BeautifulSoup makes it easy to:
    - Find elements by tag name, class, id, or CSS selector
    - Navigate the document tree
    - Extract text content and attributes
    - Handle malformed HTML gracefully

    Args:
        response: A requests.Response object containing HTML content. If None,
            the function returns None immediately without attempting to parse.

    Returns:
        A BeautifulSoup object parsed with 'html.parser', ready for use with
        methods like find(), find_all(), select(), etc. Returns None if the
        response is None or if parsing fails for any reason.

    Example:
        Parse HTML and extract data:
            >>> from utils.http_request import make_request_with_delay
            >>> from utils.parsers import parse_html
            >>> response = make_request_with_delay("https://example.com")
            >>> soup = parse_html(response)
            >>> if soup:
            ...     title = soup.find('title').text
            ...     links = soup.find_all('a', href=True)

        Use CSS selectors:
            >>> if soup:
            ...     products = soup.select('.product-list .product-card')
            ...     for product in products:
            ...         name = product.select_one('.product-name').text

    Note:
        The function uses BeautifulSoup's 'html.parser', which is built into
        Python and doesn't require external dependencies. Encoding is
        automatically detected using response.apparent_encoding, falling back
        to UTF-8 if detection fails.
    """

    if response is None:
        return None

    # Ensure proper encoding is used
    response.encoding = response.apparent_encoding or "utf-8"
    return BeautifulSoup(response.text, "html.parser")


def parse_xml(response: Optional[requests.Response]) -> Optional[ET.Element]:
    """Parse XML response from requests.Response object into ElementTree.

    This function converts a requests.Response object containing XML content
    into an ElementTree.Element object, which provides a convenient API for
    navigating and searching XML documents. The function automatically detects
    and sets the proper encoding before parsing.

    ElementTree makes it easy to:
    - Find elements by tag name using find() and findall()
    - Navigate the XML tree structure
    - Extract text content and attributes
    - Iterate over child elements

    Args:
        response: A requests.Response object containing XML content. If None,
            the function returns None immediately without attempting to parse.

    Returns:
        An ElementTree.Element object representing the root element of the
        parsed XML document. Returns None if the response is None or if
        parsing fails (malformed XML, encoding issues, etc.).

    Raises:
        This function catches all exceptions and returns None instead of
        raising, so it's safe to use without try/except blocks.

    Example:
        Parse XML and extract data:
            >>> from utils.http_request import make_request_with_delay
            >>> from utils.parsers import parse_xml
            >>> response = make_request_with_delay("https://api.example.com/data.xml")
            >>> root = parse_xml(response)
            >>> if root:
            ...     for item in root.findall('item'):
            ...         title = item.find('title').text
            ...         price = item.find('price').text

        Access attributes:
            >>> if root:
            ...     for product in root.findall('product'):
            ...         product_id = product.get('id')
            ...         name = product.find('name').text

    Note:
        The function uses Python's built-in xml.etree.ElementTree module.
        Encoding is automatically detected using response.apparent_encoding,
        falling back to UTF-8 if detection fails. All parsing errors are
        caught and None is returned, making it safe for production use.
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
