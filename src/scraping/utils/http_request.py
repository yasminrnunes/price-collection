"""HTTP request utilities for web scraping operations.

This module provides utilities for making HTTP requests with automatic retry
logic, delay management, and support for both standard requests and dynamic
content rendered with JavaScript using Playwright.

Features:
    - Automatic retry with exponential backoff for failed requests
    - Random delays to avoid rate limiting and appear more human-like
    - Session management with connection pooling and reuse
    - Encoding detection and automatic UTF-8 conversion
    - Support for dynamic content using Playwright (headless browser)
    - DNS error recovery with session recreation

The module maintains a global session instance that is reused across requests
for better performance. It automatically handles retries for common HTTP
errors (429, 500, 502, 503, 504) and recreates the session on DNS errors.

Functions:
    - make_request_with_delay: GET request with optional random delay
    - make_post_request_with_delay: POST request with optional random delay
    - make_post_request: Simple POST request without delay
    - make_dinamic_request_with_delay: Dynamic request using Playwright for
      JavaScript-rendered content

Example:
    Make a simple GET request:
        >>> from utils.http_request import make_request_with_delay
        >>> response = make_request_with_delay("https://example.com")
        >>> if response:
        ...     print(response.text)

    Make a POST request with JSON data:
        >>> response = make_post_request_with_delay(
        ...     "https://api.example.com/endpoint",
        ...     data='{"key": "value"}',
        ...     headers={"Content-Type": "application/json"}
        ... )

    Make a dynamic request for JavaScript content:
        >>> html = make_dinamic_request_with_delay(
        ...     "https://spa.example.com",
        ...     selector=".product-list",
        ...     min_count=10
        ... )
"""

import socket
import time
import random
import requests
from playwright.sync_api import sync_playwright
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Constants for delays
MIN_DELAY_SECONDS = 1
MAX_DELAY_SECONDS = 4

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/114.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def create_session():
    """Create a requests Session with automatic retry configuration.

    This function creates a new requests Session configured with retry logic
    for handling transient HTTP errors. The retry strategy uses exponential
    backoff and only retries on specific HTTP status codes (429, 500, 502,
    503, 504) and network errors.

    Returns:
        A configured requests.Session object with retry adapters mounted
        for both HTTP and HTTPS protocols.

    Note:
        The retry strategy applies only to GET requests by default. The
        backoff factor of 0.8 results in delays of 0.8s, 1.6s, 3.2s, etc.
    """
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.8,  # 0.8, 1.6, 3.2, 6.4, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# Initialize global session
_SESSION = create_session()


def _random_delay(url: str = ""):
    """Apply a random delay between MIN_DELAY_SECONDS and MAX_DELAY_SECONDS.

    This function introduces a random delay to make requests appear more
    human-like and avoid rate limiting. The delay is uniformly distributed
    between the configured minimum and maximum values.

    Args:
        url: Optional URL string for logging purposes (currently unused).
    """
    delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
    # print(f"Waiting {delay:.2f} seconds... for {url}")
    time.sleep(delay)


def _make_request(url, headers=None, timeout=30, raise_error: bool = False):
    """Internal function to make a GET request with error handling.

    This function performs the actual HTTP GET request using the global
    session. It handles encoding issues, DNS errors, and request exceptions.
    On DNS errors, it recreates the session to recover from connection issues.

    Args:
        url: The URL to make the request to.
        headers: Optional dictionary of HTTP headers to merge with default headers.
        timeout: Request timeout in seconds (default: 30).
        raise_error: If True, raises exceptions instead of returning None.

    Returns:
        A requests.Response object if successful, None if an error occurred
        and raise_error is False.

    Raises:
        requests.exceptions.RequestException: If raise_error is True and a
            request error occurs.
        socket.gaierror: If raise_error is True and a DNS resolution error occurs.
    """
    global _SESSION
    # merge default headers with provided headers
    merged_headers = {**DEFAULT_HEADERS, **(headers or {})}

    try:
        response = _SESSION.get(url, headers=merged_headers, timeout=timeout)
        response.raise_for_status()

        # Ensure proper encoding for text content
        if response.encoding is None or response.encoding.lower() in [
            "iso-8859-1",
            "windows-1252",
        ]:
            response.encoding = "utf-8"

        # content_encoding = response.headers.get('content-encoding', '').lower()
        # if content_encoding == 'br':
        #     try:
        #         response.content = brotli.decompress(response.content).decode('utf-8')
        #     except Exception as brotli_error:
        #         print(f"Brotli decompression failed: {brotli_error}, url: {url}")

        return response
    except (requests.exceptions.RequestException, socket.gaierror) as e:
        print(f"Error making request to {url}: {e}")
        if isinstance(e, socket.gaierror):
            print("DNS resolution error — recreating session")
            _SESSION.close()
            _SESSION = create_session()
        if raise_error:
            raise e
        return None


def make_request_with_delay(
    url, headers=None, timeout=30, delay=True, raise_error: bool = False
):
    """Make a GET request with optional random delay.

    This is the primary function for making GET requests in scraping operations.
    It applies an optional random delay before making the request to avoid rate
    limiting and make requests appear more human-like.

    Args:
        url: The URL to make the request to.
        headers: Optional dictionary of HTTP headers to merge with default headers.
        timeout: Request timeout in seconds (default: 30).
        delay: If True, applies a random delay before the request (default: True).
        raise_error: If True, raises exceptions instead of returning None
            (default: False).

    Returns:
        A requests.Response object if successful, None if an error occurred
        and raise_error is False.

    Example:
        >>> response = make_request_with_delay("https://example.com")
        >>> if response:
        ...     print(response.text)
    """
    if delay:
        _random_delay(url=url)

    return _make_request(url, headers, timeout, raise_error)


def make_post_request_with_delay(url, data, headers=None, timeout=30, delay=True):
    """Make a POST request with optional random delay.

    This function applies an optional random delay before making a POST request.
    It's a convenience wrapper around make_post_request that adds delay functionality.

    Args:
        url: The URL to make the POST request to.
        data: Data to send in the POST body. Can be a dict, str, or bytes.
        headers: Optional dictionary of HTTP headers to merge with default headers.
        timeout: Request timeout in seconds (default: 30).
        delay: If True, applies a random delay before the request (default: True).

    Returns:
        A requests.Response object if successful, None if an error occurred.

    Example:
        >>> response = make_post_request_with_delay(
        ...     "https://api.example.com/endpoint",
        ...     data='{"key": "value"}',
        ...     headers={"Content-Type": "application/json"}
        ... )
    """
    if delay:
        _random_delay(url=url)

    return make_post_request(url, data, headers, timeout)


def make_post_request(url, data=None, headers=None, timeout=30):
    """Make a POST request without delay.

    This function makes a POST request using the global session. It handles
    encoding detection and automatic UTF-8 conversion, similar to GET requests.
    On DNS errors, it recreates the session to recover from connection issues.

    Args:
        url: The URL to make the POST request to.
        data: Data to send in the POST body. Can be a dict, str, or bytes.
            If a dict is provided, it will be form-encoded.
        headers: Optional dictionary of HTTP headers to merge with default headers.
        timeout: Request timeout in seconds (default: 30).

    Returns:
        A requests.Response object if successful, None if an error occurred.

    Example:
        >>> response = make_post_request(
        ...     "https://api.example.com/endpoint",
        ...     data={"key": "value"},
        ...     headers={"Content-Type": "application/json"}
        ... )
    """
    global _SESSION
    # merge default headers with provided headers
    merged_headers = {**DEFAULT_HEADERS, **(headers or {})}

    try:
        response = _SESSION.post(
            url, data=data, headers=merged_headers, timeout=timeout
        )
        response.raise_for_status()

        # Ensure proper encoding for text content
        if response.encoding is None or response.encoding.lower() in [
            "iso-8859-1",
            "windows-1252",
        ]:
            response.encoding = "utf-8"

        return response

    except (requests.exceptions.RequestException, socket.gaierror) as e:
        print(f"Error making POST request to {url}: {e}")
        if isinstance(e, socket.gaierror):
            print("DNS resolution error — recreating session")
            _SESSION.close()
            _SESSION = create_session()
        return None


def make_dinamic_request_with_delay(
    url,
    selector,
    timeout=10000,
    delay=True,
    raise_error: bool = False,
    max_retries: int = 3,
    perform_scroll: bool = False,
    min_count: int = 1,
    max_loops: int = 12,
):
    """Make a dynamic request using Playwright for JavaScript-rendered content.

    This function uses Playwright to render JavaScript-heavy pages and wait
    for specific content to appear. It's useful for scraping Single Page
    Applications (SPAs) or pages that load content dynamically.

    The function:
    - Launches a headless Chromium browser
    - Navigates to the URL and waits for the selector to appear
    - Optionally scrolls the page to trigger lazy-loaded content
    - Retries up to max_retries times on failure
    - Returns the final HTML content after all elements are loaded

    Args:
        url: The URL to navigate to.
        selector: CSS selector for the target elements to wait for.
        timeout: Navigation and selector wait timeout in milliseconds
            (default: 10000).
        delay: If True, applies a random delay before navigation (default: True).
        raise_error: If True, raises exceptions instead of returning None
            (default: False).
        max_retries: Maximum number of retry attempts on failure (default: 3).
        perform_scroll: If True, scrolls to the bottom of the page to trigger
            lazy-loaded content (default: False).
        min_count: Minimum number of elements matching the selector required
            (default: 1).
        max_loops: Maximum number of scroll/check loops to reach min_count
            (default: 12).

    Returns:
        The HTML content of the page as a string if successful, None if an
        error occurred and raise_error is False.

    Raises:
        RuntimeError: If the minimum count of elements is not found after
            all retries and loops, and raise_error is True.
        Exception: If raise_error is True and any other error occurs during
            the request.

    Example:
        >>> html = make_dinamic_request_with_delay(
        ...     "https://spa.example.com/products",
        ...     selector=".product-card",
        ...     min_count=10,
        ...     perform_scroll=True
        ... )
        >>> if html:
        ...     # Parse HTML content
        ...     pass

    Note:
        This function is slower than standard requests because it launches
        a full browser. Use it only when JavaScript rendering is necessary.
    """
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent=DEFAULT_HEADERS.get("User-Agent"),
                    viewport={"width": 1366, "height": 768},
                    locale="pt-BR",
                )
                page = context.new_page()

                if delay:
                    _random_delay(url=url)

                page.goto(url, timeout=timeout)

                # Wait for initial target content
                page.wait_for_selector(selector, timeout=timeout, state="attached")

                # Try to reach the minimum number of elements
                def get_count() -> int:
                    try:
                        return page.locator(selector).count()
                    except Exception:
                        return 0

                current_count = get_count()

                for _ in range(max_loops):
                    if current_count >= min_count:
                        break

                    if perform_scroll:
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    # Small wait between attempts
                    time.sleep(0.3 + random.random() * 0.5)

                    current_count = get_count()

                if current_count < min_count:
                    raise RuntimeError(
                        f"Expected at least {min_count} elements for selector, found {current_count}"
                    )

                # Final HTML content
                html_content = page.content()

                browser.close()

                if html_content is None:
                    print(f"Error getting page content for {url}")
                    if raise_error:
                        raise ValueError("Error getting page content")
                    return None

                return html_content

        except Exception as e:
            last_error = e
            print(f"Attempt {attempt}/{max_retries} failed for {url}")
            print(f"Error: {e}")
            if attempt < max_retries:
                _random_delay(url=url)
                continue

    if raise_error and last_error:
        raise last_error
    return None
