import requests
import random
import time
import socket
import brotli
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
    delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
    # print(f"Waiting {delay:.2f} seconds... for {url}")
    time.sleep(delay)


def _make_request(url, headers=None, timeout=30, raise_error: bool = False):
    global _SESSION
    # merge default headers with provided headers
    merged_headers = {**DEFAULT_HEADERS, **(headers or {})}

    try:
        response = _SESSION.get(url, headers=merged_headers, timeout=timeout)
        response.raise_for_status()

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
    if delay:
        _random_delay(url=url)

    return _make_request(url, headers, timeout, raise_error)


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

                # Espera conteúdo alvo inicial
                page.wait_for_selector(selector, timeout=timeout, state="attached")

                # Tenta atingir a quantidade mínima de elementos
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
                    # pequena espera entre tentativas
                    time.sleep(0.3 + random.random() * 0.5)

                    current_count = get_count()

                if current_count < min_count:
                    raise RuntimeError(
                        f"Expected at least {min_count} elements for selector, found {current_count}"
                    )

                # HTML final
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
