"""Scraping script for Tenda marketplace using their public API.

This module provides functionality to scrape product data from Tenda Atacado
(tendaatacado.com.br) using their REST API. It handles authentication, category
retrieval, product fetching, and discount parsing.

Stats:
    - Products loaded:  9360
    - Time spent:       26,78 minutes

Features:
    - OAuth authentication with automatic token refresh
    - Category-based product scraping
    - Support for multiple discount types:
        * Wholesale discounts (bulk pricing)
        * Card discounts
        * Percentage quantity discounts
        * Buy X Get Y promotions
    - Automatic pagination handling
    - Progress logging and error handling
    - Async database insertion

Authentication:
    The module uses a two-step OAuth flow:
    1. Obtain anonymous credentials
    2. Exchange credentials for access token
    The token is cached globally to avoid unnecessary refresh calls.

Discount Types Supported:
    - Wholesale: Bulk pricing discounts based on minimum quantity
    - Card: Card-based discounts (often combined with wholesale)
    - Percentage Quantity: Discounts on specific units (e.g., -40% on 2nd unit)
    - Buy X Get Y: Promotions like "Buy 2 Get 1"

API Endpoints:
    - Categories: /api/recommendations/departments
    - Products: /api/public/store/category/{category_id}/products
    - Auth: /api/public/anonymous-client and /api/public/oauth/access-token

Example:
    Run the scraper:
        >>> python market_tenda_api.py

    The script will:
    1. Authenticate with the API
    2. Fetch all categories
    3. Scrape products from each category
    4. Save products to database and JSON file

Note:
    - Products are inserted into the database asynchronously for better performance
    - All prices are converted to cents (integers) using price_to_int
    - Extraction date is set when the script starts running

TODO:
    - Get the quantity and unit of measure of the products
    - Get promotion restrictions (e.g., max 10 units)
"""

from datetime import datetime
from typing import List
import time
from utils.http_request import make_request_with_delay, make_post_request
from utils.logger import Logger
from utils.encoders import price_to_int
from database.file_storage import save_scraping_products_to_file
from database.models.scraping_product import ScrapingProduct
from database.sql_client import DatabaseClient

# TODO:
# - get the quantity and unit of measure of the products
# - get promotion restrictions max quantity (max 10 unidades)

EXECUTION_TIME = datetime.now()
MARKET = "tenda"  # https://tendaatacado.com.br

URL_API = "https://api.tendaatacado.com.br/api/public/store/category/{category_id}/products?&page={page}&order=relevance"
URL_CATEGORIES = "https://api.tendaatacado.com.br/api/recommendations/departments"

URL_GET_ANONYMOUS_CREDENTIALS = (
    "https://api.tendaatacado.com.br/api/public/anonymous-client"
)
URL_GET_ACCESS_TOKEN = "https://api.tendaatacado.com.br/api/public/oauth/access-token?g-recaptcha-response=null"

LOGGER = Logger(MARKET)

# Global variable to store the token once obtained
_AUTH_HEADERS = None


def _refresh_token() -> dict:
    """Refresh access token for Tenda API authentication.

    This function performs a two-step OAuth authentication process:
    1. Obtains anonymous credentials from the API
    2. Exchanges those credentials for an access token

    The access token is used in subsequent API requests to authenticate
    and authorize access to product data.

    Returns:
        A dictionary containing the OAuth token response with 'access_token'
        and other OAuth fields, or None if authentication fails.

    Raises:
        This function logs errors but doesn't raise exceptions. Callers should
        check if the return value is None or contains the expected keys.
    """
    response_get_anonymous_credentials = make_post_request(
        URL_GET_ANONYMOUS_CREDENTIALS, timeout=5
    )

    if (
        response_get_anonymous_credentials is None
        or response_get_anonymous_credentials.status_code != 201
    ):
        LOGGER.error(
            f"Failed to get anonymous credentials, status code: {response_get_anonymous_credentials.status_code}"
        )
        return None

    data = {
        "username": response_get_anonymous_credentials.json()["user"],
        "password": response_get_anonymous_credentials.json()["password"],
        "client_id": "79ggnm96dwlojly6mqulzval0h4b94gc",
        "client_secret": "ix2tid1exrsvc8u4ta2tys1p495sa3sk3h6o6fgp0kdpu7xgmb595b8525m9rfvj",
        "grant_type": "password",
    }

    response = make_post_request(URL_GET_ACCESS_TOKEN, data=data, timeout=5)

    if response is None or response.status_code != 200:
        LOGGER.error(f"Failed to get access token, status code: {response.status_code}")
        return None

    return response.json()


def _get_auth_headers():
    """Get authentication headers with cached access token.

    This function manages the OAuth access token lifecycle. It caches the
    token globally after the first successful authentication, avoiding
    unnecessary token refresh calls. The cached token is reused for all
    subsequent API requests within the same script execution.

    Returns:
        A dictionary containing authentication headers:
        {
            "Authorization": "Bearer {access_token}",
            "X-Authorization": "Bearer {access_token}"
        }

    Raises:
        Exception: If token refresh fails after attempting to obtain a new token.
            This indicates the API authentication endpoint is unavailable or
            credentials are invalid.

    Note:
        The token is cached in the global variable `_AUTH_HEADERS`. In a
        long-running process, you may want to implement token expiration
        checking and refresh logic.
    """
    global _AUTH_HEADERS

    if _AUTH_HEADERS is None:
        LOGGER.debug("Obtaining fresh token...")
        token_data = _refresh_token()
        if token_data and "access_token" in token_data:
            access_token = token_data["access_token"]
            LOGGER.info(f"Token obtained successfully, access_token: {access_token}")
            _AUTH_HEADERS = {
                "Authorization": f"Bearer {access_token}",
                "X-Authorization": f"Bearer {access_token}",
            }
        else:
            LOGGER.error(f"Failed to refresh token")

            raise Exception("Failed to refresh token, please run the script again")

    return _AUTH_HEADERS


def _build_tenda_api_url(category_id: int, page: int = 1) -> str:
    """Build the Tenda API URL for a specific category and page.

    Args:
        category_id: The numeric identifier of the category.
        page: The page number to retrieve (default: 1).

    Returns:
        A formatted URL string for the Tenda API endpoint that returns
        products for the specified category and page.
    """
    return URL_API.format(category_id=category_id, page=page)


def _log_progress(current: int, total: int, category_name: str, page: int, url: str):
    """Log progress information for category processing.

    This function logs formatted progress information showing the current
    page being processed, total pages, percentage complete, and the URL
    being accessed. The current page number is zero-padded to match the
    width of the total for better readability.

    Args:
        current: The current page number being processed.
        total: The total number of pages for this category.
        category_name: The name of the category being processed.
        page: The page number (same as current, kept for clarity).
        url: The API URL being accessed.
    """
    percentage = (current / total) * 100

    # adjust the width of the current to the size of the total
    width = len(str(total))

    LOGGER.info(
        f"[{current:0{width}d}/{total:0{width}d}] ({percentage:.1f}%) "
        f"Processing category '{category_name}' page {page}. url: {url}"
    )


def _get_all_categories():
    """Retrieve all product categories from the Tenda API.

    This function fetches the complete list of product categories (departments)
    available in the Tenda marketplace. Each category has an ID and name that
    are used for subsequent product queries.

    Returns:
        A list of dictionaries, each containing:
        {
            "id": int,  # Category identifier for API queries
            "name": str  # Human-readable category name
        }

    Example:
        >>> categories = _get_all_categories()
        >>> print(f"Found {len(categories)} categories")
        Found 15 categories
    """
    LOGGER.info(f"Getting all categories from {URL_CATEGORIES}")

    response = make_request_with_delay(
        URL_CATEGORIES,
        headers=_get_auth_headers(),
    )

    response_json = response.json()
    categories_to_return = []
    for department in response_json:
        categories_to_return.append(
            {
                "id": department.get("idDepartment"),
                "name": department.get("nameDepartment"),
            }
        )

    LOGGER.info(f"Total categories found: {len(categories_to_return)}")
    return categories_to_return


def _process_additional_pages(
    category_id: int, category_name: str, number_of_pages: int
):
    """Process additional pages of products for a category (pages 2 and beyond).

    This function handles pagination for categories that have more than one
    page of products. It iterates through pages 2 to number_of_pages, making
    API requests and parsing the product data from each page.

    Args:
        category_id: The numeric identifier of the category.
        category_name: The name of the category being processed.
        number_of_pages: The total number of pages for this category.

    Returns:
        A list of ScrapingProduct objects extracted from pages 2 through
        number_of_pages. Returns an empty list if number_of_pages is 1 or less.

    Note:
        If a page request fails (None response or non-200 status), the function
        logs a warning and continues with the next page instead of failing
        completely.
    """
    category_products_from_additional_pages = []

    for page in range(2, number_of_pages + 1):
        category_url = _build_tenda_api_url(category_id, page)

        _log_progress(page, number_of_pages, category_name, page, category_url)

        response = make_request_with_delay(category_url, headers=_get_auth_headers())

        if response is None or response.status_code != 200:
            LOGGER.info(
                f"No response for category '{category_name}' page {page}. url: {category_url}"
            )
            continue

        category_products_from_additional_pages.extend(
            _parse_tenda_search_products(response.json(), category_url, category_name)
        )

    return category_products_from_additional_pages


def _get_all_products_for_category(category_id: int, category_name: str):
    """Retrieve all products for a specific category across all pages.

    This function handles the complete product retrieval process for a category:
    1. Fetches the first page to get total page count and product count
    2. Processes the first page products
    3. If multiple pages exist, processes additional pages
    4. Validates that the number of products found matches the expected count

    Args:
        category_id: The numeric identifier of the category.
        category_name: The name of the category being processed.

    Returns:
        A list of ScrapingProduct objects for all products in the category.
        Returns an empty list if the category has no products.

    Note:
        If the actual number of products found differs from the expected count
        reported by the API, a warning is logged but the function still returns
        all products that were successfully retrieved.
    """
    category_url = _build_tenda_api_url(category_id)
    LOGGER.debug(f"Getting all products for category {category_name} ({category_url})")

    # Get products from the first page
    response = make_request_with_delay(category_url, headers=_get_auth_headers())
    response_json = response.json()

    number_of_pages = response_json.get("total_pages")
    number_of_products = response_json.get("total_products")

    if number_of_products == 0:
        LOGGER.warning(
            f"0 products found for category '{category_name}' -> {category_url}"
        )
        return []

    all_category_products = []
    all_category_products.extend(
        _parse_tenda_search_products(response_json, category_url, category_name)
    )

    _log_progress(1, number_of_pages, category_name, 1, category_url)

    # Get products from the additional pages
    if number_of_pages > 1:
        all_category_products.extend(
            _process_additional_pages(category_id, category_name, number_of_pages)
        )

    if len(all_category_products) != number_of_products:
        LOGGER.warning(
            f"Number of products found for category '{category_name}' "
            f"is different from the number of products in the response. "
            f"actual: {len(all_category_products)} != expected: {number_of_products}"
        )
    else:
        LOGGER.info(
            f"Finished scraping category '{category_name}' - {len(all_category_products)}/{number_of_products} products retrieved"
        )

    return all_category_products


def _parse_tenda_search_products(
    search_response: dict, extraction_url: str, category_name: str
) -> List[ScrapingProduct]:
    """Parse product data from Tenda API response into ScrapingProduct objects.

    This function extracts product information from the Tenda API response and
    creates ScrapingProduct objects with all associated discounts. It handles
    multiple discount types including wholesale, card, percentage quantity, and
    buy-X-get-Y promotions.

    Discount Types Handled:
        - Wholesale: Bulk pricing with minimum quantity requirements
        - Card: Card-based discounts (often combined with wholesale)
        - "Desconto Percentual X": Direct price replacement for percentage discounts
        - "X% Off na Y unidade": Percentage discount on specific units
        - "Leve X Pague Y": Buy X Get Y promotions

    Args:
        search_response: The JSON response from the Tenda API containing product
            data in the "products" key.
        extraction_url: The URL from which the data was extracted (for tracking).
        category_name: The category name to assign to all products.

    Returns:
        A list of ScrapingProduct objects with all product information and
        discounts properly configured.

    Note:
        Unknown promotion types are logged as warnings but don't prevent
        product creation. Prices are automatically converted to cents (integers).
    """
    normalized_products: List[ScrapingProduct] = []

    for product_item in search_response.get("products", []):
        scraping_product = ScrapingProduct(
            name=product_item.get("name"),
            category=category_name,
            market=MARKET,
            price=price_to_int(product_item.get("price")),
            source_id=(
                str(product_item.get("id"))
                if product_item.get("id") is not None
                else None
            ),
            brand=product_item.get("brand"),
            # TODO: get the quantity and unit of measure
            # quantity=1,
            # unit_of_measure="UNIT",
            product_url=product_item.get("url"),
            extraction_url=extraction_url,
            extraction_date=EXECUTION_TIME,
        )

        # get wholesale discounts + card discounts
        if product_item.get("wholesalePrices"):
            for wholesale_price in product_item.get("wholesalePrices"):
                price = price_to_int(wholesale_price.get("price"))

                scraping_product.add_wholesale_discount(
                    discounted_price=price,
                    min_quantity=wholesale_price.get("minQuantity"),
                )

                scraping_product.add_card_discount(
                    discounted_price=price,
                )

        # get promotion
        promotion = product_item.get("promotion")
        if promotion:
            promotion_type = promotion.get("type")

            # get promotion type PERCENTAGE_QUANTITY for one unit
            if promotion_type == "Desconto Percentual X":
                scraping_product.price = price_to_int(promotion.get("price"))

            # get promotion type PERCENTAGE_QUANTITY
            if promotion_type == "X% Off na Y unidade":
                scraping_product.add_percentage_quantity_discount(
                    discounted_price=price_to_int(promotion.get("price")),
                    min_quantity=int(promotion.get("y")),
                    conditions_text=f"-{promotion.get('x')}% on the {int(promotion.get('y'))} unit",
                )

            # get promotion type BUY_X_GET_Y
            if promotion_type == "Leve X Pague Y":
                buy_quantity = int(promotion.get("y"))
                get_quantity = int(promotion.get("x"))

                scraping_product.add_buy_x_get_y_discount(
                    discounted_price=price_to_int(promotion.get("price")),
                    buy_quantity=buy_quantity,
                    get_quantity=get_quantity,
                    conditions_text=f"Buy {buy_quantity} Get {get_quantity}",
                )

            if promotion_type not in [
                "Leve X Pague Y",
                "X% Off na Y unidade",
                "Desconto Percentual X",
            ]:
                LOGGER.warning(
                    f"Unknown promotion type: {promotion_type} for product {scraping_product.name} - {extraction_url}"
                )

        normalized_products.append(scraping_product)

    return normalized_products


def main():
    """Main function to run the Tenda marketplace scraper.
    
    This function orchestrates the complete scraping process:
    1. Authenticates with the API (OAuth token refresh)
    2. Fetches all product categories
    3. Processes each category sequentially with pagination
    4. Inserts products into database asynchronously
    5. Saves all products to a JSON file
    6. Waits for all database insertions to complete
    7. Logs execution statistics
    
    The function uses async database insertion to improve performance when
    processing multiple categories. All products are collected in memory before
    being saved to file.
    
    Progress Tracking:
        Progress is logged for each category showing:
        - Category name and ID
        - Overall progress percentage
        - Category index and total categories
    
    Note:
        Database insertions run in separate threads to avoid blocking the
        main scraping process. The script processes categories sequentially
        to avoid overwhelming the API.
    """
    start_time = time.time()
    LOGGER.info("Starting Tenda API scraper")

    db_client = DatabaseClient(MARKET)
    active_threads = []

    categories = _get_all_categories()

    # TESTING
    # categories = [{"id": 3412, "name": "Mercearia"}]

    all_products = []
    total_categories = len(categories)
    for idx, category in enumerate(categories, 1):
        LOGGER.info(
            f"Processing category '{category["name"]}' (ID: {category["id"]})"
            f" -> progress: [{idx:02d}/{total_categories:02d}] ({(idx/total_categories)*100:.1f}%) "
        )

        category_products = _get_all_products_for_category(
            category["id"], category["name"]
        )

        if len(category_products) > 0:
            all_products.extend(category_products)
            thread = db_client.insert_scraping_products_with_discounts_async(
                category_products, category["name"]
            )
            active_threads.append(thread)

    save_scraping_products_to_file(all_products, MARKET, EXECUTION_TIME.isoformat())

    # wait for all database insertions to complete
    LOGGER.info("Waiting for all database insertions to complete...")
    for thread in active_threads:
        thread.join()

    end_time = time.time()
    total_time_seconds = end_time - start_time
    total_time_minutes = total_time_seconds / 60
    LOGGER.info(f"Tenda API scraper finished in {total_time_minutes:.2f} minutes")


if __name__ == "__main__":
    main()
