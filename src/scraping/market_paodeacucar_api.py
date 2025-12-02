"""Scraping script for Pão de Açúcar marketplace using GPA Digital API.

This module provides functionality to scrape product data from Pão de Açúcar
using the GPA Digital API (api.vendas.gpa.digital). It handles category-based
product fetching, promotion parsing, and discount extraction.

Stats:
    - Products loaded:  16366
    - Time spent:       22,15 minutes

Features:
    - Category-based product scraping with pagination support
    - Support for multiple discount types:
        * Card discounts (FIC seal color)
        * Percentage quantity discounts (AMARELO seal color)
        * Buy X Get Y promotions
    - Automatic pagination handling
    - Progress logging and error handling
    - Async database insertion

Discount Types Supported:
    - Card (FIC): Card-based discounts requiring specific payment methods
    - Percentage Quantity (AMARELO): Discounts on specific units
      - If discount applies to 1st unit: Direct price replacement
      - Otherwise: Percentage discount on specific unit (e.g., -40% on 2nd unit)
    - Buy X Get Y: Promotions where buying X items gets you Y items

API Details:
    - Endpoint: https://api.vendas.gpa.digital/pa/search/category-page
    - Method: POST
    - Authentication: None required (public API)
    - Store ID: 461 (Pão de Açúcar store)
    - Results per page: 100

Example:
    Run the scraper:
        >>> python market_paodeacucar_api.py

    The script will:
    1. Process all configured categories
    2. Fetch products with pagination
    3. Parse promotions and discounts
    4. Save products to database and JSON file

Note:
    - Products are inserted into the database asynchronously for better performance
    - All prices are converted to cents (integers) using price_to_int
    - Extraction date is set when the script starts running
    - The API returns totalPages in each response for pagination

TODO:
    - Get the quantity and unit of measure from the API (not currently available)
"""

import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from utils.http_request import make_post_request_with_delay
from utils.encoders import price_to_int
from utils.logger import Logger
from database.file_storage import save_scraping_products_to_file
from database.sql_client import DatabaseClient
from database.models.scraping_product import ScrapingProduct

# TODO:
# - quantity and unit of measure not available in the API

EXECUTION_TIME = datetime.now()
MARKET = "paodeacucar"
LOGGER = Logger(MARKET)

API_URL = "https://api.vendas.gpa.digital/pa/search/category-page"
STORE_ID = 461

HEADERS = {
    "Content-Type": "application/json",
}


def _create_request_body(category: str, page: int = 1) -> Dict[str, Any]:
    """Create API request body for Pão de Açúcar category search.

    This function constructs the JSON request body required by the GPA Digital
    API for category-based product searches. The request includes pagination
    parameters and sorting preferences.

    Args:
        category: The category identifier (e.g., "alimentos", "bebidas").
        page: The page number to retrieve (default: 1).

    Returns:
        A JSON string containing the request body with all required parameters
        for the API endpoint.

    Note:
        The request includes fixed parameters like partner ("linx"), storeId (461),
        and department ("ecom") that are specific to the Pão de Açúcar marketplace.
    """
    return json.dumps(
        {
            "partner": "linx",
            "page": page,
            "resultsPerPage": 100,
            "multiCategory": category,
            "sortBy": "relevance",
            "department": "ecom",
            "storeId": STORE_ID,
            "customerPlus": True,
        }
    )


def _make_api_request(category: str, page: int = 1) -> Optional[Dict[str, Any]]:
    """Make API request to Pão de Açúcar/GPA Digital API with error handling.

    This function makes a POST request to the Pão de Açúcar category search endpoint
    and handles errors gracefully. It includes automatic delay to avoid rate
    limiting and proper error logging.

    Args:
        category: The category identifier to search for.
        page: The page number to retrieve (default: 1).

    Returns:
        A dictionary containing the JSON response from the API, which includes:
        - products: List of product objects
        - totalPages: Total number of pages available
        - Other metadata fields
        Returns None if the request fails or returns a non-200 status code.

    Note:
        All errors are logged but exceptions are caught and None is returned,
        making it safe to call without try/except blocks.
    """
    try:
        request_body = _create_request_body(category, page)
        response = make_post_request_with_delay(API_URL, request_body, headers=HEADERS)

        if not response or response.status_code != 200:
            raise Exception(
                f"Http error {response.status_code}, response: {response.text}"
            )

        return response.json()

    except Exception as e:
        LOGGER.error(f"Error requesting '{category}', page {page}: {str(e)}")
        return None


def _extract_product_data(
    product_data: Dict[str, Any], category: str
) -> Optional[ScrapingProduct]:
    """Convert API product data to ScrapingProduct model with discounts.

    This function extracts product information from the Pão de Açúcar API response
    and creates a ScrapingProduct object with all associated promotions and discounts.
    It handles multiple promotion types based on the "promotionSealColor" field.

    Promotion Types Handled:
        - FIC (Card): Card-based discounts requiring specific payment methods
        - AMARELO (Yellow): Can be either:
            * PERCENTAGE_QUANTITY: Percentage discount on specific units
            * BUY_X_GET_Y: Buy X Get Y promotions

    Special Cases:
        - If promotionPercentOffOnUnity == 1: Direct price replacement (no discount object)
        - Buy X Get Y: Effective price is calculated as (original_price * buy) / get

    Args:
        product_data: Dictionary containing product data from the API response.
        category: The category name to assign to the product.

    Returns:
        A ScrapingProduct object with all product information and discounts
        properly configured, or None if parsing fails.

    Note:
        If parsing fails for any product, an error is logged with the product
        data, but the function returns None to allow processing to continue
        with other products.
    """
    try:
        product_price = price_to_int(product_data["price"])

        scraping_product = ScrapingProduct(
            name=product_data["name"],
            market=MARKET,
            price=product_price,
            extraction_date=EXECUTION_TIME,
            category=category,
            brand=product_data.get("brand", None),
            product_url=product_data["urlDetails"],
            # TODO: get the quantity and unit of measure from the API
            # quantity=1,
            # unit_of_measure="UNIT",
            source_id=product_data["sku"],
            extraction_url=API_URL,
        )

        if product_data.get("productPromotions", None) is not None:
            for promotion in product_data["productPromotions"]:
                promotion_price = price_to_int(promotion["unitPrice"])

                promotion_percent_off = promotion["promotionPercentOff"]
                promotion_quantity_buy = promotion["promotionQuantityPayFor"]
                promotion_quantity_get = promotion["promotionQuantityBuy"]

                if promotion["promotionSealColor"] == "FIC":
                    scraping_product.add_card_discount(
                        promotion_price, f"{promotion_percent_off}% card"
                    )

                if promotion["promotionSealColor"] == "AMARELO":
                    promotion_percent_off_on_unity = promotion[
                        "promotionPercentOffOnUnity"
                    ]

                    # determine the promotion type
                    promotion_type = "PERCENTAGE_QUANTITY"
                    if (
                        promotion_quantity_buy is not None
                        and promotion_quantity_get is not None
                    ):
                        promotion_type = "BUY_X_GET_Y"

                    if promotion_type == "PERCENTAGE_QUANTITY":
                        if promotion_percent_off_on_unity == 1:
                            scraping_product.price = promotion_price
                        else:
                            scraping_product.add_percentage_quantity_discount(
                                promotion_price,
                                promotion_percent_off_on_unity,
                                f"-{promotion_percent_off}% on the {promotion_percent_off_on_unity} unit",
                            )

                    if promotion_type == "BUY_X_GET_Y":
                        # Calculate promotion price for the buy x get y discount rounded to 2 decimal places
                        buy_x_get_y_price = price_to_int(
                            round(
                                (product_data["price"] * promotion_quantity_buy)
                                / promotion_quantity_get,
                                2,
                            )
                        )

                        scraping_product.add_buy_x_get_y_discount(
                            buy_x_get_y_price,
                            promotion_quantity_buy,
                            promotion_quantity_get,
                            conditions_text=f"Buy {promotion_quantity_buy} Get {promotion_quantity_get}",
                        )

        return scraping_product

    except Exception as e:
        LOGGER.error(
            f"Error parsing product: {str(e)}. data: {json.dumps(product_data)}"
        )
        return None


def _log_progress(current_page: int, total_pages: int, category_name: str):
    """Log progress information for category processing.

    This function logs formatted progress information showing the current
    page being processed, total pages, percentage complete, and the category
    name. The current page number is zero-padded to match the width of the
    total for better readability.

    Args:
        current_page: The current page number being processed.
        total_pages: The total number of pages for this category.
        category_name: The name of the category being processed.
    """
    percentage = (current_page / total_pages) * 100

    # adjust the width of the current to the size of the total
    width = len(str(total_pages))

    LOGGER.info(
        f"[{current_page:0{width}d}/{total_pages:0{width}d}] ({percentage:.1f}%) "
        f"Processing category '{category_name}' page {current_page}."
    )


def _get_all_products_for_category_page(
    category: str, page: int
) -> tuple[List[ScrapingProduct], int]:
    """Retrieve products for a specific category and page.

    This function makes an API request for a specific category and page,
    extracts all products from the response, and returns both the products
    and the total number of pages available for that category.

    Args:
        category: The category identifier to search for.
        page: The page number to retrieve.

    Returns:
        A tuple containing:
        - List of ScrapingProduct objects for the requested page
        - Total number of pages available for the category (int)

        If the API request fails, returns ([], 0).

    Note:
        Products that fail to parse (return None from _extract_product_data)
        are still appended to the list, so the list may contain None values.
        The caller should filter these out if needed.
    """
    response = _make_api_request(category, page)

    if response is None:
        return [], 0

    all_category_products = []
    for product in response["products"]:
        all_category_products.append(_extract_product_data(product, category))

    total_pages = response.get("totalPages", 0)
    return all_category_products, total_pages


def _get_all_products_for_category(category: str) -> List[ScrapingProduct]:
    """Retrieve all products for a specific category across all pages.

    This function handles the complete product retrieval process for a category:
    1. Fetches the first page to get total page count and products
    2. Processes the first page products
    3. If multiple pages exist, processes additional pages using
       _get_all_products_for_category_page
    4. Returns all products from all pages

    Args:
        category: The category identifier to search for (e.g., "alimentos").

    Returns:
        A list of ScrapingProduct objects for all products in the category
        across all pages. Returns an empty list if the API request fails.

    Note:
        Progress is logged for each page using _log_progress. The function
        uses the totalPages value from the first page response to determine
        how many additional pages to fetch.
    """
    all_category_products = []
    response = _make_api_request(category)

    if response is None:
        return []

    number_of_total_pages = response["totalPages"]
    raw_products = response["products"]

    _log_progress(1, number_of_total_pages, category)

    # process page 1
    for product in raw_products:
        all_category_products.append(_extract_product_data(product, category))

    # process additional pages
    for page_number in range(2, number_of_total_pages + 1):
        _log_progress(page_number, number_of_total_pages, category)
        page_products, _ = _get_all_products_for_category_page(category, page_number)
        all_category_products.extend(page_products)

    return all_category_products


def main():
    """Main function to run the Pão de Açúcar marketplace scraper.

    This function orchestrates the complete scraping process:
    1. Initializes database client
    2. Processes all configured categories sequentially
    3. Fetches products for each category with pagination
    4. Inserts products into database asynchronously
    5. Saves all products to a JSON file
    6. Waits for all database insertions to complete
    7. Logs execution statistics

    The function uses async database insertion to improve performance when
    processing multiple categories. All products are collected in memory before
    being saved to file.

    Categories Processed:
        - alimentos, bebidas, limpeza, descartaveis
        - bebe-e-crianca, perfumaria, petshop, bazar, textil
        - caras-do-brasil

    Note:
        Progress is logged for each category and overall execution time is
        reported at the end. Database insertions run in separate threads to
        avoid blocking the main scraping process.
    """
    start_time = time.time()
    LOGGER.info(f"Starting {MARKET} scraper")

    db_client = DatabaseClient(MARKET)
    active_threads = []

    # TESTING
    categories = [
        "alimentos",
        "bebidas",
        "limpeza",
        "descartaveis",
        "bebe-e-crianca",
        "perfumaria",
        "petshop",
        "bazar",
        "textil",
        "caras-do-brasil",
    ]

    total_categories = len(categories)
    all_products = []
    for idx, category in enumerate(categories, 1):
        category_products = _get_all_products_for_category(category)

        LOGGER.info(
            f"Finished processing category '{category}' {len(category_products)} products found"
            f" -> progress: {(idx/total_categories)*100:.1f}% [{idx:02d}/{total_categories:02d}]"
        )

        if len(category_products) > 0:
            thread = db_client.insert_scraping_products_with_discounts_async(
                category_products, category
            )
            active_threads.append(thread)
            all_products.extend(category_products)

    if all_products:
        LOGGER.info(f"Total products collected: {len(all_products)}")
        save_scraping_products_to_file(all_products, MARKET, EXECUTION_TIME.isoformat())

    else:
        LOGGER.warning("No products collected")

    # wait for all database insertions to complete
    LOGGER.info("Waiting for all database insertions to complete...")
    for thread in active_threads:
        thread.join()

    end_time = time.time()
    total_time_seconds = end_time - start_time
    total_time_minutes = total_time_seconds / 60
    LOGGER.info(f"{MARKET} scraper finished in {total_time_minutes:.2f} minutes")


if __name__ == "__main__":
    main()
