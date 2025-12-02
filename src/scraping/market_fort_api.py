"""Scraping script for Fort Atacadista marketplace.

This module provides functionality to scrape product data from Fort Atacadista
(deliveryfort.com.br) using their sitemap and API. It uses XML sitemaps to
discover product URLs and then queries the API for detailed product information.

Stats:
    - Products loaded:  7541
    - Time spent:       6h 21m 48s

Features:
    - Sitemap-based product discovery
    - Product URL collection from multiple sitemap files
    - API-based product detail extraction
    - Support for card discounts (Vuon 810)
    - Batch processing with async database insertion
    - Progress logging and error handling

Workflow:
    1. Download sitemap index XML
    2. Filter and collect product sitemap URLs
    3. Extract product URLs from each product sitemap
    4. Deduplicate URLs while preserving order
    5. Query API for each product's details
    6. Parse and extract product data with discounts
    7. Insert products in batches to database

API Details:
    - Base URL: https://www.deliveryfort.com.br/api/catalog_system/pub/products/search
    - Method: GET
    - Endpoint format: {API_URL}/{product_id}
    - Authentication: None required (public API)

Discount Types Supported:
    - Card (Vuon 810): Card-based discounts with specific pricing

Product Data Available:
    - Product name, category, brand
    - Price, quantity, unit of measure
    - Product URL, source ID
    - Card discounts if available

Example:
    Run the scraper:
        >>> python market_fort_api.py

    The script will:
    1. Collect all product URLs from sitemaps
    2. Process each product sequentially
    3. Insert products in batches of 50
    4. Save all products to JSON file

Note:
    - Products are inserted into the database in batches for better performance
    - All prices are converted to cents (integers) using price_to_int
    - Extraction date is set when the script starts running
    - Products with multiple sellers log a warning but use the first seller

TODO:
    - Add the price even if the product is not available (IsAvailable field)
"""

from datetime import datetime
from typing import List
import time
from utils.http_request import make_request_with_delay
from utils.encoders import price_to_int
from utils.logger import Logger
from utils.parsers import parse_xml
from database.file_storage import save_scraping_products_to_file
from database.sql_client import DatabaseClient
from database.models.scraping_product import ScrapingProduct

# TODO:
# Add the price even if the product is not available (IsAvailable)


EXECUTION_TIME = datetime.now()
MARKET = "fort"
LOGGER = Logger(MARKET)

SITEMAP_INDEX_URL = "https://www.deliveryfort.com.br/sitemap.xml"
API_URL = "https://www.deliveryfort.com.br/api/catalog_system/pub/products/search"


def _collect_all_product_urls() -> List[str]:
    """Collect all product URLs from Fort Atacadista sitemap files.

    This function retrieves product URLs by:
    1. Downloading the sitemap index XML
    2. Filtering for product-specific sitemap URLs (containing "/sitemap/product-")
    3. Downloading and parsing each product sitemap
    4. Extracting all product URLs from each sitemap
    5. Deduplicating URLs while preserving order

    The sitemap index contains references to multiple sitemap files, each
    containing a subset of product URLs. This function aggregates all URLs
    from all product sitemaps.

    Returns:
        A list of unique product URLs (strings) in order of discovery.
        Returns an empty list if the sitemap index cannot be parsed.

    Note:
        If a product sitemap fails to download or parse, an error is logged
        but processing continues with the next sitemap. The function uses
        XML namespaces to properly parse sitemap XML structure.
    """
    LOGGER.info(f"Collecting product URLs via sitemap: {SITEMAP_INDEX_URL}")

    # Download sitemap index
    sitemap_response = make_request_with_delay(SITEMAP_INDEX_URL)
    index_xml = parse_xml(sitemap_response)

    if index_xml is None:
        LOGGER.error("Failed to parse sitemap index XML")
        return []

    # Filter only product sitemaps
    product_sitemap_urls = []
    for loc in index_xml.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
        url_text = loc.text
        if url_text and "/sitemap/product-" in url_text:
            product_sitemap_urls.append(url_text.strip())

    # Iterate each product sitemap and aggregate URLs
    all_product_urls: List[str] = []
    for sm_url in product_sitemap_urls:
        try:
            sm_resp = make_request_with_delay(sm_url)
            sm_xml = parse_xml(sm_resp)

            if sm_xml is not None:
                for loc in sm_xml.findall(
                    ".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
                ):
                    if loc.text:
                        all_product_urls.append(loc.text.strip())
        except Exception as e:
            LOGGER.error(f"Failed to process sitemap {sm_url}: {e}")
            continue

    # Dedup preserving order
    seen = set()
    deduped: List[str] = []
    for u in all_product_urls:
        if u not in seen:
            deduped.append(u)
            seen.add(u)

    return deduped


def _extract_product_data(product_url: str) -> ScrapingProduct:
    """Extract product data from Fort Atacadista API.

    This function takes a product URL, constructs the API endpoint, and
    extracts detailed product information including price, category, brand,
    and discounts. It handles the API response structure and converts data
    to the ScrapingProduct model.

    The API endpoint is constructed by extracting the product ID from the
    URL (last two path segments) and appending it to the base API URL.

    Args:
        product_url: The full product URL from the sitemap
            (e.g., "https://www.deliveryfort.com.br/product-name/p").

    Returns:
        A ScrapingProduct object with all available product information
        and discounts, or None if:
        - The API request fails
        - The product is not found (empty response)
        - An error occurs during parsing

    Discount Handling:
        - Card discounts are extracted from the "Preco Vuon 810" field
        - If present, a card discount is added with the discounted price

    Note:
        If a product has multiple sellers, a warning is logged but the
        function uses the first seller's data. All prices are converted
        to cents (integers) before storage.
    """
    try:
        product_id = "/".join(product_url.split("/")[-2:])
        product_url_api = API_URL + "/" + product_id

        response = make_request_with_delay(product_url_api)
        if response is None or response.status_code != 200:
            LOGGER.error(
                f"Error fetching product {product_url_api}: {response.status_code if response else 'No response'}"
            )
            return None

        if len(response.json()) == 0:
            LOGGER.error(f"Product {product_url_api} not found")
            return None

        product_json = response.json()[0]
        product_json_items = product_json["items"][0]

        if len(product_json_items["sellers"]) > 1:
            LOGGER.warning(
                f"Product {product_json['productName']} has more than one seller, url: {product_url_api}"
            )

        scraping_product = ScrapingProduct(
            name=product_json["productName"],
            category=product_json["categories"][-1].replace("/", ""),
            brand=product_json["brand"],
            price=price_to_int(
                product_json_items["sellers"][0]["commertialOffer"]["Price"]
            ),
            unit_of_measure=product_json_items["measurementUnit"],
            quantity=product_json_items["unitMultiplier"] or 1,
            product_url=product_url,
            extraction_url=product_url_api,
            source_id=product_json["productId"],
            market=MARKET,
            extraction_date=EXECUTION_TIME,
        )

        # add discounts if available
        discount_raw = product_json.get("Preco Vuon 810")
        if discount_raw is not None:
            scraping_product.add_card_discount(
                discounted_price=price_to_int(discount_raw[0]),
            )

        return scraping_product

    except Exception as e:
        LOGGER.error(f"Error processing product {product_url}: {e}")
        return None


def main():
    """Main function to run the Fort Atacadista scraper.

    This function orchestrates the complete scraping process:
    1. Collects all product URLs from sitemaps
    2. Processes each product URL sequentially
    3. Extracts product data via API
    4. Groups products into batches (50 products per batch)
    5. Inserts batches into database asynchronously
    6. Saves all products to a JSON file
    7. Waits for all database insertions to complete
    8. Logs execution statistics

    Batch Processing:
        Products are collected and inserted in batches of 50 to balance
        performance and memory usage. Each batch is inserted asynchronously
        to avoid blocking the main scraping process.

    Progress Tracking:
        Progress is logged for each product showing percentage complete
        and current product URL being processed.

    Note:
        The script processes products sequentially to avoid overwhelming
        the API. Database insertions run in parallel using threads to
        improve overall performance.
    """
    start_time = time.time()
    LOGGER.info(f"Starting {MARKET} scraper")

    db_client = DatabaseClient(MARKET)
    active_threads = []

    products_urls = _collect_all_product_urls()

    # TESTING
    # products_urls = [
    #     "https://www.deliveryfort.com.br/agua-sanitaria-qboa-2-litros/p",
    #     "https://www.deliveryfort.com.br/pera-importada/p",
    # ]

    total_products = len(products_urls)

    products = []
    batch_number = 0
    batch_size = 50  # Insert every 50 products
    batch_products = []

    for idx, url in enumerate(products_urls, 1):
        LOGGER.info(
            f"{(idx/total_products)*100:.1f}% ({idx}/{total_products}) Processing product: {url}"
        )

        product = _extract_product_data(url)

        if product is not None:
            products.append(product)
            batch_products.append(product)

            if len(batch_products) >= batch_size:
                batch_number += 1
                thread = db_client.insert_scraping_products_with_discounts_async(
                    batch_products, batch_number
                )
                active_threads.append(thread)
                batch_products = []  # Clear the batch

    # Insert remaining products if there are any
    if batch_products:
        thread = db_client.insert_scraping_products_with_discounts_async(
            batch_products, batch_number
        )
        active_threads.append(thread)

    # save products to file
    save_scraping_products_to_file(products, MARKET, EXECUTION_TIME.isoformat())

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
