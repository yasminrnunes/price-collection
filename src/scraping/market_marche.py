"""Scraping script for St Marche marketplace.

This module provides functionality to scrape product data from St Marche
(marche.com.br) using HTML parsing. It extracts product information from
category pages with pagination support.

Stats:
    - Products loaded:  7481
    - Time spent:       17,27 minutes

Features:
    - HTML-based scraping using BeautifulSoup
    - Category discovery from homepage
    - Pagination handling with duplicate detection
    - Support for different unit of measurements (KG, L, UN, etc.)
    - Quantity extraction from product names
    - Encoding handling for special characters
    - Async database insertion

Workflow:
    1. Fetch homepage to discover categories
    2. For each category, paginate through all pages
    3. Extract product data from each page
    4. Deduplicate products by URL
    5. Parse product details (name, price, quantity, unit of measure)
    6. Insert products asynchronously to database
    7. Save all products to JSON file

Product Data Extraction:
    - Product name (with encoding fixes)
    - Price (regular or weight-based)
    - Source ID (extracted from image URL)
    - Unit of measure (KG, L, UN, etc.)
    - Quantity (for weight-based products)
    - Category name
    - Product URL

Special Features:
    - Handles weight-based pricing (price per KG/L)
    - Extracts quantity restrictions from product names
      (e.g., "(máx 24 unidades por cpf)")
    - Handles encoding issues with encode_text utility
    - Deduplicates products by URL to avoid processing same product twice

Limitations:
    - Only displays products that are available (unavailable products not shown)
    - Product brand is not available (only in product name)
    - Quantity restrictions in product name are detected but not stored

Example:
    Run the scraper:
        >>> python market_marche.py

    The script will:
    1. Discover all categories from homepage
    2. Process each category with pagination
    3. Extract and deduplicate products
    4. Insert products into database asynchronously
    5. Save all products to JSON file

Note:
    - Products are inserted into the database asynchronously for better performance
    - All prices are converted to cents (integers) using price_to_int
    - Extraction date is set when the script starts running
    - The store_id can be configured (default: 66677604431 for Pavao)

TODO:
    - Map max quantity restrictions from product names
    - Brand extraction from product names (if possible)
"""

import time
import re
from datetime import datetime
from utils.encoders import encode_text, string_to_decimal
from utils.http_request import make_request_with_delay
from utils.parsers import parse_html
from utils.logger import Logger
from utils.encoders import price_to_int
from database.sql_client import DatabaseClient
from database.models.scraping_product import ScrapingProduct
from database.file_storage import save_scraping_products_to_file

# TODO: St Marche comments
# St Marche only displays products that are available.
# The product brand is not available.
# The quantity restriction by product is registered in the product name --> Cerveja Pilsen Corona Lata 350ml (máx 24 unidades por cpf)

EXECUTION_TIME = datetime.now()
MARKET = "stMarche"
LOGGER = Logger(MARKET)

BASE_URL = "https://marche.com.br"

STORE_ID = 66677604431  # Pavao
# STORE_ID = 66677538895 # Mooca
STORE_URL = f"?store_id={STORE_ID}"

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


def _extract_max_quantity(product_name: str):
    """Extract maximum quantity restriction from product name.

    This function extracts quantity restrictions that appear in product names
    in the format "(máx XX <unit> por cpf)" where XX can be any number (integer
    or decimal) and <unit> can be any text (e.g., "unidades", "kg", "litros").

    The function also returns a cleaned product name with the restriction
    pattern removed.

    Args:
        product_name: The product name that may contain a quantity restriction
            pattern (e.g., "Cerveja Pilsen Corona Lata 350ml (máx 24 unidades por cpf)").

    Returns:
        A tuple containing:
        - clean_name (str): Product name with the restriction pattern removed
        - max_quantity (float or None): The extracted quantity as a float,
          or None if no restriction pattern is found

    Example:
        >>> name, qty = _extract_max_quantity(
        ...     "Product Name (máx 10 unidades por cpf)"
        ... )
        >>> print(name, qty)
        "Product Name" 10.0

        >>> name, qty = _extract_max_quantity("Product Name")
        >>> print(name, qty)
        "Product Name" None

    Note:
        The pattern matching is case-insensitive. Commas in numbers are
        automatically converted to dots for float conversion.
    """
    # Match any number (integer or decimal) followed by any text until "por cpf"
    match = re.search(
        r"\(máx\s+([\d.,]+)\s*[a-zA-ZÀ-ÿ%]*\s+por\s+cpf\)", product_name, re.IGNORECASE
    )

    if match:
        # Replace comma with dot for float conversion
        max_quantity = float(match.group(1).replace(",", "."))
    else:
        max_quantity = None

    # Remove the matched "(máx ... por cpf)" part from the product name
    clean_name = re.sub(
        r"\(máx\s+[\d.,]+\s*[a-zA-ZÀ-ÿ%]*\s+por\s+cpf\)",
        "",
        product_name,
        flags=re.IGNORECASE,
    ).strip()

    return clean_name, max_quantity


def _get_all_categories():
    """Retrieve all product categories from St Marche homepage.

    This function fetches the St Marche homepage (with store ID parameter)
    and extracts all category links from the category slider section. Each
    category is represented by a name and URL.

    Returns:
        A list of dictionaries, each containing:
        {
            "name": str,  # Category name (with encoding fixes)
            "url": str     # Full category URL with store_id parameter
        }

    Note:
        The function uses BeautifulSoup to parse the HTML and finds categories
        by looking for links within a div with class starting with "category-slider".
        Text encoding is fixed using encode_text utility to handle special characters.
    """
    LOGGER.info(f"Getting all categories from {BASE_URL + STORE_URL}")

    response = make_request_with_delay(
        BASE_URL + STORE_URL,
        headers=HEADERS,
    )

    soup = parse_html(response)

    categories_div = soup.find(
        "div", class_=lambda x: x and x.startswith("category-slider")
    )

    categories_to_return = []

    for category_list in categories_div.find_all("a", href=True):
        categories_to_return.append(
            {
                "name": encode_text(category_list.get_text(strip=True)),
                "url": BASE_URL + category_list["href"] + STORE_URL,
            }
        )

    LOGGER.info(f"Total categories found: {len(categories_to_return)}")
    return categories_to_return


def _extract_product_data(soup_product, category_name, category_url_with_page):
    """Extract product data from BeautifulSoup product element.

    This function extracts detailed product information from a parsed HTML
    product element. It handles different pricing structures (regular vs
    weight-based), unit of measurements, and extracts source IDs from image URLs.

    Product Data Extracted:
        - Product name (with encoding fixes)
        - Price (regular price or weight-based price)
        - Source ID (from image URL query parameter "v=")
        - Unit of measure (KG, L, UN, etc.)
        - Quantity (for weight-based products like KG/L)
        - Product URL

    Price Handling:
        - Regular products: Uses "_product-card-price-regular" class
        - Weight-based products (KG/L): Uses "_product-card-price-measurement-weight"
          and extracts quantity from "_product-card-measurement"

    Args:
        soup_product: BeautifulSoup element representing a product link.
        category_name: The category name to assign to the product.
        category_url_with_page: The category URL with page number for tracking
            extraction source.

    Returns:
        A ScrapingProduct object with all available product information,
        or None if critical data cannot be extracted.

    Note:
        The function uses safe_find_text helper to gracefully handle missing
        elements. Source ID extraction from image URL may fail silently if the
        URL structure doesn't contain "v=" parameter.
    """

    def safe_find_text(class_prefix, default="", upper=False):
        """Safely find element and extract text with error handling"""
        try:
            element = soup_product.find(
                "span", class_=lambda x: x and x.startswith(class_prefix)
            )
            if element:
                text = element.get_text(strip=True)
                return text.upper() if upper else text
            return default
        except (AttributeError, TypeError):
            return default

    product_url = soup_product["href"]

    h4_element = soup_product.find("h4")
    product_name = encode_text(h4_element.get_text(strip=True))

    # Get product price
    price = safe_find_text("_product-card-price-regular") or 0

    # Get source id
    source_id = None
    try:
        img_tag = soup_product.find("img")
        if img_tag and "src" in img_tag.attrs:
            src = img_tag["src"]
            if "v=" in src:
                source_id = src.split("v=")[1].split("&")[0]
    except (IndexError, AttributeError, KeyError, TypeError):
        LOGGER.warning(
            f"Error extracting source id from {product_name}, url: {product_url}"
        )

    # Get unit of measurement
    unit_of_measure = safe_find_text("_product-card-price-measurement", upper=True)

    quantity = None

    # Handle non-unit measurements (KG, L, etc.)
    if unit_of_measure and unit_of_measure != "UN":

        # Override price with weight-based price
        weight_price = safe_find_text("_product-card-price-measurement-weight")
        if weight_price:
            price = weight_price

        # Get quantity measurement
        measurement_text = safe_find_text("_product-card-measurement")
        if measurement_text:
            quantity = string_to_decimal(measurement_text)

    # TODO Restrictions: Map max quantity or do something else
    # product_name, max_quantity = _extract_max_quantity(product_name)

    return ScrapingProduct(
        name=product_name,
        category=category_name,
        market=MARKET,
        price=price_to_int(price),
        source_id=source_id,
        # brand=None,  # Brand not available directly, only in the product name
        quantity=quantity,
        unit_of_measure=unit_of_measure,
        product_url=BASE_URL + product_url,
        extraction_url=category_url_with_page,
        extraction_date=EXECUTION_TIME,
    )


def _get_all_products_for_category(category_name: str, category_url: str):
    """Retrieve all products for a specific category with pagination.

    This function handles pagination for a category by:
    1. Starting with page 1
    2. Fetching and parsing each page sequentially
    3. Extracting product URLs and deduplicating them
    4. Processing each unique product
    5. Continuing until no products are found on a page

    The function uses URL-based deduplication to ensure each product is only
    processed once, even if it appears on multiple pages.

    Args:
        category_name: The name of the category being processed.
        category_url: The base URL for the category (without page parameter).

    Returns:
        A list of ScrapingProduct objects for all unique products in the category
        across all pages. Returns an empty list if the category has no products.

    Pagination:
        Pages are accessed by appending "&page={page_number}" to the category URL.
        The loop continues until a page returns no products (empty product list).

    Note:
        Progress is logged for each page showing the number of products found
        on that page and the running total. Debug logging is available for
        individual product processing.
    """
    LOGGER.info(f"Getting all products for category {category_name} ({category_url})")

    all_category_products = []
    processed_product_urls = []

    page = 1
    while True:
        category_url_with_page = category_url + f"&page={page}"
        products_on_page = []

        LOGGER.debug(
            f"Processing category '{category_name}' page {page}. url: {category_url_with_page}"
        )

        response = make_request_with_delay(category_url_with_page, headers=HEADERS)
        soup = parse_html(response)

        soup_product_list = soup.find_all("div", class_="algolia-insights")

        if not soup_product_list:
            LOGGER.debug(f"No products found on category '{category_name}' page {page}")
            break

        # products_added_this_page = 0

        for soup_product in soup_product_list:
            for link in soup_product.find_all("a", href=True):
                product_url = link["href"]

                # Check if the product url is already processed (avoid duplicates)
                if product_url not in processed_product_urls:
                    LOGGER.debug(f"Processing product URL: {product_url}")
                    processed_product_urls.append(product_url)

                    product = _extract_product_data(
                        link, category_name, category_url_with_page
                    )

                    products_on_page.append(product)

                    # products_added_this_page += 1

        # # Stopping the loop if no product was added in the URL list
        # if products_added_this_page == 0:
        #     LOGGER.info(f"No new URL found on page {page}. Stopping the loop.")
        #     break

        all_category_products.extend(products_on_page)

        LOGGER.info(
            f"Category '{category_name}' page {page} - {len(products_on_page)} products found."
            f" Total: {len(all_category_products)}"
        )

        page += 1

    return all_category_products


def main():
    """Main function to run the St Marche scraper.

    This function orchestrates the complete scraping process:
    1. Discovers all categories from the homepage
    2. Processes each category sequentially
    3. Extracts products with pagination and deduplication
    4. Inserts products into database asynchronously
    5. Saves all products to a JSON file
    6. Waits for all database insertions to complete
    7. Logs execution statistics

    The function uses async database insertion to improve performance when
    processing multiple categories. All products are collected in memory
    before being saved to file.

    Progress Tracking:
        Progress is logged for each category showing:
        - Number of products found
        - Overall progress percentage
        - Category index and total

    Note:
        Database insertions run in separate threads to avoid blocking the
        main scraping process. The script processes categories sequentially
        to avoid overwhelming the server.
    """
    LOGGER.info(f"Starting {MARKET} scraper")
    start_time = time.time()

    db_client = DatabaseClient(MARKET)
    active_threads = []

    categories = _get_all_categories()

    # TESTING
    # categories = [
    #     {
    #         "name": "Peixaria",
    #         "url": "https://marche.com.br/collections/peixaria?store_id=66677604431",
    #     }
    # ]

    total_categories = len(categories)
    all_products = []
    for idx, category in enumerate(categories, 1):
        category_products = _get_all_products_for_category(
            category["name"], category["url"]
        )

        LOGGER.info(
            f"Finished processing category '{category["name"]}' {len(category_products)} products found"
            f" -> progress: {(idx/total_categories)*100:.1f}% [{idx:02d}/{total_categories:02d}]"
        )

        if len(category_products) > 0:
            thread = db_client.insert_scraping_products_with_discounts_async(
                category_products, category["name"]
            )
            active_threads.append(thread)
            all_products.extend(category_products)

    save_scraping_products_to_file(all_products, MARKET, EXECUTION_TIME.isoformat())

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
