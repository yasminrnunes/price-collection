"""
Scraping script for St Marche
"""

import time
import re
from datetime import datetime
from utils.encoders import encode_text, string_to_decimal
from utils.http_request import make_request_with_delay
from utils.html_parser import parse_html
from utils.logger import Logger
from utils.encoders import price_to_int
from database.client import DatabaseClient
from database.models.scraping_product import ScrapingProduct
from database.file_storage import save_scraping_products_to_file

# TODO:
# St Marche comments:
## The product pages that don't exist still return a product >> Keep going through the pages until there is no product URL left that hasn't been added.
## St Marche only displays products that are available.
## The product price returned not only the amount but also the currency and some letters, like 'R$\xa025,89'.
## The product brand is not available.
## The quantity restriction by product is registered in the product name --> Cerveja Pilsen Corona Lata 350ml (máx 24 unidades por cpf)

EXECUTION_TIME = datetime.now()
MARKET = "StMarche"
LOGGER = Logger(MARKET)

BASE_URL = "https://marche.com.br"

STORE_ID = 66677604431  # Pavao
# STORE_ID = 66677538895 # Mooca
STORE_URL = f"?store_id={STORE_ID}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


def _extract_max_quantity(product_name: str):
    """
    Extracts max quantity from product name if pattern '(máx XX <unit> por cpf)' exists.
    Works for any unit (e.g., unidades, kg, litros).
    Returns:
        clean_name (str): Product name without the '(máx ... por cpf)' part
        max_quantity (float or None): The extracted number, or None if not found
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


def _extract_product_data(soup_product, link, category_name, category_url_with_page):
    """Extract product data from soup and link elements"""
    product_url = link["href"]

    h4_element = link.find("h4")
    if h4_element:
        product_name = encode_text(h4_element.get_text(strip=True))
        # TODO Map max quantity or do something else
        # product_name, max_quantity = _extract_max_quantity(product_name)
    else:
        raise Exception(
            "Product name not found in the product page url %s",
            category_url_with_page,
        )

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

    # Get product price
    price = safe_find_text("_product-card-price-regular") or 0

    # Get unit of measurement
    unit_of_measure = safe_find_text("_product-card-price-measurement", upper=True)

    quantity = None

    # Handle non-unit measurements
    if unit_of_measure and unit_of_measure != "UN":

        # Override price with weight-based price
        weight_price = safe_find_text("_product-card-price-measurement-weight")
        if weight_price:
            price = weight_price

        # Get quantity measurement
        measurement_text = safe_find_text("_product-card-measurement")
        if measurement_text:
            quantity = string_to_decimal(measurement_text)

    return ScrapingProduct(
        name=product_name,
        category=category_name,
        market=MARKET,
        price=price_to_int(price),
        # source_id=,
        # brand=,
        quantity=quantity,
        unit_of_measure=unit_of_measure,
        product_url=BASE_URL + product_url,
        extraction_url=category_url_with_page,
        extraction_date=EXECUTION_TIME,
    )


def _get_all_products_for_category(category_name: str, category_url: str):
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
                        soup_product, link, category_name, category_url_with_page
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


def _insertion_callback(success, product_count, name):
    if success:
        LOGGER.info(
            f"Successfully inserted {product_count} products for category '{name}'"
        )
    else:
        LOGGER.error(f"Failed to insert {product_count} products for category '{name}'")


if __name__ == "__main__":
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
                category_products, category["name"], _insertion_callback
            )
            active_threads.append(thread)
            all_products.extend(category_products)

    save_scraping_products_to_file(
        all_products, MARKET, EXECUTION_TIME.isoformat()
    )

    # wait for all database insertions to complete
    LOGGER.info("Waiting for all database insertions to complete...")
    for thread in active_threads:
        thread.join()

    end_time = time.time()
    total_time_seconds = end_time - start_time
    total_time_minutes = total_time_seconds / 60
    LOGGER.info(f"{MARKET} scraper finished in {total_time_minutes:.2f} minutes")
