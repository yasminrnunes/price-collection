"""
Scraping script for Tenda
"""
from datetime import datetime
from typing import List
import time
from utils.http_request import make_request_with_delay
from utils.logger import Logger
from utils.encoders import price_to_int
from database.file_storage import save_scraping_products_to_file
from database.models.scraping_product import ScrapingProduct
from database.client import DatabaseClient

# TODO:
# - obtener la cantidad y la unidad de medida de los productos
# - automatizar el proceso de obtener el token

EXECUTION_TIME = datetime.now()
MARKET = "Tenda"  # https://tendaatacado.com.br

URL_API = "https://api.tendaatacado.com.br/api/public/store/category/{category_id}/products?&page={page}&order=relevance"
URL_CATEGORIES = "https://api.tendaatacado.com.br/api/recommendations/departments"

BEARER_TOKEN = "bbbbdf176d4d1b6585b76c49faed8d1b"
HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "X-Authorization": f"Bearer {BEARER_TOKEN}",
}

LOGGER = Logger(MARKET)


def _build_tenda_api_url(category_id: int, page: int = 1) -> str:
    return URL_API.format(category_id=category_id, page=page)


def _log_progress(current: int, total: int, category_name: str, page: int, url: str):
    percentage = (current / total) * 100

    # adjust the width of the current to the size of the total
    width = len(str(total))

    LOGGER.info(
        f"[{current:0{width}d}/{total:0{width}d}] ({percentage:.1f}%) "
        f"Processing category '{category_name}' page {page}. url: {url}"
    )


def _get_all_categories():
    LOGGER.info(f"Getting all categories from {URL_CATEGORIES}")

    response = make_request_with_delay(
        URL_CATEGORIES,
        headers=HEADERS,
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
    category_products_from_additional_pages = []

    for page in range(2, number_of_pages + 1):
        category_url = _build_tenda_api_url(category_id, page)

        _log_progress(page, number_of_pages, category_name, page, category_url)

        response = make_request_with_delay(category_url, headers=HEADERS)

        if response is None or response.status_code != 200:
            LOGGER.info(
                f"No response for category '{category_name}' page {page}. url: {category_url}"
            )
            continue

        category_products_from_additional_pages.extend(
            _parse_tenda_search_products(response.json(), category_url, category_name)
        )

    return category_products_from_additional_pages


def get_all_products_for_category(category_id: int, category_name: str):
    category_url = _build_tenda_api_url(category_id)
    LOGGER.debug(f"Getting all products for category {category_name} ({category_url})")

    # Get products from the first page
    response = make_request_with_delay(category_url, headers=HEADERS)
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

        normalized_products.append(scraping_product)

    return normalized_products


def _insertion_callback(success, product_count, name):
    if success:
        LOGGER.info(
            f"Successfully inserted {product_count} products for category '{name}'"
        )
    else:
        LOGGER.error(f"Failed to insert {product_count} products for category '{name}'")


if __name__ == "__main__":
    start_time = time.time()
    LOGGER.info("Starting Tenda API scraper")

    db_client = DatabaseClient(MARKET)
    active_threads = []

    categories = _get_all_categories()

    # TESTING
    # categories = [{"id": 3412, "name": "Mercearia"}]

    total_categories = len(categories)
    for idx, category in enumerate(categories, 1):
        LOGGER.info(
            f"Processing category '{category["name"]}' (ID: {category["id"]})"
            f" -> progress: [{idx:02d}/{total_categories:02d}] ({(idx/total_categories)*100:.1f}%) "
        )

        category_products = get_all_products_for_category(
            category["id"], category["name"]
        )

        if len(category_products) > 0:
            LOGGER.info(
                f"Starting async insertion of {len(category_products)} products for category '{category['name']}'"
            )
            thread = db_client.insert_scraping_products_with_discounts_async(
                category_products, category["name"], _insertion_callback
            )
            active_threads.append(thread)
            save_scraping_products_to_file(
                category_products, MARKET, EXECUTION_TIME.isoformat()
            )

    # wait for all database insertions to complete
    LOGGER.info("Waiting for all database insertions to complete...")
    for thread in active_threads:
        thread.join()

    end_time = time.time()
    total_time_seconds = end_time - start_time
    total_time_minutes = total_time_seconds / 60
    LOGGER.info(f"Tenda API scraper finished in {total_time_minutes:.2f} minutes")
