import json
import os
from datetime import datetime


def save_products_to_file(products, market, extraction_date):
    if not os.path.exists("data"):
        os.makedirs("data")

    if isinstance(extraction_date, str):
        try:
            extraction_date = datetime.fromisoformat(extraction_date)
        except ValueError:
            pass

    if isinstance(extraction_date, datetime):
        extraction_date = extraction_date.replace(microsecond=0).isoformat()

    extraction_date_str = str(extraction_date)
    filename = f"data/{market}_products_{extraction_date_str.replace(':', '-')}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, indent=2)

    print(f"Products saved in: {filename}")
    print(f"Total products saved: {len(products)}")

    return filename


def save_scraping_products_to_file(
    scraping_products: list, market: str, extraction_date
) -> str:
    if not all(hasattr(product, "to_dict") for product in scraping_products):
        raise TypeError(
            "All products must be ScrapingProduct objects with to_dict method"
        )

    if not os.path.exists("data"):
        os.makedirs("data")

    # Handle extraction_date formatting
    if isinstance(extraction_date, str):
        try:
            extraction_date = datetime.fromisoformat(extraction_date)
        except ValueError:
            pass

    if isinstance(extraction_date, datetime):
        extraction_date = extraction_date.replace(microsecond=0).isoformat()

    extraction_date_str = str(extraction_date)
    filename = f"data/{market}_products_{extraction_date_str.replace(':', '-')}.json"

    products_data = [product.to_dict() for product in scraping_products]

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(products_data, f, ensure_ascii=False, indent=2)

    # print(f"ScrapingProduct objects saved in: {filename}")
    # print(f"Total ScrapingProduct objects saved: {len(scraping_products)}")

    return filename
