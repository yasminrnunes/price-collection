"""
Save scraping products to a JSON file in the data directory.
"""

import json
import os
from datetime import datetime


def save_scraping_products_to_file(
    scraping_products: list, market: str, extraction_date
) -> str:
    """
    Save scraping products to a JSON file in the data directory.

    This function serializes a list of ScrapingProduct objects to JSON format
    and saves them to a file with a standardized naming convention.

    Args:
        scraping_products (list): List of ScrapingProduct objects to save.
                                Each object must have a to_dict() method.
        market (str): Name of the market/store (used in filename).
        extraction_date: Date/time of extraction. Can be a datetime object,
                        ISO format string, or any string representation.

    Returns:
        str: Path to the created file, or None if no products were provided.

    Raises:
        TypeError: If any product in the list doesn't have a to_dict method.

    Example:
        >>> products = [product1, product2, product3]
        >>> filename = save_scraping_products_to_file(products, "fort", datetime.now())
        >>> print(filename)  # "src/scraping/data/fort_products_2025-09-29T06-10-43.json"
    """

    if len(scraping_products) == 0:
        return None

    if not all(hasattr(product, "to_dict") for product in scraping_products):
        raise TypeError(
            "All products must be ScrapingProduct objects with to_dict method"
        )

    if not os.path.exists("src/scraping/data"):
        os.makedirs("src/scraping/data")

    # Handle extraction_date formatting
    if isinstance(extraction_date, str):
        try:
            extraction_date = datetime.fromisoformat(extraction_date)
        except ValueError:
            pass

    if isinstance(extraction_date, datetime):
        extraction_date = extraction_date.replace(microsecond=0).isoformat()

    extraction_date_str = str(extraction_date)
    filename = f"src/scraping/data/{market}_products_{extraction_date_str.replace(':', '-')}.json"

    products_data = [product.to_dict() for product in scraping_products]

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(products_data, f, ensure_ascii=False, indent=2)

    # print(f"ScrapingProduct objects saved in: {filename}")
    # print(f"Total ScrapingProduct objects saved: {len(scraping_products)}")

    return filename
