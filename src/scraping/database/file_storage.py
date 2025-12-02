"""File storage utilities for saving scraping products to JSON files.

This module provides functionality to serialize and save ScrapingProduct objects
to JSON files in a standardized format. Files are saved in the data directory
with a naming convention that includes the extraction date, market name, and
product count.

The module automatically:
    - Creates the data directory if it doesn't exist
    - Handles datetime formatting and ISO string conversion
    - Validates that products have the required serialization method
    - Ensures proper JSON encoding with UTF-8 support

File Naming Convention:
    Files are named as: {extraction_date}-{market}-{product_count}-products.json
    Example: "2025-10-26T18-24-39-extra-13583-products.json"

Note:
    The extraction_date can be provided as a datetime object, ISO format string,
    or any string representation. The function handles conversion automatically.
"""

import json
import os
from datetime import datetime


def save_scraping_products_to_file(
    scraping_products: list, market: str, extraction_date
) -> str:
    """Save scraping products to a JSON file in the data directory.

    This function serializes a list of ScrapingProduct objects to JSON format
    and saves them to a file with a standardized naming convention. The file
    is saved in the `src/scraping/data` directory, which is created automatically
    if it doesn't exist.

    The function handles various datetime formats and converts them to a
    standardized ISO format string for the filename. Colons in the datetime
    string are removed to ensure filesystem compatibility.

    Args:
        scraping_products: List of ScrapingProduct objects to save. Each object
            must have a to_dict() method for serialization. If the list is empty,
            the function returns None without creating a file.
        market: Name of the market/store (e.g., "extra", "tenda", "fort").
            Used in the filename to identify the source marketplace.
        extraction_date: Date/time of extraction. Can be:
            - A datetime object (will be converted to ISO format)
            - An ISO format string (will be parsed and reformatted)
            - Any other string representation (used as-is)

    Returns:
        The full path to the created JSON file as a string, or None if no
        products were provided (empty list).

    Raises:
        TypeError: If any product in the list doesn't have a to_dict() method,
            indicating it's not a valid ScrapingProduct object.
        OSError: If there are filesystem errors creating the directory or file.

    Example:
        Basic usage:
            >>> from datetime import datetime
            >>> products = [product1, product2, product3]
            >>> filename = save_scraping_products_to_file(
            ...     products, "extra", datetime.now()
            ... )
            >>> print(filename)
            "src/scraping/data/2025-10-26T18-24-39-extra-3-products.json"

        With ISO string date:
            >>> filename = save_scraping_products_to_file(
            ...     products, "tenda", "2025-10-26T18:24:39"
            ... )
            >>> print(filename)
            "src/scraping/data/2025-10-26T18-24-39-tenda-3-products.json"

    Note:
        - The JSON file is saved with UTF-8 encoding and proper indentation (2 spaces)
        - Non-ASCII characters are preserved (ensure_ascii=False)
        - Microseconds are removed from datetime objects for cleaner filenames
        - The data directory is created automatically if it doesn't exist
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

    filename = f"src/scraping/data/{extraction_date_str.replace(':', '')}-{market}-{len(scraping_products)}-products.json"

    products_data = [product.to_dict() for product in scraping_products]

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(products_data, f, ensure_ascii=False, indent=2)

    return filename
