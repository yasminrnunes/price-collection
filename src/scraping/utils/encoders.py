"""Encoding and conversion utilities for scraping operations.

This module provides utility functions for encoding text, normalizing numeric
strings, and converting prices to various formats. It handles common issues
with text encoding and different numeric formats found in web scraping,
particularly Brazilian and European number formats.

Functions:
    - encode_text: Fix encoding issues by converting latin1 to utf-8
    - normalize_numeric_string: Clean and normalize numeric strings with
      mixed separators
    - string_to_decimal: Convert strings to Decimal with format detection
    - price_to_int: Convert prices to integer cents (preserving 2 decimals)

The module is particularly useful for:
    - Handling Brazilian currency formats (R$ 1.234,56)
    - Converting between comma and dot decimal separators
    - Normalizing prices from different markets
    - Fixing encoding issues in scraped text

Example:
    Convert a Brazilian price format to cents:
        >>> from utils.encoders import price_to_int
        >>> price_to_int("R$ 13,60")
        1360

    Normalize a numeric string:
        >>> from utils.encoders import normalize_numeric_string
        >>> normalize_numeric_string("R$ 1.234,56")
        1234.56
"""

import re
from typing import Union
from decimal import Decimal
from decimal import InvalidOperation


def encode_text(text):
    """Fix text encoding issues by converting latin1 to utf-8.

    This function attempts to fix encoding problems that can occur when
    scraping text from websites. It tries to encode the text as latin1
    and then decode it as utf-8, which can resolve many common encoding
    issues. If the conversion fails, it returns the original text.

    Args:
        text: The text string that may have encoding issues.

    Returns:
        The text with encoding fixed if possible, or the original text
        if the conversion fails.

    Example:
        >>> encode_text("Café")
        "Café"
        >>> encode_text("Preço: R$ 10,50")
        "Preço: R$ 10,50"
    """
    try:
        return text.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def string_to_decimal(value: Union[str, None]) -> Decimal:
    """Convert string values to Decimal with automatic format detection.

    This function intelligently converts numeric strings to Decimal objects,
    handling various formats including Brazilian/European formats (comma as
    decimal separator) and US formats (dot as decimal separator). It can
    detect when dots are used as thousand separators and commas as decimal
    separators.

    The function automatically handles:
    - Currency symbols and other text (removed automatically)
    - Comma as decimal separator (European/Brazilian format)
    - Dot as decimal separator (US format)
    - Mixed formats with both dots and commas (assumes dots are thousands)
    - None values (converts to Decimal(0))

    Args:
        value: The value to convert. Can be a string with various numeric
            formats, or None.

    Returns:
        A Decimal object representing the numeric value. Returns Decimal(0)
        if value is None or an empty string.

    Raises:
        ValueError: If the value cannot be converted to a valid number,
            even after cleaning and normalization.

    Example:
        >>> string_to_decimal("R$ 5.825,10")
        Decimal('5825.10')
        >>> string_to_decimal("13,60")
        Decimal('13.60')
        >>> string_to_decimal("13.60")
        Decimal('13.60')
        >>> string_to_decimal("0,13")
        Decimal('0.13')
        >>> string_to_decimal(None)
        Decimal('0')
    """
    if value is None:
        return Decimal(0)

    try:
        # Handle string inputs
        if isinstance(value, str):
            # Remove non-numeric characters except dots and commas
            cleaned = re.sub(r"[^0-9.,]", "", value).strip()

            if not cleaned:
                return Decimal(0)

            # Check if it's a European/Brazilian format (thousands separator + decimal)
            # Pattern: numbers.numbers,numbers (like 5.825,00 or 1.234.567,89)
            if "," in cleaned and "." in cleaned:
                # Find the last comma (should be decimal separator)
                last_comma_pos = cleaned.rfind(",")
                # Find the last dot before the last comma
                dots_before_comma = cleaned[:last_comma_pos].count(".")

                if dots_before_comma > 0:
                    # European format: remove dots (thousands separators) and replace comma with dot
                    integer_part = cleaned[:last_comma_pos].replace(".", "")
                    decimal_part = cleaned[last_comma_pos + 1 :]
                    cleaned = f"{integer_part}.{decimal_part}"
                else:
                    # Ambiguous case, treat comma as decimal separator
                    cleaned = cleaned.replace(",", ".")
            elif "," in cleaned:
                # Only comma, treat as decimal separator
                cleaned = cleaned.replace(",", ".")
            # If only dots, assume US format (keep as is)

            value = cleaned

        # Convert to Decimal for precise calculation
        return Decimal(str(value))

    except (InvalidOperation, ValueError) as e:
        raise ValueError(f"Cannot convert '{value}' to a valid number") from e


def price_to_int(price: Union[float, int, str, Decimal, None]) -> int:
    """Convert price to integer representation in cents (preserving 2 decimals).

    This function converts price values of various types to an integer
    representation where the integer value represents cents. For example,
    R$ 13.60 becomes 1360 (cents), preserving exactly 2 decimal places.

    This is the standard format used throughout the scraping system for
    storing prices in the database, as it avoids floating-point precision
    issues and makes price calculations more reliable.

    The function handles multiple input types:
    - String values with various formats (uses string_to_decimal internally)
    - Numeric types (float, int, Decimal)
    - None values (converted to 0)

    Args:
        price: The price value to convert. Can be a string (with currency
            symbols, various separators), float, int, Decimal, or None.

    Returns:
        An integer representing the price in cents. For example:
        - 13.60 -> 1360
        - 0.13 -> 13
        - None -> 0

    Raises:
        ValueError: If the price cannot be converted to a valid number.
            This can occur if the string contains no valid numeric content
            after cleaning.

    Example:
        >>> price_to_int("R$ 13,60")
        1360
        >>> price_to_int(13.6)
        1360
        >>> price_to_int(13.65)
        1365
        >>> price_to_int(13)
        1300
        >>> price_to_int("0,13")
        13
        >>> price_to_int(None)
        0

    Note:
        This function uses Decimal internally for precise calculation,
        then multiplies by 100 and converts to int. This ensures that
        prices like 13.60 are correctly represented as 1360 cents.
    """

    if price is None:
        return 0

    return int(string_to_decimal(price) * 100)
