import re
from typing import Union
from decimal import Decimal
from decimal import InvalidOperation


def encode_text(text):
    try:
        return text.encode("latin1").decode("utf-8")
    except:
        return text


def normalize_numeric_string(value: str) -> float:
    """
    Keep only digits, comma, and dot. If a comma is present, remove dots
    (thousand separators) and then replace the comma with a dot.

    Examples:
    - "R$ 1.234,56" -> 1234.56
    - "Price: 99,9 kg" -> 99.9
    - "1.234" -> 1.234 (no comma, keep the dot)
    - None -> None
    """
    if value is None or str(value).strip() == "":
        return None

    text = str(value)
    # Keep only numbers, comma, and dot
    cleaned = re.sub(r"[^0-9.,]", "", text)

    # If there is a comma, remove dots and then replace comma with dot
    if "," in cleaned:
        cleaned = cleaned.replace(".", "")
    cleaned = cleaned.replace(",", ".")

    return float(cleaned)


def string_to_decimal(value: Union[str, None]) -> Decimal:
    """Convert string to Decimal

    Examples:
        "R$ 5.825,10" -> 5825.10
        "asdsa 13,60" -> 13.60
        "13.60" -> 13.60
        "13.65" -> 13.65
        "13" -> 13.00
        "0.13" -> 0.13
        "0,13" -> 0.13 (handles comma as decimal separator)
        None -> 0

    Args:
        value: Value to convert

    Returns:
        Decimal representation of value

    Raises:
        ValueError: If value cannot be converted to a valid number
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
    """Convert price to integer preserving 2 decimal places

    Examples:
        "R$ 13,60" -> 1360
        13.6 -> 1360
        13.65 -> 1365
        13 -> 1300
        0.13 -> 13
        "0,13" -> 13 (handles comma as decimal separator)
        None -> 0

    Args:
        price: Price value to convert

    Returns:
        Integer representation of price with 2 decimal places preserved

    Raises:
        ValueError: If price cannot be converted to a valid number
    """

    if price is None:
        return 0

    return int(string_to_decimal(price) * 100)
