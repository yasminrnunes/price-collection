"""Utility functions for text normalization and fuzzy matching.

This module provides utility functions for normalizing text, generating variants,
and performing fuzzy matching operations used in the data transformation process.
These utilities help with brand and product matching, text cleaning, and unit
of measurement normalization.

Main Functions:
    - normalize_word: Normalize text by removing accents, special characters, etc.
    - generate_variants: Generate normalized variants for fuzzy matching
    - remove_stopwords: Remove common stopwords from text
    - check_brand_exists: Check if a brand exists using fuzzy matching
    - check_brand_in_product: Extract brand from product name
    - check_product_exists: Check if a product exists using word-based matching
    - normalize_unit: Normalize weight, volume, and count units to standard forms

These functions are essential for:
    - Brand deduplication and matching
    - Product deduplication and matching
    - Text normalization for consistent database storage
    - Unit of measurement standardization

Example:
    Normalize a brand name:
        >>> from utils import normalize_word
        >>> normalize_word("Coca-Cola®")
        'coca cola'

    Check if brand exists:
        >>> from utils import check_brand_exists
        >>> exists, match = check_brand_exists("coca cola", ["Coca Cola", "Pepsi"])
        >>> print(exists, match)
        1 Coca Cola
"""

import re
import unicodedata
from collections import Counter


def normalize_word(word):
    """Normalize a word by removing accents, special characters, and standardizing format.

    This function performs comprehensive text normalization:
        - Converts to lowercase
        - Removes accents/diacritics (é → e, ñ → n, etc.)
        - Removes trailing punctuation (.,;:!?)
        - Removes special characters (keeps only letters, numbers, spaces, commas, periods)
        - Normalizes whitespace (collapses multiple spaces, trims)

    Args:
        word: Input word or text to normalize. Can be any type (will be
            converted to string if needed).

    Returns:
        str: Normalized word with lowercase, no accents, no special characters,
            and normalized whitespace.

    Example:
        >>> normalize_word("Coca-Cola®")
        'coca cola'
        >>> normalize_word("Açaí")
        'acai'
        >>> normalize_word("São Paulo, SP")
        'sao paulo sp'
        >>> normalize_word("Product Name!!!")
        'product name'
    """
    # Convert to string if not already
    if not isinstance(word, str):
        word = str(word)
    # lowercase
    word = word.lower()
    # remove accents
    word = "".join(
        c for c in unicodedata.normalize("NFD", word) if unicodedata.category(c) != "Mn"
    )
    # Remove punctuation at the END of the string (.,;:!?)
    word = re.sub(r"[.,;:!?]+$", "", word)
    # remove special caracters
    word = re.sub(r"[^a-z0-9., ]", "", word)
    # remover duplicated space
    word = re.sub(r"\s+", " ", word).strip()
    # # capitalize first letter
    # word = word.capitalize()
    return word


## Brand Functions
def generate_variants(word: str) -> list[str]:
    """Generate normalized variants of a word to improve fuzzy matching.

    This function creates multiple normalized variants of a word to improve
    fuzzy matching accuracy. It generates:
        - Base normalized word (using normalize_word)
        - Version without spaces
        - Singular/plural variants (adds or removes final 's')
        - Singular/plural variants of the no-space version

    Args:
        word: Input word to generate variants for.

    Returns:
        list[str]: List of normalized variants. Duplicates are automatically
            removed (using a set internally).

    Example:
        >>> variants = generate_variants("coca colas")
        >>> print(variants)
        ['coca cola', 'coca colas', 'cocacola', 'cocacolas']

        >>> variants = generate_variants("Pepsi")
        >>> print(variants)
        ['pepsi', 'pepsis', 'pepsi']
    """
    base = normalize_word(word)
    variants = {base}  # use a set to avoid duplicates

    # Remove spaces
    no_space = base.replace(" ", "")
    variants.add(no_space)

    # Handle plural/singular (basic rule: add or remove final 's')
    if base.endswith("s"):
        variants.add(base[:-1])  # remove final 's'
    else:
        variants.add(base + "s")  # add final 's'

    # Do the same for no_space version
    if no_space.endswith("s"):
        variants.add(no_space[:-1])
    else:
        variants.add(no_space + "s")

    return list(variants)


def remove_stopwords(text):
    """Remove common stopwords from text and normalize.

    This function removes predefined Portuguese stopwords from product names
    to improve matching accuracy. It normalizes the text first, then removes
    stopwords as whole words only, and finally cleans up extra whitespace.

    Stopwords removed:
        de, com, unidades, unidade, e, para, em, caixa, pote, frasco,
        sabor, kit, economico, sache, combo, refrigerante, garrafa

    Args:
        text: Input text (product name) to clean. Can be any type (will be
            converted to string if needed).

    Returns:
        str: Text with stopwords removed and whitespace normalized.

    Example:
        >>> remove_stopwords("Leite de vaca em caixa de 1 litro")
        'leite vaca litro'
        >>> remove_stopwords("Refrigerante Coca Cola em garrafa de 2 litros")
        'refrigerante coca cola litros'
    """
    # Convert to string if needed
    if not isinstance(text, str):
        text = str(text)

    stopwords = [
        "de",
        "com",
        "unidades",
        "unidade",
        "e",
        "para",
        "em",
        "caixa",
        "pote",
        "frasco",
        "sabor",
        "kit",
        "economico",
        "sache",
        "combo",
        "refrigerante",
        "garrafa",
    ]

    # Remove stopwords (whole words only)
    pattern = r"\b(" + "|".join(map(re.escape, stopwords)) + r")\b"
    text = re.sub(pattern, "", text, flags=re.IGNORECASE)

    # Remove extra spaces left by stopword removal
    text = re.sub(r"\s+", " ", text).strip()

    return text


def check_brand_exists(new_brand: str, existing_brands: list[str]):
    """Check if a brand already exists using fuzzy matching with variants.

    This function checks if a new brand matches any existing brand by:
        1. Normalizing existing brands (removing spaces)
        2. Generating variants for the new brand
        3. Comparing variants against normalized existing brands

    Args:
        new_brand: The brand name to check for existence.
        existing_brands: List of existing brand names (normalized names from
            database).

    Returns:
        tuple: A 2-tuple containing:
            - int: 1 if match found, 0 if not found
            - str or None: The matched existing brand name if found, None otherwise

    Example:
        >>> existing = ["Coca Cola", "Pepsi", "Fanta"]
        >>> exists, match = check_brand_exists("coca-cola", existing)
        >>> print(exists, match)
        1 Coca Cola

        >>> exists, match = check_brand_exists("Sprite", existing)
        >>> print(exists, match)
        0 None
    """
    # normalize existing brands
    normalized_existing = {
        normalize_word(b).replace(" ", ""): b for b in existing_brands
    }

    # generate variants for the new brand
    # variants = {v.replace(" ", "") for v in generate_variants(new_brand)}
    variants = generate_variants(new_brand)

    # Usar operações de lista
    for variant in variants:
        if variant in normalized_existing:
            return 1, normalized_existing[variant]
    return 0, None


def check_brand_in_product(brands: list[str], product: str) -> str:
    """Extract brand name from product name using flexible pattern matching.

    This function checks if any brand from the list appears in the product name,
    allowing spaces between letters of the brand. This handles cases where
    brands are written with spaces in product names (e.g., "C O C A C O L A").

    The matching is case-insensitive and uses regex patterns that allow
    optional whitespace between each character of the brand name.

    Args:
        brands: List of brand names (normalized, typically without spaces)
            to search for in the product name.
        product: Product name string to search within.

    Returns:
        str or None: The first brand found in the product name, or None
            if no brand is found.

    Example:
        >>> brands = ["cocacola", "pepsi", "fanta"]
        >>> check_brand_in_product(brands, "Refrigerante C O C A C O L A 2L")
        'cocacola'

        >>> check_brand_in_product(brands, "Bebida Pepsi Zero")
        'pepsi'

        >>> check_brand_in_product(brands, "Agua Mineral")
        None
    """

    # Clean the product string (remove leading/trailing spaces)
    product_clean = product.strip()

    for brand in brands:
        # Build a regex pattern that allows spaces between each letter of the brand
        pattern = r"".join([re.escape(c) + r"\s*" for c in brand])

        # Search for the pattern in the product (case-insensitive)
        if re.search(pattern, product_clean, re.IGNORECASE):
            return brand

    return None


## Product Functions
def check_product_exists(
    new_product: str,
    existing_products: list[str],
    existing_products_split: list[Counter],
):
    """Check if a product already exists using word-based exact matching.

    This function checks if a new product matches any existing product by
    comparing word frequency counters. It normalizes the new product name,
    creates a word counter, and compares it against pre-computed counters
    for existing products.

    The matching is exact - products must have the exact same words (normalized)
    with the same frequencies to be considered a match. This helps identify
    products that are essentially the same but with different word ordering.

    Args:
        new_product: The product name to check for existence.
        existing_products: List of existing product names (normalized names
            from database).
        existing_products_split: List of Counter objects, one per existing
            product, containing word frequencies. Should be pre-computed for
            performance (e.g., `[Counter(p.split()) for p in existing_products]`).

    Returns:
        tuple: A 2-tuple containing:
            - int: 1 if exact match found, 0 if not found
            - str or None: The matched existing product name if found, None otherwise

    Example:
        >>> existing = ["arroz tio joao 1kg", "feijao carioca 1kg"]
        >>> existing_counters = [Counter(p.split()) for p in existing]
        >>> exists, match = check_product_exists("tio joao arroz 1kg", existing, existing_counters)
        >>> print(exists, match)
        1 arroz tio joao 1kg
    """
    count_new_product = Counter(normalize_word(new_product).split())
    for i, count in enumerate(existing_products_split):
        if count_new_product == count:
            return 1, existing_products[i]  # Retorna o índice do produto encontrado
    return 0, None


# Texto base
# text1 = "Gato gato CACHORRO"

# # Lista de textos a comparar
# text2_list = [
#     "cachorro GATO gato",
#     "gato cachorro passarinho",
#     "GATO CACHORRO gato"
# ]

# text2_counters = [Counter(normalize_word(t).split()) for t in text2_list]

# result, product = check_product_exists(text1, text2_list, text2_counters)
# print(result, product)


## Unit of measurement Functions
def normalize_unit(text):
    """Normalize measurement units in product names to standard forms.

    This function detects and normalizes all measurement units in a text:
        - Weight units → converts to 'quilo' (supports: g, grama/gramas, kg, mg, miligramas)
        - Volume units → converts to 'litro' (supports: ml, mililitro/mililitros, l, litros)
        - Count units → converts to 'unidade' (supports: un, uns, und, unidade/unidades)

    The function:
        - Handles multiple units per product (e.g., '3un de 500ml')
        - Accepts both formats: '1kg' and '1 kg' (with or without space)
        - Converts commas to dots for decimal values (e.g., '2,5kg' → '2.5kg')
        - Normalizes text using normalize_word and remove_stopwords first
        - Converts smaller units to base units (mg→kg, g→kg, ml→litro)

    Weight conversions:
        - mg/miligramas → divided by 1,000,000 → quilo
        - g/gramas → divided by 1,000 → quilo
        - kg → quilo (no conversion)

    Volume conversions:
        - ml/mililitros → divided by 1,000 → litro
        - l/litros → litro (no conversion)

    Args:
        text: Product name string containing measurement units.

    Returns:
        str: Product name with all measurement units normalized to standard
            forms (quilo, litro, unidade). Whitespace is normalized.

    Example:
        >>> normalize_unit("Açúcar 1 Kg")
        'acucar 1 quilo'
        >>> normalize_unit("Leite 500ml")
        'leite 0.5 litro'
        >>> normalize_unit("Pacote com 10 unidades")
        'pacote 10 unidade'
        >>> normalize_unit("Bala 600g com 50 unidades")
        'bala 0.6 quilo 50 unidade'
    """
    text = re.sub(r"(\d+),(\d+)", r"\1.\2", text)  # convert commas to dots
    text = normalize_word(text)
    text = remove_stopwords(text)

    # --- Normalize weight units to 'quilo' ---
    def convert_weight(match):
        value = float(match.group(1))
        unit = match.group(2).lower()

        if unit in ["mg", "miligramas", "miligrama"]:
            value /= 1_000_000
        elif unit in ["g", "grama", "gramas"]:
            value /= 1000

        value_str = f"{value:.6f}".rstrip("0").rstrip(".")
        return f"{value_str} quilo"

    text = re.sub(
        r"(\d+(?:\.\d+)?)\s*(mg|miligramas?|g|gramas?|kg|kgs|quilo|quilos)\b",
        convert_weight,
        text,
        flags=re.IGNORECASE,
    )

    # --- Normalize volume units to 'litro' ---
    def convert_volume(match):
        value = float(match.group(1))
        unit = match.group(2).lower()

        if unit in ["ml", "mililitro", "mililitros"]:
            value /= 1000

        value_str = f"{value:.6f}".rstrip("0").rstrip(".")
        return f"{value_str} litro"

    text = re.sub(
        r"(\d+(?:\.\d+)?)\s*(ml|mililitros?|l|lt|lts|litros?)\b",
        convert_volume,
        text,
        flags=re.IGNORECASE,
    )

    # --- Normalize count units to 'unidade' ---
    def convert_units(match):
        value = float(match.group(1))
        value_str = (
            f"{int(value)}"
            if value.is_integer()
            else f"{value:.3f}".rstrip("0").rstrip(".")
        )
        return f"{value_str} unidade"

    text = re.sub(
        r"(\d+(?:\.\d+)?)\s*(un|uns|und|unidade|unidades)\b",
        convert_units,
        text,
        flags=re.IGNORECASE,
    )

    return text.strip()


# ## Ejemplo de unidades
# print(normalize_unit("Vela Select N3 08un"))
# print(normalize_unit("Absorvente Carefree Brisa com 40 unidades"))
# print(normalize_unit("Absorvente Mili Suave Noturno Fluxo Intenso com Abas Leve 16 Pague 14 unidades"))
# print(normalize_unit("Fralda Pampers Supersec M 30 Und"))


# ## Ejemplo de litros
# print(normalize_unit("Vinho Francês Paul Mas Claude de Val Tinto 750ml"))
# print(normalize_unit("Leite Longa Vida Desnatado Piracanjuba 1L"))
# print(normalize_unit("Leite Piracanjuba Zero Lactose Semidesnatado 1 Litro"))

# ## Ejemplo de peso
# print(normalize_unit("Sardinha Ralada com Tomate 88 110g"))
# print(normalize_unit("Abóbora Japonesa 2,2kg"))
# print(normalize_unit("Abóbora Japonesa St Marche Fracionado Kg"))
# print(normalize_unit("Açúcar Mascavo Jp Pereira 1 Kg"))

# ## Ejemplo sem medida
# print(normalize_unit("Maço de Flores Allegra COOPERFLORA"))
# print(normalize_unit("Caixa de Pizza 35cm c/ 25 unidades Econômica Sj"))

# ## Ejemplo doble medida
# print(normalize_unit("Adoçante em Pó Zero Cal Sucralose Sache 50 unidades 600mg"))
# print(normalize_unit("Bala Dadinho Tradicional 60 unidades 600g"))
# print(normalize_unit("Bis Branco - Kit com 3 unidades de 100,8g"))
# print(normalize_unit("Chá TWININGS Hortelã com 10 unidades 17,50g"))
# print(normalize_unit("Alimento para Cães Adultos 12 Meses a 7 Anos Carne e Vegetais Pedigree Leve 10,1kg Pague 9kg"))


# normalize_word("Papel Toalha Mili Grand Chef | Com 3 Rolos de 12.0 Folhas")
# Teste da função
