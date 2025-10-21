import re
import unicodedata
from collections import Counter

def normalize_word(word):
    # Convert to string if not already
    if not isinstance(word, str):
        word = str(word)
    # lowercase
    word = word.lower()
    # remove accents
    word = ''.join(c for c in unicodedata.normalize('NFD', word) if unicodedata.category(c) != 'Mn')
    # Remove punctuation at the END of the string (.,;:!?)
    word = re.sub(r'[.,;:!?]+$', '', word)
    # remove special caracters
    word = re.sub(r'[^a-z0-9., ]', '', word)
    # remover duplicated space
    word = re.sub(r'\s+', ' ', word).strip()
    # # capitalize first letter
    # word = word.capitalize()
    return word


## Brand Functions
def generate_variants(word: str) -> list[str]:
    """
    Generate normalized variants of a word to improve fuzzy matching.
    Includes:
    - Base normalized word
    - Version without spaces
    - Singular/plural variants
    """
    base = normalize_word(word)
    variants = {base}  # use a set to avoid duplicates

    # Remove spaces
    no_space = base.replace(" ", "")
    variants.add(no_space)

    # Handle plural/singular (basic rule: add or remove final 's')
    if base.endswith("s"):
        variants.add(base[:-1])   # remove final 's'
    else:
        variants.add(base + "s")  # add final 's'

    # Do the same for no_space version
    if no_space.endswith("s"):
        variants.add(no_space[:-1])
    else:
        variants.add(no_space + "s")

    return list(variants)

def remove_stopwords(text):
    """
    Normalize and clean text by:
    - Lowercasing and removing accents
    - Removing unwanted special characters (keeping . and , inside text)
    - Removing predefined stopwords: 
      sabor, de, em, frasco, economico, sache, e, +, |, com
    """
    # Convert to string if needed
    if not isinstance(text, str):
        text = str(text)

    stopwords = [
        "sabor", "de", "em", "frasco", "economico",
        "sache", "e", "+", "|", "com"
    ]
    
    # Remove stopwords (whole words only)
    pattern = r'\b(' + '|'.join(map(re.escape, stopwords)) + r')\b'
    text = re.sub(pattern, '', text, flags=re.IGNORECASE)
    
    # Remove extra spaces left by stopword removal
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def check_brand_exists(new_brand: str, existing_brands: list[str]):
    """
    Check if a new brand already exists in the database (direct).
    Returns:
        - (True, match) if found
        - (False, None) if not found
    """
    # normalize existing brands
    normalized_existing = {normalize_word(b).replace(" ", ""): b for b in existing_brands}

    # generate variants for the new brand
    #variants = {v.replace(" ", "") for v in generate_variants(new_brand)}
    variants = generate_variants(new_brand)

    # Usar operações de lista
    for variant in variants:
        if variant in normalized_existing:
            return 1, normalized_existing[variant]
    return 0, None

def check_brand_in_product(brands: list[str], product: str) -> str:
    """
    Checks which brands from the list appear in the product name.
    Allows the letters of the brand to have spaces between them in the product.
    
    Args:
        brands (list[str]): list of brand names (without spaces).
        product (str): product name.

    Returns:
        str: brand found in the product or None if not found.
    """
    
    # Clean the product string (remove leading/trailing spaces)
    product_clean = product.strip()
    
    for brand in brands:
        # Build a regex pattern that allows spaces between each letter of the brand
        pattern = r''.join([re.escape(c) + r'\s*' for c in brand])
        
        # Search for the pattern in the product (case-insensitive)
        if re.search(pattern, product_clean, re.IGNORECASE):
            return brand
    
    return None

## Product Functions
def check_product_exists(new_product: str, existing_products: list[str], existing_products_split: list[Counter]):
    """
    Check if a new product already exists in the database.
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
    """
    Detects and normalizes all measurement units in a text:
      - Weight units → 'quilo' (supports g, kg, mg)
      - Volume units → 'litro' (supports ml, l)
      - Count units → 'unidade' (supports un, uns, und, unidades)
    Handles multiple units per product (e.g., '3un de 500ml').
    Accepts both '1kg' and '1 kg'.
    """
    text = re.sub(r'(\d+),(\d+)', r'\1.\2', text)  # convert commas to dots
    text = normalize_word(text)
    text = remove_stopwords(text)

    # --- Normalize weight units to 'quilo' ---
    def convert_weight(match):
        value = float(match.group(1))
        unit = match.group(2).lower()

        if unit in ['mg', 'miligramas', 'miligrama']:
            value /= 1_000_000
        elif unit in ['g', 'grama', 'gramas']:
            value /= 1000

        value_str = f"{value:.6f}".rstrip('0').rstrip('.')
        return f"{value_str} quilo"

    text = re.sub(
        r'(\d+(?:\.\d+)?)\s*(mg|miligramas?|g|gramas?|kg|kgs|quilo|quilos)\b',
        convert_weight,
        text,
        flags=re.IGNORECASE
    )

    # --- Normalize volume units to 'litro' ---
    def convert_volume(match):
        value = float(match.group(1))
        unit = match.group(2).lower()

        if unit in ['ml', 'mililitro', 'mililitros']:
            value /= 1000

        value_str = f"{value:.6f}".rstrip('0').rstrip('.')
        return f"{value_str} litro"

    text = re.sub(
        r'(\d+(?:\.\d+)?)\s*(ml|mililitros?|l|lt|lts|litros?)\b',
        convert_volume,
        text,
        flags=re.IGNORECASE
    )

    # --- Normalize count units to 'unidade' ---
    def convert_units(match):
        value = float(match.group(1))
        value_str = f"{int(value)}" if value.is_integer() else f"{value:.3f}".rstrip('0').rstrip('.')
        return f"{value_str} unidade"

    text = re.sub(
        r'(\d+(?:\.\d+)?)\s*(un|uns|und|unidade|unidades)\b',
        convert_units,
        text,
        flags=re.IGNORECASE
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
