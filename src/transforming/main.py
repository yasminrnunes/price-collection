"""Main transformation module for processing scraped product data.

This module orchestrates the complete data transformation pipeline that
converts raw scraped product data from staging tables into normalized,
structured data in the main database tables. It handles the entire ETL
(Extract, Transform, Load) process for product data.

The transformation process includes:
    - Supermarket normalization and insertion
    - Brand extraction and normalization (with fuzzy matching)
    - Product matching and deduplication
    - Price insertion with currency handling
    - Discount processing and insertion
    - Raw product data tracking

The module processes products from the `stage_scraping_products` table and
transforms them into normalized records in:
    - supermarkets: Market/supermarket names
    - brands: Product brands with normalization
    - products: Normalized product information
    - prices: Product prices with extraction dates
    - discounts: Product discounts and promotions
    - raw_product_data: Original product URLs and metadata

Features:
    - Automatic duplicate detection and reuse
    - Fuzzy matching for brands and products
    - Brand extraction from product names when missing
    - Discount type normalization and processing
    - Progress tracking and logging

Processing Logic:
    - Supermarkets: Simple exact match on normalized name
    - Brands: Fuzzy matching with variants, fallback to product name extraction
    - Products: Fuzzy matching using word-based similarity
    - Prices: Unique constraint on (supermarket, product, extraction_date)
    - Discounts: Processed per product, normalized by type

Discount Types Handled:
    - BUY_X_GET_Y: Buy X get Y promotions
    - PERCENTAGE_QUANTITY: Percentage discounts on quantity
    - WHOLESALE: Bulk pricing discounts
    - CARD: Card-specific discounts

Example:
    The script processes products matching specific criteria (market, extraction_date,
    is_processed=false) and transforms them into the normalized database structure.
    Progress is logged throughout the transformation process.

Note:
    Modify the query constants (scraped_products_query) to change the selection
    criteria for which products to process.
"""

from sql_client import DatabaseClient
from logger import Logger
from utils import (
    normalize_word,
    check_brand_exists,
    check_brand_in_product,
    check_product_exists,
    normalize_unit,
)
from collections import Counter

LOGGER = Logger("transforming")
client = DatabaseClient("query_client")

LOGGER.debug("Starting the transforming process")

# Load existing supermarkets from database into memory for deduplication
# This dictionary maps normalized supermarket names to their IDs
supermarkets_created_query = """
    SELECT id, name 
    FROM supermarkets
    GROUP BY id, name;
"""
supermarkets_created_raw = client.execute_query(supermarkets_created_query)
supermarkets_created = {row["name"]: row["id"] for row in supermarkets_created_raw}

# Load existing brands from database into memory for deduplication
# This dictionary maps normalized brand names to their IDs
brands_created_query = """
    SELECT id, normalized_name 
    FROM brands
    GROUP BY id, normalized_name;
"""
brands_created_raw = client.execute_query(brands_created_query)
brands_created = {
    row["normalized_name"]: row["id"] for row in brands_created_raw
}

# Load existing product URLs from database into memory for deduplication
# This dictionary maps product URLs to their product IDs
raw_product_data_query = """
    SELECT product_id, product_url 
    FROM raw_product_data
    GROUP BY product_id, product_url;
"""
raw_product_data_raw = client.execute_query(raw_product_data_query)
raw_product_data = {
    row["product_url"]: row["product_id"] for row in raw_product_data_raw
}

# Load existing products from database into memory for deduplication
# This dictionary maps normalized product names to their IDs
# Also creates word-based counters for fuzzy product matching
products_created_query = """
    SELECT id, normalized_name 
    FROM products
    GROUP BY id, normalized_name;
"""
products_created_raw = client.execute_query(products_created_query)
products_created = {
    row["normalized_name"]: row["id"] for row in products_created_raw
}
# Create word-based counters for fuzzy product matching using Counter
products_created_split = [
    Counter(product.split()) for product in products_created
]

# Load existing prices from database into memory for deduplication
# This set contains tuples of (supermarket_id, product_id, extraction_date)
# to avoid inserting duplicate price records
price_created_query = """
    SELECT id_supermarket, id_product, extraction_date 
    FROM prices
    GROUP BY id_supermarket, id_product, extraction_date;
"""
price_created_raw = client.execute_query(price_created_query)
price_created = {
    (row["id_supermarket"], row["id_product"], row["extraction_date"])
    for row in price_created_raw
}


# Query products from staging table to be processed
# Modify the WHERE clause to filter products by market, extraction_date, etc.
scraped_products_query = """
        SELECT p.*
        FROM stage_scraping_products p
        WHERE --UPPER(p.name) like '%COCA COLA%'
        p.market = 'stMarche'
        --AND  p.brand = 'Piracanjuba' --
        --AND length(p.id::text) > 5
        AND p.is_processed = false
        --AND p.id = 625030582052585478
        --AND p.name = 'Suco Seleção Uva e Maçã  Maguary 1,35L'
        AND p.extraction_date = '2025-10-26 10:39:10.702245'
        --ORDER BY p.source_id DESC
        LIMIT 1000;
        """
scraped_products=client.execute_query(scraped_products_query)

# Query discounts from staging table to be processed
# These discounts will be linked to products during processing
scraped_discounts_query = """
    SELECT id
        ,product_id
        ,type
        ,discounted_price
        ,conditions_text
        ,conditions_min_quantity
        ,conditions_buy_quantity
        ,conditions_get_quantity
        ,created_at
    FROM stage_discounts;
"""
scraped_discounts = client.execute_query(scraped_discounts_query)

# Initialize counters for progress tracking
count_products = 0
total_products = len(scraped_products)

# Process each scraped product through the transformation pipeline
for scraped_product in scraped_products:
    LOGGER.debug(
        f"INITIALIZING THE TRANSFORMING PROCESS FOR PRODUCT: {scraped_product['id']}"
    )
    # Extract and normalize product data from staging record
    _id = scraped_product["id"]
    name = scraped_product["name"]
    normalized_name = normalize_unit(scraped_product["name"])
    market = normalize_word(scraped_product["market"])
    category = scraped_product["category"]
    brand = scraped_product["brand"]
    normalized_brand = normalize_word(scraped_product["brand"])
    product_url = scraped_product["product_url"]
    source_id = scraped_product["source_id"]
    price = scraped_product["price"]
    quantity = scraped_product["quantity"]
    unit = scraped_product["unit_of_measure"]
    extraction_url = scraped_product["extraction_url"]
    extraction_date = scraped_product["extraction_date"]
    currency = scraped_product["currency"]
    # created_at = scraped_product["created_at"]

    ## Product URL Check
    # If product URL doesn't exist, process as new product (insert supermarket, brand, product)
    # If product URL exists, reuse existing product ID
    if product_url not in raw_product_data:
        LOGGER.debug(
            f"Product_url {product_url} not found in the raw_product_data table."
        )

        ## Supermarket Processing
        # Check if supermarket exists, insert if new, otherwise reuse existing ID
        if market not in supermarkets_created:
            # Inserting the supermarket in the supermarkets table
            supermarket_query = """
                INSERT INTO supermarkets (name)
                VALUES (%s)
                RETURNING id;
                """
            new_supermarket = client.execute_non_query(supermarket_query, (market,))
            id_supermarket = new_supermarket[0]["id"]

            # Add to local dictionary to avoid re-insertion
            supermarkets_created[market] = id_supermarket
            LOGGER.debug(f"Inserted supermarket {market} with id {id_supermarket}.")
        else:
            # Get the id of the existing supermarket
            id_supermarket = supermarkets_created[market]
            LOGGER.debug(
                f"Supermarket {market} already exists with id {id_supermarket}."
            )

        ## Brand Processing
        # Handle brand extraction and normalization with fuzzy matching
        if brand is None:  # Brand is not informed in the scraping
            LOGGER.debug("No brand available in the scraping process.")
            # Try to extract brand from product name using fuzzy matching
            match_brand = check_brand_in_product(brands_created, normalized_name)
            if match_brand:  # if the brand is in the product name
                id_brand = brands_created[match_brand]
                LOGGER.debug(f"Brand {match_brand} was found in the product name")
            elif "st marche" in normalized_name:
                brand = "st marche"
                normalized_brand = "st marche"
                if normalized_brand not in brands_created:
                    brand_query = """
                        INSERT INTO brands (name, normalized_name)
                        VALUES (%s, %s)
                        RETURNING id;
                        """
                    new_brand = client.execute_non_query(
                        brand_query, (brand, normalized_brand)
                    )
                    id_brand = new_brand[0]["id"]
                    brands_created[normalized_brand] = id_brand
                    LOGGER.debug(f"Inserted brand {brand} with id {id_brand}.")
                else:
                    id_brand = brands_created[normalized_brand]
                    LOGGER.debug(f"Brand {brand} already exists with id {id_brand}.")
            else:  # the brand is not in the product name
                LOGGER.debug(f"No brand available in the product {normalized_name}.")
                id_brand = None
                # Don't insert a brand in the brands table
        else:  # Brand is informed in the scraping
            if (
                normalized_brand in brands_created
            ):  # If the brand is in the brands table
                id_brand = brands_created[normalized_brand]
                LOGGER.debug(f"Brand {brand} already exists with id {id_brand}.")

            else:  # The brand is not available in the normalized_brand in brands table
                match_brand, brand_name_matched = check_brand_exists(
                    normalized_brand, brands_created
                )
                # Checking if the brand exists in the brands table
                if match_brand == 1:
                    # Get the id of the existing brand
                    id_brand = brands_created[brand_name_matched]
                    LOGGER.debug(f"Brand {brand} already exists with id {id_brand}.")
                else:
                    # Inserting the brand in the brands table
                    brand_query = """
                        INSERT INTO brands (name, normalized_name)
                        VALUES (%s, %s)
                        RETURNING id;
                        """
                    new_brand = client.execute_non_query(
                        brand_query, (brand, normalized_brand)
                    )
                    id_brand = new_brand[0]["id"]
                    # Add to local dictionary to avoid re-insertion
                    brands_created[normalized_brand] = id_brand
                    LOGGER.debug(f"Inserted brand {brand} with id {id_brand}.")

        ## Unit of measurement
        # TODO: Checking if the unit of measurement is in the units_of_measure table

        ## Product Processing
        # Check if product exists using fuzzy matching, insert if new
        if normalized_name not in products_created:
            # Apply fuzzy matching to find similar products
            match_product, product_name_matched = check_product_exists(
                normalized_name, list(products_created.keys()), products_created_split
            )
            if match_product == 1:
                id_product = products_created[product_name_matched]
                LOGGER.debug(
                    f"Product {name} already exists with similar name {product_name_matched} and id {id_product}."
                )
            else:
                # Inserting the product in the products table
                product_query = """
                    INSERT INTO products (name, normalized_name, quantity, id_brand)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id;
                    """
                new_product = client.execute_non_query(
                    product_query, (name, normalized_name, quantity, id_brand)
                )
                id_product = new_product[0]["id"]

                # Add to local dictionary to avoid re-insertion
                products_created[normalized_name] = id_product
                LOGGER.debug(f"Inserted product {name} with id {id_product}.")

        else:
            # Get the id of the existing product
            id_product = products_created[normalized_name]
            LOGGER.debug(f"Product {name} already exists with id {id_product}.")

        ## Raw Product Data
        # Insert product URL mapping to track original scraped data
        raw_product_data_query = """
                    INSERT INTO raw_product_data (original_name, product_url, product_id, extraction_date, market)
                    VALUES (%s, %s, %s, %s, %s)
                    """
        client.execute_non_query(
            raw_product_data_query,
            (name, product_url, id_product, extraction_date, market),
        )

        # Add to local dictionary to avoid re-insertion
        raw_product_data[product_url] = id_product
        LOGGER.debug(
            f"Inserted product_url {product_url} with product_id {id_product} in raw_product_data table."
        )

    else:
        # Get the id of the existing product_url
        id_product = raw_product_data[product_url]
        id_supermarket = supermarkets_created[market]
        LOGGER.debug(
            f"Product_url {product_url} already exists with product id {id_product}."
        )

    ## Price Processing
    # Check if price already exists for this combination, insert if new
    if (id_supermarket, id_product, extraction_date) in price_created:
        LOGGER.debug(
            f"Price {price} already exists with id_supermarket {id_supermarket}, id_product {id_product} and extraction_date {extraction_date}."
        )
        # Products in distinct categories at the same supermarket and extraction date
    else:
        # Defining the currency
        if currency is None:
            currency = "BRL"

        # Inserting the price in the prices table
        price_query = """
            INSERT INTO prices (id_supermarket, id_product, extraction_date, value, currency)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
            """
        new_price = client.execute_non_query(
            price_query, (id_supermarket, id_product, extraction_date, price, currency)
        )

        # # Verificar se a query foi executada com sucesso
        # if new_price is None or len(new_price) == 0:
        #     LOGGER.warning(f"Price already exists or failed to insert for product {id_product}, supermarket {id_supermarket}, date {extraction_date}")
        #     # Adicionar à lista de preços criados para evitar tentativas futuras
        #     price_created.add((id_supermarket, id_product, extraction_date))
        #     continue

        id_price = new_price[0]["id"]

        # Add to local dictionary to avoid re-insertion
        price_created.add((id_supermarket, id_product, extraction_date))

        LOGGER.debug(
            f"Inserted price {price} with id_supermarket {id_supermarket}, id_product {id_product} and extraction_date {extraction_date}."
        )

        ## Discount Processing
        # Find and process all discounts associated with this product
        product_discounts = [
            discount for discount in scraped_discounts if discount["product_id"] == _id
        ]

        if product_discounts:
            LOGGER.debug(
                f"Product {name} has {len(product_discounts)} discount(s) to be processed."
            )

            # Processing each discount found
            for discount in product_discounts:
                # Extracting the discount data
                discount_type = discount["type"]
                unit_value = discount["discounted_price"]
                conditions_text = discount["conditions_text"]
                conditions_min_quantity = discount["conditions_min_quantity"]
                conditions_buy_quantity = discount["conditions_buy_quantity"]
                conditions_get_quantity = discount["conditions_get_quantity"]

                # Normalize discount format based on discount type
                if discount_type == "BUY_X_GET_Y":
                    multiple_qty = conditions_get_quantity
                    description = conditions_text

                elif discount_type == "PERCENTAGE_QUANTITY":
                    multiple_qty = conditions_min_quantity
                    description = conditions_text

                elif discount_type == "WHOLESALE":
                    multiple_qty = 1
                    description = f"From {conditions_min_quantity} units onwards"

                elif discount_type == "CARD":
                    multiple_qty = 1
                    description = "Card"

                else:
                    # Default case for unknown discount types
                    multiple_qty = conditions_min_quantity or 1
                    description = conditions_text or f"Discount type: {discount_type}"

                # Inserting the discount in the discounts table
                discount_query = """
                    INSERT INTO discounts (id_price, unit_value, condition_type, min_qty, multiple_qty, description)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                client.execute_non_query(
                    discount_query,
                    (
                        id_price,
                        unit_value,
                        discount_type,
                        conditions_min_quantity,
                        multiple_qty,
                        description,
                    ),
                )
                LOGGER.debug(
                    f"Inserted discount {unit_value} (type: {discount_type}) for product {id_product}."
                )
        else:
            LOGGER.debug(f"Product {name} has no discounts to be processed.")

    # Mark product as processed in staging table to prevent reprocessing
    stage_scraping_products_query = """
        UPDATE stage_scraping_products
        SET is_processed = true
        WHERE id = %s
        """
    client.execute_non_query(stage_scraping_products_query, (_id,))
    LOGGER.debug(
        f"Updated field is_processed in the stage_scraping_products table for product {_id}."
    )

    LOGGER.info(f"Product {name} was successfully transformed.")
    count_products += 1
    LOGGER.info(f"Total products transformed: {count_products} of {total_products}")
