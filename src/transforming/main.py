from sql_client import create_query_client
from logger import Logger
from utils import normalize_word


LOGGER = Logger("query_examples")
client = create_query_client("custom_queries")

# Checking the supermarkets in the supermarkets table
supermarkets_created_query = """
    SELECT id, name 
    FROM supermarkets
    GROUP BY id, name;
    """
supermarkets_created_raw = client.execute_query(supermarkets_created_query)
supermarkets_created = {row["name"]: row["id"] for row in supermarkets_created_raw}

# Checking the brands in the brands table
brands_created_query = """
    SELECT id, normalized_name 
    FROM brands
    GROUP BY id, normalized_name;
    """
brands_created_raw = client.execute_query(brands_created_query)
brands_created = {row["normalized_name"]: row["id"] for row in brands_created_raw}

# Checking the url in the raw_product_data table
raw_product_data_query = """
    SELECT product_id, product_url 
    FROM raw_product_data
    GROUP BY product_id, product_url;
    """
raw_product_data_raw = client.execute_query(raw_product_data_query)
raw_product_data = {row["product_url"]: row["product_id"] for row in raw_product_data_raw}

# Checking the products in the products table
products_created_query = """
    SELECT id, normalized_name 
    FROM products
    GROUP BY id, normalized_name;
    """
products_created_raw = client.execute_query(products_created_query)
products_created = {row["normalized_name"]: row["id"] for row in products_created_raw}

# Checking the prices registered in the price table
price_created_query = """
    SELECT id_supermarket, id_product, extraction_date 
    FROM prices
    GROUP BY id_supermarket, id_product, extraction_date;
    """
price_created_raw = client.execute_query(price_created_query)
price_created = {(row["id_supermarket"], row["id_product"], row["extraction_date"]) for row in price_created_raw}


# Checking products scraped to be processed
scraped_products_query = """
        SELECT p.*
        FROM stage_scraping_products p
        WHERE p.market = 'tenda'
        AND length(p.id::text) > 5
        AND p.is_processed = false
        --AND p.id = 619600713277767680
        ORDER BY p.source_id DESC
        LIMIT 10;
        """
scraped_products=client.execute_query(scraped_products_query)

# Checking discounts scraped to be processed
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
scraped_discounts=client.execute_query(scraped_discounts_query)


for scraped_product in scraped_products:
    print(scraped_product)
    _id = scraped_product["id"]
    name = scraped_product["name"]
    normalized_name = normalize_word(scraped_product["name"])
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
    #created_at = scraped_product["created_at"]

## Supermarket   
    # Checking if the supermarket is in the supermarkets table
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
        print(f"Inserted supermarket {market} with id {id_supermarket}.")
    else:
        # Get the id of the existing supermarket
        id_supermarket = supermarkets_created[market]
        print(f"Supermarket {market} already exists with id {id_supermarket}.")

## Brand
    # Checking if the brand is in the brands table
    if normalized_brand not in brands_created:
        # Inserting the brand in the brands table
        brand_query = """
            INSERT INTO brands (name, normalized_name)
            VALUES (%s, %s)
            RETURNING id;
            """
        new_brand = client.execute_non_query(brand_query, (brand, normalized_brand))
        id_brand = new_brand[0]["id"]

        # Add to local dictionary to avoid re-insertion
        brands_created[normalized_brand] = id_brand
        print(f"Inserted brand {brand} with id {id_brand}.")
    else:
        # Get the id of the existing brand
        id_brand = brands_created[normalized_brand]
        print(f"Brand {brand} already exists with id {id_brand}.")

## Unit of measurement
    # Checking if the unit of measurement is in the units_of_measure table PENDING

## Product
    # Checking if the product_url is in the raw_product_data table
    if product_url not in raw_product_data:
        # Checking if the product is already in the products table
        if normalized_name not in products_created:
            # Inserting the product in the products table
            product_query = """
                INSERT INTO products (name, normalized_name, quantity, id_brand)
                VALUES (%s, %s, %s, %s)
                RETURNING id;
                """
            new_product = client.execute_non_query(product_query, (name, normalized_name, quantity, id_brand))
            id_product = new_product[0]["id"]

            # Add to local dictionary to avoid re-insertion
            products_created[normalized_name] = id_product
            print(f"Inserted product {name} with id {id_product}.")
        
        else:
            # Get the id of the existing product
            id_product = products_created[normalized_name]
            print(f"Product {name} already exists with id {id_product}.")
    
                
        # Inserting the product_url in the raw_product_data table
        raw_product_data_query = """
                    INSERT INTO raw_product_data (original_name, product_url, product_id, extraction_date, market)
                    VALUES (%s, %s, %s, %s, %s)
                    """
        client.execute_non_query(raw_product_data_query, (name, product_url, id_product, extraction_date, market))

        # Add to local dictionary to avoid re-insertion
        raw_product_data[product_url] = product_url
        print(f"Inserted product_url {product_url} with product_id {id_product}.")
    
    else:
        # Get the id of the existing product_url
        #id_product_url = raw_product_data[product_url]
        print(f"Product_url {product_url} already exists with product id {id_product}.")

## Price
    if (id_supermarket, id_product, extraction_date) in price_created:
        print(f"Price {price} already exists with id_supermarket {id_supermarket}, id_product {id_product} and extraction_date {extraction_date}.")
        # Products in distinct categories at the same supermarket and extraction date
    else:
        # Inserting the price in the prices table
        price_query = """
            INSERT INTO prices (id_supermarket, id_product, extraction_date, value, currency)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
            """
        new_price=client.execute_non_query(price_query, (id_supermarket, id_product, extraction_date, price, currency))
        id_price = new_price[0]["id"]

        print(f"Inserted price {price} with id_supermarket {id_supermarket}, id_product {id_product} and extraction_date {extraction_date}.")

        # Checking if the product has discounts to be processed
        product_discounts = [discount for discount in scraped_discounts if discount['product_id'] == _id]

        if product_discounts:
            print(f"Product {name} has {len(product_discounts)} discount(s) to be processed.")
            
            # Processing each discount found
            for discount in product_discounts:
                # Extracting the discount data
                discount_type = discount['type']
                discounted_price = discount['discounted_price']
                conditions_text = discount['conditions_text']
                conditions_min_quantity = discount['conditions_min_quantity']
                conditions_buy_quantity = discount['conditions_buy_quantity']
                conditions_get_quantity = discount['conditions_get_quantity']
                
                # Calculating the unit price
                if discount_type in ['WHOLESALE', 'CARD'] and conditions_text is None:
                    unit_value = discounted_price
                    multiple_qty = 1
                #elif discount_type == 'BUY_X_GET_Y' and conditions_text is not None:
                #    unit_price = discounted_price / conditions_buy_quantity
               # else:
                #    unit_price = discounted_price

                # Inserting the discount in the discounts table
                discount_query = """
                    INSERT INTO discounts (id_price, unit_value, condition_type, min_qty, multiple_qty)
                    VALUES (%s, %s, %s, %s, %s)
                    """
                client.execute_non_query(discount_query, (id_price, unit_value, discount_type,conditions_min_quantity, multiple_qty))
                print(f"Inserted discount {unit_value} (type: {discount_type}) for product {id_product}.")
        else:
            print(f"Product {name} has no discounts to be processed.")

    #Update the field is_processed in the stage_scraping_products table
    stage_scraping_products_query = """
        UPDATE stage_scraping_products
        SET is_processed = true
        WHERE id = %s
        """
    client.execute_non_query(stage_scraping_products_query, (_id,))
    print(f"Updated field is_processed in the stage_scraping_products table for product {_id}.")
    