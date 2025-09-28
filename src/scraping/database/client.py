import os
import psycopg2
import threading
from dotenv import load_dotenv
from database.models.scraping_product import ScrapingProduct
from database.models.product_discount import ProductDiscount
from utils.logger import Logger

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT"),
}


class DatabaseClient:
    def __init__(self, logger_name: str = "database_client"):
        self.logger = Logger(logger_name)

    def _connect_db(self):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            self.logger.debug("Database connection established")
            return conn
        except psycopg2.Error as error:
            self.logger.error(f"Error connecting to the database: {error}")
            return None

    def _insert_scraping_products(self, scraping_products_list):
        conn = self._connect_db()

        if conn is None:
            return False

        try:
            cursor = conn.cursor()

            query = """
            INSERT INTO stage_scraping_products (
                id, name, market, category, brand, product_url, source_id,
                price, quantity, unit_of_measure, extraction_url, 
                extraction_date, currency
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """

            # Convert ScrapingProduct objects to tuples if necessary
            if scraping_products_list and isinstance(
                scraping_products_list[0], ScrapingProduct
            ):
                data_to_insert = [
                    product.to_tuple() for product in scraping_products_list
                ]
            else:
                data_to_insert = scraping_products_list

            cursor.executemany(query, data_to_insert)
            conn.commit()

            self.logger.debug(
                f"{len(scraping_products_list)} products inserted correctly"
            )
            return True

        except psycopg2.Error as error:
            self.logger.error(f"Error inserting products: {error}")
            conn.rollback()
            return False

        finally:
            cursor.close()
            conn.close()

    def _insert_product_discounts(self, discounts_list):
        """Insert product discounts into the database"""
        conn = self._connect_db()

        if conn is None:
            return False

        try:
            cursor = conn.cursor()

            query = """
            INSERT INTO stage_discounts (
                product_id, type, discounted_price, conditions_text,
                conditions_min_quantity, conditions_buy_quantity, conditions_get_quantity
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s
            )
            """

            # Convert ProductDiscount objects to tuples if necessary
            if discounts_list and isinstance(discounts_list[0], ProductDiscount):
                data_to_insert = [discount.to_tuple() for discount in discounts_list]
            else:
                data_to_insert = discounts_list

            cursor.executemany(query, data_to_insert)
            conn.commit()

            self.logger.debug(f"{len(discounts_list)} discounts inserted correctly")
            return True

        except psycopg2.Error as error:
            self.logger.error(f"Error inserting discounts: {error}")
            conn.rollback()
            return False

        finally:
            cursor.close()
            conn.close()

    def insert_scraping_products_with_discounts(self, scraping_products_list):
        """Insert products and their discounts into the database"""
        self.logger.debug(
            f"Starting insertion of {len(scraping_products_list)} products"
        )

        # Insert products first
        success = self._insert_scraping_products(scraping_products_list)

        if not success:
            self.logger.error("Failed to insert products")
            return False

        # Collect all discounts
        all_discounts = []
        for product in scraping_products_list:
            if isinstance(product, ScrapingProduct) and product.discounts:
                all_discounts.extend(product.get_discounts_for_db())

        # Insert discounts if there are any
        if all_discounts:
            self.logger.debug(f"Inserting {len(all_discounts)} discounts")
            return self._insert_product_discounts(all_discounts)

        self.logger.debug("No discounts to insert")
        return True

    def insert_scraping_products_with_discounts_async(self, scraping_products_list, name, callback=None):
        """Insert products and their discounts into the database asynchronously"""
        def _insert_worker():
            try:
                self.logger.debug(f"Starting async insertion of {len(scraping_products_list)} products for '{name}'")
                result = self.insert_scraping_products_with_discounts(scraping_products_list)
                if callback:
                    callback(result, len(scraping_products_list), name)
                return result
            except Exception as e:
                self.logger.error(f"Error in async insertion for '{name}': {e}")
                if callback:
                    callback(False, len(scraping_products_list), name)
                return False

        # execute in a separate thread
        thread = threading.Thread(target=_insert_worker)
        thread.daemon = True
        thread.start()
        return thread
