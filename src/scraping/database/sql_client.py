import os
import psycopg2
import threading
import time
from contextlib import contextmanager
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
    # Connection parameters railway
    "connect_timeout": 30,
    "keepalives_idle": 600,  # 10 minutos
    "keepalives_interval": 30,  # 30 segundos
    "keepalives_count": 3,
}


class DatabaseClient:
    def __init__(self, logger_name: str = "database_client"):
        self.logger = Logger(logger_name)
        self._connection = None
        self._connection_lock = threading.Lock()
        self._last_used = 0
        self._connection_timeout = 300  # 5 minutos

    def _is_connection_valid(self, conn):
        """Verifica se a conexão ainda está válida"""
        try:
            if conn is None or conn.closed != 0:
                return False

            # Testa a conexão com uma query simples
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except (psycopg2.Error, psycopg2.OperationalError):
            return False

    def _get_connection(self, max_retries=3, retry_delay=5):
        """
        Obtém uma conexão válida com retry automático
        """
        with self._connection_lock:
            current_time = time.time()

            # Verifica se a conexão atual ainda é válida e recente
            if (
                self._connection
                and self._is_connection_valid(self._connection)
                and (current_time - self._last_used) < self._connection_timeout
            ):

                self._last_used = current_time
                return self._connection

            # Fecha conexão antiga se existir
            if self._connection:
                try:
                    self._connection.close()
                except:
                    pass
                self._connection = None

            # Tenta criar nova conexão
            for attempt in range(max_retries + 1):
                try:
                    self._connection = psycopg2.connect(**DB_CONFIG)

                    # Testa a conexão
                    if self._is_connection_valid(self._connection):
                        self._last_used = current_time
                        self.logger.debug("Database connection established")
                        return self._connection
                    else:
                        self._connection.close()
                        self._connection = None

                except (psycopg2.Error, psycopg2.OperationalError) as error:
                    self.logger.debug(
                        f"Connection attempt {attempt + 1}/{max_retries + 1} failed: {error}"
                    )

                    if attempt < max_retries:
                        time.sleep(retry_delay)
                    else:
                        self.logger.error(
                            f"All {max_retries + 1} connection attempts failed: {error}"
                        )
                        return None

            return None

    @contextmanager
    def get_db_connection(self):
        """
        Context manager para obter e gerenciar conexões automaticamente
        """
        conn = None
        try:
            conn = self._get_connection()
            if conn is None:
                raise ConnectionError("Could not establish database connection")
            yield conn
        except Exception as error:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            raise error
        # Note: Não fechamos a conexão aqui, ela é reutilizada

    def _insert_scraping_products(self, scraping_products_list):
        """Insere produtos de scraping na base de dados"""
        try:
            with self.get_db_connection() as conn:
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
            return False

    def _insert_product_discounts(self, discounts_list):
        """Insere descontos de produtos na base de dados"""
        try:
            with self.get_db_connection() as conn:
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
                    data_to_insert = [
                        discount.to_tuple() for discount in discounts_list
                    ]
                else:
                    data_to_insert = discounts_list

                cursor.executemany(query, data_to_insert)
                conn.commit()

                self.logger.debug(f"{len(discounts_list)} discounts inserted correctly")
                return True

        except psycopg2.Error as error:
            self.logger.error(f"Error inserting discounts: {error}")
            return False

    def insert_scraping_products_with_discounts(self, scraping_products_list):
        """Insere produtos e seus descontos na base de dados"""
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

    def insert_scraping_products_with_discounts_async(
        self, scraping_products_list, name, callback=None
    ):
        """Insere produtos e descontos de forma assíncrona"""

        def _insert_worker():
            try:
                self.logger.debug(
                    f"Starting async insertion of {len(scraping_products_list)} products for '{name}'"
                )
                result = self.insert_scraping_products_with_discounts(
                    scraping_products_list
                )
                if callback:
                    callback(result, len(scraping_products_list), name)
                return result
            except (psycopg2.Error, ConnectionError, TimeoutError) as e:
                self.logger.error(f"Error in async insertion for '{name}': {e}")
                if callback:
                    callback(False, len(scraping_products_list), name)
                return False

        # execute in a separate thread
        thread = threading.Thread(target=_insert_worker)
        thread.daemon = True
        thread.start()
        return thread

    def test_connection(self):
        """Testa a conexão com a base de dados"""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT version()")
                    version = cursor.fetchone()
                    self.logger.info(
                        f"Database connection successful. Version: {version[0]}"
                    )
                    return True
        except Exception as error:
            self.logger.error(f"Database connection test failed: {error}")
            return False

    def close_connection(self):
        """Fecha a conexão com a base de dados"""
        with self._connection_lock:
            if self._connection:
                try:
                    self._connection.close()
                    self.logger.debug("Database connection closed")
                except Exception as error:
                    self.logger.error(f"Error closing database connection: {error}")
                finally:
                    self._connection = None

    def __del__(self):
        """Destructor para garantir que a conexão seja fechada"""
        self.close_connection()
