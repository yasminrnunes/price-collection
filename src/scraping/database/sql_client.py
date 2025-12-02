"""Database client for managing PostgreSQL connections and data operations.

This module provides a DatabaseClient class that handles PostgreSQL database
connections with connection pooling, automatic retry logic, and thread-safe
operations. It supports inserting scraping products and their associated
discounts into the database.

Features:
    - Connection pooling and reuse with timeout management
    - Automatic connection validation and retry logic
    - Thread-safe operations with proper locking
    - Context manager support for automatic connection handling
    - Asynchronous insertion capabilities using threads
    - Support for batch insertions of products and discounts

Connection Management:
    - Connections are reused for up to 5 minutes (configurable timeout)
    - Automatic retry on connection failures (up to 3 attempts)
    - Connection validation before each use
    - Proper cleanup on errors and program termination

The module reads database configuration from environment variables:
    - DB_HOST: Database host address
    - DB_NAME: Database name
    - DB_USER: Database user
    - DB_PASSWORD: Database password
    - DB_PORT: Database port

Example:
    Create a client and insert products:
        >>> from database.sql_client import DatabaseClient
        >>> client = DatabaseClient("my_market")
        >>> success = client.insert_scraping_products_with_discounts(products)

    Use asynchronous insertion:
        >>> thread = client.insert_scraping_products_with_discounts_async(
        ...     products, "category_name"
        ... )
        >>> thread.join()  # Wait for completion
"""

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
    # Connection parameters for Railway/postgres
    "connect_timeout": 30,
    "keepalives_idle": 600,  # 10 minutes
    "keepalives_interval": 30,  # 30 seconds
    "keepalives_count": 3,
}


class DatabaseClient:
    """Database client for managing PostgreSQL connections and operations.

    This class provides a thread-safe database client with connection pooling,
    automatic retry logic, and support for inserting scraping products and
    their discounts. It manages database connections efficiently by reusing
    them for a configurable timeout period.

    The client supports both synchronous and asynchronous operations, making
    it suitable for high-throughput scraping operations where multiple product
    batches need to be inserted concurrently.

    Attributes:
        logger: Logger instance for database operations.
        _connection: Cached database connection (reused until timeout).
        _connection_lock: Thread lock for thread-safe connection management.
        _last_used: Timestamp of when the connection was last used.
        _connection_timeout: Maximum time (seconds) to reuse a connection.

    Example:
        Basic usage:
            >>> client = DatabaseClient("market_name")
            >>> success = client.insert_scraping_products_with_discounts(products)
            >>> client.close_connection()

        Asynchronous insertion:
            >>> threads = []
            >>> for category, products in categories.items():
            ...     thread = client.insert_scraping_products_with_discounts_async(
            ...         products, category
            ...     )
            ...     threads.append(thread)
            >>> [t.join() for t in threads]  # Wait for all to complete
    """

    def __init__(self, logger_name: str = "database_client"):
        """Initialize a DatabaseClient instance.

        Args:
            logger_name: Name identifier for the logger instance. This is used
                to categorize log messages and can be set to the market name
                (e.g., "extra", "tenda") for better log organization.
        """
        self.logger = Logger(logger_name)
        self._connection = None
        self._connection_lock = threading.Lock()
        self._last_used = 0
        self._connection_timeout = 300  # 5 minutes

    def _is_connection_valid(self, conn):
        """Check if the database connection is still valid.

        This method performs a lightweight test query to verify that the
        connection is active and can execute queries. It checks both the
        connection state and actual functionality.

        Args:
            conn: The psycopg2 connection object to validate.

        Returns:
            True if the connection is valid and can execute queries,
            False otherwise.
        """
        try:
            if conn is None or conn.closed != 0:
                return False

            # Test the connection with a simple query
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except (psycopg2.Error, psycopg2.OperationalError):
            return False

    def _get_connection(self, max_retries=3, retry_delay=5):
        """Get a valid database connection with automatic retry logic.

        This method manages connection pooling and reuse. It checks if an
        existing connection is valid and recent enough to reuse. If not,
        it attempts to create a new connection with retry logic in case
        of failures.

        The connection is cached and reused for up to `_connection_timeout`
        seconds (default: 5 minutes) to improve performance and reduce
        connection overhead.

        Args:
            max_retries: Maximum number of connection attempts before giving up.
                Default is 3 attempts.
            retry_delay: Delay in seconds between retry attempts. Default is 5 seconds.

        Returns:
            A valid psycopg2 connection object if successful, or None if all
            connection attempts failed.

        Note:
            This method is thread-safe and should be called within the
            `get_db_connection` context manager for proper resource management.
        """
        with self._connection_lock:
            current_time = time.time()

            # Check if the current connection is still valid and recent
            if (
                self._connection
                and self._is_connection_valid(self._connection)
                and (current_time - self._last_used) < self._connection_timeout
            ):

                self._last_used = current_time
                return self._connection

            # Close old connection if it exists
            if self._connection:
                try:
                    self._connection.close()
                except:
                    pass
                self._connection = None

            # Try to create a new connection
            for attempt in range(max_retries + 1):
                try:
                    self._connection = psycopg2.connect(**DB_CONFIG)

                    # Test the connection
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
        """Context manager for obtaining and managing database connections.

        This context manager automatically handles connection acquisition,
        transaction rollback on errors, and connection reuse. The connection
        is not closed when exiting the context, as it's reused for subsequent
        operations within the timeout period.

        Yields:
            A valid psycopg2 connection object for database operations.

        Raises:
            ConnectionError: If a database connection could not be established
                after all retry attempts.

        Example:
            >>> with client.get_db_connection() as conn:
            ...     cursor = conn.cursor()
            ...     cursor.execute("SELECT COUNT(*) FROM products")
            ...     count = cursor.fetchone()

        Note:
            The connection is reused and not closed when exiting the context.
            It will be automatically closed when it expires (after timeout)
            or when close_connection() is called.
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
        # Note: We don't close the connection here, it's reused

    def _insert_scraping_products(self, scraping_products_list):
        """Insert scraping products into the database.

        This method inserts a batch of ScrapingProduct objects into the
        `stage_scraping_products` table. It automatically converts
        ScrapingProduct objects to tuples if needed.

        Args:
            scraping_products_list: List of ScrapingProduct objects or tuples
                to insert. If objects, they must have a to_tuple() method.

        Returns:
            True if the insertion was successful, False otherwise.
        """
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
        """Insert product discounts into the database.

        This method inserts a batch of ProductDiscount objects into the
        `stage_discounts` table. It automatically converts ProductDiscount
        objects to tuples if needed.

        Args:
            discounts_list: List of ProductDiscount objects or tuples to insert.
                If objects, they must have a to_tuple() method.

        Returns:
            True if the insertion was successful, False otherwise.
        """
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
        """Insert scraping products and their associated discounts into the database.

        This method performs a two-step insertion: first inserting all products,
        then inserting all their associated discounts. If product insertion fails,
        discount insertion is not attempted.

        The method collects all discounts from all products in the list before
        performing the discount insertion.

        Args:
            scraping_products_list: List of ScrapingProduct objects to insert.
                Each product may have associated discounts that will be
                automatically inserted.

        Returns:
            True if both product and discount insertions were successful,
            False if any step failed.

        Example:
            >>> products = [product1, product2, product3]
            >>> success = client.insert_scraping_products_with_discounts(products)
            >>> if success:
            ...     print("All products and discounts inserted successfully")
        """
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
        self, scraping_products_list, name
    ):
        """Insert scraping products and discounts asynchronously in a separate thread.

        This method creates a daemon thread to perform the insertion operation,
        allowing the caller to continue with other tasks while the database
        operation runs in the background. The thread can be joined later to
        wait for completion.

        Args:
            scraping_products_list: List of ScrapingProduct objects to insert.
            name: Identifier name for logging purposes (e.g., category name).
                Used in log messages to identify which batch is being processed.

        Returns:
            The threading.Thread object that was started. You can call join()
            on this thread to wait for the insertion to complete.

        Example:
            >>> threads = []
            >>> for category, products in categories.items():
            ...     thread = client.insert_scraping_products_with_discounts_async(
            ...         products, category
            ...     )
            ...     threads.append(thread)
            >>> # Wait for all insertions to complete
            >>> for thread in threads:
            ...     thread.join()

        Note:
            The thread is marked as a daemon thread, meaning it will not
            prevent the program from exiting if still running.
        """

        def _insert_worker():
            try:
                self.logger.debug(
                    f"Starting async insertion of {len(scraping_products_list)} products for '{name}'"
                )

                result = self.insert_scraping_products_with_discounts(
                    scraping_products_list
                )

                if result:
                    self.logger.info(
                        f"Successfully inserted {len(scraping_products_list)} products for '{name}'"
                    )
                else:
                    self.logger.error(
                        f"Failed to insert {len(scraping_products_list)} products for '{name}'"
                    )

                return result
            except (psycopg2.Error, ConnectionError, TimeoutError) as e:
                self.logger.error(
                    f"Failed to insert {len(scraping_products_list)} products for '{name}', exception: {e}"
                )
                return False

        # execute in a separate thread
        thread = threading.Thread(target=_insert_worker)
        thread.daemon = True
        thread.start()
        return thread

    def test_connection(self):
        """Test the database connection and verify it's working.

        This method performs a simple query to verify that the database
        connection is functional and can execute queries. It retrieves
        the PostgreSQL version as a test.

        Returns:
            True if the connection test was successful, False otherwise.

        Example:
            >>> if client.test_connection():
            ...     print("Database connection is working")
            ... else:
            ...     print("Database connection failed")
        """
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
        """Close the database connection and clean up resources.

        This method safely closes the cached database connection if it exists.
        It's called automatically by the destructor when the DatabaseClient
        instance is garbage collected, but can also be called explicitly to
        ensure immediate cleanup.

        Note:
            After closing, subsequent operations will create a new connection
            automatically when needed.
        """
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
        """Destructor to ensure the database connection is properly closed.

        This method is automatically called when the DatabaseClient instance
        is garbage collected. It ensures that any open database connections
        are properly closed to prevent resource leaks.
        """
        self.close_connection()
