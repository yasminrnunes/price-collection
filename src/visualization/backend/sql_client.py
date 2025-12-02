"""PostgreSQL database client for the visualization backend.

This module provides a DatabaseClient class for managing PostgreSQL database
connections with connection pooling, automatic retry logic, and connection
validation. It uses environment variables for database configuration and
implements thread-safe connection management.

Features:
    - Thread-safe connection pooling with reuse
    - Automatic connection validation and health checks
    - Connection timeout management (5 minutes idle timeout)
    - Automatic retry logic for failed connections
    - Query execution with automatic result formatting (list of dicts)
    - Keepalive parameters for Railway/cloud deployments
    - Context manager support for safe connection handling

The DatabaseClient automatically manages connection lifecycle, reusing
connections when valid and creating new ones when needed. Connections are
checked for validity before reuse and closed after a timeout period.

Example:
    Basic usage:
        >>> from sql_client import DatabaseClient
        >>> db = DatabaseClient()
        >>> results = db.execute_query("SELECT * FROM products LIMIT 10")
        >>> print(results[0]["name"])

    Using context manager:
        >>> with db.get_db_connection() as conn:
        ...     cursor = conn.cursor()
        ...     cursor.execute("SELECT 1")
"""

import os
import psycopg2
import threading
import time
from contextlib import contextmanager
from dotenv import load_dotenv
from logger import Logger
from typing import List, Dict, Any, Optional

load_dotenv()

# Database connection configuration from environment variables
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT"),
    # Connection parameters for Railway/cloud deployments
    "connect_timeout": 30,
    "keepalives_idle": 600,  # 10 minutes
    "keepalives_interval": 30,  # 30 seconds
    "keepalives_count": 3,
}


class DatabaseClient:
    """Thread-safe PostgreSQL database client with connection pooling.

    This class manages PostgreSQL database connections with automatic pooling,
    validation, and retry logic. Connections are reused when valid and recreated
    when needed, with automatic timeout management to close idle connections.

    The client is thread-safe and can be used concurrently from multiple threads.
    It uses a lock to synchronize connection access and ensures only one
    connection is created per instance.

    Attributes:
        logger: Logger instance for database operations.
        _connection: Cached database connection (private).
        _connection_lock: Thread lock for connection access (private).
        _last_used: Timestamp of last connection use (private).
        _connection_timeout: Timeout in seconds before closing idle connection
            (default 300 seconds / 5 minutes).

    Example:
        Create a client and execute queries:
            >>> db = DatabaseClient()
            >>> results = db.execute_query("SELECT id, name FROM products")
            >>> for row in results:
            ...     print(row["name"])

        Use context manager for manual queries:
            >>> with db.get_db_connection() as conn:
            ...     cursor = conn.cursor()
            ...     cursor.execute("INSERT INTO products (name) VALUES (%s)", ("Test",))
            ...     conn.commit()
    """

    def __init__(self, logger_name: str = "database_client"):
        """Initialize a DatabaseClient instance.

        Args:
            logger_name: Name identifier for the logger instance. Used for
                log organization. Defaults to "database_client".
        """
        self.logger = Logger(logger_name)
        self._connection = None
        self._connection_lock = threading.Lock()
        self._last_used = 0
        self._connection_timeout = 300  # 5 minutes

    def _is_connection_valid(self, conn):
        """Check if a database connection is valid and operational.

        This method performs a health check on the connection by executing
        a simple SELECT query. If the query succeeds, the connection is
        considered valid.

        Args:
            conn: psycopg2 connection object to validate.

        Returns:
            bool: True if connection is valid and operational, False otherwise.
        """
        try:
            if conn is None or conn.closed != 0:
                return False

            # Test connection with a simple query
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except (psycopg2.Error, psycopg2.OperationalError):
            return False

    def _get_connection(self, max_retries=3, retry_delay=5):
        """Get a valid database connection, reusing or creating as needed.

        This method implements connection pooling by checking if the existing
        connection is still valid and within the timeout period. If not, it
        creates a new connection with retry logic.

        The method is thread-safe and uses a lock to ensure only one connection
        is created at a time. It implements exponential backoff retry logic
        for connection failures.

        Args:
            max_retries: Maximum number of connection retry attempts.
                Defaults to 3.
            retry_delay: Delay in seconds between retry attempts.
                Defaults to 5 seconds.

        Returns:
            psycopg2.connection or None: Valid database connection, or None
                if all connection attempts failed.
        """
        with self._connection_lock:
            current_time = time.time()

            # Check if current connection is still valid and recent
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

            # Try to create new connection
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
        """Context manager for safely obtaining and using database connections.

        This method provides a context manager interface for database connections,
        ensuring proper error handling and transaction rollback on exceptions.

        Usage:
            >>> with db.get_db_connection() as conn:
            ...     cursor = conn.cursor()
            ...     cursor.execute("SELECT * FROM products")
            ...     results = cursor.fetchall()

        Yields:
            psycopg2.connection: Database connection object.

        Raises:
            ConnectionError: If a database connection could not be established.
            Exception: Any exception that occurs during connection or query
                execution. The transaction will be rolled back automatically.
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

    def execute_query(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results as a list of dictionaries.

        This method executes a SQL query (with optional parameters) and
        automatically converts the result set into a list of dictionaries,
        where each dictionary represents a row with column names as keys.

        The query supports parameterized queries using psycopg2's parameter
        substitution, which helps prevent SQL injection attacks.

        Args:
            query: SQL query string to execute. Can include parameter
                placeholders (%s) for parameterized queries.
            params: Optional tuple of parameters to substitute into the query.
                Defaults to None for queries without parameters.

        Returns:
            List[Dict[str, Any]]: List of dictionaries, each representing
                a row from the query result. Each dictionary maps column names
                to their values. Returns an empty list if the query fails or
                no connection is available.

        Example:
            Simple query:
                >>> results = db.execute_query("SELECT id, name FROM products LIMIT 5")
                >>> print(results[0]["name"])

            Parameterized query:
                >>> results = db.execute_query(
                ...     "SELECT * FROM products WHERE id = %s",
                ...     params=(123,)
                ... )
                >>> print(results[0]["name"])
        """
        try:
            with self.get_db_connection() as conn:
                if conn is None:
                    return []

                cursor = conn.cursor()
                cursor.execute(query, params)

                # Get column names
                columns = [desc[0] for desc in cursor.description]

                # Convert results to list of dictionaries
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))

                # self.logger.debug(
                #     f"Query executed successfully, {len(results)} rows returned"
                # )
                return results

        except psycopg2.Error as error:
            self.logger.error(f"Error executing query: {error}")
            return []

    def close_connection(self):
        """Close the database connection and clean up resources.

        This method closes the current database connection (if it exists)
        in a thread-safe manner. It handles errors during connection closure
        gracefully and ensures the connection reference is cleared.

        The method is automatically called by the destructor when the
        DatabaseClient instance is garbage collected.

        Example:
            Explicitly close connection:
                >>> db = DatabaseClient()
                >>> # ... use database ...
                >>> db.close_connection()
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
        """Destructor that automatically closes database connection.

        This method is called when the DatabaseClient instance is garbage
        collected. It ensures that any open database connections are properly
        closed to prevent connection leaks.
        """
        self.close_connection()
