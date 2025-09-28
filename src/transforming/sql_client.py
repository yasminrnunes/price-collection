import os
import psycopg2
from dotenv import load_dotenv
from logger import Logger
from typing import List, Dict, Any, Optional

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT"),
}


class DatabaseQueryClient:
    def __init__(self, logger_name: str = "query_client"):
        self.logger = Logger(logger_name)

    def _connect_db(self):
        """Establece conexión con la base de datos"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            self.logger.debug("Database connection established")
            return conn
        except psycopg2.Error as error:
            self.logger.error(f"Error connecting to the database: {error}")
            return None

    def execute_query(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """Ejecuta una query SELECT y retorna los resultados como lista de diccionarios"""
        conn = self._connect_db()
        if conn is None:
            return []

        try:
            cursor = conn.cursor()
            cursor.execute(query, params)

            # Obtener nombres de columnas
            columns = [desc[0] for desc in cursor.description]

            # Convertir resultados a lista de diccionarios
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))

            self.logger.debug(
                f"Query executed successfully, {len(results)} rows returned"
            )
            return results

        except psycopg2.Error as error:
            self.logger.error(f"Error executing query: {error}")
            return []

        finally:
            cursor.close()
            conn.close()

    def execute_non_query(self, query: str, params: Optional[tuple] = None) -> bool:
        """Ejecuta una query que no retorna datos (INSERT, UPDATE, DELETE)"""
        conn = self._connect_db()
        if conn is None:
            return None

        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()

            self.logger.debug(
                f"Non-query executed successfully, {cursor.rowcount} rows affected"
            )
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return results
            return None

        except psycopg2.Error as error:
            self.logger.error(f"Error executing non-query: {error}")
            conn.rollback()
            return None

        finally:
            cursor.close()
            conn.close()

# Función de conveniencia para uso rápido
def create_query_client(logger_name: str = "query_client") -> DatabaseQueryClient:
    """Crea una instancia del cliente de queries"""
    return DatabaseQueryClient(logger_name)
