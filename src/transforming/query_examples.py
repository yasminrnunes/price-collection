from query_client import create_query_client
from logger import Logger

LOGGER = Logger("query_examples")
client = create_query_client("custom_queries")


def example_custom_queries():
    """Ejemplos de queries personalizadas"""
    LOGGER.info("=== Ejemplos de Queries Personalizadas ===")

    # 1. Query personalizada: productos más caros
    LOGGER.info("1. Obteniendo los 5 productos más caros...")
    expensive_products_query = """
    SELECT name, market, category, brand, price, product_url
    FROM stage_scraping_products 
    WHERE price IS NOT NULL
    ORDER BY price DESC
    LIMIT 5
    """
    expensive_products = client.execute_query(expensive_products_query)
    for product in expensive_products:
        LOGGER.info(
            f"   ${product['price']:.2f} - {product['name']} ({product['market']})"
        )

    # 2. Query personalizada: productos por rango de precio
    LOGGER.info("2. Obteniendo productos entre $100 y $500...")
    price_range_query = """
    SELECT name, market, category, brand, price
    FROM stage_scraping_products 
    WHERE price BETWEEN %s AND %s
    ORDER BY price ASC
    LIMIT 10
    """
    mid_range_products = client.execute_query(price_range_query, (100, 500))
    LOGGER.info(f"   Encontrados {len(mid_range_products)} productos en este rango")

    # 3. Query personalizada: productos agregados hoy
    LOGGER.info("3. Obteniendo productos agregados hoy...")
    today_products_query = """
    SELECT name, market, category, brand, price, extraction_date
    FROM stage_scraping_products 
    WHERE DATE(extraction_date) = CURRENT_DATE
    ORDER BY extraction_date DESC
    """
    today_products = client.execute_query(today_products_query)
    LOGGER.info(f"   Encontrados {len(today_products)} productos agregados hoy")


def example_maintenance():
    """Ejemplos de operaciones de mantenimiento"""
    LOGGER.info("=== Ejemplos de Mantenimiento ===")

    client = create_query_client("maintenance_queries")

    # 1. Contar productos antiguos (sin eliminarlos)
    LOGGER.info("1. Contando productos más antiguos que 30 días...")
    old_products_query = """
    SELECT COUNT(*) as count
    FROM stage_scraping_products 
    WHERE extraction_date < NOW() - INTERVAL '30 days'
    """
    old_count = client.execute_query(old_products_query)
    if old_count:
        LOGGER.info(
            f"   Hay {old_count[0]['count']} productos más antiguos que 30 días"
        )


def example_non_query_operations():
    """Ejemplos de operaciones que no retornan datos (INSERT, UPDATE, DELETE)"""
    LOGGER.info("=== Ejemplos de execute_non_query ===")

    client = create_query_client("non_query_operations")

    # 1. Crear tabla temporal para pruebas
    LOGGER.info("1. Creando tabla temporal para pruebas...")
    create_table_query = """
    CREATE TEMP TABLE IF NOT EXISTS test_products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        price DECIMAL(10,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    success = client.execute_non_query(create_table_query)
    if success:
        LOGGER.info("   ✅ Tabla temporal creada exitosamente")
    else:
        LOGGER.error("   ❌ Error creando tabla temporal")
        return

    # 2. Insertar datos de prueba
    LOGGER.info("2. Insertando datos de prueba...")
    insert_query = """
    INSERT INTO test_products (name, price) VALUES 
    (%s, %s), (%s, %s), (%s, %s)
    """
    insert_params = (
        "Producto Test 1",
        99.99,
        "Producto Test 2",
        149.50,
        "Producto Test 3",
        299.00,
    )
    success = client.execute_non_query(insert_query, insert_params)
    if success:
        LOGGER.info("   ✅ 3 productos de prueba insertados exitosamente")
    else:
        LOGGER.error("   ❌ Error insertando productos de prueba")

    # 3. Actualizar un producto
    LOGGER.info("3. Actualizando precio de un producto...")
    update_query = """
    UPDATE test_products 
    SET price = %s, name = %s 
    WHERE name = %s
    """
    update_params = (199.99, "Producto Test 1 - ACTUALIZADO", "Producto Test 1")
    success = client.execute_non_query(update_query, update_params)
    if success:
        LOGGER.info("   ✅ Producto actualizado exitosamente")
    else:
        LOGGER.error("   ❌ Error actualizando producto")

    # 4. Eliminar un producto
    LOGGER.info("4. Eliminando un producto...")
    delete_query = "DELETE FROM test_products WHERE name = %s"
    delete_params = ("Producto Test 3",)
    success = client.execute_non_query(delete_query, delete_params)
    if success:
        LOGGER.info("   ✅ Producto eliminado exitosamente")
    else:
        LOGGER.error("   ❌ Error eliminando producto")

    # 5. Verificar los cambios con una query
    LOGGER.info("5. Verificando los cambios realizados...")
    verify_query = "SELECT name, price, created_at FROM test_products ORDER BY id"
    results = client.execute_query(verify_query)
    LOGGER.info(f"   Productos restantes en tabla temporal: {len(results)}")
    for product in results:
        LOGGER.info(f"   - {product['name']}: ${product['price']}")

    # 6. Limpiar tabla temporal (se elimina automáticamente al final de la sesión)
    LOGGER.info(
        "6. La tabla temporal se eliminará automáticamente al final de la sesión"
    )

    # 7. Ejemplo de transacción con rollback (simulado)
    LOGGER.info("7. Ejemplo de operación que falla (para demostrar rollback)...")
    invalid_query = "INSERT INTO tabla_inexistente (campo) VALUES ('valor')"
    success = client.execute_non_query(invalid_query)
    if not success:
        LOGGER.info(
            "   ✅ Error capturado correctamente, rollback ejecutado automáticamente"
        )
    else:
        LOGGER.error("   ❌ Error: la operación debería haber fallado")


def run_all_examples():
    """Ejecuta todos los ejemplos"""
    LOGGER.info("🚀 Iniciando ejemplos de DatabaseQueryClient")

    try:
        example_custom_queries()
        print()  # Línea en blanco

        example_maintenance()
        print()

        LOGGER.info("✅ Todos los ejemplos ejecutados exitosamente")

    except Exception as e:
        LOGGER.error(f"❌ Error ejecutando ejemplos: {e}")


if __name__ == "__main__":
    run_all_examples()
