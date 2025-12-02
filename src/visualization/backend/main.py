"""Flask backend API for price optimization and cart management.

This module provides a REST API for the price collection visualization frontend.
It handles product queries, cart management, and price optimization using R scripts.

Features:
    - Product and supermarket data retrieval
    - Cart creation and management with in-memory storage
    - Price optimization integration with R scripts
    - CORS support for frontend integration
    - Health check endpoint
    - Comprehensive error handling

API Endpoints:
    - GET /health: Health check endpoint
    - GET /products: Retrieve all products with available supermarkets
    - POST /carts: Create a new shopping cart
    - GET /carts/<cart_id>: Get cart optimization results
    - DELETE /carts: Clear all carts
    - GET /r-test: Test endpoint for R optimization (development)

Cart Management:
    Carts are stored in-memory using a dictionary with UUID keys. Each cart
    contains selected supermarkets and product quantities. When a cart is
    retrieved, the system runs price optimization to determine the best
    supermarket combination for the cart contents.

Example:
    Start the server:
        >>> python main.py

    The API will be available at http://localhost:5001 with endpoints for
    product queries and cart optimization.

Note:
    - Carts are stored in memory and will be lost on server restart
    - The optimization uses R scripts for price calculations
    - CORS is enabled for all origins (*) for development purposes
"""

import uuid
import os
import sys
from flask import Flask, jsonify, request
from sql_client import DatabaseClient
from opt_augmecon import run_optimization

# Add parent directories to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "transforming"))

app = Flask(__name__)

# In-memory storage for shopping carts (keyed by UUID)
carts_memory_storage = {}

# Database client for querying product and price data
db_client = DatabaseClient("visualization_backend")


@app.after_request
def after_request(response):
    """Add CORS headers to all responses.

    This function is called after each request to add Cross-Origin Resource
    Sharing (CORS) headers, allowing the frontend to make requests from different
    origins.

    Args:
        response: The Flask response object to modify.

    Returns:
        The response object with CORS headers added.

    Note:
        Currently allows all origins ("*") for development. In production,
        this should be restricted to specific frontend domains.
    """
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type,Authorization")
    response.headers.add("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
    return response


@app.route("/<path:path>", methods=["OPTIONS"])
def handle_options(path):  # pylint: disable=unused-argument
    """Handle CORS preflight OPTIONS requests.

    This endpoint handles CORS preflight requests that browsers send before
    making cross-origin requests. It returns a 200 status code to allow
    the actual request to proceed.

    Args:
        path: The request path (unused, but required for route matching).

    Returns:
        An empty response with 200 status code.
    """
    return "", 200


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for API monitoring.

    This endpoint can be used by monitoring systems or load balancers to
    verify that the API is running and responsive.

    Returns:
        A JSON response with status, message, and version information:
        {
            "status": "healthy",
            "message": "API is running",
            "version": "1.0"
        }
        HTTP 200 status code.
    """
    return jsonify({"status": "healthy", "message": "API is running", "version": "1.0"})


@app.route("/products", methods=["GET"])
def get_products():
    """Retrieve all products with their available supermarkets.

    This endpoint returns a list of all products in the database along with
    the supermarkets where each product is available. It also returns a list
    of all supermarkets for reference.

    Returns:
        A JSON response containing:
        {
            "products": [
                {
                    "id": int,
                    "name": str,
                    "supermarkets": [int, ...]  # List of supermarket IDs
                },
                ...
            ],
            "supermarkets": [
                {
                    "id": int,
                    "name": str
                },
                ...
            ]
        }
        HTTP 200 status code.

    Note:
        Products are grouped by ID and name, with supermarket IDs aggregated
        into a JSON array. Only products with available prices are included.
    """
    query = """
        SELECT 
        p.id, 
        p.name, 
        p.normalized_name, 
        JSON_AGG(
            DISTINCT pr.id_supermarket 
            ORDER BY 
            pr.id_supermarket
        ) AS supermarkets 
        FROM 
        products p 
        INNER JOIN prices pr ON p.id = pr.id_product 
        INNER JOIN (
            SELECT 
            id_product, 
            COUNT(DISTINCT id_supermarket) AS num_supermarkets 
            FROM 
            prices p 
            GROUP BY 
            id_product 
            HAVING 
            COUNT(DISTINCT id_supermarket) >= 2
        ) AS product_counts ON p.id = product_counts.id_product 
        GROUP BY 
        p.id, 
        p.name,
        p.normalized_name
        ORDER BY 
        p.id;
    """

    products = db_client.execute_query(query)

    query_supermarkets = """
      SELECT
        id,
        display_name as name
      FROM supermarkets
    """
    supermarkets = db_client.execute_query(query_supermarkets)

    return jsonify({"products": products, "supermarkets": supermarkets}), 200


@app.route("/carts", methods=["POST"])
def process_cart():
    """Create a new shopping cart and store it in memory.

    This endpoint accepts cart data including selected supermarkets and products
    with quantities. It validates the input data and stores the cart in memory
    with a unique UUID identifier.

    Request Body (JSON):
        {
            "selectedSupermarkets": [int, ...],  # List of supermarket IDs
            "products": [
                {
                    "id": int,           # Product ID
                    "quantity": int     # Quantity (must be > 0)
                },
                ...
            ]
        }

    Returns:
        On success (HTTP 201):
        {
            "id": str,  # UUID of the created cart
            "receivedData": {
                "selectedSupermarkets": [int, ...],
                "productsCount": int,
                "totalQuantity": int
            }
        }

        On validation error (HTTP 400):
        {
            "error": str  # Description of the validation error
        }

        On server error (HTTP 500):
        {
            "error": str  # Error message
        }

    Validation Rules:
        - selectedSupermarkets must be a non-empty list of integers
        - products must be a non-empty list
        - Each product must have 'id' (int) and 'quantity' (int) fields
        - Quantity must be greater than 0

    Note:
        The cart is stored in memory and will be lost on server restart.
        Use the returned cart_id to retrieve optimization results later.
    """
    try:
        data = request.get_json()

        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        if "selectedSupermarkets" not in data or "products" not in data:
            return (
                jsonify(
                    {
                        "error": "Missing required fields: 'selectedSupermarkets' and 'products'"
                    }
                ),
                400,
            )

        if not isinstance(data["selectedSupermarkets"], list):
            return jsonify({"error": "'selectedSupermarkets' must be a list"}), 400

        if not data["selectedSupermarkets"]:
            return jsonify({"error": "'selectedSupermarkets' cannot be empty"}), 400

        if not isinstance(data["products"], list):
            return jsonify({"error": "'products' must be a list"}), 400

        if not data["products"]:
            return jsonify({"error": "'products' cannot be empty"}), 400

        for product in data["products"]:
            if "id" not in product or "quantity" not in product:
                return (
                    jsonify(
                        {"error": "Each product must have 'id' and 'quantity' fields"}
                    ),
                    400,
                )

            if not isinstance(product["id"], int) or not isinstance(
                product["quantity"], int
            ):
                return (
                    jsonify(
                        {
                            "error": "'id' must be an integer and 'quantity' must be a integer"
                        }
                    ),
                    400,
                )

            if product["quantity"] <= 0:
                return jsonify({"error": "Quantity must be greater than 0"}), 400

        cart_id = str(uuid.uuid4())

        carts_memory_storage[cart_id] = data

        return (
            jsonify(
                {
                    "id": cart_id,
                    "receivedData": {
                        "selectedSupermarkets": data["selectedSupermarkets"],
                        "productsCount": len(data["products"]),
                        "totalQuantity": sum(
                            product["quantity"] for product in data["products"]
                        ),
                    },
                }
            ),
            201,
        )

    except Exception as e:
        return jsonify({"error": f"Error processing request: {str(e)}"}), 500


@app.route("/carts/<cart_id>", methods=["GET"])
def get_cart(cart_id):
    """Retrieve cart data and run price optimization.

    This endpoint retrieves a cart by its ID and runs price optimization
    using the R optimization script. The optimization determines the best
    supermarket combination and pricing for the cart contents.

    Args:
        cart_id: The UUID of the cart to retrieve (from path parameter).

    Returns:
        On success (HTTP 200):
        JSON response containing optimization results with:
        - Optimal supermarket assignments
        - Price breakdowns
        - Total costs
        - Other optimization metrics

        On cart not found (HTTP 404):
        {
            "error": "Cart not found"
        }

        On server error (HTTP 500):
        {
            "error": str  # Error message
        }

    Note:
        The optimization is run synchronously when the cart is retrieved.
        For large carts, this may take some time. The cart data must have
        been previously created via POST /carts.

    TODO:
        Add cart content details to the response.
    """
    try:
        # TODO contenido del carrito
        if cart_id not in carts_memory_storage:
            return jsonify({"error": "Cart not found"}), 404

        cart_data = carts_memory_storage[cart_id]

        # result = run_optimization_from_input(cart_data)
        result = run_optimization(cart_data, discounts=True)
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": f"Error retrieving cart: {str(e)}"}), 500


@app.route("/carts", methods=["DELETE"])
def clear_all_carts():
    """Delete all carts from memory storage.

    This endpoint clears all stored shopping carts from the in-memory storage.
    It returns the count of carts that were deleted.

    Returns:
        On success (HTTP 200):
        {
            "message": "All carts deleted successfully",
            "deletedCount": int  # Number of carts deleted
        }

        On server error (HTTP 500):
        {
            "error": str  # Error message
        }

    Note:
        This operation cannot be undone. All cart data will be lost after
        this call. Use with caution.
    """
    try:
        carts_count = len(carts_memory_storage)
        carts_memory_storage.clear()

        return (
            jsonify(
                {
                    "message": "All carts deleted successfully",
                    "deletedCount": carts_count,
                }
            ),
            200,
        )

    except Exception as e:
        return jsonify({"error": f"Error clearing carts: {str(e)}"}), 500


@app.errorhandler(404)
def not_found(_):
    """Handle 404 Not Found errors.

    This error handler is triggered when a route is not found. It returns
    a JSON error response instead of the default HTML 404 page.

    Args:
        _: The error object (unused, but required by Flask).

    Returns:
        A JSON response with error details:
        {
            "error": "Endpoint not found",
            "message": "The requested endpoint does not exist"
        }
        HTTP 404 status code.
    """
    return (
        jsonify(
            {
                "error": "Endpoint not found",
                "message": "The requested endpoint does not exist",
            }
        ),
        404,
    )


@app.errorhandler(500)
def internal_error(_):
    """Handle 500 Internal Server Error.

    This error handler is triggered when an unhandled exception occurs.
    It returns a JSON error response instead of exposing error details
    to the client.

    Args:
        _: The error object (unused, but required by Flask).

    Returns:
        A JSON response with error details:
        {
            "error": "Internal server error",
            "message": "An unexpected error occurred"
        }
        HTTP 500 status code.

    Note:
        In production, detailed error information should be logged but
        not returned to the client for security reasons.
    """
    return (
        jsonify(
            {
                "error": "Internal server error",
                "message": "An unexpected error occurred",
            }
        ),
        500,
    )


if __name__ == "__main__":
    # Run the Flask development server
    # Starts the Flask development server on http://localhost:5001 with debug
    # mode enabled. The server listens on all network interfaces (0.0.0.0)
    # to allow connections from other machines on the network.
    #
    # Note:
    #     Debug mode should be disabled in production environments.
    #     For production, use a production WSGI server like Gunicorn or uWSGI.
    print("\nStarting server on http://localhost:5001")

    app.run(debug=True, host="0.0.0.0", port=5001)
