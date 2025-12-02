import pulp
# import pandas as pd
# import matplotlib.pyplot as plt
import json
from sql_client import DatabaseClient


def data_ingestion(input_json, db_name = "visualization_backend"):
    """
    Prepare cost matrix without considering discounts.
    
    This function extracts product and supermarket data from the database based on the input JSON,
    building a cost matrix that maps (product_id, supermarket_id) pairs to their prices.
    It does not include any discount information.
    
    Args:
        input_json: JSON object or string containing:
            - selectedSupermarkets: List of supermarket IDs
            - products: List of dicts with 'id' and 'quantity' keys
        db_name: Database name to connect to (default: "visualization_backend")
    
    Returns:
        tuple: (p, q, I, J, supermarket_names, product_names, extraction_date_map, product_info)
            - p: Dictionary mapping (product_id, supermarket_id) to price
            - q: Dictionary mapping product_id to quantity
            - I: List of product IDs
            - J: List of supermarket IDs
            - supermarket_names: Dictionary mapping supermarket_id to name
            - product_names: Dictionary mapping product_id to name
            - extraction_date_map: Dictionary mapping (product_id, supermarket_id) to extraction date
            - product_info: Dictionary with detailed product information
    """
    # Parse JSON string if necessary (handle both string and dict inputs)
    if isinstance(input_json, str):
        input_data = json.loads(input_json)
    else:
        input_data = input_json

    # Extract supermarket IDs (J) and product IDs (I) from input
    J = input_json["selectedSupermarkets"]
    I = [prod["id"] for prod in input_json["products"]]
    
    # Extract quantities for each product (q[i] = quantity of product i)
    q = {prod["id"]: prod["quantity"] for prod in input_json["products"]}

    # Format IDs as comma-separated strings for SQL IN clause
    supermarket_list_sql = ", ".join(map(str, J))
    product_list_sql = ", ".join(map(str, I))

    # Build SQL query to fetch prices and related information
    # Query joins prices, products, and supermarkets tables to get complete information
    sql_query = f"""
        SELECT 
            p.id_product,
            p.id_supermarket,
            p.value,
            pr.name AS product_name,
            s.display_name AS supermarket_name,
            p.extraction_date
        FROM prices p
        LEFT JOIN products pr 
        ON p.id_product = pr.id
        LEFT JOIN supermarkets s
        ON p.id_supermarket = s.id
        WHERE p.id_supermarket IN ({supermarket_list_sql})
        AND p.id_product IN ({product_list_sql});
    """
    # Execute query and retrieve results
    db_client = DatabaseClient(db_name)
    rows = db_client.execute_query(sql_query)

    # Build price dictionary: (product_id, supermarket_id) -> price
    p = {(r["id_product"], r["id_supermarket"]): r["value"] for r in rows}
    
    # Build name mappings for supermarkets and products
    supermarket_names = {r["id_supermarket"]: r["supermarket_name"] for r in rows}
    product_names = {r["id_product"]: r["product_name"] for r in rows}
    
    # Map extraction dates for each price entry
    extraction_date_map = {(r["id_product"], r["id_supermarket"]): r["extraction_date"] for r in rows}

    # Build comprehensive product information dictionary
    # Note: discount_description will be None for this function since discounts are not considered
    product_info = {
        (r["id_product"], r["id_supermarket"]): {
            "productName": r["product_name"],
            "supermarketName": r["supermarket_name"],
            "unitPrice": r["value"],
            "extractionDate": r["extraction_date"],
            "discountDescription": r.get("discount_description")  # Will be None for regular prices
        }
        for r in rows
    }

    return p, q, I, J, supermarket_names, product_names, extraction_date_map, product_info

def data_ingestion_discounts(input_json, db_name = "visualization_backend"):
    """
    Prepare cost matrix considering discounts.
    
    This function extracts product and supermarket data from the database, including both regular
    prices and discount offers. It applies filters based on minimum quantity and multiple quantity
    requirements, and keeps only the lowest available price for each (product, supermarket) pair.
    
    Args:
        input_json: JSON object or string containing:
            - selectedSupermarkets: List of supermarket IDs
            - products: List of dicts with 'id' and 'quantity' keys
        db_name: Database name to connect to (default: "visualization_backend")
    
    Returns:
        tuple: (p, q, I, J, supermarket_names, product_names, extraction_date_map, discounts_map, product_info)
            - p: Dictionary mapping (product_id, supermarket_id) to lowest available price
            - q: Dictionary mapping product_id to quantity
            - I: List of product IDs
            - J: List of supermarket IDs
            - supermarket_names: Dictionary mapping supermarket_id to name
            - product_names: Dictionary mapping product_id to name
            - extraction_date_map: Dictionary mapping (product_id, supermarket_id) to extraction date
            - discounts_map: Dictionary mapping (product_id, supermarket_id) to discount description
            - product_info: Dictionary with detailed product information including discounts
    """
    # Parse JSON string if necessary (handle both string and dict inputs)
    if isinstance(input_json, str):
        input_data = json.loads(input_json)
    else:
        input_data = input_json

    # Extract supermarket IDs (J) and product IDs (I) from input
    J = input_json["selectedSupermarkets"]
    I = [prod["id"] for prod in input_json["products"]]
    
    # Extract quantities for each product (q[i] = quantity of product i)
    q = {prod["id"]: prod["quantity"] for prod in input_json["products"]}

    # Format IDs as comma-separated strings for SQL IN clause
    supermarket_list_sql = ", ".join(map(str, J))
    product_list_sql = ", ".join(map(str, I))

    # Build SQL query using UNION ALL to combine regular prices and discount prices
    # First SELECT: Regular prices (with NULL for discount fields)
    # Second SELECT: Discount prices from discounts table
    sql_query = f"""
        SELECT 
            p.id_product,
            p.id_supermarket,
            p.value,
            pr.name AS product_name,
            s.display_name AS supermarket_name,
            1 AS multiple_qty,
            NULL AS min_qty,
            NULL AS discount_description,
            p.extraction_date
        FROM prices p
        LEFT JOIN products pr 
        ON p.id_product = pr.id
        LEFT JOIN supermarkets s
        ON p.id_supermarket = s.id
        WHERE p.id_supermarket IN ({supermarket_list_sql})
        AND p.id_product IN ({product_list_sql})
        UNION ALL
        SELECT 
            p.id_product,
            p.id_supermarket,
            d.unit_value AS value,
            pr.name AS product_name,
            s.display_name AS supermarket_name,
            d.multiple_qty,
            d.min_qty,
            d.description AS discount_description,
            p.extraction_date
        FROM prices p
        INNER JOIN discounts d
        ON p.id = d.id_price
        LEFT JOIN products pr 
        ON p.id_product = pr.id
        LEFT JOIN supermarkets s
        ON p.id_supermarket = s.id
        WHERE p.id_supermarket IN ({supermarket_list_sql})
        AND p.id_product IN ({product_list_sql})
        ;
    """
    # Execute query and retrieve results
    db_client = DatabaseClient(db_name)
    rows = db_client.execute_query(sql_query)

    # Initialize dictionaries to store processed data
    p = {}  # Price dictionary: (product_id, supermarket_id) -> lowest price
    discounts_map = {}  # Discount descriptions for each price entry
    extraction_date_map = {}  # Extraction dates for each price entry
    product_info = {}  # Comprehensive product information

    # Process each row from the query results
    for row in rows:
        pid = row["id_product"]
        sid = row["id_supermarket"]
        price = row["value"]
        discount_description = row["discount_description"]
        extraction_date = row["extraction_date"]
        
        # Parse discount constraints (convert to int if not None)
        min_qty = int(row["min_qty"]) if row.get("min_qty") is not None else None
        multiple_qty = int(row["multiple_qty"]) if row.get("multiple_qty") is not None else None
        qty = q.get(pid)  # Get required quantity for this product

        # --- Apply discount filters ---
        # Filter 1: If discount has minimum quantity requirement, check if quantity meets it
        if min_qty is not None and qty < min_qty:
            continue  # Skip this discount if quantity requirement not met

        # Filter 2: If discount requires multiple quantity purchase, check if quantity is a multiple
        if multiple_qty is not None and qty % multiple_qty != 0:
            continue  # Skip this discount if quantity is not a multiple of required amount

        # --- Keep only the lowest price for each (product, supermarket) pair ---
        key = (pid, sid)
        if key not in p or price < p[key]:
            # Update price and related information if this is a lower price
            p[key] = price
            discounts_map[key] = discount_description
            extraction_date_map[key] = extraction_date
            # Store the cheapest option's information
            product_info[key] = {
                "productName": row["product_name"],
                "supermarketName": row["supermarket_name"],
                "unitPrice": price,
                "extractionDate": extraction_date,
                "discountDescription": discount_description
            }

    # Build name mappings for supermarkets and products
    supermarket_names = {r["id_supermarket"]: r["supermarket_name"] for r in rows}
    product_names = {r["id_product"]: r["product_name"] for r in rows}

    return p, q, I, J, supermarket_names, product_names, extraction_date_map, discounts_map, product_info

def build_model(primary_obj="f1", epsilon=None, delta=1e-5, range_f2=1, p=None, q=None, I=None, J=None, extraction_date_map=None, discounts_map=None, bigM=1e6):
    """
    Build a PuLP optimization model using the AUGMECON method for multi-objective optimization.
    
    This function creates a binary integer programming model with two objectives:
    - f1: Minimize total cost (sum of price * quantity for all product-supermarket assignments)
    - f2: Minimize number of supermarkets visited
    
    The AUGMECON method constrains the secondary objective (f2) to epsilon and minimizes
    the primary objective (f1) with a small penalty for slack.
    
    Args:
        primary_obj: Primary objective to minimize ("f1" or "f2")
        epsilon: Constraint value for secondary objective in AUGMECON (None for unconstrained)
        delta: Small penalty coefficient for slack variable (default: 1e-5)
        range_f2: Range of f2 values for normalization (default: 1)
        p: Dictionary mapping (product_id, supermarket_id) to price
        q: Dictionary mapping product_id to quantity
        I: List of product IDs
        J: List of supermarket IDs
        extraction_date_map: Dictionary mapping (product_id, supermarket_id) to extraction date (unused)
        discounts_map: Dictionary mapping (product_id, supermarket_id) to discount description (unused)
        bigM: Large penalty value for missing prices (default: 1e6)
    
    Returns:
        tuple: (model, x, y, f1, f2, s)
            - model: PuLP problem instance
            - x: Binary decision variables x[(i,j)] = 1 if product i is assigned to supermarket j
            - y: Binary decision variables y[j] = 1 if supermarket j is visited
            - f1: Total cost objective expression
            - f2: Number of supermarkets objective expression
            - s: Slack variable for epsilon constraint
    """
    # Initialize minimization problem
    model = pulp.LpProblem("AUGMECON", pulp.LpMinimize)
    
    # Decision variables:
    # x[(i,j)] = 1 if product i is purchased at supermarket j, 0 otherwise
    x = pulp.LpVariable.dicts("x", [(i, j) for i in I for j in J], cat="Binary")
    
    # y[j] = 1 if supermarket j is visited (at least one product purchased there), 0 otherwise
    y = pulp.LpVariable.dicts("y", J, cat="Binary")
    
    # Slack variable for epsilon constraint in AUGMECON method (non-negative)
    s = pulp.LpVariable("s", lowBound=0)

    # Define objective functions
    # f1: Total cost = sum over all products and supermarkets of (price * quantity * assignment)
    # Use bigM as penalty for missing prices (product not available at supermarket)
    f1 = pulp.lpSum([p.get((i, j), bigM) * q[i] * x[(i, j)] for i in I for j in J])
    
    # f2: Number of supermarkets visited = sum of y[j] for all j
    f2 = pulp.lpSum([y[j] for j in J])

    # --- Constraints ---
    # Constraint 1: Each product must be assigned to exactly one supermarket
    for i in I:
        model += pulp.lpSum([x[(i, j)] for j in J]) == 1, f"Assign_{i}"
    
    # Constraint 2: A product can only be assigned to a supermarket if that supermarket is visited
    # This links x variables to y variables: x[(i,j)] <= y[j] for all i,j
    for i in I:
        for j in J:
            model += x[(i, j)] <= y[j], f"Activate_{i}_{j}"
    
    # Constraint 3: A supermarket is visited if and only if at least one product is assigned to it
    # This ensures y[j] = 1 when any x[(i,j)] = 1, and y[j] = 0 otherwise
    for j in J:
        model += y[j] <= pulp.lpSum([x[(i, j)] for i in I]), f"Link_y_to_x_{j}"

    # --- Set objective based on primary_obj parameter ---
    if primary_obj == "f1":
        if epsilon is not None:
            # AUGMECON constraint: f2 + s = epsilon (constrain secondary objective)
            model += f2 + s == epsilon
            # Minimize f1 with normalized slack penalty
            model += f1 + delta * (s / range_f2)
        else:
            # Simple minimization of f1 without constraints
            model += f1
    elif primary_obj == "f2":
        # Minimize f2 (number of supermarkets)
        model += f2
    else:
        raise ValueError("primary_obj must be 'f1' or 'f2'")

    return model, x, y, f1, f2, s

def run_optimization(input_json, db_name="visualization_backend", discounts=True,bigM=1e6):
    """
    Run multi-objective optimization using AUGMECON method.
    
    This function implements the AUGMECON (Augmented Epsilon-Constraint) method to generate
    Pareto-optimal solutions for the shopping problem. It minimizes both total cost (f1) and
    number of supermarkets visited (f2) by sweeping through epsilon values.
    
    Args:
        input_json: JSON object or string containing:
            - selectedSupermarkets: List of supermarket IDs
            - products: List of dicts with 'id' and 'quantity' keys
        db_name: Database name to connect to (default: "visualization_backend")
        discounts: Whether to consider discounts when fetching prices (default: True)
    
    Returns:
        dict: Results containing:
            - costBySupermarket: List of supermarkets sorted by total cost (if all products available)
            - shoppingScenarios: List of Pareto-optimal shopping scenarios, each with:
                - scenarioId: Epsilon value (number of supermarkets)
                - supermarkets: List of supermarkets with assigned products
    """
    # Step 0: Data ingestion - fetch prices and product information from database
    if discounts:
        p, q, I, J, supermarket_names, product_names, extraction_date_map, discounts_map, product_info = data_ingestion_discounts(input_json, db_name)
    else:
        p, q, I, J, supermarket_names, product_names, extraction_date_map, product_info = data_ingestion(input_json, db_name)
    
    # Calculate total cost for each supermarket (if all products are available)
    # This provides a reference for users to see single-supermarket costs
    total_cost_per_supermarket = {}

    for j in J:
        # Check if supermarket j has prices for all required products
        has_all_prices = all((i, j) in p for i in I)
        
        if has_all_prices:
            # Calculate total cost for purchasing all products at supermarket j
            total_cost_per_supermarket[j] = sum(p[(i, j)] * q[i] for i in I)
        else:
            # Skip supermarkets that don't have all products available
            continue

    # Sort supermarkets by total cost in ascending order (cheapest first)
    ordered_supermarkets = sorted(total_cost_per_supermarket.items(), key=lambda x: x[1])

    # Transform to desired output format with supermarket names
    costBySupermarket = [
        {
            'supermarketId': supermarket_id,
            'supermarketName': supermarket_names.get(supermarket_id, f'Unknown_{supermarket_id}'),
            'totalCost': total_cost
        }
        for supermarket_id, total_cost in ordered_supermarkets
    ]

    #print(costBySupermarket)

    # ----- Step 1: Solve each objective separately to find bounds -----
    # Solve for f1 (minimize cost) to find minimum cost and corresponding number of supermarkets
    model_f1, x, y, f1, f2, s = build_model("f1", p=p, q=q, I=I, J=J, bigM=bigM)
    model_f1.solve(pulp.PULP_CBC_CMD(msg=0))  # msg=0 suppresses solver output
    f1_min = pulp.value(f1)  # Minimum total cost
    f2_at_f1 = pulp.value(f2)  # Number of supermarkets when cost is minimized

    # Solve for f2 (minimize number of supermarkets) to find minimum supermarkets and corresponding cost
    model_f2, x, y, f1, f2, s = build_model("f2", p=p, q=q, I=I, J=J, bigM=bigM)
    model_f2.solve(pulp.PULP_CBC_CMD(msg=0))
    f2_min = pulp.value(f2)  # Minimum number of supermarkets
    f1_at_f2 = pulp.value(f1)  # Total cost when number of supermarkets is minimized

    # Calculate range of f2 for normalization (avoid division by zero)
    range_f2 = f2_at_f1 - f2_min if f2_at_f1 != f2_min else 1

    # ----- Step 2: Sweep epsilon values to generate Pareto frontier -----
    # Generate solutions for each possible number of supermarkets between min and max
    solutions = []
    epsilons = range(int(f2_min), int(f2_at_f1) + 1)

    # For each epsilon (number of supermarkets), solve constrained optimization
    for eps in epsilons:
        # Build model with f2 constrained to epsilon, minimize f1
        model_eps, x, y, f1, f2, s = build_model("f1", epsilon=eps, range_f2=range_f2, p=p, q=q, I=I, J=J, bigM=bigM)
        model_eps.solve(pulp.PULP_CBC_CMD(msg=0))

        # If solution is optimal, extract assignment information
        if pulp.LpStatus[model_eps.status] == "Optimal":
            # Group products by supermarket based on assignment variables
            supermarkets_dict = {}

            # Extract assignments: x[(i,j)] = 1 means product i is assigned to supermarket j
            for i in I:
                for j in J:
                    if x[(i, j)].value() > 0.5:  # Check if binary variable is 1 (with tolerance)
                        # Get product information for this assignment
                        info = product_info.get((i, j), {})
                        
                        # Initialize supermarket entry if not exists
                        supermarkets_dict.setdefault(j, {
                            "supermarketId": j,
                            "supermarketName": info.get("supermarketName", f"Supermarket {j}"),
                            "products": []
                        })

                        # Add product to supermarket's product list
                        supermarkets_dict[j]["products"].append({
                            "id": i,
                            "name": info.get("productName", f"Product {i}"),
                            "quantity": q[i],
                            "unitPrice": info.get("unitPrice", p.get((i, j), bigM)),
                            "discountDescription": info.get("discountDescription"),
                            "extractionDate": info.get("extractionDate").isoformat() if info.get("extractionDate") else None
                        })

            # Build scenario structure for this epsilon value
            scenario = {
                "scenarioId": eps,  # Number of supermarkets in this scenario
                "supermarkets": list(supermarkets_dict.values())  # List of supermarkets with assigned products
            }
            solutions.append(scenario)

    return {"costBySupermarket": costBySupermarket, "shoppingScenarios": solutions}



# Example usage and testing code
# input_json = {
#     "selectedSupermarkets": [49, 50, 51, 52, 53],
#     "products": [
#         {"id": 24661, "quantity": 2},
#         {"id": 28279, "quantity": 1},
#         {"id": 24746, "quantity": 3},
#         {"id": 27780, "quantity": 3},
#         {"id": 24555, "quantity": 3}
#     ]
# }

# bigM = 1e6

# # Run optimization with discounts enabled
# run_optimization(input_json, db_name="visualization_backend", discounts=True, bigM=bigM)

# ----- Visualization code (commented out) -----
# The following code can be used to visualize the Pareto frontier:
# 
# Step 3: Visualize Pareto frontier
# df = pd.DataFrame([
#     {"epsilon": s["epsilon"], "cost": s["cost"], "supermarkets": s["supermarkets"]}
#     for s in solutions
# ])
#
# print("\nPareto frontier:")
# print(df)
#
# plt.figure(figsize=(6, 4))
# plt.plot(df["supermarkets"], df["cost"], "o-")
# plt.xlabel("Number of supermarkets visited (f2)")
# plt.ylabel("Total cost (f1)")
# plt.title("Pareto Frontier - AUGMECON+ (4 products)")
# plt.grid(True)
# plt.show()
#
# Step 4: Show assignments for each solution
# for sol in solutions:
#     print(f"\nε = {sol['epsilon']} | cost = {sol['cost']:.2f} | supermarkets = {sol['supermarkets']:.0f}")
#     for (i, j) in sol["assignments"]:
#         print(f"  Product {i} → Supermarket {j}")

