"""
Generate training data for the brand detection model.
"""

import json
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import normalize_word

FILE_PATH = "../../scraping/data/fort_products_2025-09-29T06-10-43.json"


def read_file():
    """
    Reads the JSON file and extracts only brand and name from products.

    Returns:
        list: List of dictionaries with brand and name
    """
    try:
        with open(FILE_PATH, "r", encoding="utf-8") as file:
            products = json.load(file)

        # Extract only brand and name from each product
        product_info = []
        for product in products:
            brand = product.get("brand", "N/A")
            name = product.get("name", "N/A")
            product_info.append({"brand": brand, "name": name})

        return product_info

    except FileNotFoundError:
        print(f"File {FILE_PATH} not found!")
        return []
    except json.JSONDecodeError:
        print("Error decoding the JSON file!")
        return []
    except (IOError, OSError) as e:
        print(f"I/O error: {e}")
        return []


def iterate_products(products):
    """
    Iterates through all products and processes each one.

    Args:
        products (list): List of products with brand and name

    Returns:
        tuple: (train_data, matches_count)
    """
    train_data = []
    matches = 0

    print(f"Total products found: {len(products)}")
    print("-" * 80)

    for i, product in enumerate(products, 1):
        result = process_single_product(product, i)
        if result:
            train_data.append(result[0])
            matches += 1

    return train_data, matches


def process_single_product(product, index):
    """
    Processes a single product and returns training data if brand is in name.

    Args:
        product (dict): Dictionary with brand and name
        index (int): Product index for display

    Returns:
        tuple: (training_entry, highlighted_name) or None if no match
    """
    brand = normalize_word(product["brand"].strip())
    name = normalize_word(product["name"].strip())

    if brand.upper() in name.upper():
        start_position = name.upper().find(brand.upper())
        end_position = start_position + len(brand)

        # Create highlighted name
        highlighted_name = f"{name[:start_position]}\033[92m{name[start_position:end_position]}\033[0m{name[end_position:]}"
        print(f"{index:4d}. Brand: {brand:<20} | Name: {highlighted_name}")

        # Create training entry
        training_entry = (
            f"('{name}',{{'entities':[({start_position},{end_position},'ORG')]}})"
        )

        return training_entry, highlighted_name

    return None


def write_training_data(train_data, filename="train_data.txt"):
    """
    Writes training data to a text file.

    Args:
        train_data (list): List of training entries
        filename (str): Output file name
    """
    with open(filename, "w", encoding="utf-8") as file:
        file.write("[")
        for i, line in enumerate(train_data):
            if i == len(train_data) - 1:  # Last line
                file.write(line)
            else:
                file.write(line + ",")
        file.write("]")


def main():
    """
    Main function that orchestrates the entire process.
    """
    # 1. Read file
    products = read_file()

    if not products:
        print("No products found!")
        return

    # 2. Iterate through products
    train_data, matches = iterate_products(products)

    # 3. Write training data
    write_training_data(train_data)

    print(f"Total products with brand in name: {matches}")
    print("Training data saved to train_data.txt")


if __name__ == "__main__":
    main()
