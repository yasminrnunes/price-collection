"""
Analyze the stopwords in the stage_scraping_products table
"""

import re
import unicodedata
from collections import Counter
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sql_client import DatabaseClient

client = DatabaseClient("query_client")

# Checking the product names in the stage_scraping_products table
products_query = """
    SELECT name 
    FROM stage_scraping_products
    GROUP BY name;
    """
products_raw = client.execute_query(products_query)
products = [row["name"] for row in products_raw]


def normalize_word(word):
    # Convert to string if not already
    if not isinstance(word, str):
        word = str(word)
    # lowercase
    word = word.lower()
    # remove accents
    word = ''.join(c for c in unicodedata.normalize('NFD', word) if unicodedata.category(c) != 'Mn')
    # Remove punctuation at the END of the string (.,;:!?)
    word = re.sub(r'[.,;:!?]+$', '', word)
    # remove special caracters
    word = re.sub(r'[^a-z0-9., ]', '', word)
    # remover duplicated space
    word = re.sub(r'\s+', ' ', word).strip()
    # # capitalize first letter
    # word = word.capitalize()
    return word

# Apply the normalized function and collect all words
all_words = []
for product in products:
    normalized_product = normalize_word(product)
    words = normalized_product.split()
    all_words.extend(words)

# Count frequency of each word across all products
word_frequency = Counter(all_words)

# Sort words by frequency (most frequent first)
sorted_words = word_frequency.most_common()

# Print word frequencies - top 50
print("Word frequency analysis - top 50:")
print("=" * 50)
for word, frequency in sorted_words[:50]:
    print(f"{word}: {frequency}")

print("\n" + "=" * 50)
print("PRODUCT SEARCH BY WORD")
print("=" * 50)

# Function to search for products by word
def search_products_by_word(search_word):
    """
    Search for products that contain a specific word
    """
    # Normalize the search word
    normalized_search = normalize_word(search_word)
    
    matching_products = []
    for product in products:
        normalized_product = normalize_word(product)
        if normalized_search in normalized_product.split():
            matching_products.append(product)
    
    return matching_products

search_word = "combo"  # Change this to any word you want to search for
matching_products = search_products_by_word(search_word)

print(f"\nProducts containing the word '{search_word}':")
print(f"Found {len(matching_products)} products")

for i, product in enumerate(matching_products[:20], 1):  # Show first 20 results
    print(f"{i}. {product}")
