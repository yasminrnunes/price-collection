from datetime import datetime
from bs4 import BeautifulSoup
from utils.html_parser import parse_html
from utils.http_request import make_request_with_delay, make_dinamic_request_with_delay
from database.file_storage import save_products_to_file
from utils.encoders import normalize_numeric_string

BASE_URL = "https://www.extramercado.com.br"
MARKET = "extra"
EXTRACTION_DATE = datetime.now().isoformat()

# TODO:
# - Ojo que no hay forma sencilla de obtener el peso de los productos https://www.extramercado.com.br/produto/2825/file-de-frango-congelado-3,2kg

## DATA STRUCTURE
# {
#  "name":                      -> CATEGORY_PAGE
#  "price":                     -> CATEGORY_PAGE
#  "product_url":               -> CATEGORY_PAGE
#  "extraction_url":            -> CATEGORY_PAGE
#  "market":                    -> CATEGORY_PAGE
#  "scraping_date":             -> CATEGORY_PAGE
#  "category":"Mercearia",      -> CATEGORY_PAGE
#  "discounts": [{              -> CATEGORY_PAGE
#    "type": "vuon_card",       -> CATEGORY_PAGE
#    "price": 5.50              -> CATEGORY_PAGE
#  }],
# }
#  "brand":"Club Social",       -> PRODUCT_PAGE
#  "unit_of_measurement":"un",  -> PRODUCT_PAGE
#  "quantity":1,                -> PRODUCT_PAGE

## DISCOUNT STRUCTURE
# leve 4 pague 3
# -> https://www.extramercado.com.br/produto/435571/biscoito-recheado-trakinas-chocolate-126g
# -40% na 2ª unidade
# -> https://www.extramercado.com.br/produto/701205/iogurte-parcialmente-desnatado-morango-danone-garrafa-1,25kg-embalagem-supereconomica


def _get_url_categories():
    print("Getting categories urls...")

    response = make_request_with_delay(BASE_URL, raise_error=True)
    soup = parse_html(response)

    hyperlinks = soup.select("a")

    categories = []
    for hyperlink in hyperlinks:

        link = hyperlink.get("href")

        if link:
            if "/categoria/" in link.lower():
                if link.startswith("/"):
                    link = BASE_URL + link
                categories.append(link)
            else:
                continue

    return categories


def _load_products_from_category_page(category_url: str):
    html_content = make_dinamic_request_with_delay(
        category_url, "div.MuiGrid-root.MuiGrid-item", min_count=5
    )

    if not html_content:
        return []

    soup = BeautifulSoup(html_content, "html.parser")

    html_products = soup.select("div[class*='Card']")

    products_on_category_page = []

    # Get products from the category page
    for html_card in html_products:
        html_title = html_card.select_one("a[class*='Title']")
        html_price = html_card.select_one("p[class*='PriceValue']")
        html_discount = html_card.select_one("p[class*='SealLabel']")

        if html_title and html_price:
            name = html_title.get_text(strip=True)
            price = html_price.get_text(strip=True)
            product_url = html_title.get("href")

            # Skip if any essential field is empty
            if not name or not price:
                continue

            # Extract discount from the discount element
            discount = html_discount.get_text(strip=True) if html_discount else None

            # Extract category name from URL (last part after /) removing query params
            if category_url:
                # Remove query parameters and extract last part
                clean_url = category_url.split("?")[0]  # Remove query params
                category_name = clean_url.split("/")[-1]  # Get last part after /
            else:
                category_name = "unknown"

            if product_url and not product_url.startswith("http"):
                product_url = BASE_URL + product_url

            product = {
                "name": name,
                "price": normalize_numeric_string(price),
                "category": category_name,
                "product_url": product_url,
                "extraction_url": category_url,
                "extraction_date": EXTRACTION_DATE,
                "market": MARKET,
                # "unit_of_measurement":"un",
                # "quantity":1,
                # "brand":"Club Social",
                # "source_id": "66677604431",
            }

            if discount:
                product["discounts"] = [{"type": discount}]

            # Add product to the list
            products_on_category_page.append(product)

    return products_on_category_page


def _load_all_products_from_category(category_url: str):
    all_products = []
    page_num = 1
    max_pages = 100

    while page_num <= max_pages:

        # Construct URL with page parameter
        if "?" in category_url:
            page_url = f"{category_url}&p={page_num}"
        else:
            page_url = f"{category_url}?p={page_num}"

        print(f"Processing page {page_num} - {page_url}")
        page_products = _load_products_from_category_page(page_url)

        # If no products found, stop scanning
        if not page_products:
            print(f"No products found on page {page_num}. Stopping.")
            break

        all_products.extend(page_products)
        print(f"Products found on page {page_num}: {len(page_products)}")

        page_num += 1

    print(f"Total pages processed: {page_num - 1}")
    return all_products


# Get categories urls
#category_urls = _get_url_categories()
category_urls = ["https://www.extramercado.com.br/categoria/petshop"]
print(f"Total categories found: {len(category_urls)}")

print("Processing categories... \n")

for category_index, c_url in enumerate(category_urls, 1):
    print(f"Processing category {category_index}/{len(category_urls)}: {c_url}")

    products = _load_all_products_from_category(c_url)
    # products = _load_products_from_category_page("https://www.extramercado.com.br/categoria/petshop?s=relevance&p=7")

    print(f"Total products found: {len(products)}")

    # Save products to file
    if products:
        save_products_to_file(products, MARKET, EXTRACTION_DATE)
