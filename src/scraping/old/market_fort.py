import time
from datetime import datetime
from utils.encoders import normalize_numeric_string
from utils.http_request import make_request_with_delay
from utils.html_parser import parse_html
from database.file_storage import save_products_to_file

start_time = time.time()

# Fort comments:
## HAY QUE UTILIZAR LA API O SCRAPING DINAMICA
## Las páginas de categorias enseñan los mismos productos que la primera página de esta categoría.
## Las paginas de la categioria no enseña la misma cantidad de productos que la página accedida por la web.


## DATA STRUCTURE
# {
#  "name": "Biscoito CLUB SOCIAL Original Pacote 144g",
#  "price": 7.39,
#  "category":"Mercearia",
#  "unit_of_measurement":"un",
#  "quantity":1,
#  "discounts": [{
#    "type": "vuon_card",
#    "price": 5.50
#  }],
#  "brand":"Club Social",
#  "url": "https://marche.com.br/collections/mercearia/products/biscoito-club-social-original-pacote-144g?store_id=66677604431",
#  "market":"Stmarche",
#  "extraction_date":"2025-08-03"
# }

## Variables
BASE_URL = 'https://www.deliveryfort.com.br'

EXECUTION_TIME = datetime.now().isoformat()

page = 2
category_urls = []


headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
        }

## Getting the categories urls
# Cogiendo la url de las categorias - No funciona, las urls de las categorias de la izquierda parecen no cargarse de primera
response = make_request_with_delay(BASE_URL)#, headers=headers)

soup = parse_html(response)

categories_list = soup.find_all("li", class_="submenu__item has-submenu")

for category_list in categories_list.find_all("a", href=True):
    category_urls.append(category_list["href"])




## Getting the products information in category pages
data_market = []
category_urls = ['/infantil/nutricao-infantil']#,'/pet-shop']

for category_url in category_urls:
    page = 2
    category_name = category_url.split("/")[-1]  # Getting the category name
    product_urls = []  # Getting distinct product urls
    product_prices = []
    product_vuon_card_prices = []
    product_measurements = []
    product_categories = []
    product_names = []
    product_extraction_urls = []

#    while True:
    PAGE_URL = f"?page={page}"
    category_url_with_page = "https://www.deliveryfort.com.br/bebidas?page=2"   #BASE_URL + category_url + PAGE_URL
    print(category_url_with_page)
    response = make_request_with_delay(
        category_url_with_page, headers=headers
    )

    # Accediendo a los links de los productos
    soup = parse_html(response)

    
    product_list = soup.find_all("div", class_="shelf-item__info")

    if not product_list:
        print(f"End of pagination on page {page}")
        break

    products_added_this_page = 0  # Counting distinct products added in this page

    for item in product_list:
        for link in item.find_all("a",class_="shelf-item__title-link", href=True):
            product_url = link["href"]
            product_name = link.get_text(strip=True)
            


            product_urls.append(product_url)
            product_names.append(product_name)
            
            #product_extraction_urls.append(category_url_with_page)
            print(f"Product {product_name} added to the list")
            print(f"Total products added: {len(product_urls)}")
            #print(f"Total products added: {len(product_urls)}")

        price_regular = item.find("span", class_="shelf-item__best-price")#.get_text(strip=True)

        if price_regular:
            regular_price_text = normalize_numeric_string(price_regular.get_text(strip=True))

        vuon_container = item.find("div", class_="shelf-item__vuon-price--field")

        vuon_card_text = vuon_container.find('li') if vuon_container else None
        
        vuon_discount = normalize_numeric_string(vuon_card_text.get_text(strip=True)) if vuon_card_text else None

        product_prices.append(regular_price_text)
        product_vuon_card_prices.append(vuon_discount)






            








# Cogiendo la url de los productos de una categoria
#https://www.deliveryfort.com.br/bebidas?page={x}
product_links = []
page = 7

while True:

    url = 'https://www.deliveryfort.com.br/infantil?page={page}'

    
    response = requests.get(url, headers=headers)

    #time.sleep(tempo)

    if response.status_code != 200:
        print(f'Erro ao acessar a página: {page}')
        break

# Accediendo a los links de los productos
    soup = BeautifulSoup(response.text, "html.parser")

    product_list = soup.find_all("div", class_ = "shelf-item__info")

    if not product_list:
        print(f'Fim da paginação na página {page}')
        break

    for item in product_list:
        for link in item.find_all('a', class_ = "shelf-item__title-link", href = True):
            product_links.append(link['href'])
            print(len(product_links))
        
      # Añadir la info si el producto está disponible o no para no visitar páginas de productos no disponibles 
    
    print(f"Página {page} processada com {len(product_list)} produtos.")
    page += 1
        
print(len(product_links))

# Accediendo a cada pagina de productos ,product_links[0]
#test_url = 'https://www.deliveryfort.com.br/pao-de-queijo-sertaozinho-tradicional-congelado-800g/p'
for link in product_links:
    response = requests.get(link, headers=headers)

    time.sleep(tempo)

    soup = BeautifulSoup(response.content, "html.parser")

    name = soup.find('h1').text.strip()
    brand =soup.find('div', class_ = "product-brand").find('a').text.strip()
    price = soup.find('strong', class_='skuPrice').text.strip() # Hay que quitar el R$

    # El volumen del producto no está en el html, hay que buscar en el nombre del producto

    product_info = {
        'name': name,
        'brand': brand,
        'price': price
    }








url = 'https://www.deliveryfort.com.br/infantil?page=100'

    
response = requests.get(url, headers=headers)

    #time.sleep(tempo)

if response.status_code != 200:
    print(f'Erro ao acessar a página: {page}')


# Accediendo a los links de los productos
soup = BeautifulSoup(response.text, "html.parser")

product_list = soup.find_all("div", class_ = "shelf-item__info")

if not product_list:
    print(f'Fim da paginação na página {page}')

product_list