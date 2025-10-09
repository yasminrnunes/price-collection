import time
from datetime import datetime
from utils.encoders import encode_text
from utils.http_request import make_request_with_delay
from utils.html_parser import parse_html
from database.file_storage import save_products_to_file
import re

## No se encuentran los enlaces de las categorias en la página inicial - hay que probar con acceso dinamico.
## Las páginas de las categorias no se carga toda, no hay botón de próxima página.
## Se puede acceder con API
## El precio del catalago necesita ser rehacer para no utilizar el json, ya que el volumen no sale bien.

def extrair_aproximadamente(descricao):
    padrao = r"Aproximadamente\s+([^\n]*\d[^\n]*)"
    resultado = re.search(padrao, descricao, re.IGNORECASE)
    if resultado:
        return resultado.group(1).strip()
    return None

BASE_URL = 'https://www.mambo.com.br'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
}

# eyJoaWRlVW5hdmFpbGFibGVJdGVtcyI6ZmFsc2UsInNrdXNGaWx0ZXIiOiJBTEwiLCJzaW11bGF0aW9uQmVoYXZpb3IiOiJkZWZhdWx0IiwiaW5zdGFsbG1lbnRDcml0ZXJpYSI6Ik1BWF9XSVRIT1VUX0lOVEVSRVNUIiwicHJvZHVjdE9yaWdpblZ0ZXgiOmZhbHNlLCJtYXAiOiJjIiwicXVlcnkiOiJwZXRzaG9wIiwib3JkZXJCeSI6Ik9yZGVyQnlUb3BTYWxlREVTQyIsImZyb20iOjAsInRvIjoxOSwic2VsZWN0ZWRGYWNldHMiOlt7ImtleSI6ImMiLCJ2YWx1ZSI6InBldHNob3AifV0sImZhY2V0c0JlaGF2aW9yIjoiU3RhdGljIiwiY2F0ZWdvcnlUcmVlQmVoYXZpb3IiOiJkZWZhdWx0Iiwid2l0aEZhY2V0cyI6ZmFsc2UsImFkdmVydGlzZW1lbnRPcHRpb25zIjp7InNob3dTcG9uc29yZWQiOnRydWUsInNwb25zb3JlZENvdW50IjozLCJhZHZlcnRpc2VtZW50UGxhY2VtZW50IjoidG9wX3NlYXJjaCIsInJlcGVhdFNwb25zb3JlZFByb2R1Y3RzIjp0cnVlfX0

data_market = []
category_urls = ['/petshop']

## Getting the products information
for category_url in category_urls:
    page = 1
    category_name = category_url.split("/")[-1]  # Getting the category name
    product_urls = []  # Getting distinct product urls
    product_prices = []
    product_measurements = []
    product_categories = []
    product_names = []
    product_extraction_urls = []

   # while True:
    PAGE_URL = f"&page={page}"
    category_url_with_page = BASE_URL + category_url
    response = make_request_with_delay(
        category_url_with_page, headers=headers
    )

    # Accediendo a los links de los productos
    soup = parse_html(response)

    product_list = soup.find_all("div", class_=lambda x: x and x.startswith("vtex-search-result-3-x-galleryItem")
)

    if not product_list:
        print(f"End of pagination on page {page}")
        break

    products_added_this_page = 0  # Counting distinct products added in this page

    for item in product_list:
        for link in item.find_all("a", href=True):
            product_url = link["href"]

            product_urls.append(product_url)







response = requests.get(url, headers=headers)

if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')

    scripts = soup.find_all('script', type='application/ld+json')

    for script in scripts:
        try:
            if not script.string:
                continue  # ignora scripts sem conteúdo

            cleaned = script.string.replace('\n', ' ').replace('https: //', 'https://').replace('http: //', 'http://')

            data = json.loads(cleaned)

            # Creando listas para guardar los datos
            names = []
            descriptions = []
            brands = []
            images_link = []
            precos_min = []
            precos_max = []
            disponibilidades = []
            vendedores = []
            
            # Apenas se for um ItemList
            if isinstance(data, dict) and data.get('@type') == 'ItemList':

                for elemento in data.get("itemListElement", []):
                    produto = elemento.get("item", {})

                    name = produto.get("name", "").replace('\n', ' ').strip()
                    description = extrair_aproximadamente(produto.get("description", "").replace('\n', ' ').strip())
                    brand = produto.get("brand", {}).get("name", "").replace('\n', ' ').strip()
                    image_link = produto.get("image", "").strip()

                    offers = produto.get("offers", {})
                    preco_min = offers.get("lowPrice")
                    preco_max = offers.get("highPrice")

                    # Verifica se há uma lista de ofertas
                    oferta = offers.get("offers", [])
                    if oferta and isinstance(oferta, list):
                        primeira_oferta = oferta[0]
                        disponibilidade = primeira_oferta.get("availability", "").strip()
                        vendedor = primeira_oferta.get("seller", {}).get("name", "").strip()
                    else:
                        disponibilidade = ""
                        vendedor = ""

                    # Adiciona às listas
                    names.append(name)
                    brands.append(brand)
                    images_link.append(image_link)
                    precos_min.append(preco_min)
                    precos_max.append(preco_max)
                    disponibilidades.append(disponibilidade)
                    vendedores.append(vendedor)

        except Exception as e:
            print("Erro ao processar script JSON-LD:", e)
else:
    print(f"Erro ao acessar a página: {response.status_code}")