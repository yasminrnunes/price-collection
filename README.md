# Price Collection

Automated supermarket price collection system with database storage and analysis capabilities.

## 📋 Overview

This project collects product price data from different supermarkets, stores the information in a PostgreSQL database, and provides tools for analysis and transformation of collected data.

### Features

- **Automated collection** of prices from multiple supermarkets
- **Structured storage** in PostgreSQL database
- **Discount system** with different types (card, quantity, wholesale, etc.)
- **Data analysis and transformation** tools
- **Detailed logging** for monitoring
- **JSON export** of collected data

## 🏗️ Architecture

```
src/
├── scraping/           # Data collection module
│   ├── database/       # Database client and models
│   ├── utils/          # Utilities (HTTP, parsing, logging)
│   ├── data/           # Collected data in JSON format
│   └── market_*.py     # Supermarket-specific scripts
├── transforming/       # Data transformation and analysis module
└── visualization/      # Visualization module (in development)
```

## 🛒 Supported Supermarkets

- **St Marche** - Web scraping collection
- **Tenda** - API collection

## 🚀 Installation

### Prerequisites

- Python 3.8+
- PostgreSQL
- Playwright (for some scrapers)

### Steps

1. **Clone the repository:**
```bash
git clone <repository-url>
cd private-price-collection
```

2. **Install dependencies:**
```bash
pip install -r requirements.txt
```

3. **Configure the database:**
```bash
# Create a .env file with database configuration
echo "DATABASE_URL=postgresql://user:password@localhost:5432/price_collection" > .env
```

4. **Install Playwright browsers:**
```bash
playwright install
```

## 📦 Main Dependencies

- **requests** (2.31.0) - HTTP requests
- **beautifulsoup4** (4.12.2) - HTML parsing
- **playwright** (1.45.0) - Browser automation
- **psycopg2-binary** - PostgreSQL client
- **fuzzywuzzy** (0.18.0) - String comparison
- **python-dotenv** (1.0.0) - Environment variable management

## 🎯 Usage

### Data Collection

Run collection scripts for each supermarket:

```bash
# St Marche
python src/scraping/market_marche.py

# Tenda
python src/scraping/market_tenda_api.py
```

### Analysis and Transformation

```bash
# Run transformations
python src/transforming/main.py

# Query examples
python src/transforming/query_examples.py
```

## 📊 Data Structure

### Product (ScrapingProduct)

```json
{
  "name": "Biscoito CLUB SOCIAL Original Pacote 144g",
  "price": 739,  // Price in cents
  "category": "Mercearia",
  "brand": "Club Social",
  "market": "Stmarche",
  "quantity": 1,
  "unit_of_measure": "UN",
  "product_url": "https://marche.com.br/...",
  "extraction_date": "2025-08-06T16:22:09.645284",
  "discounts": [
    {
      "type": "vuon_card",
      "price": 550,
      "conditions_text": "Vuon Card"
    }
  ]
}
```

### Discount Types

- **Card** - Discount for specific card payment
- **Quantity** - Discount for bulk purchases
- **Wholesale** - Wholesale purchase discounts
- **Buy X Get Y** - Promotions like "buy 3 get 2"

## 🔧 Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
DATABASE_URL=postgresql://user:password@localhost:5432/price_collection
LOG_LEVEL=INFO
```

### Database Configuration

The system uses PostgreSQL with the following main tables:
- `raw_product_data` - Raw product data
- `supermarkets` - Supermarket information
- `brands` - Product brands
- `product_discounts` - Product discounts

## 📈 Monitoring

The system includes detailed logging for:
- HTTP request status
- Successfully collected products
- Collection errors
- Operation performance

## 🤝 Contributing

1. Fork the project
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Commit your changes (`git commit -m 'Add new feature'`)
4. Push to the branch (`git push origin feature/new-feature`)
5. Open a Pull Request

## 📝 License

This project is private and intended for internal use.

## 🐛 Known Issues

- Some supermarkets may have rate limiting restrictions
- Products with quantity restrictions are identified in the name
- Brands may not be available for all supermarkets
