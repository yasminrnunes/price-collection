# Price Collection

This repository is a proof‑of‑concept developed as part of a master's thesis research study. Its goal is to demonstrate an end‑to‑end approach for supermarket price collection, data normalization, and multi‑objective shopping optimization, providing a minimal yet realistic stack to evaluate feasibility, trade‑offs, and UX implications.

End‑to‑end system to collect supermarket prices, normalize and store data in PostgreSQL, and visualize the best shopping combinations through a small web app with multi‑objective optimization.

- Data collection (scraping/API) for multiple supermarkets
- Transformation pipeline (cleaning, brand/product matching, discounts normalization)
- Backend API (Flask) exposing products and cart optimization
- Frontend (Vue 3 via CDN + Tailwind) to build a shopping list and compare results

---

## Table of Contents
- Overview
- Architecture and Data Flow
- Quickstart
- Configuration (Environment Variables)
- Database Schema – required tables/columns
- Modules
- API Reference
- Frontend UX Notes
- Development Guide
- Troubleshooting
- Roadmap
- License

---

## Overview

This project collects product prices from several supermarkets, normalizes them into a relational model, and exposes the data through an API used by a lightweight frontend. Users select supermarkets, build a product list, and receive optimized “shopping scenarios” that balance two objectives:

- Minimize total cost
- Minimize number of supermarkets visited

Optimization is solved with PuLP using an AUGMECON (Augmented Epsilon‑Constraint) approach and can consider discounts if available in the database.

---

## Architecture and Data Flow

```
           +------------------+         +-------------------+
           |  scraping/       |  JSON   |  transforming/    |
           |  (market_*.py)   +--------->  main.py          |
           |  utils, parsers  |         |  ETL to Postgres  |
           +------------------+         +---------+---------+
                                                  |
                                           normalized tables
                                                  |
                                  +---------------v----------------+
                                  | visualization/backend (Flask)  |
                                  |  /products, /carts, /carts/:id |
                                  +---------------+----------------+
                                                  |
                                         HTTP (CORS enabled)
                                                  |
                    +-----------------------------v---------------------------+
                    | visualization/frontend (Vue + Tailwind, no build step) |
                    | index.html  → build cart, send to API                   |
                    | results.html → compare totals & scenarios               |
                    +---------------------------------------------------------+
```

---

## Quickstart

1) Install dependencies
```bash
pip install -r requirements.txt
```

2) Set database variables (see Configuration)

3) Run the backend (Flask)
```bash
python src/visualization/backend/main.py
# → http://localhost:5001
```

4) Open the frontend
- Open `src/visualization/frontend/index.html` directly in your browser (or via a simple static server).

5) Optional quick smoke test (without full frontend flow)
- Call `GET http://localhost:5001/health` → should return healthy.

---

## Configuration (Environment Variables)

The backend reads its PostgreSQL connection from environment variables (loaded with `python-dotenv` if present):

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=price_collection
DB_USER=postgres
DB_PASSWORD=postgres
```

Notes:
- Connections are pooled with simple re‑use and timeouts (`sql_client.py`).
- CORS is enabled for all origins for development. Restrict before production.

---

## Modules

### Directory Structure

High‑level layout of the repository with responsibilities and notable files:

```
private-price-collection/
├─ README.md                         # Project documentation (this file)
├─ requirements.txt                  # Python dependencies
├─ doc/                              # Documentation and helper materials
│  ├─ dbdiagram.txt                  # DB diagram source (text)
│  ├─ example_product.json           # Example of scraped product payload
│  └─ scraping_market_analysis.txt   # Notes on scraping approaches per market
└─ src/
   ├─ scraping/                      # Data collection: scrapers and helpers
   │  ├─ data/                       # Collected JSON samples (by timestamp/market)
   │  ├─ database/
   │  │  ├─ file_storage.py          # Simple file storage helper
   │  │  ├─ snowflake_id.py          # ID generator (Snowflake-like)
   │  │  └─ models/
   │  │     ├─ product_discount.py   # Scraping discount model
   │  │     └─ scraping_product.py   # Scraping product model
   │  ├─ utils/
   │  │  ├─ encoders.py              # JSON encoders/helpers
   │  │  ├─ http_request.py          # HTTP utilities (retries, headers, timeouts)
   │  │  ├─ logger.py                # Simple logging wrapper
   │  │  └─ parsers.py               # Parsers/normalizers for raw data
   │  ├─ market_extra_api.py         # Market: Extra (API based)
   │  ├─ market_fort_api.py          # Market: Fort (API based)
   │  ├─ market_marche.py            # Market: St Marche (scraping)
   │  ├─ market_paodeacucar_api.py   # Market: Pão de Açúcar (API based)
   │  └─ market_tenda_api.py         # Market: Tenda (API based)
   │
   ├─ transforming/                  # Normalization & ETL into the database
   │  ├─ analysis/
   │  │  ├─ base_config.cfg          # Analysis/matching configs
   │  │  ├─ mapping_brand.xlsx       # Brand mapping sheet
   │  │  ├─ matching_brand.py        # Brand matching logic
   │  │  ├─ matching_product.py      # Product matching logic
   │  │  ├─ stopwords.py             # Stopwords used in matching
   │  │  ├─ train_data.spacy         # Training data (binary)
   │  │  └─ train_data.txt           # Training text data
   │  ├─ logger.py                   # Logger for ETL
   │  ├─ main.py                     # ETL pipeline (staging → normalized tables)
   │  ├─ sql_client.py               # DB client used during transformation
   │  └─ utils.py                    # Transformation helpers (normalizers, checks)
   │
   └─ visualization/
      ├─ backend/                    # Flask API for products & optimization
      │  ├─ logger.py                # API logger
      │  ├─ main.py                  # Endpoints, in‑memory carts, CORS
      │  ├─ opt_augmecon.py          # AUGMECON optimization with PuLP
      │  └─ sql_client.py            # Thread‑safe PostgreSQL client for API
      └─ frontend/                   # Vue 3 (CDN) + Tailwind UI
         ├─ index.html               # Build shopping list & send cart to API
         └─ results.html             # Totals, Pareto scenarios, product tables
```

Key notes:
- Scraping is market‑specific: each `market_*.py` encapsulates collection for one supermarket (API or scraping). Utilities centralize HTTP, parsing, and logging.
- Transformation consumes staging tables/JSON, deduplicates & normalizes entities, and writes to normalized tables used by the backend.
- Visualization backend exposes a small REST API and orchestrates optimization via PuLP (AUGMECON). Carts are in‑memory by design for development.
- Frontend is static (no build step). Open the HTML files directly; they call the API at `http://127.0.0.1:5001`.

### scraping/
Scripts like `market_tenda_api.py`, `market_marche.py`, `market_fort_api.py`, etc. Collect data into JSON or staging tables. Utilities for HTTP, parsing and logging are under `scraping/utils/`. Collected samples live in `scraping/data/`.

### transforming/
`main.py` implements an ETL pipeline:
- Supermarket and brand normalization
  - Reads from staging (`stage_scraping_products`, `stage_discounts`) and builds in‑memory maps of existing `supermarkets`, `brands`, `products`, `prices`, and `raw_product_data` to avoid duplicates.
  - Supermarkets: exact matching on a normalized name; inserts new rows when needed and updates the local cache to keep the pipeline idempotent.
  - Brands:
    - If brand is missing in the staging row, it attempts to infer it from the product name via `check_brand_in_product`.
    - If brand is present, it uses `check_brand_exists` for fuzzy matching (threshold logic in utils) to reuse existing rows, otherwise inserts a new brand.
  - Normalization utilities:
    - `normalize_word` and `normalize_unit` are used to create stable, comparable forms of textual fields (casefolding, diacritics removal, unit unification).
- Product matching and deduplication (word‑based similarity)
  - Builds a dictionary `{normalized_name -> product_id}` and a parallel list of word counters for each known product to support similarity lookups.
  - For each staging product:
    - If `normalized_name` already exists, reuse its `product_id`.
    - Else, call `check_product_exists(normalized_name, existing_names, existing_name_counters)` which compares token distributions (word frequency) to find close matches.
      - If a sufficiently similar match is found, reuse that `product_id` (prevents duplicates caused by minor spelling/format differences).
      - Otherwise inserts a new `products` row with `(name, normalized_name, quantity, id_brand)`.
  - Product/source URL mapping is preserved in `raw_product_data (original_name, product_url, product_id, extraction_date, market)` to keep lineage and enable future reprocessing.
- Price loading with duplicate protection
  - Each price is uniquely identified by `(id_supermarket, id_product, extraction_date)` for idempotence.
  - Before insert, the pipeline checks a cached set of already‑inserted tuples; on a miss, inserts into `prices (id_supermarket, id_product, extraction_date, value, currency)` and updates the cache.
  - Currency default is set (e.g., `"BRL"`) if the staging record does not provide it.
- Discounts normalization and insertion
  - Joins staged discounts (`stage_discounts`) to the product currently being processed and maps raw fields to normalized columns:
    - `unit_value` (discounted unit price), `condition_type` (BUY_X_GET_Y, PERCENTAGE_QUANTITY, WHOLESALE, CARD, …), `min_qty`, `multiple_qty`, and a human‑readable `description`.
  - Applies type‑specific normalization (e.g., for WHOLESALE or CARD) to ensure a consistent downstream representation.
  - Inserts into `discounts (id_price, unit_value, condition_type, min_qty, multiple_qty, description)`.
  - These discounts are considered at query time by the optimizer; only feasible discounts (respecting `min_qty` and `multiple_qty`) are kept and the cheapest price per (product, supermarket) wins.
- Staging bookkeeping and logging
  - After a product and its associated prices/discounts are handled, the ETL marks `stage_scraping_products.is_processed = true` to prevent reprocessing.
  - Verbose logs are emitted (start/end, inserts, reuse decisions, counts) to trace pipeline progress and decisions.
  - The whole process is designed to be restartable and idempotent: cached lookups + uniqueness checks minimize risk of duplicates.

### visualization/backend (Flask)
Endpoints in `main.py`:
- `/health`
- `/products` → returns a product list present in ≥ 2 supermarkets (with normalized_name and supermarkets array) plus the supermarkets list
- `/carts` (POST) → stores carts in memory (UUID) and returns cart id
- `/carts/<cart_id>` (GET) → runs optimization and returns cost per full‑supermarket as well as shopping scenarios
- `/carts` (DELETE) → clears in‑memory carts

Optimization (`opt_augmecon.py`):
- Builds a two‑objective model (min cost, min supermarkets) with PuLP
- Uses AUGMECON to sweep epsilon (number of supermarkets)
- Supports discounts: keeps the cheapest (price or discount) per (product, supermarket) while enforcing discount constraints (min qty / multiples)

### visualization/frontend
- Vanilla HTML with Vue 3 via CDN and Tailwind CSS
- `index.html`:
  - Select supermarkets (chips)
  - Search products by words (order‑independent, accent/symbol normalization)
  - Suggestions dropdown shows top 10 and a footer with total matches
  - Button to generate a random cart using only products available in all selected supermarkets
  - Sends cart to API and navigates to `results.html`
- `results.html`:
  - Shows total per single supermarket (cards; responsive)
  - Shows Pareto scenarios (chips with cost and +/- percent vs selected scenario)
  - Shows product lists per scenario/supermarket (responsive tables; mobile tuned for 425px)

---

## API Reference

Base URL: `http://localhost:5001`

### GET /health
Response:
```json
{"status": "healthy", "message": "API is running", "version": "1.0"}
```

### GET /products
Response:
```json
{
  "products": [
    {
      "id": 123,
      "name": "Leche Entera",
      "normalized_name": "leche entera",
      "supermarkets": [49, 50]
    }
  ],
  "supermarkets": [
    {"id": 49, "name": "Supermercado A"}
  ]
}
```
Only products present in 2 or more supermarkets are returned.

### POST /carts
Body:
```json
{
  "selectedSupermarkets": [49, 50],
  "products": [
    {"id": 24661, "quantity": 2},
    {"id": 28279, "quantity": 1}
  ]
}
```
Response (201):
```json
{
  "id": "uuid",
  "receivedData": {
    "selectedSupermarkets": [49, 50],
    "productsCount": 2,
    "totalQuantity": 3
  }
}
```

### GET /carts/{cart_id}
Runs optimization and returns:
```json
{
  "costBySupermarket": [
    {"supermarketId": 49, "supermarketName": "A", "totalCost": 12345}
  ],
  "shoppingScenarios": [
    {
      "scenarioId": 2,
      "supermarkets": [
        {
          "supermarketId": 49,
          "supermarketName": "A",
          "products": [
            {
              "id": 24661,
              "name": "Leche Entera",
              "quantity": 2,
              "unitPrice": 599,
              "discountDescription": "Card 10%",
              "extractionDate": "2025-10-26T10:39:10.702245"
            }
          ]
        }
      ]
    }
  ]
}
```

### DELETE /carts
Clears all in‑memory carts:
```json
{"message": "All carts deleted successfully", "deletedCount": 3}
```

---

## Frontend UX Notes

- Search:
  - Lower‑cases, removes accents/symbols, splits by spaces, and requires that all search words appear in the product normalized name (order‑independent).
  - Dropdown appears only when there is text; shows up to 10 suggestions + a footer with total matches.
- Random cart generation:
  - Picks up to 10 products present in all selected supermarkets; quantities randomized 1..5.
  - Scrolls to the bottom after generation for visibility.
- Results:
  - “Total per supermarket” cards are responsive; on mobile (≤ 425px) two cards per row with compact spacing.
  - Scenario chips display cost and a green (cheaper, “-x%”) or red (more expensive, “+x%”) badge relative to the currently selected scenario.
  - Product tables are tuned for 425px: small fonts, reduced padding, nowrap on numeric columns, horizontal scroll when needed.

---

## Development Guide

Install dependencies:
```bash
pip install -r requirements.txt
```

Run backend (Flask):
```bash
python src/visualization/backend/main.py
```

Open frontend:
- `src/visualization/frontend/index.html`

Scraping and transformation:
- Run/extend `src/scraping/market_*.py` scripts per supermarket.
- Process staging to normalized tables with `src/transforming/main.py` (adjust queries to your ingestion).

Code style notes:
- Backend uses simple Flask + psycopg2. SQL is plain strings in the code; consider moving to migrations/DDL files for production.
- Optimization uses PuLP CBC solver (default). For larger problems, consider an alternative solver or pre‑filtering.

---

## Troubleshooting

- CORS errors in the browser
  - Backend adds permissive CORS headers for dev; ensure you’re hitting `http://127.0.0.1:5001` as in the frontend.
- Empty /products response
  - Ensure `products`, `prices`, and `supermarkets` are populated and that products appear in ≥ 2 supermarkets.
- Optimization returns empty scenarios
  - Ensure every selected product has at least one available price in the selected supermarkets.
  - If using discounts, verify discount constraints (min quantity / multiples) are satisfied by the requested quantities.
- Database connection issues
  - Verify `DB_*` environment variables and that PostgreSQL is reachable.

