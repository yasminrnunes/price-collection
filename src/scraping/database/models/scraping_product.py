"""Scraping product model for the price collection system.

This module provides the ScrapingProduct dataclass, which represents a product
collected from a web scraping operation. It includes product information such as
name, price, category, brand, and supports multiple discount types through the
ProductDiscount model.

The ScrapingProduct class provides:
    - Product information storage (name, price, market, category, brand, etc.)
    - Discount management with helper methods for each discount type
    - Serialization methods for database insertion and JSON export
    - Automatic ID generation using Snowflake IDs
    - Field normalization (currency and unit of measure to uppercase)

Example:
    Create a product with discounts:
        product = ScrapingProduct(
            name="Product Name",
            market="extra",
            price=1990,  # R$ 19.90 in cents
            extraction_date=datetime.now(),
            category="alimentos",
            brand="Brand Name"
        )

        product.add_card_discount(
            discounted_price=1791,  # 10% discount
            conditions_text="10% card discount"
        )

        product.add_percentage_quantity_discount(
            discounted_price=1194,  # 40% discount
            min_quantity=2,
            conditions_text="-40% on the 2nd unit"
        )

Note:
    - All prices are stored as integers representing cents
    - Product ID is automatically generated using Snowflake algorithm
    - Currency and unit_of_measure are automatically converted to uppercase
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List
from decimal import Decimal
from database.snowflake_id import generate_id
from database.models.product_discount import ProductDiscount


@dataclass
class ScrapingProduct:
    """Represents a product collected from a web scraping operation.

    This class models a product with all its relevant information including
    pricing, categorization, and associated discounts. It supports multiple
    discount types and provides convenient methods for adding and managing
    product promotions.

    Attributes:
        name: The product name as displayed on the marketplace.
        market: The marketplace identifier where the product was scraped
            (e.g., "extra", "tenda", "fort").
        price: The product price in cents (integer). Example: 1990 = R$ 19.90.
        extraction_date: The datetime when the product was scraped.

        category: Optional product category (e.g., "alimentos", "bebidas").
        brand: Optional product brand name.
        product_url: Optional URL to the product page on the marketplace.
        source_id: Optional source-specific product identifier (SKU, etc.).
        quantity: Optional product quantity (e.g., 500 for 500ml).
        unit_of_measure: Optional unit of measure (e.g., "ML", "KG", "UNIT").
            Automatically converted to uppercase.
        extraction_url: Optional URL from which the product data was extracted.
        currency: Optional currency code (default: "BRL"). Automatically
            converted to uppercase.

        id: Auto-generated unique product identifier using Snowflake algorithm.
        discounts: List of ProductDiscount objects associated with this product.
        created_at: Optional timestamp of when the product was inserted into
            the database (filled automatically on insertion).
    """

    # Required fields
    name: str
    market: str
    price: int
    extraction_date: datetime

    # Optional fields
    category: Optional[str] = None
    brand: Optional[str] = None
    product_url: Optional[str] = None
    source_id: Optional[str] = None
    quantity: Optional[Decimal] = None
    unit_of_measure: Optional[str] = None
    extraction_url: Optional[str] = None
    currency: Optional[str] = "BRL"

    # Auto-generated fields
    id: int = field(default_factory=generate_id)

    # Discounts
    discounts: List[ProductDiscount] = field(default_factory=list)

    # Database fields (filled automatically)
    created_at: Optional[datetime] = None

    def __post_init__(self):
        """Post-initialization processing to normalize field values.

        Automatically converts currency and unit_of_measure to uppercase for
        data consistency across all products.
        """
        # Convert to uppercase for consistency
        if self.unit_of_measure:
            self.unit_of_measure = self.unit_of_measure.upper()
        if self.currency:
            self.currency = self.currency.upper()

    def to_tuple(self) -> tuple:
        """Convert the product object to a tuple for database insertion.

        Returns:
            A tuple containing all product fields in the order required for
            database insertion. The tuple includes all fields except discounts
            (which are handled separately) and created_at (auto-generated).

        Note:
            Discounts are not included in this tuple. Use get_discounts_for_db()
            to obtain discount tuples separately.
        """
        return (
            self.id,
            self.name,
            self.market,
            self.category,
            self.brand,
            self.product_url,
            self.source_id,
            self.price,
            self.quantity,
            self.unit_of_measure,
            self.extraction_url,
            self.extraction_date,
            self.currency,
        )

    def to_dict(self) -> dict:
        """Convert the product object to a dictionary for JSON serialization.

        Returns:
            A dictionary containing all product fields with datetime objects
            converted to ISO format strings and Decimal objects converted to
            floats. Includes all discounts serialized as dictionaries.
            Suitable for JSON serialization and API responses.

        Note:
            The extraction_date and created_at are serialized as ISO format
            strings. Quantity is converted from Decimal to float for JSON
            compatibility. All discounts are included as a list of dictionaries.
        """
        return {
            "name": self.name,
            "market": self.market,
            "price": self.price,
            "extraction_date": (
                self.extraction_date.isoformat() if self.extraction_date else None
            ),
            "category": self.category,
            "brand": self.brand,
            "product_url": self.product_url,
            "source_id": self.source_id,
            "quantity": float(self.quantity) if self.quantity else None,
            "unit_of_measure": self.unit_of_measure,
            "extraction_url": self.extraction_url,
            "id": self.id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "currency": self.currency,
            "discounts": [discount.to_dict() for discount in self.discounts],
        }

    def add_discount(self, discount: ProductDiscount) -> None:
        """Add a discount to the product.

        This method sets the discount's product_id to this product's ID and
        appends it to the discounts list. Used internally by the convenience
        methods for adding specific discount types.

        Args:
            discount: The ProductDiscount instance to add to this product.
        """
        discount.product_id = self.id
        self.discounts.append(discount)

    def add_percentage_quantity_discount(
        self, discounted_price: int, min_quantity: int, conditions_text: str = None
    ) -> None:
        """Add a percentage quantity discount to this product.

        Creates and adds a discount that applies a percentage discount to
        specific units when purchasing a minimum quantity (e.g., -40% on
        the 2nd unit when buying 2 or more).

        Args:
            discounted_price: The discounted price per unit in cents (integer).
            min_quantity: The minimum quantity required for the discount to apply.
            conditions_text: Optional description of the discount conditions
                (e.g., "-40% on the 2nd unit").
        """
        discount = ProductDiscount.create_percentage_quantity_discount(
            product_id=self.id,
            discounted_price=discounted_price,
            min_quantity=min_quantity,
            conditions_text=conditions_text,
        )
        self.add_discount(discount)

    def add_card_discount(
        self, discounted_price: int, conditions_text: str = None
    ) -> None:
        """Add a card-based discount to this product.

        Creates and adds a discount that requires payment with a specific
        credit or debit card (typically a store-branded card) to receive
        the discounted price.

        Args:
            discounted_price: The discounted price per unit in cents (integer).
            conditions_text: Optional description of the card discount
                (e.g., "10% card discount" or "Cartão Extra").
        """
        discount = ProductDiscount.create_card_discount(
            product_id=self.id,
            discounted_price=discounted_price,
            conditions_text=conditions_text,
        )
        self.add_discount(discount)

    def add_wholesale_discount(
        self, discounted_price: int, min_quantity: int, conditions_text: str = None
    ) -> None:
        """Add a wholesale (bulk pricing) discount to this product.

        Creates and adds a discount that offers a lower price when purchasing
        above a minimum quantity threshold. All units purchased qualify for
        the discounted price when the minimum quantity is met.

        Args:
            discounted_price: The discounted price per unit in cents (integer).
            min_quantity: The minimum quantity required to qualify for wholesale pricing.
            conditions_text: Optional description of the wholesale conditions
                (e.g., "Starting from 5 units").
        """
        discount = ProductDiscount.create_wholesale_discount(
            product_id=self.id,
            discounted_price=discounted_price,
            min_quantity=min_quantity,
            conditions_text=conditions_text,
        )
        self.add_discount(discount)

    def add_buy_x_get_y_discount(
        self,
        discounted_price: int,
        buy_quantity: int,
        get_quantity: int,
        conditions_text: str = None,
    ) -> None:
        """Add a buy X get Y discount to this product.

        Creates and adds a discount representing promotions where purchasing
        X items allows you to receive Y items (e.g., buy 2 get 1 free, buy 3 get 2).
        The discounted_price represents the effective price per unit after
        applying the promotion.

        Args:
            discounted_price: The effective discounted price per unit in cents
                (integer), calculated as: (original_price * buy_quantity) / get_quantity.
            buy_quantity: The number of items that must be purchased.
            get_quantity: The total number of items received (purchased + free).
            conditions_text: Optional description of the promotion
                (e.g., "Buy 2 Get 1 Free" or "Buy 3 Get 2").
        """
        discount = ProductDiscount.create_buy_x_get_y_discount(
            product_id=self.id,
            discounted_price=discounted_price,
            buy_quantity=buy_quantity,
            get_quantity=get_quantity,
            conditions_text=conditions_text,
        )
        self.add_discount(discount)

    def get_discounts_for_db(self) -> List[tuple]:
        """Get all discounts as tuples for database insertion.

        Returns:
            A list of tuples, where each tuple contains the discount fields
            in the order required for database insertion. Each discount's
            product_id is automatically set to this product's ID.

        Note:
            This method is typically used when inserting discounts into a
            separate discounts table in the database. Use to_tuple() for
            the product data itself.
        """
        return [discount.to_tuple() for discount in self.discounts]
