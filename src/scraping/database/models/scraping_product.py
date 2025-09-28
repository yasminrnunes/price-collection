from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List
from decimal import Decimal
from database.snowflake_id import generate_id
from database.models.product_discount import ProductDiscount


@dataclass
class ScrapingProduct:
    """Class to represent a scraping product"""

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
        """Post-initialization processing to normalize field values"""
        # Convert to uppercase for consistency
        if self.unit_of_measure:
            self.unit_of_measure = self.unit_of_measure.upper()
        if self.currency:
            self.currency = self.currency.upper()

    def to_tuple(self) -> tuple:
        """Convert the object to a tuple for inserting into the database"""
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
        """Convert the object to a dictionary for JSON serialization"""
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
            "discounts": [discount.to_dict() for discount in self.discounts],
            "currency": self.currency,
        }

    def add_discount(self, discount: ProductDiscount) -> None:
        """Add a discount to the product"""
        discount.product_id = self.id
        self.discounts.append(discount)

    def add_percentage_quantity_discount(
        self, discounted_price: int, min_quantity: int, conditions_text: str = None
    ) -> None:
        """Add a percentage quantity discount"""
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
        """Add a card discount"""
        discount = ProductDiscount.create_card_discount(
            product_id=self.id,
            discounted_price=discounted_price,
            conditions_text=conditions_text,
        )
        self.add_discount(discount)

    def add_wholesale_discount(
        self, discounted_price: int, min_quantity: int, conditions_text: str = None
    ) -> None:
        """Add a wholesale discount"""
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
        """Add a buy X get Y discount"""
        discount = ProductDiscount.create_buy_x_get_y_discount(
            product_id=self.id,
            discounted_price=discounted_price,
            buy_quantity=buy_quantity,
            get_quantity=get_quantity,
            conditions_text=conditions_text,
        )
        self.add_discount(discount)

    def get_discounts_for_db(self) -> List[tuple]:
        """Get all discounts as tuples for inserting into the database"""
        return [discount.to_tuple() for discount in self.discounts]

