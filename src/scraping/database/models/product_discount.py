from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from decimal import Decimal
from enum import Enum


class DiscountType(Enum):
    """Types of discounts available"""

    PERCENTAGE_QUANTITY = "PERCENTAGE_QUANTITY"  # -40% na 2ª unidade
    CARD = "CARD"  # Descuento con tarjeta
    WHOLESALE = "WHOLESALE"  # Apartir de X unidades
    BUY_X_GET_Y = "BUY_X_GET_Y"  # 2x1, 3x2


@dataclass
class ProductDiscount:
    """Model for product discounts"""

    # Required fields
    product_id: int
    discount_type: DiscountType
    discounted_price: int

    # Optional fields
    conditions_text: Optional[str] = None
    conditions_min_quantity: Optional[int] = None
    conditions_buy_quantity: Optional[int] = None
    conditions_get_quantity: Optional[int] = None

    # Database fields
    id: Optional[int] = None
    created_at: Optional[datetime] = None

    def to_tuple(self) -> tuple:
        """Convert the object to a tuple for inserting into the database"""
        return (
            self.product_id,
            self.discount_type.value,
            self.discounted_price,
            self.conditions_text,
            self.conditions_min_quantity,
            self.conditions_buy_quantity,
            self.conditions_get_quantity,
        )

    def to_dict(self) -> dict:
        """Convert the object to a dictionary for JSON serialization"""
        return {
            "product_id": self.product_id,
            "discount_type": self.discount_type.value,
            "discounted_price": self.discounted_price,
            "conditions_text": self.conditions_text,
            "conditions_min_quantity": self.conditions_min_quantity,
            "conditions_buy_quantity": self.conditions_buy_quantity,
            "conditions_get_quantity": self.conditions_get_quantity,
            "id": self.id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    @classmethod
    def create_percentage_quantity_discount(
        cls,
        product_id: int,
        discounted_price: int,
        min_quantity: int,
        conditions_text: str = None,
    ) -> "ProductDiscount":
        """Create a percentage quantity discount"""

        return cls(
            product_id=product_id,
            discount_type=DiscountType.PERCENTAGE_QUANTITY,
            discounted_price=discounted_price,
            conditions_text=conditions_text,
            conditions_min_quantity=min_quantity,
        )

    @classmethod
    def create_card_discount(
        cls, product_id: int, discounted_price: int, conditions_text: str = None
    ) -> "ProductDiscount":
        """Create a card discount"""

        return cls(
            product_id=product_id,
            discount_type=DiscountType.CARD,
            discounted_price=discounted_price,
            conditions_text=conditions_text,
        )

    @classmethod
    def create_wholesale_discount(
        cls,
        product_id: int,
        discounted_price: int,
        min_quantity: int,
        conditions_text: str = None,
    ) -> "ProductDiscount":
        """Create a wholesale discount"""

        return cls(
            product_id=product_id,
            discount_type=DiscountType.WHOLESALE,
            discounted_price=discounted_price,
            conditions_text=conditions_text,
            conditions_min_quantity=min_quantity,
        )

    @classmethod
    def create_buy_x_get_y_discount(
        cls,
        product_id: int,
        discounted_price: int,
        buy_quantity: int,
        get_quantity: int,
        conditions_text: str = None,
    ) -> "ProductDiscount":
        """Create a buy X get Y discount"""

        return cls(
            product_id=product_id,
            discount_type=DiscountType.BUY_X_GET_Y,
            discounted_price=discounted_price,
            conditions_text=conditions_text,
            conditions_buy_quantity=buy_quantity,
            conditions_get_quantity=get_quantity,
        )
