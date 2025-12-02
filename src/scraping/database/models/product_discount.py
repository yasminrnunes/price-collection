"""Product discount models for the price collection system.

This module provides data models for representing and managing various types of
discounts and promotions that can be applied to products. It includes an enumeration
of discount types and a ProductDiscount dataclass for modeling discount information.

Discount Types:
    - PERCENTAGE_QUANTITY: Discounts applied to specific units when buying a minimum
      quantity (e.g., -40% on the 2nd unit)
    - CARD: Card-based discounts requiring specific payment methods (e.g., store cards)
    - WHOLESALE: Bulk pricing discounts for purchasing above minimum quantities
    - BUY_X_GET_Y: Promotions where buying X items gets you Y items (e.g., buy 2 get 1)

The ProductDiscount class provides:
    - Storage of discount information with type-specific conditions
    - Factory methods for creating each discount type
    - Serialization methods for database insertion and JSON export

Example:
    Create a percentage quantity discount:
        discount = ProductDiscount.create_percentage_quantity_discount(
            product_id=12345,
            discounted_price=1500,  # R$ 15.00 in cents
            min_quantity=2,
            conditions_text="-40% on the 2nd unit"
        )

    Create a buy X get Y discount:
        discount = ProductDiscount.create_buy_x_get_y_discount(
            product_id=12345,
            discounted_price=667,  # Effective price: (2 * 1000) / 3
            buy_quantity=2,
            get_quantity=3,
            conditions_text="Buy 2 Get 1 Free"
        )

Note:
    All prices are stored as integers representing cents. For example, R$ 19.90
    is stored as 1990.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from enum import Enum


class DiscountType(Enum):
    """Enumeration of available discount types for products.

    This enum defines the different types of promotional discounts that can be
    applied to products, each with specific conditions and pricing structures.
    """

    PERCENTAGE_QUANTITY = "PERCENTAGE_QUANTITY"
    """Percentage discount applied when purchasing a minimum quantity.
    
    Example: -40% on the 2nd unit, where the discount applies to specific
    units after reaching a minimum purchase quantity.
    """

    CARD = "CARD"
    """Card-based discount requiring payment with a specific credit/debit card.
    
    Example: Discount available only when using store-brand credit card.
    """

    WHOLESALE = "WHOLESALE"
    """Bulk pricing discount when purchasing above a minimum quantity threshold.
    
    Example: Discounted price when buying 5 or more units.
    """

    BUY_X_GET_Y = "BUY_X_GET_Y"
    """Buy X items and get Y items promotion (e.g., buy 2 get 1, buy 3 get 2).
    
    Example: Buy 2 get 1 free, or Buy 3 get 2, where the effective price per
    unit is calculated based on the total quantity purchased and received.
    """


@dataclass
class ProductDiscount:
    """Represents a discount or promotion applied to a product.

    This class models various types of discounts that can be associated with
    products, including percentage-based discounts, card discounts, wholesale
    pricing, and buy-X-get-Y promotions. Each discount type has specific
    conditions and pricing information.

    Attributes:
        product_id: The unique identifier of the product this discount applies to.
        discount_type: The type of discount from the DiscountType enumeration.
        discounted_price: The discounted price per unit in cents (integer).
            Example: 1990 represents R$ 19.90.

        conditions_text: Optional human-readable description of the discount conditions.
        conditions_min_quantity: Optional minimum quantity required for PERCENTAGE_QUANTITY
            or WHOLESALE discounts.
        conditions_buy_quantity: Optional quantity to buy for BUY_X_GET_Y discounts.
        conditions_get_quantity: Optional quantity received for BUY_X_GET_Y discounts.

        id: Optional database primary key, automatically assigned on insertion.
        created_at: Optional timestamp of when the discount was created in the database.
    """

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
        """Convert the discount object to a tuple for database insertion.

        Returns:
            A tuple containing all discount fields in the order required for
            database insertion. The discount_type is converted to its string value.

        Note:
            This method excludes the database fields (id, created_at) as they
            are typically auto-generated or set by the database.
        """
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
        """Convert the discount object to a dictionary for JSON serialization.

        Returns:
            A dictionary containing all discount fields with datetime objects
            converted to ISO format strings. Suitable for JSON serialization and
            API responses.

        Note:
            The discount_type is converted to its string value, and created_at
            is serialized as an ISO format string if present.
        """
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
        """Create a percentage quantity discount instance.

        This discount type applies a percentage discount to specific units when
        purchasing a minimum quantity. For example, -40% on the 2nd unit means
        when buying 2 or more units, the second unit has a 40% discount.

        Args:
            product_id: The unique identifier of the product.
            discounted_price: The discounted price per unit in cents (integer).
            min_quantity: The minimum quantity required for the discount to apply.
            conditions_text: Optional description of the discount conditions
                (e.g., "-40% on the 2nd unit").

        Returns:
            A ProductDiscount instance with PERCENTAGE_QUANTITY type configured.
        """
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
        """Create a card-based discount instance.

        This discount type requires payment with a specific credit or debit card
        (typically a store-branded card) to receive the discounted price.

        Args:
            product_id: The unique identifier of the product.
            discounted_price: The discounted price per unit in cents (integer).
            conditions_text: Optional description of the card discount
                (e.g., "10% card discount" or "Cartão Extra").

        Returns:
            A ProductDiscount instance with CARD type configured.
        """
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
        """Create a wholesale (bulk pricing) discount instance.

        This discount type offers a lower price when purchasing above a minimum
        quantity threshold. All units purchased qualify for the discounted price
        when the minimum quantity is met.

        Args:
            product_id: The unique identifier of the product.
            discounted_price: The discounted price per unit in cents (integer).
            min_quantity: The minimum quantity required to qualify for wholesale pricing.
            conditions_text: Optional description of the wholesale conditions
                (e.g., "Starting from 5 units").

        Returns:
            A ProductDiscount instance with WHOLESALE type configured.
        """
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
        """Create a buy X get Y discount instance.

        This discount type represents promotions where purchasing X items allows
        you to receive Y items (e.g., buy 2 get 1 free, buy 3 get 2). The
        discounted_price represents the effective price per unit after applying
        the promotion.

        Example:
            Buy 2 get 1 free with original price of R$ 10.00:
            - buy_quantity = 2, get_quantity = 3
            - discounted_price = (2 * 1000) / 3 = 667 cents (R$ 6.67 per unit)

        Args:
            product_id: The unique identifier of the product.
            discounted_price: The effective discounted price per unit in cents
                (integer), calculated as: (original_price * buy_quantity) / get_quantity.
            buy_quantity: The number of items that must be purchased.
            get_quantity: The total number of items received (purchased + free).
            conditions_text: Optional description of the promotion
                (e.g., "Buy 2 Get 1 Free" or "Buy 3 Get 2").

        Returns:
            A ProductDiscount instance with BUY_X_GET_Y type configured.
        """
        return cls(
            product_id=product_id,
            discount_type=DiscountType.BUY_X_GET_Y,
            discounted_price=discounted_price,
            conditions_text=conditions_text,
            conditions_buy_quantity=buy_quantity,
            conditions_get_quantity=get_quantity,
        )
