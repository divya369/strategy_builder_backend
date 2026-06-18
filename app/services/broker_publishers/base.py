from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict
import pandas as pd


class BrokerPublisherAdapter(ABC):
    """
    Base adapter for broker-specific Publisher / Offsite / Basket flows.

    The live investment engine must remain equitycase-style and broker-agnostic.
    Only this adapter layer should know broker-specific payload/postback format.
    """

    broker: str

    @abstractmethod
    def build_payload(self, *, strategy: Any, buy_df: pd.DataFrame, sell_df: pd.DataFrame) -> Dict[str, Any]:
        """Return frontend-consumable broker payload/button config."""
        raise NotImplementedError

    @abstractmethod
    def normalize_postback(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert broker postback into standard internal format:
            tag
            order_id
            status
            filled_quantity
            average_price
            client_id
            transaction_type
            tradingsymbol
        """
        raise NotImplementedError

    def verify_checksum(self, payload: Dict[str, Any]) -> bool | None:
        """Verify postback authenticity using broker-specific checksum.

        Returns:
            True  — checksum is valid
            False — checksum is invalid (tampered/fake)
            None  — cannot verify (secret not configured or fields missing)

        Default: returns None (no verification).
        Override in broker-specific adapter to implement verification.
        """
        return None
