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

    def fetch_orderbook(self, user_access_token: str) -> list:
        """Fetch full day's orderbook for a user via broker API.

        Uses the Publisher/app API key + the user's access_token (obtained
        via token exchange during Publisher redirect) to fetch the complete
        orderbook for that user for the current day.

        Returns:
            List of order dicts from the broker, or empty list on failure.

        Raises:
            NotImplementedError if the broker doesn't support orderbook fetch.
        """
        raise NotImplementedError(
            f"Broker {self.broker} does not support orderbook fetch."
        )

    def exchange_token(self, request_token: str) -> Dict[str, Any]:
        """Exchange a broker-specific request_token for access credentials.

        Called after Publisher/offsite redirect returns a request_token.
        Each broker adapter implements its own exchange logic.

        Returns:
            {
                "access_token": str,
                "user_id": str,
                "user_name": str | None,
                "email": str | None,
                "broker": str,
                "expires_at": datetime | None,
                "profile": dict,     # broker-specific raw profile data
            }

        Raises:
            NotImplementedError if the broker doesn't support token exchange.
            requests.HTTPError if the exchange API call fails.
        """
        raise NotImplementedError(
            f"Broker {self.broker} does not support token exchange."
        )

