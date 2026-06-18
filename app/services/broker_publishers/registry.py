from __future__ import annotations
from .base import BrokerPublisherAdapter
from .zerodha import ZerodhaPublisherAdapter

_ADAPTERS = {
    ZerodhaPublisherAdapter.broker: ZerodhaPublisherAdapter(),
}


def get_publisher_adapter(broker: str) -> BrokerPublisherAdapter:
    key = (broker or "").upper()
    if key not in _ADAPTERS:
        supported = ", ".join(sorted(_ADAPTERS))
        raise ValueError(f"Broker {broker!r} is not supported for Publisher flow yet. Supported: {supported}")
    return _ADAPTERS[key]
