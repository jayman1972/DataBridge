"""
Base class for Bloomberg clients.
Defines the interface that all Bloomberg clients must implement.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BloombergClientBase(ABC):
    """Base class for Bloomberg clients - ensures consistent interface"""
    
    @abstractmethod
    def get_historical_data(
        self,
        ticker: str,
        fields: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        periodicity: str | None = None,
        overrides: dict[str, str] | None = None,
        adjustment_profile: str | None = None,
        currency: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch historical data for a ticker.
        
        Args:
            ticker: Bloomberg ticker (e.g., "SPX Index", "AAPL US Equity")
            fields: List of field names (e.g., ["PX_LAST", "PX_VOLUME"])
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            adjustment_profile: raw, capital, or full. When set, the request
                must not inherit workstation DPDF adjustment settings.
            currency: Optional Bloomberg reporting currency (for example,
                ``USD`` for announcement-date market capitalization).
            
        Returns:
            List of records, each containing 'date' and field values.
            Example: [
                {"date": "2024-01-01", "PX_LAST": 4500.0, "PX_VOLUME": 1000000},
                {"date": "2024-01-02", "PX_LAST": 4510.0, "PX_VOLUME": 1100000},
                ...
            ]
        """
    
    @abstractmethod
    def get_reference_data(
        self,
        tickers: list[str],
        fields: list[str],
        overrides: dict[str, str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """
        Fetch reference/EOD data for tickers.
        
        Args:
            tickers: List of Bloomberg tickers
            fields: List of field names
            overrides: Optional Bloomberg overrides dict (e.g. BDP overrides)
            
        Returns:
            Dictionary mapping ticker to field values.
            Example: {
                "SPX Index": {"PX_LAST": 4500.0, "PX_VOLUME": 1000000},
                "VIX Index": {"PX_LAST": 15.5}
            }
        """
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if the client is available/connected"""

