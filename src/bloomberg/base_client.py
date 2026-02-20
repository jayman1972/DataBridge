"""
Base class for Bloomberg clients.
Defines the interface that all Bloomberg clients must implement.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class BloombergClientBase(ABC):
    """Base class for Bloomberg clients - ensures consistent interface"""
    
    @abstractmethod
    def get_historical_data(
        self,
        ticker: str,
        fields: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        periodicity: Optional[str] = None,
        overrides: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch historical data for a ticker.
        
        Args:
            ticker: Bloomberg ticker (e.g., "SPX Index", "AAPL US Equity")
            fields: List of field names (e.g., ["PX_LAST", "PX_VOLUME"])
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of records, each containing 'date' and field values.
            Example: [
                {"date": "2024-01-01", "PX_LAST": 4500.0, "PX_VOLUME": 1000000},
                {"date": "2024-01-02", "PX_LAST": 4510.0, "PX_VOLUME": 1100000},
                ...
            ]
        """
        pass
    
    @abstractmethod
    def get_reference_data(
        self,
        tickers: List[str],
        fields: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch reference/EOD data for tickers.
        
        Args:
            tickers: List of Bloomberg tickers
            fields: List of field names
            
        Returns:
            Dictionary mapping ticker to field values.
            Example: {
                "SPX Index": {"PX_LAST": 4500.0, "PX_VOLUME": 1000000},
                "VIX Index": {"PX_LAST": 15.5}
            }
        """
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if the client is available/connected"""
        pass

