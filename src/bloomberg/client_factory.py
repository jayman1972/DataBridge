"""
Factory for creating Bloomberg clients.
Supports both BLPAPI and BQL clients with configuration-based selection.
"""

import os
from enum import Enum
from typing import Optional
from .base_client import BloombergClientBase
from .blpapi_client import BLPAPIClient

# Try to import BQL client
try:
    from .bql_client import BQLClient
    BQL_CLIENT_AVAILABLE = True
except ImportError:
    BQL_CLIENT_AVAILABLE = False
    BQLClient = None


class BloombergClientType(str, Enum):
    """Bloomberg client type enumeration"""
    BLPAPI = "blpapi"
    BQL = "bql"
    AUTO = "auto"  # Try BQL first, fallback to BLPAPI


def get_bloomberg_client(
    client_type: Optional[str] = None,
    **kwargs
) -> BloombergClientBase:
    """
    Factory function to get the Bloomberg client.
    
    Currently uses BLPAPI (Bloomberg Terminal API).
    BQL is not available outside of BQuant IDE.
    
    Args:
        client_type: Ignored - always uses BLPAPI
        **kwargs: Additional arguments passed to BLPAPIClient
                  (e.g., host, port)
    
    Returns:
        BLPAPIClient instance
        
    Raises:
        ImportError: If blpapi package is not available
        ConnectionError: If Bloomberg Terminal connection cannot be established
    """
    # Use BLPAPI (BQL is only available in BQuant IDE, not in external IDEs)
    host = kwargs.get("host", os.getenv("BLOOMBERG_HOST", "localhost"))
    port = int(kwargs.get("port", os.getenv("BLOOMBERG_PORT", "8194")))
    
    try:
        return BLPAPIClient(host=host, port=port)
    except ImportError as e:
        raise ImportError(
            f"blpapi package not available: {e}\n\n"
            "Install with: pip install blpapi\n\n"
            "Note: BQL is only available within BQuant IDE, not in external IDEs."
        ) from e
    except Exception as e:
        raise ConnectionError(
            f"Failed to initialize BLPAPI client: {e}\n\n"
            "Ensure:\n"
            "1. Bloomberg Terminal is running\n"
            "2. You are logged in to Bloomberg Terminal\n"
            "3. blpapi package is installed: pip install blpapi"
        ) from e

