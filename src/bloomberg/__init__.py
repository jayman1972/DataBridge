"""
Bloomberg client package for Data Bridge.

Supports both BLPAPI (Bloomberg Terminal API) and BQL (Bloomberg Query Language) clients.
"""

from .base_client import BloombergClientBase
from .client_factory import BloombergClientType, get_bloomberg_client

__all__ = ['BloombergClientBase', 'BloombergClientType', 'get_bloomberg_client']

