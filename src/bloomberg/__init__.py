"""
Bloomberg client package for Data Bridge.

Supports both BLPAPI (Bloomberg Terminal API) and BQL (Bloomberg Query Language) clients.
"""

from .client_factory import get_bloomberg_client, BloombergClientType
from .base_client import BloombergClientBase

__all__ = ['get_bloomberg_client', 'BloombergClientType', 'BloombergClientBase']

