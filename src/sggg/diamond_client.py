"""
SGGG Diamond API client.
Authenticates with username/password, caches AuthKey (1hr expiry), and calls GetPortfolio, GetPortfolioTrades, etc.
Spec: Diamond API v2.03 - https://api.sgggfsi.com/api/v1/
"""

import os
import time
from typing import Any, Dict, List, Optional

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    requests = None

BASE_URL = "https://api.sgggfsi.com/api/v1"
AUTH_EXPIRY_BUFFER_SEC = 300  # Refresh AuthKey 5 min before expiry


class DiamondAPIClient:
    def __init__(
        self,
        username: str,
        password: str,
        base_url: str = BASE_URL,
    ):
        self.username = username
        self.password = password
        self.base_url = base_url.rstrip("/")
        self._auth_key: Optional[str] = None
        self._auth_key_expires_at: float = 0

    def _ensure_auth(self) -> str:
        now = time.time()
        if self._auth_key and now < self._auth_key_expires_at - AUTH_EXPIRY_BUFFER_SEC:
            return self._auth_key
        self._auth_key = self._login()
        self._auth_key_expires_at = now + 3600  # 1 hour
        return self._auth_key

    def _login(self) -> str:
        if not REQUESTS_AVAILABLE:
            raise ImportError("requests package required. pip install requests")
        url = f"{self.base_url}/login/"
        payload = {"Username": self.username, "Password": self.password}
        resp = requests.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        auth_key = data.get("AuthKey") or data.get("Authkey")
        if not auth_key:
            raise ValueError("Login response missing AuthKey")
        return auth_key

    def _post(
        self,
        path: str,
        payload: Dict[str, Any],
        accept_json: bool = True,
    ) -> Any:
        auth = self._ensure_auth()
        url = f"{self.base_url}/{path.lstrip('/')}"
        headers = {
            "Authorization": auth,
            "Content-Type": "application/json",
        }
        resp = requests.post(url, json=payload, headers=headers, timeout=120)
        resp.raise_for_status()
        if accept_json:
            return resp.json()
        return resp.text

    def get_portfolio(
        self,
        fund_id: str,
        valuation_date: str,
        reference_date: Optional[str] = None,
        exclude_flat_positions: bool = False,
        exclude_not_priced_positions: bool = True,
    ) -> Any:
        """
        Get finalized portfolio data for a fund.
        valuation_date: yyyy-mm-dd
        reference_date: optional, yyyy-mm-dd (default: Jan 1 of valuation year)
        """
        payload = {
            "FundID": fund_id,
            "ValuationDate": valuation_date,
            "ExcludeFlatPositions": exclude_flat_positions,
            "ExcludeNotPricedPositions": exclude_not_priced_positions,
        }
        if reference_date:
            payload["ReferenceDate"] = reference_date
        return self._post("GetPortfolio/", payload)

    def get_portfolio_trades(
        self,
        fund_parent_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        date_type: str = "ValuationDate",
    ) -> Any:
        """
        Get portfolio trade details for a fund.
        start_date, end_date: yyyy-mm-dd (range cannot exceed 1 month)
        date_type: ValuationDate (V), SettlementDate (S), TradeDate (T), ProcessDate (P)
        """
        payload = {"FundParentID": fund_parent_id, "DateType": date_type}
        if start_date:
            payload["StartDate"] = start_date
        if end_date:
            payload["EndDate"] = end_date
        return self._post("GetPortfolioTrades/", payload)

    def get_nav_sheet(self, fund_id: str, valuation_date: str) -> Any:
        payload = {"FundID": fund_id, "ValuationDate": valuation_date}
        return self._post("GetNAVSheet/", payload)

    def get_fund_details(self, fund_id: str) -> Any:
        payload = {"FundID": fund_id}
        return self._post("GetFundDetails/", payload)


def get_diamond_client() -> Optional[DiamondAPIClient]:
    """Create Diamond client from env vars. Returns None if not configured."""
    username = os.environ.get("SGGG_DIAMOND_USERNAME", "").strip()
    password = os.environ.get("SGGG_DIAMOND_PASSWORD", "").strip()
    if not username or not password:
        return None
    return DiamondAPIClient(username=username, password=password)
