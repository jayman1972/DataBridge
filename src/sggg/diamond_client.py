"""
SGGG Diamond API client.
Authenticates with username/password, caches AuthKey (1hr expiry), and calls GetPortfolio, GetPortfolioTrades, etc.
Spec: Diamond API v2.03 - https://api.sgggfsi.com/api/v1/
"""

import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from sggg.nav_sheet_parse import parse_diamond_nav_unavailable

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    requests = None

BASE_URL = "https://api.sgggfsi.com/api/v1"
AUTH_EXPIRY_BUFFER_SEC = 300  # Refresh AuthKey 5 min before expiry


class DiamondNavUnavailableError(Exception):
    """NAV sheet not finalized for the requested valuation period."""

    def __init__(self, message: str, *, end_date: str):
        self.end_date = end_date
        self.user_message = message
        super().__init__(message)


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
        self._auth_lock = threading.Lock()

    def _ensure_auth(self) -> str:
        now = time.time()
        if self._auth_key and now < self._auth_key_expires_at - AUTH_EXPIRY_BUFFER_SEC:
            return self._auth_key
        with self._auth_lock:
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
        try:
            resp.raise_for_status()
        except Exception as e:
            body = (resp.text or "").strip()
            snippet = body[:2000] + ("...(truncated)" if len(body) > 2000 else "")
            raise RuntimeError(f"Diamond login failed: HTTP {getattr(resp, 'status_code', '?')}: {snippet}") from e
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
        # Diamond docs: send `Authorization: AuthKey <token>`.
        # API error text mentions "Missing AuthKey", so also include
        # a direct `AuthKey` header for maximum compatibility.
        headers = {
            "Authorization": f"AuthKey {auth}",
            "AuthKey": auth,
            "Content-Type": "application/json",
        }
        # Use module-level requests.post (not Session) so parallel fund fetches are thread-safe.
        resp = requests.post(url, json=payload, headers=headers, timeout=120)
        try:
            resp.raise_for_status()
        except Exception as e:
            body = (resp.text or "").strip()
            snippet = body[:2000] + ("...(truncated)" if len(body) > 2000 else "")
            raise RuntimeError(f"Diamond request failed: {path} HTTP {getattr(resp, 'status_code', '?')}: {snippet}") from e
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
        try:
            return self._post("GetNAVSheet/", payload)
        except RuntimeError as exc:
            parsed = parse_diamond_nav_unavailable(exc, valuation_date)
            if parsed:
                raise DiamondNavUnavailableError(
                    parsed["message"],
                    end_date=parsed["end_date"],
                ) from exc
            raise

    def get_fund_details(self, fund_id: str) -> Any:
        payload = {"FundID": fund_id}
        return self._post("GetFundDetails/", payload)


_cached_diamond_client: Optional[DiamondAPIClient] = None
_cached_diamond_credentials: Optional[Tuple[str, str]] = None


def get_diamond_client() -> Optional[DiamondAPIClient]:
    """Return a process-wide Diamond client from env vars, or None if not configured."""
    global _cached_diamond_client, _cached_diamond_credentials
    username = os.environ.get("SGGG_DIAMOND_USERNAME", "").strip()
    password = os.environ.get("SGGG_DIAMOND_PASSWORD", "").strip()
    if not username or not password:
        return None
    creds = (username, password)
    if _cached_diamond_client is None or _cached_diamond_credentials != creds:
        _cached_diamond_client = DiamondAPIClient(username=username, password=password)
        _cached_diamond_credentials = creds
    return _cached_diamond_client
