"""
Bloomberg Terminal API (blpapi) client implementation.
This is the existing implementation wrapped in the new interface.
"""

import os
import math
import numbers
from datetime import date, datetime, time as dt_time
from typing import List, Dict, Any, Optional
try:
    import blpapi
    BLPAPI_AVAILABLE = True
except ImportError:
    BLPAPI_AVAILABLE = False
    blpapi = None

from .base_client import BloombergClientBase


def _coerce_blp_reference_value(val: Any) -> Any:
    """Convert Bloomberg getValue() results to JSON-serializable Python types."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    if isinstance(val, str):
        return val
    if BLPAPI_AVAILABLE and isinstance(val, blpapi.Datetime):
        try:
            py_dt = val.toDatetime()
            if py_dt is None:
                return None
            if (
                py_dt.hour == 0
                and py_dt.minute == 0
                and py_dt.second == 0
                and py_dt.microsecond == 0
            ):
                # Keep datetime.date so callers (e.g. economic calendar) keep working; Flask jsonifies dates
                return py_dt.date()
            return py_dt
        except Exception:
            return str(val)
    if isinstance(val, datetime):
        if val.time() == dt_time(0, 0, 0):
            return val.date()
        return val
    if isinstance(val, date):
        return val
    if isinstance(val, numbers.Integral):
        return int(val)
    if isinstance(val, numbers.Real):
        x = float(val)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    return str(val)


class BLPAPIClient(BloombergClientBase):
    """Bloomberg Terminal API (blpapi) client - existing implementation"""
    
    def __init__(self, host: str = "localhost", port: int = 8194):
        """
        Initialize BLPAPI client.
        
        Args:
            host: Bloomberg Terminal host (default: localhost)
            port: Bloomberg Terminal port (default: 8194)
        """
        if not BLPAPI_AVAILABLE:
            raise ImportError(
                "blpapi package not available. "
                "Install with: pip install blpapi"
            )
        
        self.host = host
        self.port = port
        self.service = "//blp/refdata"
        self._session = None
    
    def is_available(self) -> bool:
        """Check if blpapi is available and Terminal is running"""
        if not BLPAPI_AVAILABLE:
            return False
        
        try:
            session = self._create_session()
            if session:
                session.stop()
                return True
            return False
        except Exception:
            return False
    
    def _create_session(self):
        """Create and start Bloomberg session"""
        session_options = blpapi.SessionOptions()
        session_options.setServerHost(self.host)
        session_options.setServerPort(self.port)
        session_options.setAutoRestartOnDisconnection(True)
        
        session = blpapi.Session(session_options)
        if not session.start():
            return None
        
        if not session.openService(self.service):
            session.stop()
            return None
        
        return session
    
    def get_historical_data(
        self,
        ticker: str,
        fields: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        periodicity: Optional[str] = None,
        overrides: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch historical data using blpapi.

        HistoricalDataRequest returns securityData as a single HistoricalDataTable
        (not an array). Use it directly; iterate over fieldData with numValues()/getValue(i).
        periodicity: DAILY (default). overrides: e.g. {"RELEASE_STAGE_OVERRIDE": "P"} for prelim-only (PMI).
        """
        periodicity = (periodicity or "DAILY").upper()
        _debug = os.environ.get("DATA_BRIDGE_DEBUG", "").lower() in ("1", "true", "yes")
        if _debug:
            print(f"[BLPAPI HistoricalDataRequest] ticker={ticker!r} fields={fields} start_date={start_date!r} end_date={end_date!r} periodicity={periodicity} overrides={overrides}")

        session = self._create_session()
        if not session:
            raise Exception("Failed to start Bloomberg session. Is Bloomberg Terminal running and logged in?")
        
        try:
            refDataService = session.getService(self.service)
            request = refDataService.createRequest("HistoricalDataRequest")

            # Set security (match old bloomberg_service: append)
            request.append("securities", ticker)

            # Set fields
            for field in fields:
                request.append("fields", field)

            # Set date range (Bloomberg API expects YYYYMMDD format)
            if start_date:
                start_date_bbg = start_date.replace("-", "")
                request.set("startDate", start_date_bbg)
            if end_date:
                end_date_bbg = end_date.replace("-", "")
                request.set("endDate", end_date_bbg)

            request.set("periodicitySelection", periodicity)

            # Optional overrides (e.g. RELEASE_STAGE_OVERRIDE=P for preliminary-only PMI)
            if overrides:
                overrides_el = request.getElement("overrides")
                for field_id, value in overrides.items():
                    ov = overrides_el.appendElement()
                    ov.setElement("fieldId", field_id)
                    ov.setElement("value", str(value))

            if _debug:
                print(f"[BLPAPI] Request: securities=[{ticker}] fields={fields} periodicity={periodicity} startDate={start_date_bbg if start_date else None} endDate={end_date_bbg if end_date else None}")

            session.sendRequest(request)

            records = []
            while True:
                event = session.nextEvent(500)

                if event.eventType() in (blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE):
                    for msg in event:
                        if msg.hasElement("securityData"):
                            # HistoricalDataRequest: securityData is single element, not array
                            securityData = msg.getElement("securityData")

                            # Check for security error (can exist even with fieldData)
                            if securityData.hasElement("securityError"):
                                err = securityData.getElement("securityError")
                                raise Exception(
                                    f"Security error: {err.getElementAsString('category')} - "
                                    f"{err.getElementAsString('message')}"
                                )

                            if securityData.hasElement("fieldData"):
                                fieldData = securityData.getElement("fieldData")
                                if _debug:
                                    print(f"[BLPAPI] fieldData: numValues={fieldData.numValues()}")

                                if fieldData.numValues() > 0:
                                    for i in range(fieldData.numValues()):
                                        dataPoint = fieldData.getValue(i)
                                        date_obj = dataPoint.getElementAsDatetime("date")
                                        date_str = date_obj.strftime("%Y-%m-%d") if hasattr(date_obj, "strftime") else str(date_obj)

                                        record: Dict[str, Any] = {"date": date_str}

                                        # Map PX_* to *_price for edge function compatibility
                                        if dataPoint.hasElement("PX_OPEN"):
                                            record["open_price"] = dataPoint.getElementAsFloat("PX_OPEN")
                                        if dataPoint.hasElement("PX_HIGH"):
                                            record["high_price"] = dataPoint.getElementAsFloat("PX_HIGH")
                                        if dataPoint.hasElement("PX_LOW"):
                                            record["low_price"] = dataPoint.getElementAsFloat("PX_LOW")
                                        if dataPoint.hasElement("PX_OFFICIAL_CLOSE"):
                                            record["close_price"] = dataPoint.getElementAsFloat("PX_OFFICIAL_CLOSE")
                                            record["adjusted_close"] = record["close_price"]
                                        elif dataPoint.hasElement("PX_LAST"):
                                            record["close_price"] = dataPoint.getElementAsFloat("PX_LAST")
                                            record["adjusted_close"] = record["close_price"]
                                        if dataPoint.hasElement("PX_VOLUME"):
                                            record["volume"] = dataPoint.getElementAsInteger("PX_VOLUME")

                                        # Extract all requested fields generically
                                        for requested_field in fields:
                                            if dataPoint.hasElement(requested_field):
                                                try:
                                                    record[requested_field] = dataPoint.getElementAsFloat(requested_field)
                                                except Exception:
                                                    try:
                                                        record[requested_field] = dataPoint.getElementAsString(requested_field)
                                                    except Exception:
                                                        if _debug:
                                                            print(f"[BLPAPI] Could not extract field {requested_field}")

                                        if "close_price" in record or len(record) > 1:
                                            records.append(record)
                                elif _debug:
                                    print(f"[BLPAPI] No fieldData values - response empty")
                            elif _debug:
                                print(f"[BLPAPI] No fieldData element")

                            # Log field exceptions (can exist alongside fieldData)
                            if securityData.hasElement("fieldExceptions"):
                                fieldExceptions = securityData.getElement("fieldExceptions")
                                for j in range(fieldExceptions.numValues()):
                                    fe = fieldExceptions.getValue(j)
                                    errInfo = fe.getElement("errorInfo")
                                    errMsg = errInfo.getElementAsString("message")
                                    if _debug:
                                        print(f"[BLPAPI] fieldException: {errMsg}")

                if event.eventType() == blpapi.Event.RESPONSE:
                    break

            if _debug:
                print(f"[BLPAPI] Response: {len(records)} records for {ticker!r}")
            return records

        finally:
            session.stop()
    
    def get_reference_data(
        self,
        tickers: List[str],
        fields: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch reference/EOD data using blpapi (BDP)"""
        _debug = os.environ.get("DATA_BRIDGE_DEBUG", "").lower() in ("1", "true", "yes")
        if _debug:
            print(f"[BLPAPI ReferenceDataRequest] securities={tickers} fields={fields}")
        session = self._create_session()
        if not session:
            raise Exception("Failed to start Bloomberg session. Is Bloomberg Terminal running and logged in?")
        
        try:
            refDataService = session.getService(self.service)
            request = refDataService.createRequest("ReferenceDataRequest")
            
            # Add securities
            for ticker in tickers:
                request.getElement("securities").appendValue(ticker)
            
            # Add fields
            for field in fields:
                request.getElement("fields").appendValue(field)
            
            # Send request
            session.sendRequest(request)
            
            result = {}
            
            while True:
                event = session.nextEvent(500)
                
                if event.eventType() in (blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE):
                    for msg in event:
                        if msg.hasElement("securityData"):
                            security_data_array = msg.getElement("securityData")
                            # Use indexed access (documented BLPAPI pattern); .values() is unreliable across builds
                            for idx in range(security_data_array.numValues()):
                                security_data = security_data_array.getValueAsElement(idx)
                                ticker = security_data.getElementAsString("security")
                                
                                # Check for security error
                                if security_data.hasElement("securityError"):
                                    err = security_data.getElement("securityError")
                                    result[ticker] = {
                                        "error": f"{err.getElementAsString('category')} - {err.getElementAsString('message')}"
                                    }
                                    continue
                                
                                result[ticker] = {}
                                
                                # Extract field values
                                field_data = security_data.getElement("fieldData")
                                for field in fields:
                                    if field_data.hasElement(field):
                                        field_elem = field_data.getElement(field)
                                        if not field_elem.isNull():
                                            raw = field_elem.getValue()
                                            result[ticker][field] = _coerce_blp_reference_value(raw)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
            
            return result
        
        finally:
            session.stop()

