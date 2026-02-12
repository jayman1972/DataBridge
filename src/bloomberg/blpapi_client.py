"""
Bloomberg Terminal API (blpapi) client implementation.
This is the existing implementation wrapped in the new interface.
"""

import os
from typing import List, Dict, Any, Optional
try:
    import blpapi
    BLPAPI_AVAILABLE = True
except ImportError:
    BLPAPI_AVAILABLE = False
    blpapi = None

from .base_client import BloombergClientBase


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
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Fetch historical data using blpapi"""
        import os
        _debug = os.environ.get("DATA_BRIDGE_DEBUG", "").lower() in ("1", "true", "yes")
        if _debug:
            print(f"[BLPAPI HistoricalDataRequest] ticker={ticker!r} fields={fields} start_date={start_date!r} end_date={end_date!r}")

        session = self._create_session()
        if not session:
            raise Exception("Failed to start Bloomberg session. Is Bloomberg Terminal running and logged in?")
        
        try:
            refDataService = session.getService(self.service)
            request = refDataService.createRequest("HistoricalDataRequest")
            
            # Set security
            request.getElement("securities").appendValue(ticker)
            
            # Set fields
            for field in fields:
                request.getElement("fields").appendValue(field)
            
            # Set date range (Bloomberg API expects YYYYMMDD format)
            if start_date:
                start_date_bbg = start_date.replace("-", "")
                request.set("startDate", start_date_bbg)
            if end_date:
                end_date_bbg = end_date.replace("-", "")
                request.set("endDate", end_date_bbg)
            
            # Set periodicity (daily)
            request.set("periodicityAdjustment", "ACTUAL")
            request.set("periodicitySelection", "DAILY")
            
            if _debug:
                print(f"[BLPAPI] Request: securities=[{ticker}] fields={fields} startDate={start_date_bbg if start_date else None} endDate={end_date_bbg if end_date else None}")
            
            # Send request
            session.sendRequest(request)
            
            records = []
            while True:
                event = session.nextEvent(500)
                
                if event.eventType() in (blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE):
                    for msg in event:
                        if msg.hasElement("securityData"):
                            securityDataArray = msg.getElement("securityData")
                            if _debug:
                                print(f"[BLPAPI] securityData: iterating with .values()")
                            for securityData in securityDataArray.values():
                                # Check for security error
                                if securityData.hasElement("securityError"):
                                    err = securityData.getElement("securityError")
                                    raise Exception(
                                        f"Security error: {err.getElementAsString('category')} - "
                                        f"{err.getElementAsString('message')}"
                                    )
                                
                                if _debug:
                                    sec_name = securityData.getElementAsString("security") if securityData.hasElement("security") else "?"
                                    print(f"[BLPAPI] security={sec_name!r} hasFieldData={securityData.hasElement('fieldData')}")
                                
                                if securityData.hasElement("fieldData"):
                                    fieldDataArray = securityData.getElement("fieldData")
                                    if _debug:
                                        print(f"[BLPAPI] fieldData: iterating with .values()")
                                    for fieldData in fieldDataArray.values():
                                        record = {"date": None}
                                        if fieldData.hasElement("date"):
                                            date_elem = fieldData.getElement("date")
                                            if date_elem.isNull():
                                                continue
                                            date_val = date_elem.getValue()
                                            if isinstance(date_val, blpapi.Datetime):
                                                record["date"] = date_val.toDatetime().strftime("%Y-%m-%d")
                                            else:
                                                record["date"] = str(date_val)
                                        for field in fields:
                                            if fieldData.hasElement(field):
                                                field_elem = fieldData.getElement(field)
                                                if not field_elem.isNull():
                                                    value = field_elem.getValue()
                                                    record[field] = value
                                        if record["date"]:
                                            records.append(record)
                                elif _debug:
                                    print(f"[BLPAPI] No fieldData element - response may be empty for this security")
                
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
        """Fetch reference/EOD data using blpapi"""
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
                            securityDataArray = msg.getElement("securityData")
                            for securityData in securityDataArray.values():
                                ticker = securityData.getElementAsString("security")
                                
                                # Check for security error
                                if securityData.hasElement("securityError"):
                                    err = securityData.getElement("securityError")
                                    result[ticker] = {
                                        "error": f"{err.getElementAsString('category')} - {err.getElementAsString('message')}"
                                    }
                                    continue
                                
                                result[ticker] = {}
                                
                                # Extract field values
                                fieldData = securityData.getElement("fieldData")
                                for field in fields:
                                    if fieldData.hasElement(field):
                                        field_elem = fieldData.getElement(field)
                                        if not field_elem.isNull():
                                            value = field_elem.getValue()
                                            result[ticker][field] = value
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
            
            return result
        
        finally:
            session.stop()

