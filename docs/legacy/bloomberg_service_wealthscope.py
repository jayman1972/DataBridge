#!/usr/bin/env python3
"""
Complete Bloomberg Bridge Service with both quotes and historical data endpoints
"""

import sys
import json
import logging
import re
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
import blpapi

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Bloomberg session
session = None
service = None

def initialize_bloomberg():
    """Initialize Bloomberg session and service"""
    global session, service
    
    try:
        # Create and start session
        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost("localhost")
        sessionOptions.setServerPort(8194)
        
        session = blpapi.Session(sessionOptions)
        
        if not session.start():
            logger.error("Failed to start Bloomberg session")
            return False
            
        if not session.openService("//blp/refdata"):
            logger.error("Failed to open //blp/refdata service")
            return False
            
        service = session.getService("//blp/refdata")
        logger.info("Bloomberg session initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing Bloomberg: {e}")
        return False

def get_realtime_quotes(symbols, security_types=None):
    """Get real-time quotes for symbols
    
    Args:
        symbols: List of Bloomberg symbols
        security_types: Optional list of security types (same length as symbols)
    """
    quotes = {}
    errors = []
    
    if not session or not service:
        errors.append("Bloomberg session not initialized")
        return quotes, errors
    
    # If security_types not provided, default to empty list
    if security_types is None:
        security_types = [''] * len(symbols)
    
    try:
        # Create request
        request = service.createRequest("ReferenceDataRequest")
        
        # Add securities
        for symbol in symbols:
            request.getElement("securities").appendValue(symbol)
        
        # Add fields for real-time quotes
        fields = [
            "PX_LAST",           # Last price (for real-time)
            "PX_BID",            # Bid price
            "PX_ASK",            # Ask price
            "PREV_CLOSE_VAL",    # Previous close value
            "VOLUME",            # Volume
            "PX_OPEN",           # Open price
            "PX_HIGH",           # High price
            "PX_LOW",            # Low price
            "CHG_PCT_1D",        # 1-day change percent
            "CHG_NET_1D",        # 1-day change amount
            "CRNCY",             # Currency
            "NAME",              # Security name
            "ID_EXCH_SYMBOL",    # Exchange symbol
        ]
        
        for field in fields:
            request.getElement("fields").appendValue(field)
        
        # Send request
        session.sendRequest(request)
        
        # Process response
        while True:
            event = session.nextEvent(500)
            
            if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                for msg in event:
                    logger.info(f"Bloomberg response for {msg.correlationId()}: {msg}")
                    
                    if msg.hasElement("securityData"):
                        securityDataArray = msg.getElement("securityData")
                        
                        for i in range(securityDataArray.numValues()):
                            securityData = securityDataArray.getValue(i)
                            security = securityData.getElementAsString("security")
                            
                            # Determine if this is a Canadian option for real-time pricing
                            # Check for .TO suffix, Canadian exchange keywords, or " CN " in Bloomberg format
                            is_canadian_option = (security.endswith('.TO') or 
                                                 'TORONTO' in security.upper() or 
                                                 'TSX' in security.upper() or
                                                 ' CN ' in security.upper())
                            
                            # Always check for fieldData first (even if some fields have exceptions)
                            if securityData.hasElement("fieldData"):
                                fieldData = securityData.getElement("fieldData")
                                
                                # For real-time pricing:
                                # - US options: use PX_LAST
                                # - Canadian options: use PX_BID
                                if is_canadian_option and fieldData.hasElement("PX_BID"):
                                    current_price = fieldData.getElementAsFloat("PX_BID")
                                elif fieldData.hasElement("PX_LAST"):
                                    current_price = fieldData.getElementAsFloat("PX_LAST")
                                else:
                                    current_price = None
                                
                                # Extract quote data (each field is optional)
                                quote = {
                                    "symbol": security,
                                    "last_price": current_price,
                                    "bid": fieldData.getElementAsFloat("PX_BID") if fieldData.hasElement("PX_BID") else None,
                                    "ask": fieldData.getElementAsFloat("PX_ASK") if fieldData.hasElement("PX_ASK") else None,
                                    "close_price": fieldData.getElementAsFloat("PREV_CLOSE_VAL") if fieldData.hasElement("PREV_CLOSE_VAL") else None,
                                    "volume": fieldData.getElementAsInteger("VOLUME") if fieldData.hasElement("VOLUME") else None,
                                    "open_price": fieldData.getElementAsFloat("PX_OPEN") if fieldData.hasElement("PX_OPEN") else None,
                                    "high_price": fieldData.getElementAsFloat("PX_HIGH") if fieldData.hasElement("PX_HIGH") else None,
                                    "low_price": fieldData.getElementAsFloat("PX_LOW") if fieldData.hasElement("PX_LOW") else None,
                                    "change_percent": fieldData.getElementAsFloat("CHG_PCT_1D") if fieldData.hasElement("CHG_PCT_1D") else None,
                                    "change_amount": fieldData.getElementAsFloat("CHG_NET_1D") if fieldData.hasElement("CHG_NET_1D") else None,
                                    "currency": fieldData.getElementAsString("CRNCY") if fieldData.hasElement("CRNCY") else None,
                                    "name": fieldData.getElementAsString("NAME") if fieldData.hasElement("NAME") else None,
                                    "exchange": fieldData.getElementAsString("ID_EXCH_SYMBOL") if fieldData.hasElement("ID_EXCH_SYMBOL") else None,
                                }
                                
                                quotes[security] = quote
                                logger.info(f"Quote for {security}: {quote}")
                            
                            # Log field exceptions as warnings (but don't skip the quote!)
                            if securityData.hasElement("fieldExceptions"):
                                fieldExceptions = securityData.getElement("fieldExceptions")
                                for j in range(fieldExceptions.numValues()):
                                    fieldException = fieldExceptions.getValue(j)
                                    fieldId = fieldException.getElementAsString("fieldId")
                                    errorInfo = fieldException.getElement("errorInfo")
                                    errorMessage = errorInfo.getElementAsString("message")
                                    logger.warning(f"Field exception for {security}.{fieldId}: {errorMessage}")
                            
                            # Only treat as error if there's a security error (no fieldData at all)
                            if securityData.hasElement("securityError"):
                                securityError = securityData.getElement("securityError")
                                errorMessage = securityError.getElementAsString("message")
                                errors.append(f"{security}: {errorMessage}")
                                logger.warning(f"Security error for {security}: {errorMessage}")
            
            if event.eventType() == blpapi.Event.RESPONSE:
                break
                
    except Exception as e:
        logger.error(f"Error getting quotes: {e}")
        errors.append(f"Error getting quotes: {str(e)}")
    
    return quotes, errors

def get_historical_data(symbols, start_date, end_date, security_types=None, fields=None):
    """Get historical data for symbols
    
    Args:
        symbols: List of Bloomberg symbols
        start_date: Start date for historical data (None means fetch all available)
        end_date: End date for historical data (None means use today)
        security_types: Optional list of security types (same length as symbols)
        fields: Optional list of Bloomberg fields to request (defaults to OHLC)
    """
    historical_data = {}
    errors = []
    
    if not session or not service:
        errors.append("Bloomberg session not initialized")
        return historical_data, errors
    
    # If security_types not provided, default to empty list
    if security_types is None:
        security_types = [''] * len(symbols)
    
    # Default fields for OHLC data if not provided
    if fields is None:
        fields = ["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST", "PX_VOLUME"]
    
    # Set default dates if None (Bloomberg API requires dates)
    # If start_date is None, use Jan 1, 1983 to get all available data
    if start_date is None:
        start_date = datetime(1983, 1, 1)
    # If end_date is None, use today
    if end_date is None:
        end_date = datetime.now()
    
    try:
        for idx, symbol in enumerate(symbols):
            try:
                # Get security type for this symbol (if provided)
                security_type = security_types[idx] if idx < len(security_types) else ''
                
                # Determine if this is a Canadian symbol (ends with .TO, contains Canadian exchange, or " CN " in Bloomberg format)
                is_canadian = (symbol.endswith('.TO') or 
                              'TORONTO' in symbol.upper() or 
                              'TSX' in symbol.upper() or
                              ' CN ' in symbol.upper())
                
                # Determine if this is an option based on security_type OR symbol pattern (has date and strike)
                # Symbol pattern: options typically have dates like "03/20/26" and strikes like "C8.50" or "P7.50"
                is_option_by_type = security_type and 'option' in security_type.lower()
                is_option_by_pattern = bool(re.search(r'\d{2}/\d{2}/\d{2}', symbol)) and bool(re.search(r'[CP]\d+\.?\d*', symbol))
                is_option = is_option_by_type or is_option_by_pattern
                
                # Canadian options need PX_OFFICIAL_CLOSE
                is_canadian_option = is_option and is_canadian
                
                # For options, use different fields based on exchange:
                # - Canadian options: PX_OFFICIAL_CLOSE for historical data
                # - US options: PX_LAST for historical data
                request_fields = fields.copy()
                if is_option and "PX_LAST" in request_fields:
                    if is_canadian_option:
                        # Canadian options use PX_OFFICIAL_CLOSE for historical data
                        request_fields = ["PX_OFFICIAL_CLOSE" if f == "PX_LAST" else f for f in request_fields]
                    else:
                        # US options use PX_LAST for historical data
                        # Keep PX_LAST as is
                        pass
                
                logger.info(f"Getting historical data for {symbol}, security_type='{security_type}', is_option={is_option}, is_canadian_option={is_canadian_option}, fields={request_fields}")
                
                # Create request
                request = service.createRequest("HistoricalDataRequest")
                request.append("securities", symbol)
                
                # Add all requested fields
                for field in request_fields:
                    request.append("fields", field)
                request.set("startDate", start_date.strftime("%Y%m%d"))
                request.set("endDate", end_date.strftime("%Y%m%d"))
                request.set("periodicitySelection", "DAILY")
                
                # Send request
                session.sendRequest(request)
                
                # Process response
                while True:
                    event = session.nextEvent(500)
                    
                    if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                        for msg in event:
                            logger.info(f"Bloomberg historical response for {symbol}: {msg}")
                            
                            if msg.hasElement("securityData"):
                                securityData = msg.getElement("securityData")
                                
                                # Check for security error first (can exist even with fieldData)
                                if securityData.hasElement("securityError"):
                                    securityError = securityData.getElement("securityError")
                                    errorMessage = securityError.getElementAsString("message")
                                    errors.append(f"{symbol}: {errorMessage}")
                                    logger.warning(f"Historical security error for {symbol}: {errorMessage}")
                                
                                # Check if fieldData exists and has records (regardless of empty fieldExceptions)
                                if securityData.hasElement("fieldData"):
                                    fieldData = securityData.getElement("fieldData")
                                    
                                    # Only process if there are actual data points
                                    if fieldData.numValues() > 0:
                                        symbol_data = []
                                        for i in range(fieldData.numValues()):
                                            dataPoint = fieldData.getValue(i)
                                            date = dataPoint.getElementAsDatetime("date")
                                            
                                            # Extract all available fields
                                            record = {
                                                "date": date.strftime("%Y-%m-%d") if hasattr(date, 'strftime') else str(date)
                                            }
                                            
                                            # Extract OHLC and volume fields (for backward compatibility)
                                            if dataPoint.hasElement("PX_OPEN"):
                                                record["open_price"] = dataPoint.getElementAsFloat("PX_OPEN")
                                            if dataPoint.hasElement("PX_HIGH"):
                                                record["high_price"] = dataPoint.getElementAsFloat("PX_HIGH")
                                            if dataPoint.hasElement("PX_LOW"):
                                                record["low_price"] = dataPoint.getElementAsFloat("PX_LOW")
                                            
                                            # Close price (PX_LAST or PX_OFFICIAL_CLOSE for options)
                                            if dataPoint.hasElement("PX_OFFICIAL_CLOSE"):
                                                record["close_price"] = dataPoint.getElementAsFloat("PX_OFFICIAL_CLOSE")
                                                record["adjusted_close"] = dataPoint.getElementAsFloat("PX_OFFICIAL_CLOSE")
                                            elif dataPoint.hasElement("PX_LAST"):
                                                record["close_price"] = dataPoint.getElementAsFloat("PX_LAST")
                                                record["adjusted_close"] = dataPoint.getElementAsFloat("PX_LAST")
                                            
                                            if dataPoint.hasElement("PX_VOLUME"):
                                                record["volume"] = dataPoint.getElementAsInteger("PX_VOLUME")
                                            
                                            # Extract ALL requested fields generically (not just OHLC)
                                            # This handles custom fields like RSI, PCT_MEMB_*, etc.
                                            for requested_field in fields:
                                                if dataPoint.hasElement(requested_field):
                                                    try:
                                                        # Try to get as float first (most fields are numeric)
                                                        field_value = dataPoint.getElementAsFloat(requested_field)
                                                        # Store with original field name
                                                        record[requested_field] = field_value
                                                    except:
                                                        try:
                                                            # If not float, try as string
                                                            field_value = dataPoint.getElementAsString(requested_field)
                                                            record[requested_field] = field_value
                                                        except:
                                                            # If neither works, skip this field
                                                            logger.warning(f"Could not extract field {requested_field} for {symbol}")
                                            
                                            # Only add record if we have at least a close price or any other field
                                            if "close_price" in record or len(record) > 1:  # More than just date
                                                symbol_data.append(record)
                                        
                                        if symbol_data:  # Only add if we got data
                                            historical_data[symbol] = symbol_data
                                            logger.info(f"Historical data for {symbol}: {len(symbol_data)} records")
                                        else:
                                            logger.warning(f"No valid price data for {symbol}")
                                
                                # Check for field exceptions (can exist alongside fieldData)
                                if securityData.hasElement("fieldExceptions"):
                                    fieldExceptions = securityData.getElement("fieldExceptions")
                                    for j in range(fieldExceptions.numValues()):
                                        fieldException = fieldExceptions.getValue(j)
                                        errorInfo = fieldException.getElement("errorInfo")
                                        errorMessage = errorInfo.getElementAsString("message")
                                        errors.append(f"{symbol}: {errorMessage}")
                                        logger.warning(f"Historical field exception for {symbol}: {errorMessage}")
                    
                    if event.eventType() == blpapi.Event.RESPONSE:
                        break
                        
            except Exception as e:
                logger.error(f"Error getting historical data for {symbol}: {e}")
                errors.append(f"{symbol}: {str(e)}")
                
    except Exception as e:
        logger.error(f"Error in historical data request: {e}")
        errors.append(f"Error in historical data request: {str(e)}")
    
    return historical_data, errors

def get_reference_data(symbols, fields):
    """Get reference/EOD data for symbols using ReferenceDataRequest
    
    Args:
        symbols: List of Bloomberg symbols
        fields: List of Bloomberg fields to request (e.g., PX_LAST_EOD, LAST_UPDATE_DATE_EOD)
    """
    reference_data = {}
    errors = []
    
    if not session or not service:
        errors.append("Bloomberg session not initialized")
        return reference_data, errors
    
    try:
        # Create request
        request = service.createRequest("ReferenceDataRequest")
        
        # Add securities
        for symbol in symbols:
            request.getElement("securities").appendValue(symbol)
        
        # Add fields
        for field in fields:
            request.getElement("fields").appendValue(field)
        
        # Send request
        session.sendRequest(request)
        
        # Process response
        while True:
            event = session.nextEvent(500)
            
            if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                for msg in event:
                    logger.info(f"Bloomberg reference response: {msg}")
                    
                    if msg.hasElement("securityData"):
                        securityDataArray = msg.getElement("securityData")
                        
                        for i in range(securityDataArray.numValues()):
                            securityData = securityDataArray.getValue(i)
                            security = securityData.getElementAsString("security")
                            
                            if securityData.hasElement("fieldData"):
                                fieldData = securityData.getElement("fieldData")
                                
                                # Extract date from LAST_UPDATE_DATE_EOD if available, otherwise use current date
                                date_str = None
                                if fieldData.hasElement("LAST_UPDATE_DATE_EOD"):
                                    try:
                                        date_value = fieldData.getElementAsDatetime("LAST_UPDATE_DATE_EOD")
                                        if hasattr(date_value, 'strftime'):
                                            date_str = date_value.strftime("%Y-%m-%d")
                                        else:
                                            date_str = str(date_value)
                                    except:
                                        date_str = datetime.now().strftime("%Y-%m-%d")
                                else:
                                    # If no date field, use today's date
                                    date_str = datetime.now().strftime("%Y-%m-%d")
                                
                                # Extract all requested fields
                                record = {
                                    "date": date_str
                                }
                                
                                for field in fields:
                                    if fieldData.hasElement(field):
                                        try:
                                            # Try to get as float first
                                            field_value = fieldData.getElementAsFloat(field)
                                            record[field] = field_value
                                        except:
                                            try:
                                                # If not float, try as string
                                                field_value = fieldData.getElementAsString(field)
                                                record[field] = field_value
                                            except:
                                                # If neither works, try as datetime
                                                try:
                                                    field_value = fieldData.getElementAsDatetime(field)
                                                    if hasattr(field_value, 'strftime'):
                                                        record[field] = field_value.strftime("%Y-%m-%d")
                                                    else:
                                                        record[field] = str(field_value)
                                                except:
                                                    logger.warning(f"Could not extract field {field} for {security}")
                                
                                # Store record (ReferenceDataRequest typically returns single record per symbol)
                                if security not in reference_data:
                                    reference_data[security] = []
                                reference_data[security].append(record)
                                logger.info(f"Reference data for {security}: {record}")
                            
                            # Log field exceptions as warnings
                            if securityData.hasElement("fieldExceptions"):
                                fieldExceptions = securityData.getElement("fieldExceptions")
                                for j in range(fieldExceptions.numValues()):
                                    fieldException = fieldExceptions.getValue(j)
                                    fieldId = fieldException.getElementAsString("fieldId")
                                    errorInfo = fieldException.getElement("errorInfo")
                                    errorMessage = errorInfo.getElementAsString("message")
                                    logger.warning(f"Field exception for {security}.{fieldId}: {errorMessage}")
                            
                            # Check for security errors
                            if securityData.hasElement("securityError"):
                                securityError = securityData.getElement("securityError")
                                errorMessage = securityError.getElementAsString("message")
                                errors.append(f"{security}: {errorMessage}")
                                logger.warning(f"Security error for {security}: {errorMessage}")
            
            if event.eventType() == blpapi.Event.RESPONSE:
                break
                
    except Exception as e:
        logger.error(f"Error getting reference data: {e}")
        errors.append(f"Error getting reference data: {str(e)}")
    
    return reference_data, errors

@app.route('/quotes', methods=['POST'])
def quotes():
    """Handle quotes requests"""
    try:
        data = request.get_json()
        symbols = data.get('symbols', [])
        security_types = data.get('security_types', [])  # Get security types from request
        
        logger.info("=" * 80)
        logger.info(f"[QUOTES REQUEST] Received at {datetime.now().isoformat()}")
        logger.info(f"[QUOTES REQUEST] Symbols ({len(symbols)}): {symbols}")
        logger.info(f"[QUOTES REQUEST] Security types: {security_types}")
        logger.info(f"[QUOTES REQUEST] Request details: {json.dumps(data, indent=2)}")
        
        quotes, errors = get_realtime_quotes(symbols, security_types)
        
        response = {
            "quotes": quotes,
            "errors": errors,
            "timestamp": datetime.now().isoformat(),
            "source": "bloomberg"
        }
        
        logger.info(f"[QUOTES RESPONSE] Returning {len(quotes)} quotes and {len(errors)} errors")
        if quotes:
            logger.info(f"[QUOTES RESPONSE] Quote symbols: {list(quotes.keys())}")
            for symbol, quote_data in list(quotes.items())[:5]:  # Log first 5 quotes
                logger.info(f"[QUOTES RESPONSE] {symbol}: {json.dumps(quote_data, indent=2, default=str)}")
        if errors:
            logger.warning(f"[QUOTES RESPONSE] Errors: {errors}")
        logger.info("=" * 80)
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error in quotes endpoint: {e}")
        return jsonify({
            "quotes": {},
            "errors": [f"Error processing request: {str(e)}"],
            "timestamp": datetime.now().isoformat(),
            "source": "bloomberg"
        }), 500

@app.route('/historical', methods=['POST'])
def historical():
    """Handle historical data requests"""
    try:
        data = request.get_json()
        symbols = data.get('symbols', [])
        security_types = data.get('security_types', [])  # Get security types from request
        fields = data.get('fields', None)  # Get fields from request
        start_date_str = data.get('start_date')
        end_date_str = data.get('end_date')
        
        logger.info("=" * 80)
        logger.info(f"[HISTORICAL REQUEST] Received at {datetime.now().isoformat()}")
        logger.info(f"[HISTORICAL REQUEST] Symbols ({len(symbols)}): {symbols}")
        logger.info(f"[HISTORICAL REQUEST] Date range: {start_date_str} to {end_date_str}")
        logger.info(f"[HISTORICAL REQUEST] Security types: {security_types}")
        logger.info(f"[HISTORICAL REQUEST] Fields: {fields}")
        logger.info(f"[HISTORICAL REQUEST] Full request: {json.dumps(data, indent=2)}")
        
        # Parse dates - only if provided (None means fetch all available data)
        start_date = None
        end_date = None
        if start_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        if end_date_str:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        
        historical_data, errors = get_historical_data(symbols, start_date, end_date, security_types, fields)
        
        response = {
            "historical_data": historical_data,
            "errors": errors,
            "timestamp": datetime.now().isoformat(),
            "source": "bloomberg"
        }
        
        logger.info(f"[HISTORICAL RESPONSE] Returning data for {len(historical_data)} symbols and {len(errors)} errors")
        if historical_data:
            for symbol, records in list(historical_data.items())[:3]:  # Log first 3 symbols
                logger.info(f"[HISTORICAL RESPONSE] {symbol}: {len(records)} records")
                if records:
                    logger.info(f"[HISTORICAL RESPONSE] {symbol} sample (first record): {json.dumps(records[0], indent=2, default=str)}")
        if errors:
            logger.warning(f"[HISTORICAL RESPONSE] Errors: {errors}")
        logger.info("=" * 80)
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error in historical endpoint: {e}")
        return jsonify({
            "historical_data": {},
            "errors": [f"Error processing request: {str(e)}"],
            "timestamp": datetime.now().isoformat(),
            "source": "bloomberg"
        }), 500

@app.route('/reference', methods=['POST'])
def reference():
    """Handle reference/EOD data requests using ReferenceDataRequest"""
    try:
        data = request.get_json()
        symbols = data.get('symbols', [])
        fields = data.get('fields', [])
        
        logger.info(f"Received reference request for {len(symbols)} symbols: {symbols}")
        logger.info(f"Fields: {fields}")
        
        reference_data, errors = get_reference_data(symbols, fields)
        
        response = {
            "reference_data": reference_data,
            "errors": errors,
            "timestamp": datetime.now().isoformat(),
            "source": "bloomberg"
        }
        
        logger.info(f"Returning reference data for {len(reference_data)} symbols and {len(errors)} errors")
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error in reference endpoint: {e}")
        return jsonify({
            "reference_data": {},
            "errors": [f"Error processing request: {str(e)}"],
            "timestamp": datetime.now().isoformat(),
            "source": "bloomberg"
        }), 500

@app.before_request
def log_request_info():
    """Log all incoming requests"""
    logger.info(f"[INCOMING REQUEST] {request.method} {request.path}")
    logger.info(f"[INCOMING REQUEST] Remote address: {request.remote_addr}")
    logger.info(f"[INCOMING REQUEST] Headers: {dict(request.headers)}")
    if request.is_json:
        try:
            data = request.get_json()
            logger.info(f"[INCOMING REQUEST] JSON body: {json.dumps(data, indent=2)}")
        except:
            logger.info(f"[INCOMING REQUEST] JSON body: (could not parse)")

@app.after_request
def log_response_info(response):
    """Log all outgoing responses"""
    logger.info(f"[OUTGOING RESPONSE] {request.method} {request.path} - Status: {response.status_code}")
    return response

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    bloomberg_status = session is not None and service is not None
    status_msg = "healthy" if bloomberg_status else "unhealthy (Bloomberg not connected)"
    
    logger.info(f"[HEALTH CHECK] Status: {status_msg}, Bloomberg connected: {bloomberg_status}")
    
    return jsonify({
        "status": status_msg,
        "bloomberg_connected": bloomberg_status,
        "timestamp": datetime.now().isoformat()
    })

if __name__ == '__main__':
    logger.info("Starting Bloomberg Bridge Service...")
    
    if not initialize_bloomberg():
        logger.error("Failed to initialize Bloomberg session")
        sys.exit(1)
    
    logger.info("Bloomberg Bridge Service started successfully")
    app.run(host='0.0.0.0', port=5000, debug=True)

