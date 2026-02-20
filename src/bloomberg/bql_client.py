"""
Bloomberg Query Language (BQL) client implementation using PyBQL.
"""

import os
from typing import List, Dict, Any, Optional
import pandas as pd
try:
    import bql
    BQL_AVAILABLE = True
except ImportError:
    BQL_AVAILABLE = False
    bql = None

from .base_client import BloombergClientBase
from .field_mapper import get_bql_field_name, is_special_field


class BQLClient(BloombergClientBase):
    """
    Bloomberg Query Language (BQL) client using PyBQL.
    
    Note: BQL typically requires Bloomberg Terminal access or BQuant environment.
    Authentication is usually handled automatically by the Bloomberg infrastructure.
    """
    
    def __init__(self):
        """Initialize BQL client"""
        if not BQL_AVAILABLE:
            raise ImportError(
                "bql package not installed. BQL may require special setup through BQuant/Terminal."
            )
        
        try:
            self.bq = bql.Service()
            self._connected = True
        except Exception as e:
            self._connected = False
            raise ConnectionError(f"Failed to connect to BQL Service: {e}")
    
    def is_available(self) -> bool:
        """Check if BQL client is available and connected"""
        if not BQL_AVAILABLE:
            return False
        
        if not self._connected:
            return False
        
        try:
            # Try a simple request to verify connection
            test_request = bql.Request('AAPL US Equity', self.bq.data.id())
            self.bq.execute(test_request)
            return True
        except Exception:
            return False
    
    def _get_bql_data_item(self, field: str):
        """
        Get BQL data item for a field.
        
        Args:
            field: Bloomberg Terminal API field name (e.g., "PX_LAST")
            
        Returns:
            BQL data item
        """
        # Map field name to BQL data item name
        bql_field_name = get_bql_field_name(field)
        if not bql_field_name:
            raise ValueError(f"Field '{field}' cannot be mapped to BQL data item")
        
        # Get the data item from BQL service
        if hasattr(self.bq.data, bql_field_name):
            return getattr(self.bq.data, bql_field_name)()
        else:
            # Try alternative naming conventions
            # Remove common suffixes/prefixes
            field_variants = [
                bql_field_name,
                bql_field_name.replace("_", ""),
                bql_field_name.replace("px_", "px"),
            ]
            
            for variant in field_variants:
                if hasattr(self.bq.data, variant):
                    return getattr(self.bq.data, variant)()
            
            raise ValueError(
                f"BQL data item not found for field '{field}' (tried: {bql_field_name}, {field_variants})"
            )
    
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
        Fetch historical data using BQL.
        
        Args:
            ticker: Bloomberg ticker (e.g., "AAPL US Equity")
            fields: List of Bloomberg Terminal API field names (e.g., ["PX_LAST", "PX_VOLUME"])
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of records with date and field values (using original field names)
        """
        # Check for special fields that need different handling
        special_fields = [f for f in fields if is_special_field(f)]
        if special_fields:
            raise NotImplementedError(
                f"Special fields not yet implemented in BQL: {special_fields}. "
                f"These may require index membership calculations."
            )
        
        # Build data items dictionary
        # Use original field names as keys for output consistency
        data_items_dict = {}
        for field in fields:
            try:
                data_item = self._get_bql_data_item(field)
                
                # Apply date range if provided
                if start_date or end_date:
                    date_range = self.bq.func.range(
                        start_date if start_date else '-10y',  # Default to 10 years back
                        end_date if end_date else '0d'         # Default to today
                    )
                    data_item = data_item.with_updated_parameters(dates=date_range)
                
                data_items_dict[field] = data_item
            except Exception as e:
                raise ValueError(f"Error mapping field '{field}' to BQL: {e}")
        
        # Create and execute request
        request = bql.Request(ticker, data_items_dict)
        response = self.bq.execute(request)
        
        # Combine all responses into a single DataFrame
        dfs = []
        for item_response in response:
            dfs.append(item_response.df())
        
        # Merge DataFrames on ID and DATE (if multiple fields)
        if len(dfs) == 1:
            result_df = dfs[0]
        else:
            # Merge on common columns (ID, DATE, etc.)
            result_df = dfs[0]
            for df in dfs[1:]:
                # Merge on index (ID) and DATE column
                result_df = pd.merge(
                    result_df.reset_index(),
                    df.reset_index(),
                    on=['ID', 'DATE'] if 'DATE' in df.columns else ['ID'],
                    how='outer'
                )
        
        # Reset index to get ID as a column
        if isinstance(result_df.index, pd.MultiIndex):
            result_df = result_df.reset_index()
        
        # Convert to list of dictionaries with original field names
        records = []
        for _, row in result_df.iterrows():
            record = {}
            
            # Extract date
            if 'DATE' in row.index:
                date_val = row['DATE']
                if pd.notna(date_val):
                    if isinstance(date_val, pd.Timestamp):
                        record["date"] = date_val.strftime("%Y-%m-%d")
                    else:
                        record["date"] = str(date_val)
            
            # Extract field values (using original field names as keys)
            for field in fields:
                # BQL returns column names like "PX_LAST()" - try variations
                possible_cols = [
                    field + '()',
                    field,
                    get_bql_field_name(field) + '()',
                    get_bql_field_name(field),
                ]
                
                for col in possible_cols:
                    if col in row.index:
                        value = row[col]
                        if pd.notna(value):
                            record[field] = float(value) if isinstance(value, (int, float)) else value
                        break
            
            if "date" in record:
                records.append(record)
        
        return records
    
    def get_reference_data(
        self,
        tickers: List[str],
        fields: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch reference/EOD data using BQL.
        
        Args:
            tickers: List of Bloomberg tickers
            fields: List of Bloomberg Terminal API field names
            
        Returns:
            Dictionary mapping ticker to field values (using original field names)
        """
        # Check for special fields
        special_fields = [f for f in fields if is_special_field(f)]
        if special_fields:
            raise NotImplementedError(
                f"Special fields not yet implemented in BQL: {special_fields}"
            )
        
        # Build data items dictionary
        data_items_dict = {}
        for field in fields:
            try:
                data_item = self._get_bql_data_item(field)
                data_items_dict[field] = data_item
            except Exception as e:
                raise ValueError(f"Error mapping field '{field}' to BQL: {e}")
        
        # Create and execute request
        request = bql.Request(tickers, data_items_dict)
        response = self.bq.execute(request)
        
        # Combine responses
        dfs = []
        for item_response in response:
            dfs.append(item_response.df())
        
        # Merge DataFrames
        if len(dfs) == 1:
            result_df = dfs[0]
        else:
            result_df = dfs[0]
            for df in dfs[1:]:
                result_df = pd.merge(
                    result_df.reset_index(),
                    df.reset_index(),
                    on=['ID'],
                    how='outer'
                )
        
        # Reset index
        if isinstance(result_df.index, pd.MultiIndex):
            result_df = result_df.reset_index()
        
        # Convert to expected dictionary format
        result_dict = {}
        for _, row in result_df.iterrows():
            ticker = row.get('ID', '')
            result_dict[ticker] = {}
            
            for field in fields:
                # Try different column name variations
                possible_cols = [
                    field + '()',
                    field,
                    get_bql_field_name(field) + '()',
                    get_bql_field_name(field),
                ]
                
                for col in possible_cols:
                    if col in row.index:
                        value = row[col]
                        if pd.notna(value):
                            result_dict[ticker][field] = float(value) if isinstance(value, (int, float)) else value
                        break
        
        return result_dict

