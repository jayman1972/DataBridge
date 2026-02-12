"""
Field mapping between Bloomberg Terminal API fields and BQL data items.
"""

from typing import Optional

# Mapping from Bloomberg Terminal API field names to BQL data item names
# Note: Some fields might need special handling in BQL (e.g., index statistics)
FIELD_MAPPING = {
    # Standard price fields
    "PX_LAST": "px_last",
    "PX_OPEN": "px_open",
    "PX_HIGH": "px_high",
    "PX_LOW": "px_low",
    "PX_VOLUME": "px_volume",
    
    # RSI fields (may need calculation in BQL)
    "RSI 14D": "rsi_14d",  # Might need to calculate using func
    "RSI 30D": "rsi_30d",
    
    # Index membership statistics (these are complex in BQL)
    "PCT_MEMB_WITH_14D_RSI_GT_70": None,  # Needs special handling
    "PCT_MEMB_PX_ABV_UPPER_BOLL_BAND": None,  # Needs special handling
    "PCT_MEMBERS_WITH_NEW_52W_HIGHS": None,  # Needs special handling
    "PCT_MEMB_PX_BLW_LWR_BOLL_BAND": None,  # Needs special handling
    "PCT_MEMB_PX_GT_50D_MOV_AVG": None,  # Needs special handling
    "PCT_MEMB_PX_GT_10D_MOV_AVG": None,  # Needs special handling
    "PCT_MEMB_WITH_14D_RSI_LT_30": None,  # Needs special handling
}

def get_bql_field_name(terminal_field: str) -> Optional[str]:
    """
    Convert Bloomberg Terminal API field name to BQL data item name.
    
    Args:
        terminal_field: Bloomberg Terminal API field name (e.g., "PX_LAST")
        
    Returns:
        BQL data item name (e.g., "px_last") or None if not mappable
    """
    # Direct mapping
    if terminal_field in FIELD_MAPPING:
        return FIELD_MAPPING[terminal_field]
    
    # Try converting common patterns
    field_lower = terminal_field.lower()
    
    # PX_* fields -> px_*
    if field_lower.startswith("px_"):
        return field_lower
    
    # RSI fields (format: "RSI 14D" -> need to handle space and number)
    if "rsi" in field_lower:
        # Extract number and format as rsi_14d, rsi_30d, etc.
        import re
        match = re.search(r'(\d+)', terminal_field)
        if match:
            num = match.group(1)
            # BQL might use different naming, but try common patterns
            # Note: RSI might need to be calculated in BQL using functions
            return f"rsi_{num}d"
    
    # Default: try lowercase with underscores
    return field_lower.replace(" ", "_").replace("-", "_")

def is_special_field(terminal_field: str) -> bool:
    """
    Check if a field requires special handling (e.g., index statistics).
    
    Args:
        terminal_field: Bloomberg Terminal API field name
        
    Returns:
        True if field needs special handling
    """
    special_fields = [
        "PCT_MEMB_",
        "PCT_MEMBERS_",
    ]
    return any(terminal_field.startswith(sf) for sf in special_fields)

