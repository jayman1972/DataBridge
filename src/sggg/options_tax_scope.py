"""Select options by standardized identity when AlphaDesk labels them Futures."""

# MySQL-compatible regular expression. A dated C/P code, strike and asset class
# distinguish an option from an outright future. Keep ordinary equity options.
FUTURES_OPTION_TICKER_PATTERN = (
    r"^[A-Z]{1,4}[FGHJKMNQUVXZ][0-9]{1,2}[CP][ ]+[0-9]+([.][0-9]+)?"
    r"([ ]+(US|COMB|PIT))?[ ]+(COMDTY|INDEX)$"
)
OPTION_TRADE_SCOPE = (
    "(SECURITY_TYPE = 'EquityOption' OR UPPER(TRIM(BBG_TICKER)) REGEXP '"
    + FUTURES_OPTION_TICKER_PATTERN
    + "')"
)
