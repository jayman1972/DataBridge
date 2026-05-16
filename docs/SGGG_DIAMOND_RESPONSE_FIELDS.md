# SGGG Diamond API — response fields (from vendor XML samples)

Leaf element names extracted from the checked-in samples under [vendor/](./vendor/). For full definitions and additional endpoints, use **[Diamond API Specifications v1.17.docx](./vendor/Diamond%20API%20Specifications%20v1.17.docx)**.

## GetPortfolio (`GetPortfolio_v1.14.xml`)

**Header:** `ManagementCompanyCode`, `FundParent`, `ReferenceDate`, `ValuationDate`

**Per position (`PortfolioRecordDetails`):**

| Field | Field | Field |
|-------|-------|-------|
| LongShort | Currency | SecurityID |
| SecurityParentID | SecurityName | PrimaryBBGID |
| CompositeBBGID | UnderlyingBBGID | UnderlyingSecurity |
| CUSIP | ISIN | SEDOL |
| Quantity | QuantityMultiplier | LocalCostPerShare |
| LocalBookValue | BaseBookValue | PricingPolicy |
| PriceSource | PricingTicker | PricingDataSource |
| QuoteDate | QuoteType | QuoteCurrency |
| PreDiscountPrice | PriceDiscount | PortfolioPrice |
| PriceMultiplier | LocalMarketValue | BaseMarketValue |
| LocalTotalUnrealizedGainLoss | BaseTotalUnrealizedGainLoss | BaseUnrealizedGainLossDueToPriceChange |
| BaseUnrealizedGainLossDueToFX | BaseUnrealizedGainLossSinceReferenceDate | BaseUnrealizedGainLossSinceReferenceDateDueToPriceChange |
| BaseUnrealizedGainLossSinceReferenceDateDueToFX | BaseRealizedGainLossSinceReferenceDate | BaseRealizedGainLossSinceReferenceDateDueToPriceChange |
| BaseRealizedGainLossSinceReferenceDateDueToFX | BaseDividendIncomeSinceReferenceDate | BaseInterestIncomeSinceReferenceDate |
| BaseWithholdingTaxSinceReferenceDate | | |

### Bridge mapping (current app)

See `market-dashboard/docs/SGGG_API.md` — e.g. `PrimaryBBGID` / `PricingTicker` → `bbg_ticker`, `Quantity` → `quantity`, `LocalMarketValue` / `BaseMarketValue` → `value` / `exposure`.

## GetPortfolioTrades (`GetPortfolioTrades_v1.14.xml`)

**Per trade (`PortfolioTrade`):**

| Field | Field | Field |
|-------|-------|-------|
| FundParentID | TradeID | TradeParentID |
| ValuationDate | TradeDate | SettlementDate |
| TradeType | SecurityID | SecurityParentID |
| SecurityName | SecurityCurrency | BBGID |
| ISIN | CUSIP | Ticker |
| PriceMultiplier | QuantityMultiplier | Quantity |
| QuantityBalance | LocalAmountPerShare | LocalNetAmount |
| BaseNetAmount | LocalCommission | BaseCommission |
| LocalAccruedInterest | BaseAccruedInterest | LocalBookValue |
| BaseBookValue | LocalRealizedGainLoss | BaseRealizedGainLoss |
| IsCancel | IsReversal | IsBackDated |

Not yet stored in `dashboard_portfolio`; planned for Fund Admin trades view.
