"""
Fund Admin operations CSV reports (country breakdown, gross PnL by side, ETFs).

Mirrors legacy utilities scripts:
  get_country_breakdown.py
  get_gross_pnl_by_side.py
  get_etfs.py
"""

from __future__ import annotations

import csv
import io
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Tuple

REPORT_COUNTRY_BREAKDOWN = "country_breakdown"
REPORT_GROSS_PNL_BY_SIDE = "gross_pnl_by_side"
REPORT_ETFS = "etfs"

OPERATIONS_REPORT_TYPES = frozenset(
    {REPORT_COUNTRY_BREAKDOWN, REPORT_GROSS_PNL_BY_SIDE, REPORT_ETFS}
)

# PSC portfolios used by all three legacy scripts.
OPERATIONS_PORTFOLIOS: Tuple[str, ...] = (
    "EHP Adv Alt",
    "EHP Adv Intl Alt",
    "EHP Advantage Intl Alt",
    "EHP Found Alt",
    "EHP Found Intl Alt",
    "EHP Foundation Intl Alt",
    "EHP Global Arb",
    "EHP Select Alt",
    "EHP Advantage",
    "EHP Foundation Fund",
    "EHP GMS Fund",
    "EHP GMS Alt",
    "EHP GMS Alt Hedge",
    "EHP Strat Inc Alt",
    "EHP Global ESG Alt",
    "EHP Global ESG Alt H",
    "EHP Multi Asset Fund",
    "EHP Multi Asset Hedg",
    "The Soar Fund",
    "The Soar Fund Hedge",
    "EHP Tact Growth Alt",
    "EHP TactGrowth Alt H",
    "EHP Alpha",
    "EHP Alpha Hedge",
)

# Default ETF / levered product list from get_etfs.py (SECURITY column format).
DEFAULT_ETF_SECURITIES: Tuple[str, ...] = (
    "CJP.CA", "SPXS.US", "SPXL.US", "TZA.US", "TNA.US", "SHY.US", "TLT.US", "IEF.US",
    "XRE.CA", "IHYG.GB", "HYG.US", "EMB.US", "XIU.CA", "XEG.CA", "XFN.CA", "XGD.CA",
    "IYR.US", "SVXY.US", "TBT.US", "JNK.US", "QQQ.US", "HOU.CA", "HGD.CA", "IWM.US",
    "SPY.US", "GDXJ.US", "ERX.US", "ERY.US", "SQQQ.US", "XOP.US", "TVIX.US", "TVIXF.US",
    "VXX.US", "DIA.US", "EHF100I", "EHF200I", "EHF250I", "EHF300I", "EHF400I", "EHF500I",
    "EHF550I", "EHF600I", "EHF700I", "EHF800I", "ARKK.US", "EEM.US", "EWD.US", "EWG.US",
    "EWH.US", "EWJ.US", "EWL.US", "FXI.US", "MDY.US", "XLK.US", "XLV.US", "XLY.US",
    "USO.US", "JPST.US", "TQQQ.US", "DBA.US", "DBC.US", "GLD.US", "MYY.US", "NNRG.CA",
    "RWM.US", "SPXU.US", "VIXM.US", "VIXY.US", "XLF.US", "SARK.US", "EWC.US",
    "FXC.US", "DLR.CA", "XLE.US", "XLU.US", "XLP.US", "XLI.US", "XLB.US", "RCD.US",
    "SLX.US", "SOXX.US", "SKYY.US", "KWEB.US", "XBI.US", "XME.US", "SMH.US", "TBF.US",
    "ETHH.CA", "BNO.US", "COPX.US", "EFZ.US", "SLV.US", "URA.US", "U.U.CA", "URNM.US",
    "HOD.CA", "XHB.US", "EWA.US", "VIS.US", "SVIX.US", "YOLO.US", "MSOS.US", "ZAG.CA",
    "SPHB.US", "UVIX.US", "UUP.US", "SDS.US", "RSP.US", "IBIT.US", "UGA.US",
    "IYT.US", "MAGS.US", "BIL.US", "BILS.US", "SGOV.US", "SHV.US", "TBIL.US",
    "IYH.US", "IYM.US", "VCR.US", "XLB.CA", "PSQ.US", "ETHA.US", "IYK.US",
    "IHF.US", "VNQ.US", "EWZ.US", "EWZS.US", "KRE.US", "VB.US", "UNL.US", "SOLQ.CA",
    "ITB.US", "IDV.US", "VGLT.US", "TUA.US", "BWX.US", "BTAL.US", "PPH.US", "EFA.US",
    "GDX.US", "IVV.US", "VTWO.US", "USMV.US", "EWY.US", "IJR.US",
)

ETF_SECURITY_RE = re.compile(r"^[A-Z0-9]+(?:\.[A-Z0-9]+)*(?:\.[A-Z]{2,3})?$")

DEFAULT_OUTPUT_DIR = os.environ.get(
    "OPERATIONS_REPORTS_DIR",
    r"W:\Operations Reports",
)


def parse_etf_securities_text(text: str) -> List[str]:
    """Parse comma/newline separated SECURITY values (e.g. SPY.US, EHF100I)."""
    raw = re.split(r"[\s,]+", (text or "").strip())
    out: List[str] = []
    seen: set[str] = set()
    for token in raw:
        sym = token.strip().upper()
        if not sym or sym in seen:
            continue
        if not ETF_SECURITY_RE.match(sym):
            raise ValueError(
                f"Invalid security {sym!r}. Use TICKER.EXCHANGE (e.g. SPY.US) or internal codes like EHF100I."
            )
        seen.add(sym)
        out.append(sym)
    if not out:
        raise ValueError("At least one ETF security is required.")
    return out


def format_etf_securities_text(securities: Sequence[str]) -> str:
    return ", ".join(securities)


def _portfolio_placeholders(n: int) -> str:
    return ",".join("?" * n)


def _fetchall_dicts(cursor) -> Tuple[List[str], List[dict]]:
    columns = [d[0] for d in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return columns, rows


def _rows_to_csv(columns: Sequence[str], rows: Sequence[dict]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\r\n")
    writer.writerow(columns)
    for row in rows:
        writer.writerow([row.get(col) for col in columns])
    return buf.getvalue()


def _save_csv(csv_text: str, report_type: str, stamp: Optional[datetime] = None) -> str:
    stamp = stamp or datetime.now()
    out_dir = Path(DEFAULT_OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{report_type}_{stamp.strftime('%Y%m%d_%H%M%S')}.csv"
    path = out_dir / filename
    # UTF-8 BOM helps Excel open numeric/date columns reliably on Windows.
    path.write_text(csv_text, encoding="utf-8-sig", newline="")
    return str(path)


def _country_breakdown_sql() -> str:
    portfolio_ph = _portfolio_placeholders(len(OPERATIONS_PORTFOLIOS))
    return f"""
        SELECT
            POSN_DATE,
            PORTFOLIO,
            SECURITY_TYPE,
            COUNTRY,
            LONG_SHORT,
            SUM(VALUE) AS VALUE,
            SUM(REALIZED_PROFIT) as REALIZED,
            SUM(UNREALIZED_PROFIT) as UNREALIZED,
            SUM(INTEREST) as INTEREST,
            SUM(ACCR_INT) as ACCR_INT,
            SUM(DIVIDENDS) as DIVIDENDS,
            SUM(FEES) as FEES,
            SUM(DAY_PROFIT) as DAY_PROFIT
        FROM psc_position_history
        WHERE POSN_DATE >= ? AND POSN_DATE <= ?
        AND PORTFOLIO in ({portfolio_ph})
        GROUP BY
            POSN_DATE, PORTFOLIO, SECURITY_TYPE, COUNTRY, LONG_SHORT
        ORDER BY
            POSN_DATE, PORTFOLIO, SECURITY_TYPE, COUNTRY, LONG_SHORT
    """


def _gross_pnl_by_side_sql() -> str:
    portfolio_ph = _portfolio_placeholders(len(OPERATIONS_PORTFOLIOS))
    return f"""
        SELECT
            POSN_DATE,
            STRATEGY,
            PORTFOLIO,
            SUM((VALUE>=0)*VALUE) As [Long Market Value],
            SUM((VALUE<0)*VALUE) As [Short Market Value],
            SUM((VALUE>=0)*DAY_PROFIT) As [Long PnL],
            SUM((VALUE<0)*DAY_PROFIT) As [Short PnL],
            SUM((VALUE>=0)*EXPOSURE)/PORTFOLIO_NAV As [Long Exposure],
            SUM((VALUE<0)*EXPOSURE)/PORTFOLIO_NAV As [Short Exposure],
            SUM(EXPOSURE)/PORTFOLIO_NAV As [Net Exposure],
            (SUM((VALUE>=0)*EXPOSURE)-SUM((VALUE<0)*EXPOSURE))/PORTFOLIO_NAV As [Gross Exposure],
            PORTFOLIO_NAV
        FROM psc_position_history
        WHERE POSN_DATE >= ? AND POSN_DATE <= ?
        AND PORTFOLIO in ({portfolio_ph})
        GROUP BY POSN_DATE, PORTFOLIO, STRATEGY
        ORDER BY POSN_DATE, PORTFOLIO, STRATEGY
    """


def _etfs_sql(etf_count: int) -> str:
    portfolio_ph = _portfolio_placeholders(len(OPERATIONS_PORTFOLIOS))
    etf_ph = _portfolio_placeholders(etf_count)
    return f"""
        SELECT POSN_DATE, SECURITY, DESCRIPTION, QUANTITY,
               VALUE / FX_SETTLE_TO_BASE as LOCAL_VALUE, VALUE, PORTFOLIO, ACCOUNT
        FROM psc_position_history
        WHERE PORTFOLIO in ({portfolio_ph})
        AND POSN_DATE >= ? AND POSN_DATE <= ?
        AND SECURITY in ({etf_ph})
        ORDER BY SECURITY, PORTFOLIO, POSN_DATE
    """


def run_operations_report(
    cursor,
    report_type: str,
    start_compact: str,
    end_compact: str,
    etf_securities: Optional[Sequence[str]] = None,
    *,
    save_to_disk: bool = True,
) -> dict[str, Any]:
    if report_type not in OPERATIONS_REPORT_TYPES:
        raise ValueError(f"Unknown report_type: {report_type}")

    params_base: List[Any] = [start_compact, end_compact, *OPERATIONS_PORTFOLIOS]

    if report_type == REPORT_COUNTRY_BREAKDOWN:
        cursor.execute(_country_breakdown_sql(), params_base)
    elif report_type == REPORT_GROSS_PNL_BY_SIDE:
        cursor.execute(_gross_pnl_by_side_sql(), params_base)
    else:
        securities = list(etf_securities or DEFAULT_ETF_SECURITIES)
        cursor.execute(
            _etfs_sql(len(securities)),
            [*OPERATIONS_PORTFOLIOS, start_compact, end_compact, *securities],
        )

    columns, rows = _fetchall_dicts(cursor)
    csv_text = _rows_to_csv(columns, rows)
    saved_path: Optional[str] = None
    if save_to_disk:
        saved_path = _save_csv(csv_text, report_type)

    return {
        "report_type": report_type,
        "start_date": start_compact,
        "end_date": end_compact,
        "row_count": len(rows),
        "columns": columns,
        "csv": csv_text,
        "filename": f"{report_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        "saved_path": saved_path,
    }
