"""Three-page, letter-size SOAR risk report matching the legacy print layout."""

from __future__ import annotations

import io
import math
from datetime import date
from pathlib import Path
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas

PAGE_WIDTH, PAGE_HEIGHT = letter
BLUE = colors.HexColor("#1F4E79")
DARK = colors.HexColor("#454545")
GRID = colors.HexColor("#B7B7B7")
STRIPE = colors.HexColor("#EEEEEE")
ORANGE = colors.HexColor("#FFCC99")
CYAN = colors.HexColor("#00A8E1")
FONT = "Helvetica"
FONT_BOLD = "Helvetica-Bold"


def _register_fonts() -> None:
    global FONT, FONT_BOLD
    regular = Path(r"C:\Windows\Fonts\calibri.ttf")
    bold = Path(r"C:\Windows\Fonts\calibrib.ttf")
    if regular.is_file() and bold.is_file():
        if "SoarCalibri" not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont("SoarCalibri", str(regular)))
        if "SoarCalibri-Bold" not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont("SoarCalibri-Bold", str(bold)))
        FONT = "SoarCalibri"
        FONT_BOLD = "SoarCalibri-Bold"


def _text(
    pdf: canvas.Canvas, x: float, top: float, value: object, *,
    size: float = 9, bold: bool = False, color: colors.Color = DARK,
    align: str = "left",
) -> None:
    pdf.setFont(FONT_BOLD if bold else FONT, size)
    pdf.setFillColor(color)
    text = str(value)
    y = PAGE_HEIGHT - top
    if align == "right":
        pdf.drawRightString(x, y, text)
    elif align == "center":
        pdf.drawCentredString(x, y, text)
    else:
        pdf.drawString(x, y, text)


def _title(pdf: canvas.Canvas, x: float, top: float, value: str, size: float = 10) -> None:
    _text(pdf, x, top, value, size=size, bold=True, color=BLUE)


def _line(pdf: canvas.Canvas, x1: float, top1: float, x2: float, top2: float) -> None:
    pdf.setStrokeColor(GRID)
    pdf.setLineWidth(0.55)
    pdf.line(x1, PAGE_HEIGHT - top1, x2, PAGE_HEIGHT - top2)


def _box(pdf: canvas.Canvas, x: float, top: float, width: float, height: float) -> None:
    pdf.setStrokeColor(GRID)
    pdf.setLineWidth(0.55)
    pdf.rect(x, PAGE_HEIGHT - top - height, width, height, stroke=1, fill=0)


def _table(
    pdf: canvas.Canvas, x: float, top: float, widths: list[float],
    rows: list[list[object]], *, row_height: float = 12.6,
    alignments: list[str] | None = None, striped: bool = True,
    orange_columns: set[int] | None = None,
) -> None:
    right = x + sum(widths)
    for row_index, row in enumerate(rows):
        row_top = top + row_index * row_height
        if striped and row_index > 0 and row_index % 2 == 1:
            pdf.setFillColor(STRIPE)
            pdf.rect(x, PAGE_HEIGHT - row_top - row_height, right - x, row_height, stroke=0, fill=1)
        cursor = x
        for col_index, width in enumerate(widths):
            if orange_columns and col_index in orange_columns and row_index > 0:
                pdf.setFillColor(ORANGE)
                pdf.rect(cursor, PAGE_HEIGHT - row_top - row_height, width, row_height, stroke=0, fill=1)
            value = row[col_index] if col_index < len(row) else ""
            text = "" if value is None else str(value)
            if text:
                pdf.setFont(FONT, 8.8)
                max_width = width - 4
                while text and pdfmetrics.stringWidth(text, FONT, 8.8) > max_width:
                    text = text[:-1]
                alignment = alignments[col_index] if alignments else "left"
                if alignment == "right":
                    _text(pdf, cursor + width - 2, row_top + 9.6, text, size=8.8, align="right")
                elif alignment == "center":
                    _text(pdf, cursor + width / 2, row_top + 9.6, text, size=8.8, align="center")
                else:
                    _text(pdf, cursor + 2, row_top + 9.6, text, size=8.8)
            cursor += width
    for col in range(len(widths) + 1):
        xpos = x + sum(widths[:col])
        _line(pdf, xpos, top, xpos, top + len(rows) * row_height)
    for row in range(len(rows) + 1):
        ypos = top + row * row_height
        _line(pdf, x, ypos, right, ypos)


def _pct(value: float | None, decimals: int = 1, dash: bool = False) -> str:
    if value is None or (dash and abs(value) < 0.0000001):
        return "-"
    return f"{value * 100:.{decimals}f}%"


def _weight_pct(value: float | None, decimals: int = 1, dash: bool = False) -> str:
    if value is None or (dash and abs(value) < 0.0000001):
        return "-"
    return f"{value * 100:.{decimals}f}"


def _date(value: str) -> str:
    day = date.fromisoformat(value)
    return f"{day.month:02d}/{day.day:02d}/{day.year}"


def _page_one(pdf: canvas.Canvas, report: dict[str, Any]) -> None:
    _title(pdf, 38, 48, "The Soar Fund Risk Report", 12)
    labels = (
        ("Position Date", _date(report["position_date"])),
        ("Report Date", _date(report["report_date"])),
        ("Fund NAV", f"{report['fund_nav']:,.0f}"),
        ("Class B NAV/Unit", f"CAD {report['class_b_navpu']:.4f}"),
    )
    for index, (label, value) in enumerate(labels):
        top = 63 + index * 12
        _text(pdf, 38, top, label, bold=True, color=BLUE)
        _text(pdf, 132, top, value)
    _box(pdf, 309, 45, 262, 58)
    confirmation = (
        "EHP confirms that all trades proposed/directed by Phillips",
        "Wealth Management were executed, and no trades for the",
        "Fund were executed without pre-approval from the PM of",
        "Phillips Wealth Management.",
    )
    for index, line in enumerate(confirmation):
        _text(pdf, 315, 58 + index * 11, line, size=8.6, color=colors.black)

    _title(pdf, 38, 114, "Performance")
    performance = [["", "Fund", "TSX Comp", "S&P 500"]]
    performance.extend(
        [row["label"], _pct(row["fund"]), _pct(row["tsx"]), _pct(row["spx"])]
        for row in report["performance"]
    )
    _table(pdf, 36, 118, [70, 75, 62, 67], performance, alignments=["left", "right", "right", "right"])

    _title(pdf, 38, 214, "Positions")
    positions = [["Symbol", "Name", "Weight"]]
    positions.extend(
        [row["symbol"], row["name"], _pct(row["weight"], decimals=0)]
        for row in report["holdings"][:10]
    )
    positions.extend([["", "", ""] for _ in range(10 - len(positions) + 1)])
    _table(pdf, 36, 221, [70, 203, 70], positions, alignments=["left", "left", "right"])

    _title(
        pdf, 38, 379,
        f"Trades for the week of {_date(report['trade_week_start'])} to {_date(report['position_date'])}",
    )
    trades = [["Date", "Symbol", "Description", "Side", "Shares", "Avg Price"]]
    trades.extend(
        [
            row["date"].replace("-", ""), row["symbol"], row["description"],
            row["side"], f"{row['shares']:,.0f}", f"{row['price']:.3f}",
        ]
        for row in report["trades"]
    )
    trades.extend([["", "", "", "", "", ""] for _ in range(22 - len(trades) + 1)])
    _table(
        pdf, 36, 386, [70, 75, 198, 66, 66, 62], trades,
        alignments=["left", "left", "left", "left", "right", "right"],
    )
    pdf.showPage()


def _exposure_table(pdf: canvas.Canvas, report: dict[str, Any]) -> None:
    x, top = 36, 51
    widths = [145, 63, 67, 70, 66]
    _box(pdf, x + widths[0], top, sum(widths[1:]), 12)
    _text(pdf, x + widths[0] + 2, top + 9.6, "Weight (%)", size=8.8)
    exposure = report["exposure"]
    rows = [
        ["", "Long", "Short", "Net", "Gross"],
        ["The Soar Fund", *(_weight_pct(exposure[key], dash=key == "short") for key in ("long", "short", "net", "gross"))],
        ["Funds", *(_weight_pct(exposure[key], dash=key == "short") for key in ("long", "short", "net", "gross"))],
    ]
    pdf.setFillColor(colors.HexColor("#C3C3C3"))
    pdf.rect(x, PAGE_HEIGHT - (top + 24.6) - 12.6, sum(widths), 12.6, stroke=0, fill=1)
    _table(pdf, x, top + 12, widths, rows, alignments=["left", "right", "right", "right", "right"], striped=False)


def _liquidity_table(pdf: canvas.Canvas, top: float, data: dict[str, Any]) -> None:
    x, widths = 36, [70, 68, 68, 68, 70, 68, 68, 68]
    _box(pdf, x, top, 274, 12)
    _box(pdf, x + 274, top, 274, 12)
    _text(pdf, x + 2, top + 9.5, "Long", size=8.8)
    _text(pdf, x + 276, top + 9.5, "Short", size=8.8)
    header = ["# Names", "# Liquid", "% Names", "% Notional"] * 2
    values: list[str] = []
    for side in ("long", "short"):
        row = data[side]
        values.extend(
            [
                str(row["names"]) if row["names"] else "-",
                str(row["liquid"]) if row["names"] else "-",
                _pct(row["pct_names"], decimals=0),
                _pct(row["pct_notional"], decimals=0),
            ]
        )
    _table(pdf, x, top + 12, widths, [header, values], alignments=["right"] * 8, striped=False)


def _page_two(pdf: canvas.Canvas, report: dict[str, Any]) -> None:
    _title(pdf, 38, 47, "Exposure Summary")
    _exposure_table(pdf, report)
    _title(pdf, 38, 124, "Beta (vs S&P 500)")
    beta = report["beta"]
    _box(pdf, 36, 129, 145, 12)
    _box(pdf, 181, 129, 198, 12)
    _text(pdf, 38, 138.5, "$ Average Beta", size=8.8)
    _text(pdf, 183, 138.5, "Beta-Adjusted Weight (%)", size=8.8)
    beta_rows = [
        ["Long", "Short", "Long", "Short", "Net"],
        [
            f"{beta['long']:.2f}" if beta["long"] is not None else "-",
            f"{beta['short']:.2f}" if beta["short"] is not None else "-",
            _weight_pct(beta["adjusted_long"]),
            _weight_pct(beta["adjusted_short"], dash=True),
            _weight_pct(beta["adjusted_net"]),
        ],
    ]
    _table(pdf, 36, 141, [70, 75, 70, 65, 99], beta_rows, alignments=["right"] * 5, striped=False)

    _title(pdf, 38, 188, "Liquidity")
    _title(pdf, 38, 201, "Position sizes less than 1/3 of 20 day average trading volume considered liquid", 8.8)
    _liquidity_table(pdf, 207, report["liquidity"])
    _title(pdf, 38, 263, "Stressed Liquidity Environment - Assume 10% of normal market volumes", 8.8)
    _liquidity_table(pdf, 269, report["stressed_liquidity"])

    _title(pdf, 38, 330, "Short Interest")
    _text(pdf, 38, 343, "Short Positions with Largest SI Days to Trade", size=8.8, bold=True)
    _text(pdf, 311, 343, "Short Positions with Largest SI % of Float", size=8.8, bold=True)
    left = [["Symbol", "Weight", "SI Days to Trade"]]
    right = [["Symbol", "Weight", "SI % of Float"]]
    left.extend(
        [row["symbol"], _pct(row["weight"]), f"{row['short_interest_days']:.1f}"]
        for row in report["short_interest"]
    )
    right.extend(
        [row["symbol"], _pct(row["weight"]), f"{row['short_interest_float']:.1f}"]
        for row in report["short_interest_float"]
    )
    left.extend([["", "", ""] for _ in range(5 - len(left) + 1)])
    right.extend([["", "", ""] for _ in range(5 - len(right) + 1)])
    _table(pdf, 36, 347, [70, 75, 62], left, alignments=["left", "right", "right"])
    _table(pdf, 309, 347, [70, 75, 62], right, alignments=["left", "right", "right"])
    pdf.showPage()


def _chart(pdf: canvas.Canvas, chart: dict[str, Any]) -> None:
    x0, top, width, height = 72.0, 358.0, 466.0, 244.0
    actual = [(date.fromisoformat(day), float(value)) for day, value in chart["actual_growth"]]
    model = [(date.fromisoformat(day), float(value)) for day, value in chart["model_growth"]]
    slippage = [(date.fromisoformat(day), float(value)) for day, value in chart["slippage"]]
    all_growth = [value for _, value in actual + model]
    minimum = min(90.0, math.floor(min(all_growth) / 10) * 10)
    maximum = max(210.0, math.ceil(max(all_growth) / 10) * 10)
    slips = [value for _, value in slippage] or [0.0]
    slip_min = min(-5.0, math.floor(min(slips) / 5) * 5)
    slip_max = max(40.0, math.ceil(max(slips) / 5) * 5)
    first = min(actual[0][0], model[0][0])
    last = max(actual[-1][0], model[-1][0])
    span = max(1, (last - first).days)

    def xpos(day: date) -> float:
        return x0 + width * (day - first).days / span

    def growth_y(value: float) -> float:
        return PAGE_HEIGHT - top - height + height * (value - minimum) / (maximum - minimum)

    def slip_y(value: float) -> float:
        return PAGE_HEIGHT - top - height + height * (value - slip_min) / (slip_max - slip_min)

    _text(pdf, 306, 326, "The SOAR Fund Model vs Actual", size=11, color=colors.HexColor("#636363"), align="center")
    for tick in range(int(minimum), int(maximum) + 1, 20):
        y = growth_y(tick)
        pdf.setStrokeColor(colors.HexColor("#D8D8D8"))
        pdf.setLineWidth(0.45)
        pdf.line(x0, y, x0 + width, y)
        _text(pdf, x0 - 7, PAGE_HEIGHT - y + 2, str(tick), size=7, align="right")
    for tick in range(int(slip_min), int(slip_max) + 1, 5):
        _text(pdf, x0 + width + 8, PAGE_HEIGHT - slip_y(tick) + 2, f"{tick:.2f}", size=7)
    pdf.setStrokeColor(colors.HexColor("#B7B7B7"))
    pdf.setLineWidth(0.42)
    zero_y = slip_y(0)
    bar_width = max(0.25, width / max(1200, len(slippage)))
    for day, value in slippage:
        x = xpos(day)
        pdf.setFillColor(colors.HexColor("#D1D1D1"))
        pdf.rect(x, min(zero_y, slip_y(value)), bar_width, abs(slip_y(value) - zero_y), stroke=0, fill=1)

    def draw_curve(rows: list[tuple[date, float]], color: colors.Color) -> None:
        pdf.setStrokeColor(color)
        pdf.setLineWidth(1.65)
        path = pdf.beginPath()
        for index, (day, value) in enumerate(rows):
            x, y = xpos(day), growth_y(value)
            if index == 0:
                path.moveTo(x, y)
            else:
                path.lineTo(x, y)
        pdf.drawPath(path)

    draw_curve(actual, BLUE)
    draw_curve(model, CYAN)
    for step in range(0, span + 1, 95):
        day = date.fromordinal(first.toordinal() + step)
        x = xpos(day)
        pdf.saveState()
        pdf.translate(x, PAGE_HEIGHT - top - height - 7)
        pdf.rotate(45)
        pdf.setFont(FONT, 6.5)
        pdf.setFillColor(DARK)
        pdf.drawRightString(0, 0, day.isoformat())
        pdf.restoreState()

    legend_top = 662
    pdf.setFillColor(colors.HexColor("#CFCFCF"))
    pdf.rect(166, PAGE_HEIGHT - legend_top - 5, 13, 5, stroke=0, fill=1)
    _text(pdf, 183, legend_top, "Slippage ($,RHS)", size=7.5)
    pdf.setStrokeColor(BLUE)
    pdf.setLineWidth(2)
    pdf.line(255, PAGE_HEIGHT - legend_top + 2, 273, PAGE_HEIGHT - legend_top + 2)
    _text(pdf, 278, legend_top, "Actual Growth of $100", size=7.5)
    pdf.setStrokeColor(CYAN)
    pdf.line(389, PAGE_HEIGHT - legend_top + 2, 407, PAGE_HEIGHT - legend_top + 2)
    _text(pdf, 412, legend_top, "Model Growth of $100", size=7.5)


def _page_three(pdf: canvas.Canvas, report: dict[str, Any]) -> None:
    _title(pdf, 38, 47, "1 Day VaR")
    _title(
        pdf, 38, 61,
        "95% confidence interval maximum 1 day portfolio loss based on 1 year of historical daily fund returns",
        8.8,
    )
    _table(
        pdf, 36, 64, [145, 63],
        [["", "VaR"], ["The Soar Fund", _pct(report["var_95_1d"])]],
        alignments=["left", "right"], striped=False, orange_columns={1},
    )
    _title(pdf, 38, 109, "Scenario Analysis")
    scenario_rows = [["Scenario", "Est Fund PnL"]]
    scenario_rows.extend([row["label"], _pct(row["return"])] for row in report["scenarios"])
    _table(
        pdf, 36, 116, [207, 67], scenario_rows,
        alignments=["left", "right"], striped=False, orange_columns={1},
    )
    _box(pdf, 36, 192, 410, 75)
    notes = (
        "Notes:",
        "-Historical Scenarios use risk factor returns from the following periods:",
        "  -Sept 11, 2001: 9/5/2001 - 9/26/2001 (SPX -11%)",
        "  -Lehman Default: 9/15/2008 - 10/14/2008 (SPX -16.3%)",
        "  -Equity Markets Rebound in 2009: 3/4/2009 - 6/1/2009 (SPX +34.9%)",
        "  -Covid-19 Sell Off: 2/3/2020 - 4/30/2020 (SPX -10.4%)",
    )
    for index, line in enumerate(notes):
        _text(pdf, 38, 202 + index * 11, line, size=8.7)
    _title(pdf, 38, 290, "Model vs Actual")
    _chart(pdf, report["chart"])
    pdf.showPage()


def build_soar_risk_pdf(report: dict[str, Any]) -> bytes:
    """Render the complete report. Missing metrics are rejected by collection."""
    _register_fonts()
    if len(report["holdings"]) > 10 or len(report["trades"]) > 22:
        raise ValueError("SOAR report data would overflow the reference print area")
    output = io.BytesIO()
    pdf = canvas.Canvas(output, pagesize=letter, pageCompression=1)
    pdf.setTitle("The Soar Fund Risk Report")
    pdf.setAuthor("EHP")
    _page_one(pdf, report)
    _page_two(pdf, report)
    _page_three(pdf, report)
    pdf.save()
    return output.getvalue()
