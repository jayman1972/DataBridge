"""Unit tests for PSC boxed position detection."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sggg.psc_boxed_positions import detect_boxed_positions


def test_tag_mismatch_box() -> None:
    positions = [
        {
            "bbg_ticker": "AAPL US Equity",
            "description": "APPLE INC",
            "long_short": "LONG",
            "quantity": 1000,
            "strategy": "S1",
            "trade_group": "TG1",
            "account": "PB1",
        },
        {
            "bbg_ticker": "AAPL US Equity",
            "description": "APPLE INC",
            "long_short": "SHORT",
            "quantity": 1000,
            "strategy": "S2",
            "trade_group": "TG2",
            "account": "PB1",
        },
    ]
    boxes = detect_boxed_positions(positions)
    assert len(boxes) == 1
    assert boxes[0]["box_type"] == "tag_mismatch"
    assert boxes[0]["long_quantity"] == 1000
    assert boxes[0]["short_quantity"] == 1000


def test_separate_accounts_box() -> None:
    positions = [
        {
            "bbg_ticker": "MSFT US Equity",
            "description": "MICROSOFT",
            "long_short": "L",
            "quantity": 500,
            "strategy": "S1",
            "trade_group": "TG1",
            "account": "ACCT-A",
        },
        {
            "bbg_ticker": "MSFT US Equity",
            "description": "MICROSOFT",
            "long_short": "S",
            "quantity": 500,
            "strategy": "S1",
            "trade_group": "TG1",
            "account": "ACCT-B",
        },
    ]
    boxes = detect_boxed_positions(positions)
    assert len(boxes) == 1
    assert boxes[0]["box_type"] == "separate_accounts"


def test_no_box_when_only_long() -> None:
    positions = [
        {
            "bbg_ticker": "X",
            "long_short": "LONG",
            "quantity": 100,
            "strategy": "S",
            "trade_group": "T",
            "account": "A",
        },
    ]
    assert detect_boxed_positions(positions) == []


if __name__ == "__main__":
    test_tag_mismatch_box()
    test_separate_accounts_box()
    test_no_box_when_only_long()
    print("ok")
