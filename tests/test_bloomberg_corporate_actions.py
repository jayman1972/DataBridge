from __future__ import annotations

import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock, patch

from bloomberg.blpapi_client import BLPAPIClient


class _Name:
    def __init__(self, value: str) -> None:
        self.value = value

    def __str__(self) -> str:
        return self.value


class _ScalarElement:
    def __init__(self, name: str, value: object) -> None:
        self._name = name
        self._value = value

    def name(self) -> _Name:
        return _Name(self._name)

    def isNull(self) -> bool:
        return self._value is None

    def getValue(self) -> object:
        return self._value


class _RowElement:
    def __init__(self, values: dict[str, object]) -> None:
        self._elements = [_ScalarElement(key, value) for key, value in values.items()]

    def numElements(self) -> int:
        return len(self._elements)

    def getElement(self, index: int) -> _ScalarElement:
        return self._elements[index]


class _BulkElement:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = [_RowElement(row) for row in rows]

    def isNull(self) -> bool:
        return False

    def numValues(self) -> int:
        return len(self._rows)

    def getValue(self, index: int) -> _RowElement:
        return self._rows[index]


class _ElementOnlyBulk(_BulkElement):
    def getValue(self, index: int) -> _RowElement:
        raise TypeError("complex rows require getValueAsElement")

    def getValueAsElement(self, index: int) -> _RowElement:
        return self._rows[index]


class BloombergCorporateActionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = object.__new__(BLPAPIClient)

    def test_bulk_rows_preserve_all_columns_and_skip_null_cells(self) -> None:
        rows = self.client._extract_bulk_field_rows(
            _BulkElement(
                [
                    {"Ex-Date": date(2026, 8, 10), "Dividend Amount": 0.27},
                    {"Ex-Date": date(2026, 11, 9), "Optional": None},
                ]
            )
        )

        self.assertEqual(
            rows,
            [
                {"Ex-Date": "2026-08-10", "Dividend Amount": 0.27},
                {"Ex-Date": "2026-11-09"},
            ],
        )

    def test_bulk_rows_support_complex_element_accessor(self) -> None:
        rows = self.client._extract_bulk_field_rows(
            _ElementOnlyBulk([{"Action": "Ticker Change", "Effective": date(2026, 7, 8)}])
        )

        self.assertEqual(
            rows,
            [{"Action": "Ticker Change", "Effective": "2026-07-08"}],
        )

    def test_get_bds_rows_builds_one_security_request_with_bounded_fields(self) -> None:
        securities = Mock()
        fields = Mock()
        override_row = Mock()
        overrides = Mock()
        overrides.appendElement.return_value = override_row
        request = Mock()
        request.getElement.side_effect = lambda name: {
            "securities": securities,
            "fields": fields,
            "overrides": overrides,
        }[name]
        service = Mock()
        service.createRequest.return_value = request

        event = Mock()
        event.eventType.return_value = 1
        event.__iter__ = Mock(return_value=iter(()))
        session = Mock()
        session.getService.return_value = service
        session.nextEvent.return_value = event
        self.client.service = "//blp/refdata"
        self.client._create_session = Mock(return_value=session)
        self.client._clear_request_error = Mock()

        fake_blpapi = SimpleNamespace(
            Event=SimpleNamespace(RESPONSE=1, PARTIAL_RESPONSE=2)
        )
        with patch("bloomberg.blpapi_client.blpapi", fake_blpapi):
            result = self.client.get_bds_rows(
                "AAPL US Equity",
                ["DVD_HIST_ALL", "EQY_DVD_HIST_SPLITS"],
                {"DVD_START_DT": "20260725", "DVD_END_DT": "20261013"},
            )

        securities.appendValue.assert_called_once_with("AAPL US Equity")
        self.assertEqual(
            [call.args[0] for call in fields.appendValue.call_args_list],
            ["DVD_HIST_ALL", "EQY_DVD_HIST_SPLITS"],
        )
        self.assertEqual(overrides.appendElement.call_count, 2)
        self.assertEqual(
            [call.args for call in override_row.setElement.call_args_list],
            [
                ("fieldId", "DVD_START_DT"),
                ("value", "20260725"),
                ("fieldId", "DVD_END_DT"),
                ("value", "20261013"),
            ],
        )
        self.assertEqual(
            result,
            {
                "fields": {
                    "DVD_HIST_ALL": [],
                    "EQY_DVD_HIST_SPLITS": [],
                },
                "errors": [],
            },
        )
        session.sendRequest.assert_called_once_with(request)
        session.stop.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
