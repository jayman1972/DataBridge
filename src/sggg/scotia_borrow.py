"""Scotia locate CSVs and audited, duplicate-protected SFTP delivery.

The legacy WinSCP script supplies credentials only. Its wildcard upload command
is never executed. Each request uploads exactly the server-validated draft rows.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import os
import re
import sqlite3
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit
from uuid import UUID

import paramiko

LOGGER = logging.getLogger("data_bridge.sggg.scotia_borrow")
SCOTIA_HOST = "files.prd.scotiabankprime.com"
REMOTE_DIR = "/drop/SL"
DEFAULT_ROOT = Path("W:/James Park/EHP Funds/Short Requests")
HEADERS = ("CUSIP", "ISIN", "SEDOL", "RIC", "COUNTRY", "QUANTITY")


class DeliveryReviewRequired(RuntimeError):
    """A previous attempt may have reached Scotia; do not submit it again."""


@dataclass(frozen=True)
class LocateRow:
    key: str
    cells: tuple[str, ...]

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(json.dumps(self.cells).encode()).hexdigest()


def _identifier(value: Any, pattern: str, label: str) -> str:
    value = str(value or "").strip().upper()
    if value and not re.fullmatch(pattern, value):
        raise ValueError(
            f"Invalid {label} in Scotia short-sale draft; correct it before sending."
        )
    return value


def load_scotia_rows(client: Any, draft_ids: Any, trade_date: Any) -> list[LocateRow]:
    """Reload selected ready drafts, never accept browser-provided securities/size."""
    if (
        not isinstance(trade_date, str)
        or date.fromisoformat(trade_date).isoformat() != trade_date
    ):
        raise ValueError("A valid trade date is required.")
    if not isinstance(draft_ids, list) or not 1 <= len(draft_ids) <= 100:
        raise ValueError("Select between 1 and 100 ready trade drafts.")
    try:
        ids = sorted({str(UUID(value)) for value in draft_ids})
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("Invalid trade draft ID.") from exc
    if len(ids) != len(draft_ids):
        raise ValueError("Duplicate trade draft IDs are not allowed.")
    drafts = (
        client.table("bt_sggg_trade_drafts")
        .select(
            "id,status,trade_date,dashboard_fund_name,dashboard_strategy_name,export_rows,"
            "source_run:bt_backtest_runs!bt_sggg_trade_drafts_source_run_id_fkey(status)"
        )
        .in_("id", ids)
        .execute()
        .data
    )
    if not isinstance(drafts, list) or {draft["id"] for draft in drafts} != set(ids):
        raise ValueError(
            "Some selected drafts are unavailable. Refresh the draft list."
        )
    routes = (
        client.table("bt_sggg_trade_account_routes")
        .select(
            "dashboard_fund_name,dashboard_strategy_name,side_scope,prime,sggg_portfolio_name"
        )
        .eq("active", True)
        .execute()
        .data
        or []
    )
    rows = []
    for draft in sorted(drafts, key=lambda item: item["id"]):
        source = draft.get("source_run") or {}
        if (
            draft.get("status") != "ready"
            or draft.get("trade_date") != trade_date
            or source.get("status") == "failed"
        ):
            raise ValueError(
                "Selected drafts must be ready and use the selected trade date."
            )
        trades = draft.get("export_rows")
        if not isinstance(trades, list):
            raise TypeError("The selected draft has no valid trade rows.")
        for index, trade in enumerate(trades):
            if trade.get("side") != "SS":
                continue
            matching = [
                route
                for route in routes
                if route["dashboard_fund_name"] == draft.get("dashboard_fund_name")
                and route.get("dashboard_strategy_name")
                in (None, draft.get("dashboard_strategy_name"))
                and route["side_scope"] in ("both", "short")
            ]
            # Strategy-specific routing wins over a fund-wide default.
            specific = [
                route for route in matching if route.get("dashboard_strategy_name")
            ]
            matching = specific or matching
            if len(matching) != 1 or trade.get("prime") != matching[0]["prime"]:
                raise ValueError(
                    "Short-sale prime routing changed. Regenerate the selected draft."
                )
            if matching[0]["prime"] != "Scotia":
                continue
            if (
                trade.get("sgggPortfolio") != matching[0]["sggg_portfolio_name"]
                or trade.get("tradeDate") != trade_date
            ):
                raise ValueError(
                    "Scotia draft portfolio or trade date does not match its routing."
                )
            quantity = trade.get("orderQuantity")
            if (
                isinstance(quantity, bool)
                or not isinstance(quantity, (int, float))
                or not 0 < quantity <= 2**53 - 1
                or int(quantity) != quantity
            ):
                raise ValueError(
                    "Scotia short-sale quantities must be positive whole shares."
                )
            currency = trade.get("currency")
            if currency not in ("USD", "CAD"):
                raise ValueError(
                    "Scotia North American locates require USD or CAD currency."
                )
            cells = (
                _identifier(trade.get("cusip"), r"[0-9A-Z*@#]{9}", "CUSIP"),
                _identifier(trade.get("isin"), r"[A-Z]{2}[0-9A-Z]{9}[0-9]", "ISIN"),
                _identifier(trade.get("sedol"), r"[0-9A-Z]{7}", "SEDOL"),
                _identifier(
                    trade.get("custodianId"), r"[0-9A-Z][0-9A-Z.\-/]{0,63}", "RIC"
                ),
                "US" if currency == "USD" else "CA",
                str(int(quantity)),
            )
            if not any(cells[:4]):
                raise ValueError(
                    f"{trade.get('symbol', 'A short sale')} has no security identifier for Scotia."
                )
            rows.append(LocateRow(f"{draft['id']}:{index}", cells))
    if not rows:
        raise ValueError("No Scotia short-sale (SS) trades are selected.")
    return rows


def locate_csv(rows: list[LocateRow]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\r\n")
    writer.writerow(HEADERS)
    writer.writerows(row.cells for row in rows)
    return output.getvalue().encode("ascii")


@dataclass(frozen=True)
class ScotiaConnection:
    username: str = field(repr=False)
    password: str = field(repr=False)
    host_key: Any = field(repr=False)


def load_connection(script_path: Path | None = None) -> ScotiaConnection:
    """Reuse the existing local credentials and previously trusted WinSCP RSA key."""
    path = script_path or Path(
        os.environ.get(
            "SCOTIA_LOCATE_SCRIPT",
            str(DEFAULT_ROOT / "WinSCP_Put_Short_Request_File_Scotia.txt"),
        )
    )
    # Do not execute the script or log its contents (the URL contains a password).
    commands = [
        line.strip()
        for line in path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    opens = [
        line[5:].strip().strip('"')
        for line in commands
        if line.lower().startswith("open ")
    ]
    if len(opens) != 1:
        raise ValueError(
            "The local Scotia transfer script must contain one SFTP connection."
        )
    parsed = urlsplit(opens[0])
    if (
        parsed.scheme != "sftp"
        or parsed.hostname != SCOTIA_HOST
        or parsed.port not in (None, 22)
        or not parsed.username
        or not parsed.password
    ):
        raise ValueError(
            "The local transfer script must use the configured Scotia SFTP server."
        )
    import winreg

    with winreg.OpenKey(
        winreg.HKEY_CURRENT_USER, r"Software\Martin Prikryl\WinSCP 2\SshHostKeys"
    ) as key:
        raw_key, _ = winreg.QueryValueEx(key, f"rsa2@22:{SCOTIA_HOST}")
    if not re.fullmatch(r"0x[0-9a-fA-F]+,0x[0-9a-fA-F]+", raw_key):
        raise ValueError("Scotia's trusted WinSCP server key could not be read.")
    exponent, modulus = (int(value, 16) for value in raw_key.split(","))
    message = paramiko.Message()
    message.add_string("ssh-rsa")
    message.add_mpint(exponent)
    message.add_mpint(modulus)
    return ScotiaConnection(
        unquote(parsed.username),
        unquote(parsed.password),
        paramiko.RSAKey(data=message.asbytes()),
    )


@contextmanager
def scotia_sftp(connection: ScotiaConnection):
    with paramiko.SSHClient() as client:
        client.get_host_keys().add(
            SCOTIA_HOST, connection.host_key.get_name(), connection.host_key
        )
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
        client.connect(
            SCOTIA_HOST,
            port=22,
            username=connection.username,
            password=connection.password,
            allow_agent=False,
            look_for_keys=False,
            timeout=15,
            auth_timeout=15,
            banner_timeout=15,
        )
        with client.open_sftp() as sftp:
            sftp.get_channel().settimeout(30)
            yield sftp


def upload_file(sftp: Any, filename: str, payload: bytes) -> None:
    remote = f"{REMOTE_DIR}/{filename}"
    temporary = remote + ".filepart"
    # An exclusive temporary file and non-overwriting rename avoid replacing an
    # existing request. Rename publishes only the complete, checked CSV.
    with sftp.open(temporary, "wx") as output:
        output.write(payload)
        output.flush()
    if sftp.stat(temporary).st_size != len(payload):
        raise OSError("Scotia upload size verification failed.")
    sftp.rename(temporary, remote)


def deliver_scotia_locates(
    rows: list[LocateRow],
    trade_date: str,
    user_id: str,
    *,
    output_dir: Path | None = None,
    audit_dir: Path | None = None,
    connection_factory: Any = None,
) -> dict[str, Any]:
    """Persist each row before upload; retries cannot duplicate or overlap it."""
    output_dir = output_dir or Path(
        os.environ.get("SCOTIA_LOCATE_OUTPUT_DIR", str(DEFAULT_ROOT / "Scotia"))
    )
    audit_dir = audit_dir or Path(
        os.environ.get("SCOTIA_LOCATE_AUDIT_DIR", "C:/SGGGPSC/ScotiaBorrowRequests")
    )
    audit_dir.mkdir(parents=True, exist_ok=True)
    if not output_dir.is_dir():
        raise OSError("The Scotia locate output folder is unavailable.")
    with (
        closing(sqlite3.connect(audit_dir / "deliveries.sqlite3", timeout=5)) as audit,
        audit,
    ):
        audit.execute(
            "CREATE TABLE IF NOT EXISTS deliveries (id INTEGER PRIMARY KEY, filename TEXT NOT NULL, status TEXT NOT NULL, user_id TEXT NOT NULL, created_at TEXT NOT NULL, sent_at TEXT, payload_sha256 TEXT NOT NULL)"
        )
        audit.execute(
            "CREATE TABLE IF NOT EXISTS items (item_key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, delivery_id INTEGER NOT NULL REFERENCES deliveries(id))"
        )
        audit.execute("BEGIN IMMEDIATE")
        pending = []
        previous = set()
        for row in rows:
            prior = audit.execute(
                "SELECT i.fingerprint,d.status,d.filename FROM items i JOIN deliveries d ON d.id=i.delivery_id WHERE i.item_key=?",
                (row.key,),
            ).fetchone()
            if prior:
                if prior[0] != row.fingerprint:
                    raise DeliveryReviewRequired(
                        "A selected draft changed after its Scotia request was prepared. Review the earlier request before submitting changed quantities."
                    )
                if prior[1] != "sent":
                    raise DeliveryReviewRequired(
                        f"Delivery of {prior[2]} needs review before resubmitting. Check Scotia and the bridge delivery audit to avoid a duplicate request."
                    )
                previous.add(prior[2])
            else:
                pending.append(row)
        if not pending:
            return {
                "success": True,
                "already_sent": True,
                "filenames": sorted(previous),
                "row_count": 0,
                "skipped_count": len(rows),
            }
        payload = locate_csv(pending)
        # Connect before reserving any rows. A failed login can be retried safely.
        factory = connection_factory or (lambda: scotia_sftp(load_connection()))
        with factory() as sftp:
            number = 1
            while True:
                filename = f"EDGEHILL_Scotia_Locates_NA_{trade_date.replace('-', '')}_{number}.csv"
                try:
                    with (output_dir / filename).open("xb") as local:
                        local.write(payload)
                        local.flush()
                        os.fsync(local.fileno())
                    break
                except FileExistsError:
                    number += 1
            digest = hashlib.sha256(payload).hexdigest()
            cursor = audit.execute(
                "INSERT INTO deliveries(filename,status,user_id,created_at,payload_sha256) VALUES (?, 'sending', ?, ?, ?)",
                (filename, user_id, datetime.now(UTC).isoformat(), digest),
            )
            delivery_id = cursor.lastrowid
            audit.executemany(
                "INSERT INTO items(item_key,fingerprint,delivery_id) VALUES (?,?,?)",
                [(row.key, row.fingerprint, delivery_id) for row in pending],
            )
            audit.commit()
            try:
                upload_file(sftp, filename, payload)
            except Exception:
                # The connection may have broken after Scotia accepted the file.
                # Keep rows reserved. Never automatically resend uncertain work.
                LOGGER.exception(
                    "Scotia locate delivery requires review filename=%s", filename
                )
                audit.execute(
                    "UPDATE deliveries SET status='review' WHERE id=?", (delivery_id,)
                )
                audit.commit()
                raise DeliveryReviewRequired(
                    f"Scotia delivery of {filename} could not be confirmed. Check Scotia before retrying; no automatic duplicate will be sent."
                ) from None
            audit.execute(
                "UPDATE deliveries SET status='sent',sent_at=? WHERE id=?",
                (datetime.now(UTC).isoformat(), delivery_id),
            )
            audit.commit()
            LOGGER.info(
                "Scotia locates delivered user=%s filename=%s rows=%s sha256=%s",
                user_id,
                filename,
                len(pending),
                digest,
            )
            return {
                "success": True,
                "already_sent": False,
                "filename": filename,
                "filenames": [*sorted(previous), filename],
                "row_count": len(pending),
                "skipped_count": len(rows) - len(pending),
                "payload_sha256": digest,
            }
