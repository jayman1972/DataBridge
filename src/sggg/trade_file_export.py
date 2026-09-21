"""Validated, atomic writes for AlphaDesk production trade imports."""

from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import tempfile
from pathlib import Path
from typing import Any

DEFAULT_TRADE_EXPORT_DIR = Path(r"C:\SGGGPSC\Data")
MAX_TRADE_FILE_BYTES = 5 * 1024 * 1024
_FILENAME_PATTERN = re.compile(r"Pending_Trade_Import_\d{8}\.csv")
_CUSIP_PATTERN = re.compile(r"[A-Z0-9]{9}")


def _validate_trade_csv(csv_content: str) -> bytes:
    encoded = csv_content.encode("utf-8")
    if not encoded or len(encoded) > MAX_TRADE_FILE_BYTES:
        raise ValueError("Trade CSV must contain between 1 byte and 5 MB.")

    reader = csv.DictReader(io.StringIO(csv_content.lstrip("\ufeff"), newline=""))
    if not reader.fieldnames or "CUSIP" not in reader.fieldnames:
        raise ValueError("Trade CSV is missing the CUSIP column.")
    for line_number, row in enumerate(reader, start=2):
        cusip = (row.get("CUSIP") or "").strip().upper()
        if cusip and not _CUSIP_PATTERN.fullmatch(cusip):
            raise ValueError(
                f"Trade CSV row {line_number} has an invalid CUSIP; "
                "scientific notation is not allowed."
            )
    return encoded


def save_alphadesk_trade_file(
    *,
    filename: str,
    csv_content: str,
    expected_sha256: str,
    target_dir: Path | str = DEFAULT_TRADE_EXPORT_DIR,
) -> dict[str, Any]:
    """Validate and atomically replace a production AlphaDesk trade file."""
    if not _FILENAME_PATTERN.fullmatch(filename):
        raise ValueError("Trade filename is invalid.")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise ValueError("Trade file SHA-256 is invalid.")

    encoded = _validate_trade_csv(csv_content)
    actual_sha256 = hashlib.sha256(encoded).hexdigest()
    if actual_sha256 != expected_sha256:
        raise ValueError("Trade file SHA-256 does not match its contents.")

    directory = Path(target_dir).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / filename
    overwritten = destination.exists()
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=directory,
            prefix=f".{filename}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(encoded)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, destination)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    return {
        "filename": filename,
        "path": str(destination),
        "bytes": len(encoded),
        "sha256": actual_sha256,
        "overwritten": overwritten,
    }
