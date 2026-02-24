"""
General-purpose utility functions.
"""

import io
import pandas as pd


def detect_header_row(data: bytes, is_excel: bool = False, sep: str = ",", max_scan: int = 20, min_named: int = 4) -> int:
    """
    Auto-detect the header row in a file.

    Scans up to max_scan rows looking for the first row where
    at least min_named columns are not 'Unnamed'.

    Args:
        data: Raw file bytes.
        is_excel: True for .xlsx/.xls, False for CSV.
        sep: CSV separator (ignored for Excel).
        max_scan: Maximum rows to scan before giving up.
        min_named: Minimum non-Unnamed columns to accept as header.

    Returns:
        0-indexed row number to use as header.
    """
    for row in range(max_scan):
        try:
            if is_excel:
                df = pd.read_excel(io.BytesIO(data), header=row, nrows=0)
            else:
                df = pd.read_csv(io.BytesIO(data), header=row, nrows=0, sep=sep)
            named = [c for c in df.columns if not str(c).startswith("Unnamed")]
            if len(named) >= min_named:
                return row
        except Exception:
            continue
    return 0
