from datetime import datetime
from dateutil.relativedelta import relativedelta


class DateHelper:
    """Helper for date values used in SQL queries (anyomes, mes, anyo, etc.)."""

    def __init__(self, dt=None):
        self._dt = dt or datetime.now()

    @property
    def anyo(self) -> int:
        """Full year: 2025"""
        return self._dt.year

    @property
    def anyo_short(self) -> int:
        """Two-digit year: 25"""
        return self._dt.year % 100

    @property
    def mes(self) -> int:
        """Month: 1–12"""
        return self._dt.month

    @property
    def anyomes(self) -> int:
        """YYMM format: 2501 = January 2025"""
        return self.anyo_short * 100 + self.mes

    @property
    def fecha(self) -> str:
        """Full date: YYYY-MM-DD"""
        return self._dt.strftime("%Y-%m-%d")

    def offset(self, years=0, months=0):
        """Return a new DateHelper shifted by N years and/or months."""
        return DateHelper(self._dt + relativedelta(years=years, months=months))
