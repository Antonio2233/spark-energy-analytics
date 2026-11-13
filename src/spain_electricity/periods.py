"""Calendar-month helpers for monthly downloads."""

import calendar
from datetime import date


def month_end(month: date) -> date:
    """Return the last day of the month that contains ``month``."""
    return date(month.year, month.month, calendar.monthrange(month.year, month.month)[1])


def month_starts(start: date, end: date) -> list[date]:
    """Return the first day of every month touched by the range ``start``..``end``."""
    months = []
    current = date(start.year, start.month, 1)
    while current <= end:
        months.append(current)
        current = date(current.year + current.month // 12, current.month % 12 + 1, 1)
    return months
