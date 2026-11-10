from datetime import date

import pytest

from spain_electricity.national_holidays import good_friday, holidays_frame, national_holidays


@pytest.mark.parametrize(
    "year, expected",
    [
        (2021, date(2021, 4, 2)),
        (2024, date(2024, 3, 29)),
        (2025, date(2025, 4, 18)),
        (2026, date(2026, 4, 3)),
    ],
)
def test_good_friday(year, expected):
    assert good_friday(year) == expected


def test_national_holidays_2025():
    days = national_holidays(2025)
    assert len(days) == 10
    assert days == sorted(days)
    assert date(2025, 12, 8) in days and date(2025, 4, 18) in days and date(2025, 1, 6) in days


def test_holidays_frame(spark):
    frame = holidays_frame(spark, [2024, 2025])
    assert frame.count() == 20
    assert set(frame.columns) == {"local_date", "is_holiday"}
    assert frame.where("local_date = date'2024-03-29'").first().is_holiday
