import pytest
from dagster_covid_data.utils.common import generate_dates

def test_generate_dates_basic():
    start = "01-01-2024"
    end = "01-03-2024"
    expected = ["01-01-2024", "01-02-2024", "01-03-2024"]
    assert generate_dates(start, end) == expected

def test_generate_dates_single_day():
    start = "06-02-2025"
    end = "06-02-2025"
    expected = ["06-02-2025"]
    assert generate_dates(start, end) == expected

def test_generate_dates_invalid_order():
    start = "01-05-2024"
    end = "01-03-2024"
    # Should return empty list if start > end
    assert generate_dates(start, end) == []

def test_generate_dates_month_boundary():
    start = "01-30-2024"
    end = "02-02-2024"
    expected = ["01-30-2024", "01-31-2024", "02-01-2024", "02-02-2024"]
    assert generate_dates(start, end) == expected

def test_generate_dates_invalid_format():
    with pytest.raises(ValueError):
        generate_dates("2024-01-01", "2024-01-03")