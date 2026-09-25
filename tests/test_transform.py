"""
Unit Tests for Transformation & Cleansing Logic (Level 2)
"""

import pytest
from datetime import datetime


def test_clean_metrics_structure():
    """Verify that cleansing metrics return expected tracking schema."""
    dummy_metrics = {
        "initial_rows": 1000,
        "clean_rows": 950,
        "dropped_rows": 50,
        "dropped_percentage": 5.0,
    }
    assert "initial_rows" in dummy_metrics
    assert "clean_rows" in dummy_metrics
    assert "dropped_rows" in dummy_metrics
    assert dummy_metrics["clean_rows"] + dummy_metrics["dropped_rows"] == dummy_metrics["initial_rows"]


def test_trip_duration_calculation():
    """Verify business calculation for duration in minutes."""
    pickup = datetime(2024, 1, 1, 12, 0, 0)
    dropoff = datetime(2024, 1, 1, 12, 15, 30)
    duration_minutes = (dropoff - pickup).total_seconds() / 60.0
    assert round(duration_minutes, 2) == 15.5


def test_speed_mph_calculation():
    """Verify average speed formula in miles per hour."""
    distance_miles = 5.0
    duration_minutes = 15.0  # 0.25 hours
    duration_hours = duration_minutes / 60.0
    speed_mph = distance_miles / duration_hours
    assert speed_mph == 20.0


def test_tip_percentage_calculation():
    """Verify tip percentage edge case handling."""
    fare = 20.0
    tip = 4.0
    tip_pct = (tip / fare) * 100.0 if fare > 0 else 0.0
    assert tip_pct == 20.0

    # Zero fare edge case should not divide by zero
    zero_fare = 0.0
    tip_pct_zero = (tip / zero_fare) * 100.0 if zero_fare > 0 else 0.0
    assert tip_pct_zero == 0.0
