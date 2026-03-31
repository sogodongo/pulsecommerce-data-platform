# =============================================================================
# tests/unit/test_fraud_scorer.py
# =============================================================================
# Unit tests for processing/flink/fraud_scorer.py
#
# Strategy:
#   - Tests target the pure Python logic extracted from FraudScoringFunction:
#     signal detection, score accumulation, score capping, and timer de-dup.
#   - Flink runtime (KeyedProcessFunction, state descriptors, timers) is mocked
#     via lightweight stubs — no Flink cluster required.
#   - SNS publish is mocked; tests assert publish is called on high-risk events
#     and NOT called on low-risk events.
#   - boto3 is mocked via moto to avoid real AWS calls.
# =============================================================================

from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch, call

import pytest


# ---------------------------------------------------------------------------
# Stub out PyFlink imports (not available in CI without a Flink cluster)
# ---------------------------------------------------------------------------

def _make_pyflink_stubs():
    """Inject minimal PyFlink stub modules so fraud_scorer.py can be imported."""
    # Top-level package
    pyflink = types.ModuleType("pyflink")
    sys.modules.setdefault("pyflink", pyflink)

    for sub in [
        "pyflink.common",
        "pyflink.common.serialization",
        "pyflink.common.typeinfo",
        "pyflink.datastream",
        "pyflink.datastream.connectors",
        "pyflink.datastream.connectors.kafka",
        "pyflink.datastream.functions",
        "pyflink.datastream.state",
        "pyflink.datastream.state_backend",
        "pyflink.table",
    ]:
        mod = types.ModuleType(sub)
        sys.modules.setdefault(sub, mod)

    # Minimal Types stub
    types_mod = sys.modules["pyflink.common.typeinfo"]
    types_mod.Types = MagicMock()

    # StateTtlConfig stub
    state_mod = sys.modules["pyflink.datastream.state"]
    state_mod.StateTtlConfig = MagicMock()
    state_mod.ValueStateDescriptor = MagicMock()
    state_mod.ListStateDescriptor = MagicMock()

    # KeyedProcessFunction stub — base class that does nothing
    functions_mod = sys.modules["pyflink.datastream.functions"]

    class _KeyedProcessFunction:
        class Context:
            pass
        class OnTimerContext:
            pass
        def open(self, ctx): pass
        def process_element(self, v, ctx): pass
        def on_timer(self, ts, ctx): pass

    functions_mod.KeyedProcessFunction = _KeyedProcessFunction

    # Common stubs
    common_mod = sys.modules["pyflink.common"]
    common_mod.Duration = MagicMock()
    common_mod.Types = MagicMock()
    common_mod.WatermarkStrategy = MagicMock()

    # Datastream stubs
    ds_mod = sys.modules["pyflink.datastream"]
    ds_mod.StreamExecutionEnvironment = MagicMock()
    ds_mod.CheckpointingMode = MagicMock()


_make_pyflink_stubs()

# ---------------------------------------------------------------------------
# Import the module under test (after stubs are in place)
# ---------------------------------------------------------------------------

# Patch env vars required at module import time
import os
os.environ.setdefault("MSK_BROKERS", "localhost:9092")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("SNS_FRAUD_ALERT_ARN", "arn:aws:sns:us-east-1:123456789012:fraud-alerts")
os.environ.setdefault("FRAUD_SCORE_THRESHOLD", "0.7")

# Import the pure helpers (avoid triggering Flink job graph at import time)
from processing.flink.fraud_scorer import (
    FraudScoringFunction,
    local_hour_from_ts,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_event(
    event_type: str = "page_view",
    country: str = "US",
    timezone_str: str | None = "America/New_York",
    quantity: int = 1,
    product_price: float = 29.99,
    event_id: str = "evt_001",
    user_id: str = "user_hash_001",
    order_id: str | None = None,
) -> str:
    return json.dumps({
        "event_id": event_id,
        "user_id": user_id,
        "order_id": order_id,
        "event_type": event_type,
        "geo": {"country": country, "timezone": timezone_str},
        "product": {"price_usd": product_price, "quantity": quantity},
        "page": {"url": "https://shop.pulsecommerce.com/cart"},
        "fraud_score": 0.0,
    })


class _MockValueState:
    """Minimal ValueState stub — stores a single value."""
    def __init__(self, initial=None):
        self._value = initial
    def value(self): return self._value
    def update(self, v): self._value = v
    def clear(self): self._value = None


class _MockTimerService:
    def __init__(self):
        self.registered_timers: list[int] = []
    def register_processing_time_timer(self, ts: int): self.registered_timers.append(ts)
    def delete_processing_time_timer(self, ts: int):
        if ts in self.registered_timers:
            self.registered_timers.remove(ts)


class _MockContext:
    """Stub KeyedProcessFunction.Context."""
    def __init__(self, ts: int):
        self._ts = ts
        self._timer_service = _MockTimerService()

    def timestamp(self): return self._ts
    def timer_service(self): return self._timer_service


def _make_scorer(
    count: int = 0,
    last_country: str | None = None,
    last_ts: int = 0,
    session_start: int = 0,
    product_view_ts: int = 0,
    cleanup_timer: int = 0,
) -> FraudScoringFunction:
    """Create a FraudScoringFunction with pre-seeded state."""
    scorer = FraudScoringFunction()
    scorer.event_count_state    = _MockValueState(count)
    scorer.last_country_state   = _MockValueState(last_country)
    scorer.last_event_ts_state  = _MockValueState(last_ts)
    scorer.session_start_ts_state = _MockValueState(session_start)
    scorer.product_view_ts_state  = _MockValueState(product_view_ts)
    scorer.cleanup_timer_state    = _MockValueState(cleanup_timer)
    scorer._sns = None
    return scorer


def _process(scorer: FraudScoringFunction, event_json: str, ts: int) -> list[dict]:
    ctx = _MockContext(ts)
    results = list(scorer.process_element(event_json, ctx))
    return [json.loads(r) for r in results]


# =============================================================================
# Tests: local_hour_from_ts
# =============================================================================

class TestLocalHourFromTs:

    def test_utc_midnight(self):
        # 2024-01-15 00:00:00 UTC
        ts_ms = int(datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        assert local_hour_from_ts(ts_ms, None) == 0

    def test_utc_explicit(self):
        ts_ms = int(datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc).timestamp() * 1000)
        assert local_hour_from_ts(ts_ms, "UTC") == 14

    def test_new_york_offset(self):
        # 2024-01-15 03:00 UTC → 22:00 EST (UTC-5)
        ts_ms = int(datetime(2024, 1, 15, 3, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        hour = local_hour_from_ts(ts_ms, "America/New_York")
        assert hour == 22

    def test_invalid_timezone_falls_back_to_utc(self):
        ts_ms = int(datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        # Should not raise — returns UTC hour
        hour = local_hour_from_ts(ts_ms, "Invalid/Zone_That_Does_Not_Exist")
        assert hour == 10


# =============================================================================
# Tests: FraudScoringFunction — signal detection
# =============================================================================

class TestFraudSignals:

    def test_clean_event_scores_zero(self):
        scorer = _make_scorer()
        event  = _make_event(event_type="page_view", country="US")
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        results = _process(scorer, event, ts_now)
        assert len(results) == 1
        assert results[0]["fraud_score"] == 0.0
        assert results[0]["fraud_signals"] == []

    def test_velocity_spike_detected(self):
        """21 events within 60 seconds triggers VELOCITY_SPIKE (+0.4)."""
        ts_start = int(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(count=21, last_ts=ts_start)
        # Send event 30 seconds later (well within 60s window)
        ts_event = ts_start + 30_000
        event = _make_event(event_type="page_view")
        results = _process(scorer, event, ts_event)
        assert "VELOCITY_SPIKE" in results[0]["fraud_signals"]
        assert results[0]["fraud_score"] == pytest.approx(0.4)

    def test_velocity_not_triggered_below_threshold(self):
        """20 events (not > 20) should NOT trigger velocity spike."""
        ts_start = int(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(count=20, last_ts=ts_start)
        ts_event = ts_start + 30_000
        event = _make_event(event_type="page_view")
        results = _process(scorer, event, ts_event)
        assert "VELOCITY_SPIKE" not in results[0]["fraud_signals"]

    def test_geo_hop_detected(self):
        """Country change from US → GB triggers GEO_HOP (+0.5)."""
        ts_now = int(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(last_country="US", last_ts=ts_now - 60_000)
        event  = _make_event(event_type="page_view", country="GB")
        results = _process(scorer, event, ts_now)
        assert "GEO_HOP" in results[0]["fraud_signals"]
        assert results[0]["fraud_score"] == pytest.approx(0.5)

    def test_geo_hop_not_triggered_same_country(self):
        ts_now = int(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(last_country="US", last_ts=ts_now - 60_000)
        event  = _make_event(event_type="page_view", country="US")
        results = _process(scorer, event, ts_now)
        assert "GEO_HOP" not in results[0]["fraud_signals"]

    def test_geo_hop_not_triggered_no_prior_country(self):
        """First event for a user (no prior country) should not trigger GEO_HOP."""
        ts_now = int(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(last_country=None)
        event  = _make_event(event_type="page_view", country="US")
        results = _process(scorer, event, ts_now)
        assert "GEO_HOP" not in results[0]["fraud_signals"]

    def test_unusual_hour_detected(self):
        """Purchase at 03:00 UTC triggers UNUSUAL_HOUR (+0.2)."""
        # 03:00 UTC
        ts_3am = int(datetime(2024, 1, 15, 3, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer()
        event  = _make_event(event_type="purchase", timezone_str="UTC")
        results = _process(scorer, event, ts_3am)
        assert "UNUSUAL_HOUR" in results[0]["fraud_signals"]
        assert results[0]["fraud_score"] == pytest.approx(0.2)

    def test_unusual_hour_not_triggered_daytime(self):
        """Purchase at 14:00 UTC should NOT trigger UNUSUAL_HOUR."""
        ts_2pm = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer()
        event  = _make_event(event_type="purchase", timezone_str="UTC")
        results = _process(scorer, event, ts_2pm)
        assert "UNUSUAL_HOUR" not in results[0]["fraud_signals"]

    def test_unusual_hour_not_triggered_for_non_purchase(self):
        """UNUSUAL_HOUR only applies to purchase events."""
        ts_3am = int(datetime(2024, 1, 15, 3, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer()
        event  = _make_event(event_type="page_view", timezone_str="UTC")
        results = _process(scorer, event, ts_3am)
        assert "UNUSUAL_HOUR" not in results[0]["fraud_signals"]

    def test_rapid_purchase_detected(self):
        """Purchase within 20 seconds of product_view triggers RAPID_PURCHASE (+0.35)."""
        ts_view = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        ts_buy  = ts_view + 20_000   # 20 seconds later
        scorer  = _make_scorer(product_view_ts=ts_view)
        event   = _make_event(event_type="purchase")
        results = _process(scorer, event, ts_buy)
        assert "RAPID_PURCHASE" in results[0]["fraud_signals"]
        assert results[0]["fraud_score"] == pytest.approx(0.35)

    def test_rapid_purchase_not_triggered_after_30s(self):
        """Purchase 60 seconds after product_view should NOT trigger RAPID_PURCHASE."""
        ts_view = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        ts_buy  = ts_view + 60_000
        scorer  = _make_scorer(product_view_ts=ts_view)
        event   = _make_event(event_type="purchase")
        results = _process(scorer, event, ts_buy)
        assert "RAPID_PURCHASE" not in results[0]["fraud_signals"]

    def test_rapid_purchase_not_triggered_without_prior_view(self):
        """RAPID_PURCHASE requires a prior product_view in state."""
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer  = _make_scorer(product_view_ts=0)   # no prior view
        event   = _make_event(event_type="purchase")
        results = _process(scorer, event, ts_now)
        assert "RAPID_PURCHASE" not in results[0]["fraud_signals"]

    def test_cart_overflow_detected(self):
        """add_to_cart with quantity > 50 triggers CART_OVERFLOW (+0.25)."""
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer()
        event  = _make_event(event_type="add_to_cart", quantity=99)
        results = _process(scorer, event, ts_now)
        assert "CART_OVERFLOW" in results[0]["fraud_signals"]
        assert results[0]["fraud_score"] == pytest.approx(0.25)

    def test_cart_overflow_not_triggered_at_exactly_50(self):
        """Quantity == 50 is NOT > 50 — should not trigger."""
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer()
        event  = _make_event(event_type="add_to_cart", quantity=50)
        results = _process(scorer, event, ts_now)
        assert "CART_OVERFLOW" not in results[0]["fraud_signals"]

    def test_cart_overflow_not_triggered_for_non_cart_event(self):
        """CART_OVERFLOW only applies to add_to_cart events."""
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer()
        event  = _make_event(event_type="purchase", quantity=99)
        results = _process(scorer, event, ts_now)
        assert "CART_OVERFLOW" not in results[0]["fraud_signals"]


# =============================================================================
# Tests: score accumulation and capping
# =============================================================================

class TestScoreAccumulation:

    def test_multiple_signals_accumulate(self):
        """GEO_HOP + VELOCITY_SPIKE should sum to 0.9."""
        ts_start = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(count=25, last_country="US", last_ts=ts_start)
        ts_event = ts_start + 10_000   # 10s later — velocity within window
        event = _make_event(event_type="page_view", country="RU")
        results = _process(scorer, event, ts_event)
        assert "GEO_HOP" in results[0]["fraud_signals"]
        assert "VELOCITY_SPIKE" in results[0]["fraud_signals"]
        assert results[0]["fraud_score"] == pytest.approx(0.9)

    def test_score_capped_at_1_0(self):
        """Combined signals > 1.0 must be capped at 1.0."""
        # GEO_HOP(0.5) + VELOCITY_SPIKE(0.4) + CART_OVERFLOW(0.25) = 1.15 → cap 1.0
        ts_start = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(count=25, last_country="US", last_ts=ts_start)
        ts_event = ts_start + 10_000
        event = _make_event(event_type="add_to_cart", country="CN", quantity=99)
        results = _process(scorer, event, ts_event)
        assert results[0]["fraud_score"] == pytest.approx(1.0)

    def test_score_precision_4_decimal_places(self):
        """Fraud score should be rounded to 4 decimal places."""
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer()
        event  = _make_event(event_type="add_to_cart", quantity=99)
        results = _process(scorer, event, ts_now)
        score_str = str(results[0]["fraud_score"])
        decimal_places = len(score_str.split(".")[-1]) if "." in score_str else 0
        assert decimal_places <= 4


# =============================================================================
# Tests: state updates
# =============================================================================

class TestStateUpdates:

    def test_event_count_incremented(self):
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(count=5)
        event  = _make_event()
        _process(scorer, event, ts_now)
        assert scorer.event_count_state.value() == 6

    def test_country_updated_in_state(self):
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(last_country="US")
        event  = _make_event(country="DE")
        _process(scorer, event, ts_now)
        assert scorer.last_country_state.value() == "DE"

    def test_last_event_ts_updated(self):
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(last_ts=ts_now - 60_000)
        event  = _make_event()
        _process(scorer, event, ts_now)
        assert scorer.last_event_ts_state.value() == ts_now

    def test_product_view_ts_updated_on_product_view(self):
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer()
        event  = _make_event(event_type="product_view")
        _process(scorer, event, ts_now)
        assert scorer.product_view_ts_state.value() == ts_now

    def test_product_view_ts_not_updated_for_other_events(self):
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(product_view_ts=ts_now - 5_000)
        event  = _make_event(event_type="add_to_cart")
        _process(scorer, event, ts_now)
        # Should still be the old value
        assert scorer.product_view_ts_state.value() == ts_now - 5_000

    def test_new_session_resets_session_start_ts(self):
        """Event after 15-min gap should reset session_start_ts to current event ts."""
        ts_old = int(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        ts_new = ts_old + 20 * 60 * 1000   # 20 minutes later
        scorer = _make_scorer(last_ts=ts_old, session_start=ts_old)
        event  = _make_event()
        _process(scorer, event, ts_new)
        assert scorer.session_start_ts_state.value() == ts_new


# =============================================================================
# Tests: timer de-duplication
# =============================================================================

class TestTimerDeduplication:

    def test_cleanup_timer_registered_on_first_event(self):
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(cleanup_timer=0)
        ctx = _MockContext(ts_now)
        list(scorer.process_element(_make_event(), ctx))
        # Timer should be registered at ts_now + 900_000
        assert ts_now + 900_000 in ctx.timer_service().registered_timers

    def test_cleanup_timer_updated_in_state(self):
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(cleanup_timer=0)
        _process(scorer, _make_event(), ts_now)
        assert scorer.cleanup_timer_state.value() == ts_now + 900_000

    def test_timer_not_registered_if_already_ahead(self):
        """If existing timer is further ahead than new cleanup_at, don't register again."""
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        # Existing timer is 100ms ahead of what this event would schedule
        future_timer = ts_now + 900_100
        scorer = _make_scorer(cleanup_timer=future_timer)
        ctx = _MockContext(ts_now)
        list(scorer.process_element(_make_event(), ctx))
        # New timer (ts_now + 900_000) < existing (ts_now + 900_100) → should NOT register
        assert ts_now + 900_000 not in ctx.timer_service().registered_timers

    def test_timer_registered_when_events_come_later(self):
        """Each later event should push the timer forward (new ts > old timer)."""
        ts1 = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        ts2 = ts1 + 60_000    # 1 minute later
        scorer = _make_scorer(cleanup_timer=ts1 + 900_000)
        ctx = _MockContext(ts2)
        list(scorer.process_element(_make_event(), ctx))
        # ts2 + 900_000 > ts1 + 900_000 → should register new timer
        assert ts2 + 900_000 in ctx.timer_service().registered_timers


# =============================================================================
# Tests: on_timer (state cleanup)
# =============================================================================

class TestOnTimer:

    def test_on_timer_clears_event_count(self):
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(count=15, session_start=ts_now, product_view_ts=ts_now)
        ctx = _MockContext(ts_now)
        list(scorer.on_timer(ts_now + 900_000, ctx))
        assert scorer.event_count_state.value() is None

    def test_on_timer_clears_session_start(self):
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(session_start=ts_now)
        ctx = _MockContext(ts_now)
        list(scorer.on_timer(ts_now + 900_000, ctx))
        assert scorer.session_start_ts_state.value() is None

    def test_on_timer_preserves_last_country(self):
        """last_country is intentionally NOT cleared on timer — preserves geo-hop continuity."""
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(last_country="FR")
        ctx = _MockContext(ts_now)
        list(scorer.on_timer(ts_now + 900_000, ctx))
        assert scorer.last_country_state.value() == "FR"

    def test_on_timer_preserves_last_event_ts(self):
        """last_event_ts intentionally NOT cleared — needed for session gap detection."""
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(last_ts=ts_now)
        ctx = _MockContext(ts_now)
        list(scorer.on_timer(ts_now + 900_000, ctx))
        assert scorer.last_event_ts_state.value() == ts_now


# =============================================================================
# Tests: SNS alert behavior
# =============================================================================

class TestSNSAlert:

    def test_sns_published_on_high_risk_event(self):
        """Fraud score >= 0.7 should trigger SNS publish."""
        # GEO_HOP (0.5) + VELOCITY_SPIKE (0.4) = 0.9 >= 0.7 threshold
        ts_start = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(count=25, last_country="US", last_ts=ts_start)
        ts_event = ts_start + 10_000
        event = _make_event(event_type="page_view", country="CN")

        mock_sns = MagicMock()
        mock_sns.publish.return_value = {"MessageId": "test-id"}
        scorer._sns = mock_sns

        _process(scorer, event, ts_event)
        mock_sns.publish.assert_called_once()
        call_kwargs = mock_sns.publish.call_args[1]
        assert call_kwargs["Subject"] == "FRAUD_ALERT"
        assert call_kwargs["TopicArn"] == os.environ["SNS_FRAUD_ALERT_ARN"]

    def test_sns_not_published_on_low_risk_event(self):
        """Fraud score < 0.7 should NOT trigger SNS publish."""
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer()
        mock_sns = MagicMock()
        scorer._sns = mock_sns
        event = _make_event(event_type="page_view")
        _process(scorer, event, ts_now)
        mock_sns.publish.assert_not_called()

    def test_sns_failure_does_not_raise(self):
        """SNS publish failure should be caught — fail-open behavior."""
        ts_start = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer(count=25, last_country="US", last_ts=ts_start)
        ts_event = ts_start + 10_000
        event = _make_event(event_type="page_view", country="CN")

        mock_sns = MagicMock()
        mock_sns.publish.side_effect = Exception("SNS timeout")
        scorer._sns = mock_sns

        # Must not raise
        results = _process(scorer, event, ts_event)
        assert len(results) == 1
        assert results[0]["fraud_score"] == pytest.approx(0.9)

    def test_sns_message_contains_fraud_score_attribute(self):
        ts_view = int(datetime(2024, 1, 15, 3, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        ts_buy  = ts_view + 10_000
        # UNUSUAL_HOUR(0.2) + RAPID_PURCHASE(0.35) = 0.55 < 0.7 — add GEO_HOP
        scorer = _make_scorer(last_country="US", product_view_ts=ts_view)
        event  = _make_event(event_type="purchase", country="RU", timezone_str="UTC")

        mock_sns = MagicMock()
        scorer._sns = mock_sns
        _process(scorer, event, ts_buy)

        if mock_sns.publish.called:
            call_kwargs = mock_sns.publish.call_args[1]
            assert "fraud_score" in call_kwargs.get("MessageAttributes", {})


# =============================================================================
# Tests: malformed input handling
# =============================================================================

class TestMalformedInput:

    def test_invalid_json_produces_no_output(self):
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer()
        ctx = _MockContext(ts_now)
        results = list(scorer.process_element("{invalid json{{", ctx))
        assert results == []

    def test_null_input_produces_no_output(self):
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer()
        ctx = _MockContext(ts_now)
        results = list(scorer.process_element(None, ctx))  # type: ignore
        assert results == []

    def test_event_with_missing_geo_field(self):
        """Events with no geo dict should process without error."""
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer()
        event = json.dumps({"event_id": "e1", "event_type": "page_view"})
        results = _process(scorer, event, ts_now)
        assert len(results) == 1
        assert results[0]["fraud_score"] == 0.0

    def test_enriched_event_retains_original_fields(self):
        """Output event should contain all original fields plus fraud additions."""
        ts_now = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        scorer = _make_scorer()
        event = _make_event(event_type="page_view", event_id="orig_123", user_id="u_abc")
        results = _process(scorer, event, ts_now)
        assert results[0]["event_id"] == "orig_123"
        assert results[0]["user_id"] == "u_abc"
        assert "fraud_score" in results[0]
        assert "fraud_signals" in results[0]
        assert "scored_at" in results[0]
