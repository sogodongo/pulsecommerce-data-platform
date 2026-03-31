# =============================================================================
# tests/unit/test_session_stitcher.py
# =============================================================================
# Unit tests for processing/flink/session_stitcher.py
#
# Strategy:
#   - Tests focus on compute_session_metrics() — a pure function with no Flink
#     dependencies, making it directly testable without stubs.
#   - SessionStitcherFunction is tested via lightweight Flink runtime stubs
#     (same pattern as test_fraud_scorer.py).
#   - Tests cover: funnel stage derivation, session metric computation,
#     timer sliding behavior, state accumulation, and edge cases.
# =============================================================================

from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# PyFlink stubs (must come before any import of session_stitcher)
# ---------------------------------------------------------------------------

def _make_pyflink_stubs():
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

    # Types
    ti = sys.modules["pyflink.common.typeinfo"]
    ti.Types = MagicMock()

    common = sys.modules["pyflink.common"]
    common.Duration = MagicMock()
    common.Types = MagicMock()
    common.WatermarkStrategy = MagicMock()

    ds = sys.modules["pyflink.datastream"]
    ds.StreamExecutionEnvironment = MagicMock()
    ds.CheckpointingMode = MagicMock()

    state_mod = sys.modules["pyflink.datastream.state"]
    state_mod.ValueStateDescriptor = MagicMock()
    state_mod.ListStateDescriptor = MagicMock()
    state_mod.StateTtlConfig = MagicMock()

    functions_mod = sys.modules["pyflink.datastream.functions"]

    class _KeyedProcessFunction:
        class Context: pass
        class OnTimerContext: pass
        def open(self, ctx): pass
        def process_element(self, v, ctx): pass
        def on_timer(self, ts, ctx): pass

    functions_mod.KeyedProcessFunction = _KeyedProcessFunction

    table_mod = sys.modules["pyflink.table"]
    table_mod.StreamTableEnvironment = MagicMock()


_make_pyflink_stubs()

import os
os.environ.setdefault("MSK_BROKERS", "localhost:9092")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("LAKEHOUSE_BUCKET", "s3://test-bucket/")

from processing.flink.session_stitcher import (
    SessionStitcherFunction,
    compute_session_metrics,
    FUNNEL_ORDER,
    FUNNEL_STAGES,
    SESSION_GAP_MS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_TS = int(datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)


def _evt(
    event_type: str,
    offset_ms: int = 0,
    country: str = "US",
    price_usd: float = 0.0,
    quantity: int = 1,
    fraud_score: float = 0.0,
    url: str = "https://shop.example.com/",
    referrer: str = "",
    device_type: str = "desktop",
    os: str = "macOS",
    ab_cohort: str = "control",
) -> dict:
    ts = BASE_TS + offset_ms
    return {
        "event_type": event_type,
        "event_ts_ms": ts,
        "fraud_score": fraud_score,
        "geo": {"country": country},
        "product": {"price_usd": price_usd, "quantity": quantity},
        "page": {"url": url, "referrer": referrer},
        "device": {"type": device_type, "os": os},
        "flags": {"ab_cohort": ab_cohort},
    }


# ---------------------------------------------------------------------------
# State stubs
# ---------------------------------------------------------------------------

class _MockValueState:
    def __init__(self, v=None): self._v = v
    def value(self): return self._v
    def update(self, v): self._v = v
    def clear(self): self._v = None


class _MockListState:
    def __init__(self): self._items: list = []
    def add(self, v): self._items.append(v)
    def get(self): return iter(self._items)
    def clear(self): self._items.clear()


class _MockTimerService:
    def __init__(self):
        self.timers: set[int] = set()
    def register_processing_time_timer(self, ts): self.timers.add(ts)
    def delete_processing_time_timer(self, ts): self.timers.discard(ts)


class _MockContext:
    def __init__(self, ts: int):
        self._ts = ts
        self._timer_service = _MockTimerService()
    def timestamp(self): return self._ts
    def timer_service(self): return self._timer_service


def _make_stitcher(
    events: list[str] | None = None,
    session_id: str | None = None,
    last_ts: int = 0,
    gap_timer_ts: int = 0,
) -> SessionStitcherFunction:
    s = SessionStitcherFunction()
    s.session_events_state = _MockListState()
    if events:
        for e in events:
            s.session_events_state.add(e)
    s.session_id_state = _MockValueState(session_id)
    s.last_event_ts_state = _MockValueState(last_ts)
    s.gap_timer_ts_state = _MockValueState(gap_timer_ts)
    return s


def _process(stitcher: SessionStitcherFunction, event_json: str, ts: int) -> list[dict]:
    ctx = _MockContext(ts)
    results = list(stitcher.process_element(event_json, ctx))
    return [json.loads(r) for r in results if r]


def _fire_timer(stitcher: SessionStitcherFunction, ts: int) -> list[dict]:
    ctx = _MockContext(ts)
    results = list(stitcher.on_timer(ts, ctx))
    return [json.loads(r) for r in results if r]


# =============================================================================
# Tests: compute_session_metrics — pure function
# =============================================================================

class TestComputeSessionMetrics:

    def test_empty_events_returns_empty(self):
        assert compute_session_metrics([]) == {}

    def test_single_page_view(self):
        events = [_evt("page_view", offset_ms=0)]
        m = compute_session_metrics(events)
        assert m["funnel_stage_reached"] == "browse"
        assert m["page_views"] == 1
        assert m["product_views"] == 0
        assert m["purchases"] == 0
        assert m["event_count"] == 1
        assert m["session_duration_s"] == 0

    def test_funnel_deepest_stage_browse(self):
        events = [_evt("page_view"), _evt("search")]
        m = compute_session_metrics(events)
        assert m["funnel_stage_reached"] == "browse"

    def test_funnel_deepest_stage_product_view(self):
        events = [_evt("page_view"), _evt("product_view")]
        m = compute_session_metrics(events)
        assert m["funnel_stage_reached"] == "product_view"

    def test_funnel_deepest_stage_add_to_cart(self):
        events = [_evt("page_view"), _evt("product_view"), _evt("add_to_cart")]
        m = compute_session_metrics(events)
        assert m["funnel_stage_reached"] == "add_to_cart"

    def test_funnel_deepest_stage_checkout_start(self):
        events = [_evt("page_view"), _evt("product_view"), _evt("add_to_cart"), _evt("checkout_start")]
        m = compute_session_metrics(events)
        assert m["funnel_stage_reached"] == "checkout_start"

    def test_funnel_deepest_stage_purchase(self):
        events = [
            _evt("page_view"), _evt("product_view"),
            _evt("add_to_cart"), _evt("checkout_start"),
            _evt("purchase", price_usd=49.99),
        ]
        m = compute_session_metrics(events)
        assert m["funnel_stage_reached"] == "purchase"

    def test_funnel_uses_deepest_not_last(self):
        """If user goes back after checkout_start, deepest stage should still be checkout_start."""
        events = [
            _evt("checkout_start", offset_ms=0),
            _evt("page_view", offset_ms=5000),     # went back
        ]
        m = compute_session_metrics(events)
        assert m["funnel_stage_reached"] == "checkout_start"

    def test_session_duration_computed_correctly(self):
        events = [
            _evt("page_view", offset_ms=0),
            _evt("product_view", offset_ms=30_000),   # 30 seconds later
            _evt("add_to_cart", offset_ms=60_000),    # 60 seconds from start
        ]
        m = compute_session_metrics(events)
        assert m["session_duration_s"] == 60

    def test_session_duration_single_event_is_zero(self):
        events = [_evt("page_view")]
        m = compute_session_metrics(events)
        assert m["session_duration_s"] == 0

    def test_revenue_attributed_for_purchases(self):
        events = [
            _evt("purchase", offset_ms=0, price_usd=29.99),
            _evt("purchase", offset_ms=5_000, price_usd=10.01),
        ]
        m = compute_session_metrics(events)
        assert m["revenue_attributed_usd"] == pytest.approx(40.0)

    def test_revenue_zero_for_non_purchase_session(self):
        events = [_evt("page_view"), _evt("add_to_cart", price_usd=99.0)]
        m = compute_session_metrics(events)
        assert m["revenue_attributed_usd"] == pytest.approx(0.0)

    def test_cart_value_computed(self):
        events = [
            _evt("add_to_cart", price_usd=50.0, quantity=2),   # +100
            _evt("remove_from_cart", price_usd=50.0),           # -50
        ]
        m = compute_session_metrics(events)
        assert m["cart_value_usd"] == pytest.approx(50.0)

    def test_cart_value_clamped_to_zero_on_net_negative(self):
        """Net negative cart value (more removes than adds) should clamp to 0."""
        events = [
            _evt("add_to_cart", price_usd=10.0),
            _evt("remove_from_cart", price_usd=99.0),
        ]
        m = compute_session_metrics(events)
        assert m["cart_value_usd"] == pytest.approx(0.0)

    def test_cart_abandonment_true_when_no_purchase(self):
        events = [_evt("add_to_cart"), _evt("page_view")]
        m = compute_session_metrics(events)
        assert m["cart_abandonment"] is True

    def test_cart_abandonment_false_when_purchased(self):
        events = [_evt("add_to_cart"), _evt("purchase", price_usd=20.0)]
        m = compute_session_metrics(events)
        assert m["cart_abandonment"] is False

    def test_cart_abandonment_false_when_no_cart_add(self):
        """No add_to_cart → can't be cart abandonment."""
        events = [_evt("page_view"), _evt("product_view")]
        m = compute_session_metrics(events)
        assert m["cart_abandonment"] is False

    def test_max_fraud_score_across_events(self):
        events = [
            _evt("page_view", fraud_score=0.1),
            _evt("add_to_cart", fraud_score=0.85),
            _evt("purchase", fraud_score=0.3),
        ]
        m = compute_session_metrics(events)
        assert m["max_fraud_score"] == pytest.approx(0.85)
        assert m["fraud_flagged"] is True

    def test_fraud_flagged_false_below_threshold(self):
        events = [_evt("page_view", fraud_score=0.5), _evt("purchase", fraud_score=0.3)]
        m = compute_session_metrics(events)
        assert m["fraud_flagged"] is False

    def test_entry_and_exit_page_urls(self):
        events = [
            _evt("page_view", offset_ms=0, url="https://shop.example.com/home"),
            _evt("product_view", offset_ms=5_000, url="https://shop.example.com/product/123"),
            _evt("purchase", offset_ms=60_000, url="https://shop.example.com/checkout"),
        ]
        m = compute_session_metrics(events)
        assert m["entry_page_url"] == "https://shop.example.com/home"
        assert m["exit_page_url"] == "https://shop.example.com/checkout"

    def test_entry_referrer_from_first_event(self):
        events = [
            _evt("page_view", referrer="https://google.com/search?q=shoes"),
            _evt("product_view", referrer="https://shop.example.com/home"),
        ]
        m = compute_session_metrics(events)
        assert m["entry_referrer"] == "https://google.com/search?q=shoes"

    def test_device_type_from_first_event(self):
        events = [
            _evt("page_view", offset_ms=0, device_type="mobile"),
            _evt("purchase", offset_ms=30_000, device_type="desktop"),
        ]
        m = compute_session_metrics(events)
        assert m["device_type"] == "mobile"

    def test_geo_country_from_first_event(self):
        events = [
            _evt("page_view", offset_ms=0, country="DE"),
            _evt("purchase", offset_ms=30_000, country="US"),
        ]
        m = compute_session_metrics(events)
        assert m["geo_country"] == "DE"

    def test_ab_cohort_from_first_event(self):
        events = [_evt("page_view", ab_cohort="variant_b")]
        m = compute_session_metrics(events)
        assert m["ab_cohort"] == "variant_b"

    def test_event_count_matches_input_length(self):
        events = [_evt("page_view")] * 7
        m = compute_session_metrics(events)
        assert m["event_count"] == 7

    def test_events_sorted_by_timestamp(self):
        """Events provided out-of-order should be sorted before metric computation."""
        events = [
            _evt("purchase", offset_ms=60_000, url="https://shop.example.com/checkout"),
            _evt("page_view", offset_ms=0, url="https://shop.example.com/home"),
        ]
        m = compute_session_metrics(events)
        # Entry should be the page_view (earliest), exit should be purchase (latest)
        assert m["entry_page_url"] == "https://shop.example.com/home"
        assert m["exit_page_url"] == "https://shop.example.com/checkout"

    def test_checkout_and_purchase_both_counted(self):
        events = [
            _evt("checkout_start"),
            _evt("checkout_start"),   # second attempt
            _evt("purchase"),
        ]
        m = compute_session_metrics(events)
        assert m["checkout_attempts"] == 2
        assert m["purchases"] == 1

    def test_searches_counted(self):
        events = [_evt("search"), _evt("search"), _evt("product_view")]
        m = compute_session_metrics(events)
        assert m["searches"] == 2


# =============================================================================
# Tests: FUNNEL_ORDER constants
# =============================================================================

class TestFunnelOrder:

    def test_purchase_has_highest_index(self):
        assert FUNNEL_ORDER["purchase"] == max(FUNNEL_ORDER.values())

    def test_page_view_has_lowest_index(self):
        assert FUNNEL_ORDER["page_view"] == 0

    def test_checkout_complete_equals_purchase_depth(self):
        assert FUNNEL_ORDER["checkout_complete"] == FUNNEL_ORDER["purchase"]

    def test_add_and_remove_cart_at_same_depth(self):
        assert FUNNEL_ORDER["add_to_cart"] == FUNNEL_ORDER["remove_from_cart"]


# =============================================================================
# Tests: SessionStitcherFunction — state management
# =============================================================================

class TestSessionStitcherState:

    def test_event_added_to_session_buffer(self):
        stitcher = _make_stitcher()
        event = json.dumps(_evt("page_view"))
        _process(stitcher, event, BASE_TS)
        buffered = list(stitcher.session_events_state.get())
        assert len(buffered) == 1

    def test_multiple_events_accumulate_in_buffer(self):
        stitcher = _make_stitcher()
        for et in ["page_view", "product_view", "add_to_cart"]:
            _process(stitcher, json.dumps(_evt(et)), BASE_TS + 1000)
        buffered = list(stitcher.session_events_state.get())
        assert len(buffered) == 3

    def test_session_id_assigned_on_first_event(self):
        stitcher = _make_stitcher(session_id=None)
        _process(stitcher, json.dumps(_evt("page_view")), BASE_TS)
        assert stitcher.session_id_state.value() is not None

    def test_session_id_stable_across_events(self):
        stitcher = _make_stitcher(session_id=None)
        _process(stitcher, json.dumps(_evt("page_view")), BASE_TS)
        sid1 = stitcher.session_id_state.value()
        _process(stitcher, json.dumps(_evt("product_view")), BASE_TS + 5_000)
        sid2 = stitcher.session_id_state.value()
        assert sid1 == sid2

    def test_last_event_ts_updated(self):
        stitcher = _make_stitcher()
        ts = BASE_TS + 30_000
        _process(stitcher, json.dumps(_evt("page_view")), ts)
        assert stitcher.last_event_ts_state.value() == ts

    def test_gap_timer_registered_on_first_event(self):
        stitcher = _make_stitcher()
        ctx = _MockContext(BASE_TS)
        list(stitcher.process_element(json.dumps(_evt("page_view")), ctx))
        expected_timer = BASE_TS + SESSION_GAP_MS
        assert expected_timer in ctx.timer_service().timers

    def test_gap_timer_slides_on_each_event(self):
        """Each new event should delete the old timer and register a new one further ahead."""
        stitcher = _make_stitcher(gap_timer_ts=BASE_TS + SESSION_GAP_MS)
        ts2 = BASE_TS + 60_000   # 1 minute later
        ctx = _MockContext(ts2)
        list(stitcher.process_element(json.dumps(_evt("product_view")), ctx))
        new_timer = ts2 + SESSION_GAP_MS
        old_timer = BASE_TS + SESSION_GAP_MS
        assert new_timer in ctx.timer_service().timers
        assert old_timer not in ctx.timer_service().timers

    def test_gap_timer_ts_state_updated(self):
        stitcher = _make_stitcher()
        _process(stitcher, json.dumps(_evt("page_view")), BASE_TS)
        assert stitcher.gap_timer_ts_state.value() == BASE_TS + SESSION_GAP_MS


# =============================================================================
# Tests: SessionStitcherFunction — on_timer (session close)
# =============================================================================

class TestSessionClose:

    def _make_session_events(self) -> list[str]:
        return [
            json.dumps(_evt("page_view", offset_ms=0, url="https://shop.example.com/")),
            json.dumps(_evt("product_view", offset_ms=10_000, url="https://shop.example.com/p/1")),
            json.dumps(_evt("add_to_cart", offset_ms=20_000, price_usd=29.99)),
            json.dumps(_evt("purchase", offset_ms=40_000, price_usd=29.99)),
        ]

    def test_timer_emits_session_summary(self):
        stitcher = _make_stitcher(
            events=self._make_session_events(),
            session_id="test-session-001",
        )
        results = _fire_timer(stitcher, BASE_TS + SESSION_GAP_MS)
        assert len(results) == 1
        summary = results[0]
        assert summary.get("record_type") == "session_summary"

    def test_session_summary_contains_session_id(self):
        stitcher = _make_stitcher(
            events=self._make_session_events(),
            session_id="sess-abc-123",
        )
        results = _fire_timer(stitcher, BASE_TS + SESSION_GAP_MS)
        assert results[0].get("session_id") == "sess-abc-123"

    def test_session_summary_contains_funnel_stage(self):
        stitcher = _make_stitcher(events=self._make_session_events(), session_id="s1")
        results = _fire_timer(stitcher, BASE_TS + SESSION_GAP_MS)
        assert results[0].get("funnel_exit_stage") == "purchase"

    def test_session_summary_contains_revenue(self):
        stitcher = _make_stitcher(events=self._make_session_events(), session_id="s1")
        results = _fire_timer(stitcher, BASE_TS + SESSION_GAP_MS)
        assert results[0].get("revenue_attributed_usd") == pytest.approx(29.99)

    def test_session_summary_contains_event_count(self):
        stitcher = _make_stitcher(events=self._make_session_events(), session_id="s1")
        results = _fire_timer(stitcher, BASE_TS + SESSION_GAP_MS)
        assert results[0].get("event_count") == 4

    def test_state_cleared_after_session_close(self):
        stitcher = _make_stitcher(events=self._make_session_events(), session_id="s1")
        _fire_timer(stitcher, BASE_TS + SESSION_GAP_MS)
        # State should be cleared for next session
        buffered = list(stitcher.session_events_state.get())
        assert buffered == []
        assert stitcher.session_id_state.value() is None

    def test_empty_buffer_on_timer_produces_no_output(self):
        """Timer firing with no buffered events (e.g. duplicate timer) should emit nothing."""
        stitcher = _make_stitcher(events=[], session_id="s1")
        results = _fire_timer(stitcher, BASE_TS + SESSION_GAP_MS)
        assert results == []

    def test_cart_abandonment_session_emits_summary(self):
        events = [
            json.dumps(_evt("page_view")),
            json.dumps(_evt("add_to_cart", price_usd=49.99)),
            # No purchase — cart abandonment
        ]
        stitcher = _make_stitcher(events=events, session_id="s2")
        results = _fire_timer(stitcher, BASE_TS + SESSION_GAP_MS)
        assert results[0]["cart_abandonment"] is True
        assert results[0]["funnel_exit_stage"] == "add_to_cart"

    def test_browse_only_session(self):
        events = [json.dumps(_evt("page_view")), json.dumps(_evt("search"))]
        stitcher = _make_stitcher(events=events, session_id="s3")
        results = _fire_timer(stitcher, BASE_TS + SESSION_GAP_MS)
        assert results[0]["funnel_exit_stage"] == "browse"
        assert results[0]["revenue_attributed_usd"] == pytest.approx(0.0)
        assert results[0]["cart_abandonment"] is False


# =============================================================================
# Tests: session gap constant
# =============================================================================

class TestSessionGapConstant:

    def test_gap_is_15_minutes_in_ms(self):
        assert SESSION_GAP_MS == 15 * 60 * 1000

    def test_funnel_stages_list_ordered(self):
        """FUNNEL_STAGES must be ordered from shallowest to deepest."""
        assert FUNNEL_STAGES[0] == "browse"
        assert FUNNEL_STAGES[-1] == "purchase"
        assert len(FUNNEL_STAGES) == 5


# =============================================================================
# Tests: malformed input resilience
# =============================================================================

class TestMalformedInputs:

    def test_invalid_json_does_not_raise(self):
        stitcher = _make_stitcher()
        ctx = _MockContext(BASE_TS)
        # Should not raise — silently skip
        try:
            list(stitcher.process_element("{bad json", ctx))
        except Exception as e:
            pytest.fail(f"process_element raised on bad JSON: {e}")

    def test_event_missing_event_type(self):
        stitcher = _make_stitcher()
        event = json.dumps({"event_ts_ms": BASE_TS, "user_id": "u1"})
        # No event_type — should fall back to "browse" in funnel
        ctx = _MockContext(BASE_TS)
        list(stitcher.process_element(event, ctx))
        buffered = list(stitcher.session_events_state.get())
        assert len(buffered) == 1

    def test_event_missing_ts_ms(self):
        """Events without event_ts_ms should not crash session duration computation."""
        events_raw = [{"event_type": "page_view"}, {"event_type": "purchase"}]
        m = compute_session_metrics(events_raw)
        # ts defaults to 0 — duration should be 0
        assert m["session_duration_s"] == 0

    def test_event_with_null_product(self):
        """Events with null product dict should compute revenue as 0."""
        events = [{"event_type": "purchase", "product": None, "event_ts_ms": BASE_TS}]
        m = compute_session_metrics(events)
        assert m["revenue_attributed_usd"] == pytest.approx(0.0)

    def test_event_with_null_geo(self):
        events = [{"event_type": "page_view", "geo": None, "event_ts_ms": BASE_TS}]
        m = compute_session_metrics(events)
        assert m.get("geo_country") is None

    def test_high_fraud_score_session(self):
        events = [
            {"event_type": "purchase", "fraud_score": 0.95, "event_ts_ms": BASE_TS,
             "product": {"price_usd": 500.0}, "geo": {"country": "US"},
             "page": {"url": "u", "referrer": ""}, "device": {"type": "mobile", "os": "Android"},
             "flags": {"ab_cohort": "control"}},
        ]
        m = compute_session_metrics(events)
        assert m["max_fraud_score"] == pytest.approx(0.95)
        assert m["fraud_flagged"] is True
