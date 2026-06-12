import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.engine import TradingEngine


class FakeAggregator:
    def __init__(self, price):
        self.current_price = price


class FakeSnapshot:
    def __init__(self, atr):
        self.atr = atr


class FakePipeline:
    def __init__(self, price, atr):
        self.aggregator = FakeAggregator(price)
        self.snapshot = FakeSnapshot(atr)


class FakeCircuitBreaker:
    def __init__(self):
        self.price_anomalies = []

    def on_price_anomaly(self, reason):
        self.price_anomalies.append(reason)


class FakeRiskManager:
    def __init__(self):
        self.circuit_breaker = FakeCircuitBreaker()


class TestPriceAnomalyDataGap(unittest.TestCase):
    def make_engine(self, price=1600, atr=100):
        engine = TradingEngine()
        engine.instruments = ["TMF"]
        engine.pipelines = {"TMF": FakePipeline(price, atr)}
        engine.risk_manager = FakeRiskManager()
        return engine

    def test_recent_large_jump_still_triggers_emergency(self):
        engine = self.make_engine(price=1600, atr=100)
        pipeline = engine.pipelines["TMF"]
        pipeline._last_heartbeat_price = 1000
        pipeline._last_heartbeat_at = datetime.now() - timedelta(seconds=60)

        engine._check_price_anomaly()

        self.assertEqual(len(engine.risk_manager.circuit_breaker.price_anomalies), 1)
        self.assertEqual(engine._last_price_anomaly["TMF"]["status"], "triggered")
        self.assertTrue(engine._last_price_anomaly["TMF"]["triggered"])

    def test_stale_baseline_resets_without_emergency(self):
        engine = self.make_engine(price=1600, atr=100)
        pipeline = engine.pipelines["TMF"]
        pipeline._last_heartbeat_price = 1000
        pipeline._last_heartbeat_at = datetime.now() - timedelta(seconds=181)

        engine._check_price_anomaly()

        self.assertEqual(engine.risk_manager.circuit_breaker.price_anomalies, [])
        anomaly = engine._last_price_anomaly["TMF"]
        self.assertEqual(anomaly["status"], "data_gap_reset")
        self.assertEqual(anomaly["severity"], "warning")
        self.assertFalse(anomaly["triggered"])
        self.assertIn("hold_until", anomaly)
        self.assertEqual(pipeline._last_heartbeat_price, 1600)

    def test_recent_three_atr_warning_does_not_enter_emergency(self):
        engine = self.make_engine(price=1350, atr=100)
        pipeline = engine.pipelines["TMF"]
        pipeline._last_heartbeat_price = 1000
        pipeline._last_heartbeat_at = datetime.now() - timedelta(seconds=60)

        engine._check_price_anomaly()

        self.assertEqual(engine.risk_manager.circuit_breaker.price_anomalies, [])
        self.assertEqual(engine._last_price_anomaly["TMF"]["status"], "warning")


if __name__ == "__main__":
    unittest.main()
