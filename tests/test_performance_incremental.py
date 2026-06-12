import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.performance import PerformanceTracker


class TestPerformanceIncrementalFallback(unittest.TestCase):
    def test_daily_summary_reads_incremental_live_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            daily_dir = root / "daily"
            daily_dir.mkdir(parents=True)
            (daily_dir / "2026-06-09_live.json").write_text(
                json.dumps({
                    "date": "2026-06-09",
                    "trading_mode": "paper",
                    "trades": [{"net_pnl": 120}, {"pnl": -30}],
                    "paper_signals": [{"action": "buy", "price": 44000}],
                    "updated_at": "2026-06-09T13:00:00",
                }),
                encoding="utf-8",
            )

            data = PerformanceTracker(data_dir=str(root), trading_mode="paper").get_daily_summary("2026-06-09")

            self.assertEqual(data["source"], "incremental_live")
            self.assertEqual(data["total_trades"], 2)
            self.assertEqual(data["daily_pnl"], 90)
            self.assertEqual(len(data["paper_signals"]), 1)


if __name__ == "__main__":
    unittest.main()
