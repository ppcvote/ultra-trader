"""
Paper account persistence tests.
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.paper_account import PaperAccountStore


class TestPaperAccountStore(unittest.TestCase):
    def test_state_file_has_priority(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "paper_account"
            state_dir.mkdir(parents=True)
            (state_dir / "state.json").write_text(
                json.dumps({
                    "balance": 107500,
                    "trading_mode": "paper",
                    "instruments": ["TMF"],
                    "open_positions_summary": [],
                }),
                encoding="utf-8",
            )

            balance, source, warnings = PaperAccountStore(root).resolve_initial_balance(
                trading_mode="paper",
                instruments=["TMF"],
                env_initial_balance=100000,
            )

            self.assertEqual(balance, 107500)
            self.assertEqual(source, "paper_state")
            self.assertEqual(warnings, [])

    def test_falls_back_to_latest_trading_daily_balance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            daily = root / "data" / "performance" / "daily"
            daily.mkdir(parents=True)
            (daily / "2026-06-08.json").write_text(
                json.dumps({
                    "ending_balance": 100000,
                    "total_trades": 0,
                }),
                encoding="utf-8",
            )
            (daily / "2026-06-06.json").write_text(
                json.dumps({
                    "ending_balance": 112830,
                    "total_trades": 10,
                }),
                encoding="utf-8",
            )

            balance, source, _ = PaperAccountStore(root).resolve_initial_balance(
                trading_mode="paper",
                instruments=["TMF"],
                env_initial_balance=100000,
            )

            self.assertEqual(balance, 112830)
            self.assertEqual(source, "performance_fallback")

    def test_live_mode_ignores_paper_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "paper_account"
            state_dir.mkdir(parents=True)
            (state_dir / "state.json").write_text(
                json.dumps({"balance": 107500}),
                encoding="utf-8",
            )

            balance, source, _ = PaperAccountStore(root).resolve_initial_balance(
                trading_mode="live",
                instruments=["TMF"],
                env_initial_balance=43000,
            )

            self.assertEqual(balance, 43000)
            self.assertEqual(source, "initial_balance")


if __name__ == "__main__":
    unittest.main()
