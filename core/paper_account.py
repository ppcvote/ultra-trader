"""
Paper account persistence.

This stores the simulated account balance across paper-mode restarts without
restoring paper positions or touching live account state.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger


class PaperAccountStore:
    """Persistent paper account balance store."""

    def __init__(self, project_root: Path):
        self.project_root = Path(project_root)
        self.data_dir = self.project_root / "data" / "paper_account"
        self.state_path = self.data_dir / "state.json"
        self.daily_dir = self.project_root / "data" / "performance" / "daily"

    def resolve_initial_balance(
        self,
        trading_mode: str,
        instruments: list[str],
        env_initial_balance: float,
    ) -> tuple[float, str, list[str]]:
        """Resolve startup balance and source for paper mode."""
        warnings: list[str] = []
        if trading_mode != "paper":
            return env_initial_balance, "initial_balance", warnings

        state = self._read_json(self.state_path)
        if state:
            balance = self._as_positive_float(state.get("balance"))
            if balance is not None:
                open_positions = state.get("open_positions_summary") or []
                if open_positions:
                    warnings.append("existing paper position summary found; restoring balance only")
                old_instruments = state.get("instruments") or []
                if old_instruments and sorted(old_instruments) != sorted(instruments):
                    warnings.append(
                        f"paper state instruments {old_instruments} differ from current {instruments}; "
                        "restoring shared balance only"
                    )
                return balance, "paper_state", warnings

        perf_balance = self._latest_performance_balance(require_trades=True)
        if perf_balance is not None:
            return perf_balance, "performance_fallback", warnings

        perf_balance = self._latest_performance_balance(require_trades=False)
        if perf_balance is not None:
            return perf_balance, "performance_fallback", warnings

        if env_initial_balance > 0:
            return env_initial_balance, "initial_balance", warnings
        return 100000.0, "initial_balance", warnings

    def save(
        self,
        trading_mode: str,
        risk_profile: str,
        instruments: list[str],
        position_manager: Any,
        source: str,
    ) -> None:
        """Persist current paper balance. Live/simulation modes are ignored."""
        if trading_mode != "paper" or position_manager is None:
            return

        open_positions = []
        for instrument, pos in getattr(position_manager, "positions", {}).items():
            if pos and not getattr(pos, "is_flat", True):
                side = getattr(pos, "side", None)
                entry_time = getattr(pos, "entry_time", None)
                open_positions.append({
                    "instrument": instrument,
                    "side": getattr(side, "value", str(side)),
                    "entry_price": getattr(pos, "entry_price", 0.0),
                    "quantity": getattr(pos, "quantity", 0),
                    "entry_time": entry_time.isoformat() if entry_time else None,
                })

        payload = {
            "balance": float(getattr(position_manager, "balance", 0.0)),
            "last_updated": datetime.now().isoformat(),
            "source": source,
            "trading_mode": trading_mode,
            "risk_profile": risk_profile,
            "instruments": instruments,
            "open_positions_summary": open_positions,
        }

        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning(f"[PaperAccount] save failed: {exc}")

    def _latest_performance_balance(self, require_trades: bool) -> float | None:
        if not self.daily_dir.exists():
            return None

        for path in sorted(self.daily_dir.glob("*.json"), reverse=True):
            data = self._read_json(path)
            if not data:
                continue
            if require_trades and int(data.get("total_trades") or 0) <= 0:
                continue
            balance = self._as_positive_float(data.get("ending_balance"))
            if balance is not None:
                return balance
        return None

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any] | None:
        try:
            if not path.exists():
                return None
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning(f"[PaperAccount] read failed {path}: {exc}")
            return None

    @staticmethod
    def _as_positive_float(value: Any) -> float | None:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if number <= 0:
            return None
        return number
