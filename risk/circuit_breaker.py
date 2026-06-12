"""
UltraTrader 熔斷機制
異常狀況自動停機，保護帳戶安全
"""

import threading
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

from loguru import logger


class CircuitState(Enum):
    ACTIVE = "active"            # 正常運行
    COOLDOWN = "cooldown"        # 冷卻中（暫停交易）
    HALTED = "halted"            # 停機（需手動恢復或等待新交易日）
    EMERGENCY_STOP = "emergency" # 緊急停機（需手動處理）


class CircuitBreaker:
    """
    熔斷機制 — 四種觸發條件（線程安全版）

    1. 每日虧損超限 → HALTED（當日停機）
    2. 連續虧損 N 筆 → COOLDOWN（冷卻 M 分鐘）
    3. 短時間內過多交易 → COOLDOWN
    4. 單筆異常虧損（> 預期 2 倍）→ EMERGENCY_STOP
    """

    def __init__(
        self,
        max_daily_loss: float = 500,
        max_consecutive_loss: int = 4,
        cooldown_minutes: int = 15,
        max_trades_per_window: int = 5,
        trade_window_minutes: int = 10,
    ):
        self._lock = threading.Lock()
        self.max_daily_loss = max_daily_loss
        self.max_consecutive_loss = max_consecutive_loss
        self.cooldown_minutes = cooldown_minutes
        self.max_trades_per_window = max_trades_per_window
        self.trade_window_minutes = trade_window_minutes

        self._state = CircuitState.ACTIVE
        self._cooldown_until: Optional[datetime] = None
        self._halt_reason: str = ""
        self._halt_type: str = ""
        self._trade_timestamps: list[datetime] = []
        self._daily_loss: float = 0.0
        self._consecutive_losses: int = 0
        self._today: Optional[str] = None

    @property
    def state(self) -> CircuitState:
        """取得當前狀態（自動檢查冷卻結束 + 新日重置）— 線程安全"""
        with self._lock:
            # 冷卻結束自動恢復
            if self._state == CircuitState.COOLDOWN and self._cooldown_until:
                if datetime.now() >= self._cooldown_until:
                    logger.info("⏰ 冷卻結束，恢復交易")
                    self._state = CircuitState.ACTIVE
                    self._cooldown_until = None
            # 新交易日自動重置 HALTED
            if self._state == CircuitState.HALTED and self._today:
                today = datetime.now().strftime("%Y-%m-%d")
                if today != self._today:
                    self._today = today
                    self._daily_loss = 0
                    self._consecutive_losses = 0
                    self._state = CircuitState.ACTIVE
                    self._halt_reason = ""
                    self._halt_type = ""
                    logger.info("🔄 新交易日，熔斷自動重置")
            return self._state

    @property
    def can_trade(self) -> bool:
        """是否允許交易"""
        return self.state == CircuitState.ACTIVE

    def on_trade(self, pnl: float, expected_max_loss: float = 0):
        """
        交易完成時呼叫 — 線程安全

        pnl: 該筆損益（正=獲利，負=虧損）
        expected_max_loss: 預期最大虧損（停損距離 × 點值）
        """
        with self._lock:
            now = datetime.now()
            today = now.strftime("%Y-%m-%d")

            # 新的一天，重置日內計數
            if self._today != today:
                self._today = today
                self._daily_loss = 0
                self._consecutive_losses = 0
                if self._state == CircuitState.HALTED:
                    self._state = CircuitState.ACTIVE
                    logger.info("🔄 新交易日，熔斷重置")

            # 記錄交易時間
            self._trade_timestamps.append(now)
            self._trade_timestamps = [
                t for t in self._trade_timestamps
                if now - t < timedelta(minutes=self.trade_window_minutes)
            ]

            if pnl < 0:
                self._daily_loss += abs(pnl)
                self._consecutive_losses += 1
            else:
                self._consecutive_losses = 0

            # ---- 檢查觸發條件 ----

            # 條件 1：每日虧損超限
            if self._daily_loss >= self.max_daily_loss:
                self._state = CircuitState.HALTED
                self._halt_reason = f"每日虧損超限: {self._daily_loss:,.0f} 元 >= {self.max_daily_loss:,.0f} 元"
                self._halt_type = "daily_loss"
                logger.warning(f"🛑 熔斷觸發: {self._halt_reason}")
                return

            # 條件 2：連續虧損
            if self._consecutive_losses >= self.max_consecutive_loss:
                self._enter_cooldown(
                    f"連續虧損 {self._consecutive_losses} 筆"
                )
                self._consecutive_losses = 0
                return

            # 條件 3：短時間過多交易
            if len(self._trade_timestamps) >= self.max_trades_per_window:
                self._enter_cooldown(
                    f"{self.trade_window_minutes}分鐘內交易 {len(self._trade_timestamps)} 筆"
                )
                return

            # 條件 4：單筆異常虧損
            if pnl < 0 and expected_max_loss > 0 and abs(pnl) > expected_max_loss * 2:
                self._state = CircuitState.EMERGENCY_STOP
                self._halt_reason = f"單筆異常虧損: {pnl:,.0f} 元（預期最大 {expected_max_loss:,.0f} 元）"
                self._halt_type = "abnormal_loss"
                logger.error(f"🚨 緊急停機: {self._halt_reason}")
                return

    def on_connection_lost(self):
        """連線中斷"""
        with self._lock:
            self._state = CircuitState.EMERGENCY_STOP
            self._halt_reason = "券商連線中斷"
            self._halt_type = "connection_lost"
            logger.error("🚨 緊急停機: 券商連線中斷")

    def on_connection_restored(self):
        """連線恢復"""
        with self._lock:
            if (
                self._state == CircuitState.EMERGENCY_STOP
                and (self._halt_type == "connection_lost" or "連線" in self._halt_reason)
            ):
                self._state = CircuitState.ACTIVE
                self._halt_reason = ""
                self._halt_type = ""
                logger.info("✅ 連線恢復，解除緊急停機")

    def on_price_anomaly(self, reason: str):
        """價格異常：與券商連線中斷分開記錄"""
        with self._lock:
            self._state = CircuitState.EMERGENCY_STOP
            self._halt_reason = reason
            self._halt_type = "price_anomaly"
            logger.error(f"🚨 緊急停機: {reason}")

    def manual_resume(self):
        """手動恢復交易"""
        with self._lock:
            old_state = self._state
            self._state = CircuitState.ACTIVE
            self._halt_reason = ""
            self._halt_type = ""
            self._consecutive_losses = 0
            logger.info(f"🔄 手動恢復交易（從 {old_state.value}）")

    def _enter_cooldown(self, reason: str):
        """進入冷卻"""
        self._state = CircuitState.COOLDOWN
        self._cooldown_until = datetime.now() + timedelta(minutes=self.cooldown_minutes)
        self._halt_reason = reason
        self._halt_type = "cooldown"
        logger.warning(
            f"⏸️ 進入冷卻 {self.cooldown_minutes} 分鐘: {reason}"
        )

    def update_settings(
        self,
        max_daily_loss: float = None,
        max_consecutive_loss: int = None,
        cooldown_minutes: int = None,
    ):
        """更新熔斷設定"""
        if max_daily_loss is not None:
            self.max_daily_loss = max_daily_loss
        if max_consecutive_loss is not None:
            self.max_consecutive_loss = max_consecutive_loss
        if cooldown_minutes is not None:
            self.cooldown_minutes = cooldown_minutes

    def to_dict(self) -> dict:
        """序列化（供 Dashboard 顯示）"""
        state = self.state  # 觸發自動檢查冷卻
        return {
            "state": state.value,
            "can_trade": state == CircuitState.ACTIVE,
            "halt_reason": self._halt_reason,
            "halt_type": self._halt_type,
            "daily_loss": round(self._daily_loss, 0),
            "max_daily_loss": self.max_daily_loss,
            "daily_loss_pct": round(self._daily_loss / self.max_daily_loss * 100, 1) if self.max_daily_loss > 0 else 0,
            "consecutive_losses": self._consecutive_losses,
            "max_consecutive_loss": self.max_consecutive_loss,
            "cooldown_until": self._cooldown_until.isoformat() if self._cooldown_until else None,
            "recent_trades": len(self._trade_timestamps),
        }
