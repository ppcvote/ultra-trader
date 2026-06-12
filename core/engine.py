"""
UltraTrader 交易引擎（多商品版）
核心迴圈：串接 Broker → MarketData → Strategy → Risk → Dashboard
支援同時交易多個商品（如 TMF + TGF）
"""

import math
import os
import sys
import threading
import queue
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional, Callable


def _safe_round(val, decimals=1, default=0):
    """安全 round — NaN/Inf 返回 default"""
    if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
        return default
    return round(val, decimals)

from loguru import logger
from dotenv import load_dotenv

# 確保 UltraTrader 根目錄在 sys.path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.broker import BaseBroker, MockBroker, ShioajiBroker, OrderResult, AccountInfo
from core.market_data import Tick, KBar, TickAggregator, IndicatorEngine, MarketSnapshot
from core.position import PositionManager, Position, Side
from core.logger import setup_logger, log_trade, log_order, log_fill, log_pnl
from core.instrument_config import INSTRUMENT_SPECS, get_spec, InstrumentSpec
from strategy.base import BaseStrategy, Signal, SignalDirection
from strategy.momentum import AdaptiveMomentumStrategy
from strategy.gold_trend import GoldTrendStrategy
from strategy.filters import MarketRegime, SessionManager, SessionPhase
from risk.manager import RiskManager
from core.performance import PerformanceTracker
from intelligence.data_collector import DataCollector
from intelligence.left_side_score import LeftSideScoreEngine


class EngineState(Enum):
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class InstrumentPipeline:
    """每個商品的獨立處理管線（升級版：含多時框指標引擎）"""
    code: str
    spec: InstrumentSpec
    aggregator: TickAggregator = field(default=None)
    indicator_engine: IndicatorEngine = field(default=None)
    strategy: BaseStrategy = field(default=None)
    snapshot: MarketSnapshot = field(default_factory=MarketSnapshot)
    # 多時框指標引擎
    indicator_engine_5m: IndicatorEngine = field(default=None)
    indicator_engine_15m: IndicatorEngine = field(default=None)
    snapshot_5m: MarketSnapshot = field(default_factory=MarketSnapshot)
    snapshot_15m: MarketSnapshot = field(default_factory=MarketSnapshot)

    def __post_init__(self):
        if self.aggregator is None:
            self.aggregator = TickAggregator(intervals=[1, 5, 15])
        if self.indicator_engine is None:
            self.indicator_engine = IndicatorEngine(lookback_period=200)
        if self.indicator_engine_5m is None:
            self.indicator_engine_5m = IndicatorEngine(lookback_period=200)
        if self.indicator_engine_15m is None:
            self.indicator_engine_15m = IndicatorEngine(lookback_period=200)


def _create_strategy(strategy_type: str) -> BaseStrategy:
    """根據類型建立策略"""
    if strategy_type == "gold_trend":
        return GoldTrendStrategy()
    return AdaptiveMomentumStrategy()


class TradingEngine:
    """
    交易引擎 — 多商品事件驅動架構

    資料流（每個商品獨立管線）：
    Tick → TickAggregator → K棒完成 → IndicatorEngine → Strategy → RiskManager → Broker
    """

    def __init__(self):
        self.state = EngineState.INITIALIZING
        self._lock = threading.Lock()
        self._event_queue: queue.Queue = queue.Queue(maxsize=10000)

        # 元件
        self.broker: Optional[BaseBroker] = None
        self.risk_manager: Optional[RiskManager] = None
        self.position_manager: Optional[PositionManager] = None

        # 多商品管線
        self.instruments: list[str] = []
        self.pipelines: dict[str, InstrumentPipeline] = {}

        # 向後相容（單商品時使用）
        self.aggregator: Optional[TickAggregator] = None
        self.indicator_engine: Optional[IndicatorEngine] = None
        self.strategy: Optional[BaseStrategy] = None
        self.snapshot: MarketSnapshot = MarketSnapshot()

        # 設定
        self.trading_mode: str = "simulation"
        self.risk_profile: str = "balanced"
        self.timeframe: int = 1

        # Intelligence
        self.data_collector: Optional[DataCollector] = None
        self.left_side_engine: Optional[LeftSideScoreEngine] = None

        # 績效
        self.performance: Optional[PerformanceTracker] = None

        # Dashboard
        self._ws_broadcast: Optional[Callable] = None

        # 狀態
        self._running = False
        self._engine_thread: Optional[threading.Thread] = None
        self._heartbeat_count = 0
        self._tick_count = 0

        # 下單失敗冷卻（防止無限重試轟炸券商 API）
        self._order_fail_cooldown: dict[str, datetime] = {}  # instrument → 冷卻到期時間
        self._ENTRY_FAIL_COOLDOWN_SEC = 60   # 進場失敗冷卻 60 秒
        self._EXIT_FAIL_COOLDOWN_SEC = 30    # 出場失敗冷卻 30 秒（防止連續轟炸）
        self._exit_fail_count: dict[str, int] = {}  # 出場連續失敗次數
        self._EXIT_MAX_RETRIES = 3           # 出場最多重試 3 次，超過強制重置持倉
        self._exiting: set = set()           # 正在執行出場的商品（防止雙重平倉）
        self._exit_lock = threading.Lock()   # 保護 _exiting set 的線程安全
        self._market_data_gap_until: dict[str, datetime] = {}
        self._last_price_anomaly: dict[str, dict] = {}
        self._PRICE_BASELINE_MAX_AGE_SEC = 180
        self._DATA_GAP_HOLD_SEC = 180

    def initialize(self):
        """初始化所有元件"""
        logger.info("[Engine] initializing...")

        # 保留 CLI 已設定的環境變數
        _cli_mode = os.environ.get("TRADING_MODE")
        _cli_risk = os.environ.get("RISK_PROFILE")

        env_path = PROJECT_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=False)
            logger.info("[Config] .env loaded")

        # CLI 參數優先
        if _cli_mode:
            os.environ["TRADING_MODE"] = _cli_mode
        if _cli_risk:
            os.environ["RISK_PROFILE"] = _cli_risk

        self.trading_mode = os.getenv("TRADING_MODE", "simulation")
        self.risk_profile = os.getenv("RISK_PROFILE", "balanced")

        # ---- 解析商品列表 ----
        instruments_str = os.getenv("INSTRUMENTS", "").strip()
        if instruments_str:
            self.instruments = [i.strip() for i in instruments_str.split(",") if i.strip()]
        else:
            self.instruments = [os.getenv("CONTRACT_CODE", "TMF")]

        logger.info(f"[Config] instruments: {self.instruments}")

        # ---- 建立每個商品的 Pipeline ----
        for code in self.instruments:
            spec = get_spec(code)
            strategy = _create_strategy(spec.strategy_type)
            pipeline = InstrumentPipeline(
                code=code,
                spec=spec,
                strategy=strategy,
            )
            # 註冊 K 棒回調（1m/5m/15m）
            pipeline.aggregator.on_kbar_complete(
                self.timeframe,
                lambda kbar, inst=code: self._on_kbar_complete(inst, kbar)
            )
            pipeline.aggregator.on_kbar_complete(
                5,
                lambda kbar, inst=code: self._on_kbar_5m_complete(inst, kbar)
            )
            pipeline.aggregator.on_kbar_complete(
                15,
                lambda kbar, inst=code: self._on_kbar_15m_complete(inst, kbar)
            )
            self.pipelines[code] = pipeline
            logger.info(f"[Pipeline] {code}: {spec.name} | strategy={spec.strategy_type} | point_value={spec.point_value}")

        # 向後相容 — 第一個商品
        first = self.pipelines[self.instruments[0]]
        self.aggregator = first.aggregator
        self.indicator_engine = first.indicator_engine
        self.strategy = first.strategy
        self.snapshot = first.snapshot

        # ---- 建立 Broker ----
        if self.trading_mode == "simulation":
            sim_balance = float(os.getenv("INITIAL_BALANCE", "100000"))
            mock_instruments = {}
            for code in self.instruments:
                spec = get_spec(code)
                mock_instruments[code] = {
                    "initial_price": spec.default_initial_price,
                    "volatility": 0.6 if code == "TMF" else 0.3,
                }
            self.broker = MockBroker(
                initial_price=mock_instruments[self.instruments[0]]["initial_price"],
                tick_interval=0.5,
                volatility=0.6,
                initial_balance=sim_balance,
                instruments=mock_instruments,
            )
            logger.info("[Mode] simulation (local)")
        else:
            api_key = os.getenv("SHIOAJI_API_KEY", "")
            secret_key = os.getenv("SHIOAJI_SECRET_KEY", "")
            if not api_key or not secret_key:
                logger.error("[Config] missing SHIOAJI_API_KEY / SHIOAJI_SECRET_KEY")
                self.state = EngineState.ERROR
                return False

            self.broker = ShioajiBroker(
                api_key=api_key,
                secret_key=secret_key,
                ca_path=os.getenv("SHIOAJI_CA_PATH", ""),
                ca_password=os.getenv("SHIOAJI_CA_PASSWORD", ""),
                person_id=os.getenv("SHIOAJI_PERSON_ID", ""),
                simulation=False,
                contract_codes=self.instruments,
            )
            if self.trading_mode == "paper":
                logger.info("[Mode] paper (real data, no orders)")
            else:
                logger.info("[Mode] LIVE trading")

        # ---- 風控 ----
        self.risk_manager = RiskManager(profile=self.risk_profile)

        # ---- 部位管理（多商品共用餘額）----
        initial_balance = float(os.getenv("INITIAL_BALANCE", "0"))
        configs = {code: get_spec(code) for code in self.instruments}
        self.position_manager = PositionManager(
            instruments=self.instruments,
            configs=configs,
            initial_balance=initial_balance,
        )
        if initial_balance > 0:
            logger.info(f"[Account] balance: {initial_balance:,.0f}")

        # ---- 績效追蹤 ----
        perf_dir = str(PROJECT_ROOT / "data" / "performance")
        self.performance = PerformanceTracker(
            data_dir=perf_dir,
            trading_mode=self.trading_mode,
        )
        self.performance.starting_balance = initial_balance

        # ---- Intelligence ----
        self.data_collector = DataCollector()
        self.left_side_engine = LeftSideScoreEngine()
        self.data_collector.set_on_update(self._on_intelligence_update)
        logger.info("[Intelligence] module initialized")

        self.state = EngineState.INITIALIZING
        logger.info(f"[Engine] initialized with {len(self.instruments)} instruments")
        return True

    def start(self):
        """啟動交易引擎"""
        if self.state == EngineState.RUNNING:
            logger.warning("引擎已在運行中")
            return

        if not self.broker.connect():
            logger.error("[Broker] connection failed")
            self.state = EngineState.ERROR
            return

        # ---- 暖機（模擬模式）----
        if self.trading_mode == "simulation" and isinstance(self.broker, MockBroker):
            for code, pipeline in self.pipelines.items():
                warmup_ticks = self.broker.generate_warmup_ticks(instrument=code, minutes=60, ticks_per_bar=30)
                for tick in warmup_ticks:
                    pipeline.aggregator.on_tick(tick)

                # 清空暖機事件
                while not self._event_queue.empty():
                    try:
                        self._event_queue.get_nowait()
                    except queue.Empty:
                        break

                # 逐根 K 棒更新指標
                bars = pipeline.aggregator.get_bars(self.timeframe, count=200)
                for i, bar in enumerate(bars):
                    df = pipeline.aggregator.get_bars_dataframe(self.timeframe, count=i + 1)
                    if len(df) >= 5:
                        pipeline.snapshot = pipeline.indicator_engine.update(df)
                        if hasattr(pipeline.strategy, 'regime_classifier'):
                            pipeline.strategy.regime_classifier.classify(pipeline.snapshot)

                # 最終快照
                df = pipeline.aggregator.get_bars_dataframe(self.timeframe, count=200)
                if len(df) >= 5:
                    pipeline.snapshot = pipeline.indicator_engine.update(df)
                logger.info(f"[Warmup] {code}: {len(bars)} bars | ADX={pipeline.snapshot.adx:.1f} RSI={pipeline.snapshot.rsi:.1f}")

                # 廣播暖機 K 棒
                for bar in bars:
                    self._broadcast("kbar", {
                        "instrument": code,
                        "time": bar.datetime.isoformat(),
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                        "interval": bar.interval,
                    })

        # ---- 暖機（live/paper 模式）— 用 Shioaji 歷史 K 棒 ----
        if self.trading_mode in ("live", "paper") and hasattr(self.broker, 'get_historical_kbars'):
            for code, pipeline in self.pipelines.items():
                hist_bars = self.broker.get_historical_kbars(instrument=code, count=2000)
                if hist_bars:
                    for bar in hist_bars:
                        pipeline.aggregator._completed_bars[1].append(bar)

                    # 從 1 分 K 合成 5 分 / 15 分 K 棒
                    for interval in pipeline.aggregator.intervals:
                        if interval == 1:
                            continue
                        merged = []
                        bucket = None
                        for b in hist_bars:
                            dt = b.datetime
                            mins = dt.hour * 60 + dt.minute
                            bar_mins = (mins // interval) * interval
                            bt = dt.replace(hour=bar_mins // 60, minute=bar_mins % 60, second=0, microsecond=0)
                            # 日期+時間都要一樣才合併，避免跨日合成出巨大K棒
                            if bucket is None or bucket.datetime != bt or bucket.datetime.date() != bt.date():
                                if bucket is not None:
                                    merged.append(bucket)
                                from core.market_data import KBar
                                bucket = KBar(datetime=bt, open=b.open, high=b.high,
                                              low=b.low, close=b.close, volume=b.volume, interval=interval)
                            else:
                                bucket.high = max(bucket.high, b.high)
                                bucket.low = min(bucket.low, b.low)
                                bucket.close = b.close
                                bucket.volume += b.volume
                        if bucket is not None:
                            merged.append(bucket)
                        pipeline.aggregator._completed_bars[interval] = merged
                        logger.info(f"[Warmup] {code}: {len(merged)} bars synthesized for {interval}m")

                    # 更新指標
                    df = pipeline.aggregator.get_bars_dataframe(self.timeframe, count=200)
                    if len(df) >= 5:
                        pipeline.snapshot = pipeline.indicator_engine.update(df)
                        if hasattr(pipeline.strategy, 'regime_classifier'):
                            pipeline.strategy.regime_classifier.classify(pipeline.snapshot)
                    logger.info(f"[Warmup] {code}: {len(hist_bars)} historical bars loaded")

            # 同步真實持倉到引擎（僅 live 模式，paper 模式的持倉由策略信號產生）
            if self.trading_mode == "live" and hasattr(self.broker, 'get_real_positions'):
                try:
                    real_positions = self.broker.get_real_positions()
                    for rp in real_positions:
                        # 從 real position code (e.g. TMFC6) 反查 instrument (TMF)
                        inst = None
                        for code in self.instruments:
                            if rp['code'].startswith(code):
                                inst = code
                                break
                        if not inst:
                            continue

                        pos = self.position_manager.positions.get(inst)
                        if pos and not pos.is_flat:
                            continue  # 已有持倉，跳過

                        side = Side.SHORT if 'Sell' in rp['direction'] else Side.LONG
                        pipeline = self.pipelines.get(inst)
                        atr = pipeline.snapshot.atr if pipeline and pipeline.snapshot.atr > 0 else 0
                        entry = rp['price']
                        current_price = pipeline.snapshot.price if pipeline else entry

                        # 安全 SL/TP：用較寬的 4x ATR 停損，防止啟動時立刻觸發
                        # 如果已有未實現虧損超過 4x ATR，不設停損（SL=0），交給人工判斷
                        sl = 0.0
                        tp = 0.0
                        if atr > 0:
                            unrealized = abs(current_price - entry)
                            if unrealized < atr * 4:  # 虧損在合理範圍內才設 SL
                                if side == Side.LONG:
                                    sl = round(entry - atr * 4)
                                    tp = round(entry + atr * 6)
                                else:
                                    sl = round(entry + atr * 4)
                                    tp = round(entry - atr * 6)
                            else:
                                logger.warning(f"[Sync] {inst} 未實現虧損 {unrealized:.0f} > 4xATR({atr*4:.0f})，不設自動停損")

                        self.position_manager.open_position(
                            instrument=inst,
                            side=side,
                            price=entry,
                            quantity=rp['quantity'],
                            stop_loss=sl,
                            take_profit=tp,
                        )
                        logger.info(f"[Sync] 同步真實持倉: {inst} {side.value} x{rp['quantity']} @ {entry} | SL={sl} TP={tp}")
                except Exception as e:
                    logger.warning(f"[Sync] 同步持倉失敗: {e}")

        # 更新向後相容快照
        self.snapshot = self.pipelines[self.instruments[0]].snapshot

        # 訂閱 Tick
        self.broker.subscribe_tick(self._on_tick)

        # 啟動券商心跳監控（live 模式）
        if self.trading_mode == "live" and hasattr(self.broker, 'start_heartbeat_monitor'):
            self.broker.set_connection_callbacks(
                on_lost=lambda: self.risk_manager.circuit_breaker.on_connection_lost() if self.risk_manager else None,
                on_restored=lambda: self.risk_manager.circuit_breaker.on_connection_restored() if self.risk_manager else None,
            )
            self.broker.start_heartbeat_monitor(tick_timeout_sec=30)

        self._running = True
        self.state = EngineState.RUNNING

        self._engine_thread = threading.Thread(target=self._engine_loop, daemon=True)
        self._engine_thread.start()

        if self.data_collector:
            self.data_collector.start()

        if self.performance:
            self.performance.on_signal_scan({
                "type": "engine_start",
                "message": f"Engine started | {self.trading_mode} | {','.join(self.instruments)} | balance {self.position_manager.balance:,.0f}",
                "data": {"mode": self.trading_mode, "instruments": self.instruments}
            })

        logger.info(f"[Engine] started ({','.join(self.instruments)})")
        self._broadcast("engine_state", {"state": "running"})

    def stop(self):
        """停止交易引擎"""
        logger.info("[Engine] stopping...")

        # 平倉所有商品
        for inst in self.instruments:
            pos = self.position_manager.positions.get(inst)
            if pos and not pos.is_flat:
                self._force_close(inst, "engine stop - force close")

        if self.performance and self.position_manager:
            try:
                self.performance.on_session_end(self.position_manager.balance)
            except Exception as e:
                logger.warning(f"[Performance] session end error: {e}")

        self._running = False
        self.state = EngineState.STOPPED

        if self.data_collector:
            self.data_collector.stop()
        if self.broker:
            self.broker.disconnect()

        logger.info("[Engine] stopped")
        self._broadcast("engine_state", {"state": "stopped"})

    def pause(self):
        self.state = EngineState.PAUSED
        logger.info("[Engine] paused")
        self._broadcast("engine_state", {"state": "paused"})

    def resume(self):
        if self.state == EngineState.PAUSED:
            self.state = EngineState.RUNNING
            logger.info("[Engine] resumed")
            self._broadcast("engine_state", {"state": "running"})

    def manual_open(self, instrument: str, side: str, quantity: int = 1,
                    stop_loss: float = 0, take_profit: float = 0):
        """手動建倉"""
        if not instrument:
            instrument = self.instruments[0]

        pipeline = self.pipelines.get(instrument)
        if not pipeline:
            logger.error(f"[ManualOpen] 找不到 pipeline: {instrument}")
            return {"error": f"找不到商品 {instrument}"}

        # 檢查是否已有持倉
        pos = self.position_manager.positions.get(instrument)
        if pos and not pos.is_flat:
            logger.warning(f"[ManualOpen] {instrument} 已有 {pos.side.value} 持倉")
            return {"error": f"{instrument} 已有持倉"}

        price = pipeline.aggregator.current_price
        if price <= 0:
            return {"error": "無法取得現價"}

        # 預設停損停利（用 ATR 計算）
        atr = pipeline.snapshot.atr if pipeline.snapshot else 0
        if atr <= 0:
            atr = price * 0.005  # fallback: 0.5%

        if side.upper() == "BUY":
            action = "BUY"
            pos_side = Side.LONG
            if stop_loss <= 0:
                stop_loss = round(price - atr * 2)
            if take_profit <= 0:
                take_profit = round(price + atr * 4)
        else:
            action = "SELL"
            pos_side = Side.SHORT
            if stop_loss <= 0:
                stop_loss = round(price + atr * 2)
            if take_profit <= 0:
                take_profit = round(price - atr * 4)

        logger.info(f"[ManualOpen] {action} {quantity} {instrument} @ ~{price} | SL={stop_loss} TP={take_profit}")

        # Paper 模式：不真的下單，用當前價格模擬成交
        if self.trading_mode == "paper":
            logger.info(f"[PAPER] [ManualOpen] {action} {quantity} {instrument} @ {price}")
            fill_price = price
        else:
            result = self.broker.place_order(
                action=action,
                quantity=quantity,
                price_type="MKT",
                instrument=instrument,
            )

            if not result.success:
                logger.error(f"[ManualOpen] 下單失敗: {result.message}")
                return {"error": result.message}

            fill_price = result.fill_price if result.fill_price > 0 else price

        self.position_manager.open_position(
            instrument=instrument,
            side=pos_side,
            price=fill_price,
            quantity=quantity,
            stop_loss=stop_loss,
            take_profit=take_profit,
        )

        self._broadcast("trade", {
            "time": datetime.now().isoformat(),
            "instrument": instrument,
            "action": action.lower(),
            "price": fill_price,
            "quantity": quantity,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "reason": "手動建倉",
            "signal_strength": 1.0,
        })

        return {
            "status": "ok",
            "action": action,
            "price": fill_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
        }

    def manual_close(self, instrument: str = ""):
        """手動平倉（指定商品或全部）"""
        if instrument:
            pos = self.position_manager.positions.get(instrument)
            if pos and not pos.is_flat:
                self._force_close(instrument, "手動平倉")
            else:
                logger.info(f"{instrument} 沒有持倉")
        else:
            for inst in self.instruments:
                pos = self.position_manager.positions.get(inst)
                if pos and not pos.is_flat:
                    self._force_close(inst, "手動平倉")

    def set_risk_profile(self, profile: str):
        self.risk_profile = profile
        if self.risk_manager:
            self.risk_manager.set_profile(profile)
        # 立即更新所有策略的 adaptive_params
        for code, pipeline in self.pipelines.items():
            if hasattr(pipeline.strategy, 'signal_generator'):
                sg = pipeline.strategy.signal_generator
                sg.risk_profile = profile
                # 重新 update 再疊加風險等級
                if pipeline.snapshot:
                    atr_ratio = getattr(pipeline.snapshot, 'atr_ratio', 1.0)
                    regime_cls = getattr(pipeline.strategy, 'regime_classifier', None)
                    from strategy.signals import MarketRegime
                    current_regime = regime_cls._current_regime if regime_cls else MarketRegime.RANGE_BOUND
                    sg.params.update(atr_ratio, current_regime)
                sg.params.apply_risk_profile(profile)
                logger.info(f"[RiskProfile] {code} → {profile}: SL={sg.params.stop_loss_multiplier:.1f}x, "
                           f"signal≥{sg.params.min_signal_strength:.2f}, trail={sg.params.trailing_trigger:.1f}x")

                # 立即重算現有持倉的 SL/TP
                pos = self.position_manager.positions.get(code)
                if pos and not pos.is_flat and pipeline.snapshot and pipeline.snapshot.atr > 0:
                    atr = pipeline.snapshot.atr
                    sl_mult = sg.params.stop_loss_multiplier
                    tp_mult = sl_mult * 2  # TP = SL 的 2 倍
                    if pos.side == Side.LONG:
                        pos.stop_loss = round(pos.entry_price - atr * sl_mult)
                        pos.take_profit = round(pos.entry_price + atr * tp_mult)
                    else:
                        pos.stop_loss = round(pos.entry_price + atr * sl_mult)
                        pos.take_profit = round(pos.entry_price - atr * tp_mult)
                    logger.info(f"[RiskProfile] {code} 持倉更新: SL={pos.stop_loss} TP={pos.take_profit} (ATR={atr:.1f}×{sl_mult:.1f})")

        self._broadcast("settings", {"risk_profile": profile})

    def set_ws_broadcast(self, callback: Callable):
        self._ws_broadcast = callback

    # ============================================================
    # 事件處理
    # ============================================================

    def _on_tick(self, tick: Tick):
        """Tick 回調（Broker 執行緒）— 硬停損有最高優先級"""
        # 檢查是否有持倉觸及硬停損 — 這些 tick 優先處理
        pos = self.position_manager.positions.get(tick.instrument)
        is_urgent = False
        if pos and not pos.is_flat and pos.stop_loss > 0:
            if (pos.side == Side.LONG and tick.price <= pos.stop_loss) or \
               (pos.side == Side.SHORT and tick.price >= pos.stop_loss):
                is_urgent = True

        try:
            if is_urgent:
                # 緊急 tick 放到隊列前面（用 deque 不行，改用 priority 機制）
                # 清空隊列中較舊的同商品 tick，優先處理停損
                self._event_queue.put(("tick", tick), timeout=0.1)
            else:
                self._event_queue.put_nowait(("tick", tick))
        except (queue.Full, Exception):
            # Queue 滿時，如果是緊急 tick，強制塞入（犧牲舊 tick）
            if is_urgent:
                try:
                    self._event_queue.get_nowait()  # 丟掉最舊的
                    self._event_queue.put_nowait(("tick", tick))
                except Exception:
                    pass

    def _on_kbar_complete(self, instrument: str, kbar: KBar):
        """K 棒完成回調"""
        try:
            self._event_queue.put_nowait(("kbar", (instrument, kbar)))
        except queue.Full:
            logger.warning(f"[Queue] K 棒丟棄！{instrument} @ {kbar.datetime} — 事件佇列已滿（{self._event_queue.qsize()}）")

    def _on_kbar_5m_complete(self, instrument: str, kbar: KBar):
        """5 分 K 完成 — 更新 MTF 指標"""
        pipeline = self.pipelines.get(instrument)
        if not pipeline:
            return
        df = pipeline.aggregator.get_bars_dataframe(5, count=200)
        if len(df) >= 5:
            pipeline.snapshot_5m = pipeline.indicator_engine_5m.update(df)

    def _on_kbar_15m_complete(self, instrument: str, kbar: KBar):
        """15 分 K 完成 — 更新 MTF 指標"""
        pipeline = self.pipelines.get(instrument)
        if not pipeline:
            return
        df = pipeline.aggregator.get_bars_dataframe(15, count=200)
        if len(df) >= 5:
            pipeline.snapshot_15m = pipeline.indicator_engine_15m.update(df)

    def _engine_loop(self):
        """引擎主迴圈"""
        logger.info("[Engine] event loop started")

        while self._running:
            try:
                event_type, data = self._event_queue.get(timeout=1.0)

                if event_type == "tick":
                    self._process_tick(data)
                elif event_type == "kbar":
                    instrument, kbar = data
                    self._process_kbar(instrument, kbar)

            except queue.Empty:
                self._heartbeat_count += 1
                if self._heartbeat_count % 60 == 0:
                    self._heartbeat()
            except Exception as e:
                logger.error(f"引擎迴圈錯誤: {e}")

        logger.info("[Engine] event loop ended")

    def _process_tick(self, tick: Tick):
        """處理 Tick — 路由到對應的商品管線"""
        self._tick_count += 1

        instrument = tick.instrument
        pipeline = self.pipelines.get(instrument)
        if not pipeline:
            return

        # 更新聚合器
        pipeline.aggregator.on_tick(tick)

        # 更新持倉追蹤
        self.position_manager.update_price(instrument, tick.price)

        # 更新 MockBroker 損益
        if isinstance(self.broker, MockBroker):
            prices = {inst: p.aggregator.current_price for inst, p in self.pipelines.items()}
            total_pnl = self.position_manager.get_total_unrealized_pnl(prices)
            self.broker.update_pnl(total_pnl)

        # 盤中停損停利（每個 Tick 都檢查）
        pos = self.position_manager.positions.get(instrument)
        if self.state == EngineState.RUNNING and pos and not pos.is_flat:
            # === 快速硬停損（不經策略，直接檢查價格 vs stop_loss）===
            hard_stop_hit = False
            if pos.stop_loss > 0:
                if pos.side == Side.LONG and tick.price <= pos.stop_loss:
                    hard_stop_hit = True
                elif pos.side == Side.SHORT and tick.price >= pos.stop_loss:
                    hard_stop_hit = True

            if hard_stop_hit:
                from strategy.base import Signal, SignalDirection
                hard_signal = Signal(
                    direction=SignalDirection.CLOSE, strength=1.0,
                    stop_loss=0, take_profit=0,
                    reason=f"硬停損 @ {tick.price:.0f}（停損價 {pos.stop_loss:.0f}）",
                    source="hard_stop",
                )
                self._execute_exit(instrument, hard_signal, tick.price)
            else:
                # 完整策略出場檢查
                pipeline.snapshot.price = tick.price
                pipeline.snapshot.timestamp = tick.datetime
                exit_signal = pipeline.strategy.check_exit(pos, pipeline.snapshot)
                if exit_signal:
                    self._execute_exit(instrument, exit_signal, tick.price)

        # 廣播 Tick
        self._broadcast("tick", {
            "instrument": instrument,
            "price": tick.price,
            "volume": tick.volume,
            "time": tick.datetime.isoformat(),
            "bid": tick.bid_price,
            "ask": tick.ask_price,
        })

        # 定期廣播完整狀態
        if self._tick_count % 10 == 0:
            self._broadcast("state", self.get_state())

    def _process_kbar(self, instrument: str, kbar: KBar):
        """處理完成的 K 棒"""
        pipeline = self.pipelines.get(instrument)
        if not pipeline:
            return

        # 更新指標
        df = pipeline.aggregator.get_bars_dataframe(self.timeframe, count=200)
        if len(df) < 5:
            return

        pipeline.snapshot = pipeline.indicator_engine.update(df)

        # 更新向後相容
        if instrument == self.instruments[0]:
            self.snapshot = pipeline.snapshot

        # 更新持倉 K 棒計數
        self.position_manager.increment_bars(instrument)

        # 廣播 K 棒
        self._broadcast("kbar", {
            "instrument": instrument,
            "time": kbar.datetime.isoformat(),
            "open": kbar.open,
            "high": kbar.high,
            "low": kbar.low,
            "close": kbar.close,
            "volume": kbar.volume,
            "interval": kbar.interval,
            "ema20": round(pipeline.snapshot.ema20, 1),
            "ema60": round(pipeline.snapshot.ema60, 1),
            "ema200": round(pipeline.snapshot.ema200, 1) if hasattr(pipeline.snapshot, 'ema200') and pipeline.snapshot.ema200 else None,
        })

        # 廣播指標
        regime_info = {}
        if hasattr(pipeline.strategy, 'regime_classifier'):
            regime_info = pipeline.strategy.regime_classifier.get_regime_info()
        self._broadcast("signal", {
            "instrument": instrument,
            "regime": regime_info,
            "signal_strength": pipeline.strategy._last_signal_strength if hasattr(pipeline.strategy, '_last_signal_strength') else 0,
            "adx": round(pipeline.snapshot.adx, 1),
            "rsi": round(pipeline.snapshot.rsi, 1),
            "atr": round(pipeline.snapshot.atr, 1),
            "ema20": round(pipeline.snapshot.ema20, 1),
            "ema60": round(pipeline.snapshot.ema60, 1),
        })

        if self.state != EngineState.RUNNING:
            return

        # 收盤前強制平倉（SessionManager 版：支援日盤 + 夜盤 + 不同商品）
        if not hasattr(self, '_sessions'):
            self._sessions = {}
        if instrument not in self._sessions:
            self._sessions[instrument] = SessionManager(instrument)
        session = self._sessions[instrument]
        phase = session.get_phase()
        minutes_left = session.minutes_to_close()

        if phase == SessionPhase.CLOSING or (phase == SessionPhase.LAST_30 and minutes_left <= 5):
            pos = self.position_manager.positions.get(instrument)
            if pos and not pos.is_flat:
                self._force_close(instrument, f"session close ({minutes_left}m left)")
            if minutes_left <= 2:
                if not getattr(self, '_session_ended_today', '') == datetime.now().strftime("%Y-%m-%d"):
                    if self.performance and self.position_manager:
                        try:
                            self.performance.on_session_end(self.position_manager.balance)
                            self._session_ended_today = datetime.now().strftime("%Y-%m-%d")
                        except Exception:
                            pass
            return

        # ---- 策略決策 ----
        pos = self.position_manager.positions.get(instrument)

        # 先檢查出場
        if pos and not pos.is_flat:
            exit_signal = pipeline.strategy.check_exit(pos, pipeline.snapshot)
            if exit_signal:
                self._execute_exit(instrument, exit_signal, pipeline.snapshot.price)
                return
            else:
                unrealized = pos.unrealized_pnl(pipeline.snapshot.price)
                if self.performance:
                    self.performance.on_signal_scan({
                        "type": "holding",
                        "message": f"[{instrument}] 持倉中 {pos.side.value} @ {pos.entry_price:.1f} | 未實現 {unrealized:+.0f} | K{pos.bars_since_entry}",
                        "data": {"instrument": instrument, "unrealized": unrealized, "bars": pos.bars_since_entry}
                    })

        # 再檢查進場
        if pos and pos.is_flat:
            entry_signal = pipeline.strategy.on_kbar(
                kbar, pipeline.snapshot,
                snapshot_5m=pipeline.snapshot_5m,
                snapshot_15m=pipeline.snapshot_15m,
            )
            if entry_signal:
                self._execute_entry(instrument, entry_signal)
            else:
                signal_str = pipeline.strategy._last_signal_strength if hasattr(pipeline.strategy, '_last_signal_strength') else 0
                if self.performance:
                    self.performance.on_signal_scan({
                        "type": "scan_no_signal",
                        "message": f"[{instrument}] K棒掃描 | 訊號 {signal_str:.2f} | 價格 {pipeline.snapshot.price:.1f}",
                        "data": {"instrument": instrument, "price": pipeline.snapshot.price}
                    })

    def _execute_entry(self, instrument: str, signal: Signal):
        """執行進場（指定商品）"""
        # 交易時段檢查（防止非交易時段下單）
        if not hasattr(self, '_sessions'):
            self._sessions = {}
        if instrument not in self._sessions:
            self._sessions[instrument] = SessionManager(instrument)
        phase = self._sessions[instrument].get_phase()
        if phase in (SessionPhase.CLOSED, SessionPhase.CLOSING):
            return

        # 下單失敗冷卻中 → 跳過（所有模式適用）
        if self._is_order_cooled_down(instrument):
            return

        hold_until = self._market_data_gap_until.get(instrument)
        if hold_until:
            if datetime.now() < hold_until:
                logger.info(f"[{instrument}] 資料斷層觀察中，跳過進場至 {hold_until.isoformat()}")
                return
            self._market_data_gap_until.pop(instrument, None)

        pipeline = self.pipelines[instrument]
        price = pipeline.snapshot.price

        # Paper 模式：用真實行情，不下單，但要追蹤持倉（才能模擬損益和出場）
        if self.trading_mode == "paper":
            # Paper 也要受熔斷保護，否則模擬結果不可靠
            if self.risk_manager and not self.risk_manager.circuit_breaker.can_trade:
                logger.info(f"[PAPER] [{instrument}] 熔斷中，跳過進場")
                return

            action = "BUY" if signal.is_buy else "SELL"
            side = Side.LONG if signal.is_buy else Side.SHORT
            logger.info(f"[PAPER] [{instrument}] {action} | strength {signal.strength:.2f} | {signal.reason}")

            # 關鍵：Paper 模式也要開倉追蹤，否則永遠不會觸發出場信號
            self.position_manager.open_position(
                instrument=instrument,
                side=side,
                price=price,
                quantity=1,
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                take_profit_levels=signal.take_profit_levels,
            )

            signal_data = {
                "time": datetime.now().isoformat(),
                "instrument": instrument,
                "action": action.lower(),
                "price": price,
                "quantity": 1,
                "reason": signal.reason,
                "signal_strength": round(signal.strength, 2),
                "stop_loss": signal.stop_loss,
                "take_profit": signal.take_profit,
            }
            self._broadcast("trade", {**signal_data, "reason": f"[PAPER] {signal.reason}"})
            if self.performance:
                self.performance.on_paper_signal(signal_data)
            return

        # 風控評估 — Shioaji 期貨帳戶無法 API 查餘額，用 PositionManager 餘額
        account = self.broker.get_account_info()
        if account.balance <= 0 and account.equity <= 0:
            pm_balance = self.position_manager.balance
            account.balance = pm_balance
            account.equity = pm_balance + self.position_manager.get_total_unrealized_pnl({})
        decision = self.risk_manager.evaluate(
            signal, self.position_manager, account, pipeline.snapshot, instrument=instrument
        )

        if not decision.approved:
            logger.info(f"[Risk] [{instrument}] rejected: {decision.rejection_reason}")
            return

        # 下單
        action = "BUY" if signal.is_buy else "SELL"
        log_order(action, price, decision.quantity, f"MKT {instrument}")

        result = self.broker.place_order(
            action=action,
            quantity=decision.quantity,
            price_type="MKT",
            instrument=instrument,
        )

        if not result.success:
            logger.error(f"[Order] [{instrument}] failed: {result.message}")
            self._set_order_cooldown(instrument)
            return

        fill_price = result.fill_price if result.fill_price > 0 else price
        side = Side.LONG if signal.is_buy else Side.SHORT

        try:
            self.position_manager.open_position(
                instrument=instrument,
                side=side,
                price=fill_price,
                quantity=decision.quantity,
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                take_profit_levels=signal.take_profit_levels,
            )
        except Exception as e:
            # 嚴重：券商已成交但 PositionManager 記錄失敗 → 立即反向平倉
            logger.critical(f"[GHOST] [{instrument}] 開倉記錄失敗: {e} — 嘗試反向平倉")
            try:
                reverse = "SELL" if action == "BUY" else "BUY"
                self.broker.place_order(action=reverse, quantity=decision.quantity, price_type="MKT", instrument=instrument)
            except Exception as e2:
                logger.critical(f"[GHOST] [{instrument}] 反向平倉也失敗: {e2} — 請手動處理！")
            return

        log_fill(action, fill_price, decision.quantity)

        self._broadcast("trade", {
            "time": datetime.now().isoformat(),
            "instrument": instrument,
            "action": action.lower(),
            "price": fill_price,
            "quantity": decision.quantity,
            "stop_loss": signal.stop_loss,
            "take_profit": signal.take_profit,
            "reason": signal.reason,
            "signal_strength": round(signal.strength, 2),
        })

    def _is_order_cooled_down(self, instrument: str) -> bool:
        """檢查該商品是否在下單失敗冷卻中"""
        cooldown_until = self._order_fail_cooldown.get(instrument)
        if cooldown_until and datetime.now() < cooldown_until:
            return True
        return False

    def _set_order_cooldown(self, instrument: str, seconds: int = None):
        """設定下單失敗冷卻"""
        cd = seconds or self._ENTRY_FAIL_COOLDOWN_SEC
        self._order_fail_cooldown[instrument] = datetime.now() + timedelta(seconds=cd)
        logger.warning(f"[Order] [{instrument}] 下單失敗，冷卻 {cd} 秒")

    def _execute_exit(self, instrument: str, signal: Signal, price: float):
        """執行出場（指定商品，線程安全）"""
        # 防止雙重平倉（manual_close vs 自動出場競爭）
        with self._exit_lock:
            if instrument in self._exiting:
                return
            self._exiting.add(instrument)

        try:
            self._execute_exit_inner(instrument, signal, price)
        finally:
            with self._exit_lock:
                self._exiting.discard(instrument)

    def _execute_exit_inner(self, instrument: str, signal: Signal, price: float):
        """出場內部邏輯"""
        pos = self.position_manager.positions.get(instrument)
        if not pos or pos.is_flat:
            return

        # 下單失敗冷卻中 → 跳過（所有模式都適用，防止無限重試轟炸）
        # 但硬停損和盤別收盤不受冷卻限制 — 這些是保命的
        is_hard_stop = getattr(signal, 'source', '') == 'hard_stop'
        is_session_close = '收盤' in getattr(signal, 'reason', '')
        if self._is_order_cooled_down(instrument):
            if not is_hard_stop and not is_session_close:
                return
            logger.warning(f"[Order] [{instrument}] 冷卻中但硬停損/收盤強制執行")

        # Paper 模式：不真的下單，但要關閉持倉（否則會無限重複觸發平倉信號）
        if self.trading_mode == "paper":
            action = "SELL" if pos.side == Side.LONG else "BUY"
            pnl = round(pos.unrealized_pnl(price), 0)
            logger.info(f"[PAPER] [{instrument}] CLOSE {action} | PnL={pnl} | {signal.reason}")

            # 關鍵：Paper 模式也要關閉持倉，否則下次掃描又會觸發
            trade = self.position_manager.close_position(instrument, price, f"[PAPER] {signal.reason}")

            self._broadcast("trade", {
                "time": datetime.now().isoformat(),
                "instrument": instrument,
                "action": "close",
                "price": price,
                "pnl": trade.net_pnl if trade else pnl,
                "reason": f"[PAPER] {signal.reason}",
                "side": pos.side.value,
            })

            if trade and self.risk_manager:
                self.risk_manager.on_trade_closed(trade.net_pnl)

            if self.performance:
                if trade:
                    self.performance.on_trade_closed(trade.to_perf_dict())
                else:
                    self.performance.on_paper_signal({
                        "time": datetime.now().isoformat(),
                        "instrument": instrument,
                        "action": "close",
                        "price": price,
                        "reason": signal.reason,
                    })
            return

        action = "SELL" if pos.side == Side.LONG else "BUY"
        log_order(action, price, pos.quantity, f"MKT 平倉 {instrument}")

        result = self.broker.place_order(
            action=action,
            quantity=pos.quantity,
            price_type="MKT",
            instrument=instrument,
        )

        if not result.success:
            # 出場連續失敗計數
            fail_count = self._exit_fail_count.get(instrument, 0) + 1
            self._exit_fail_count[instrument] = fail_count
            logger.error(f"[Order] [{instrument}] close failed ({fail_count}/{self._EXIT_MAX_RETRIES}): {result.message}")

            if fail_count >= self._EXIT_MAX_RETRIES:
                # 超過最大重試次數 → 強制重置持倉（券商端可能已經平了）
                logger.critical(
                    f"[EMERGENCY] [{instrument}] 出場連續失敗 {fail_count} 次！"
                    f"強制重置持倉狀態 — 請手動確認券商帳戶"
                )
                from core.position import Position
                self.position_manager.positions[instrument] = Position()
                if instrument == self.position_manager.instruments[0]:
                    self.position_manager.position = self.position_manager.positions[instrument]
                self._exit_fail_count[instrument] = 0
                # 長冷卻 5 分鐘
                self._set_order_cooldown(instrument, 300)
            else:
                self._set_order_cooldown(instrument, self._EXIT_FAIL_COOLDOWN_SEC)
            return

        # 成交成功 → 重置失敗計數
        self._exit_fail_count[instrument] = 0

        fill_price = result.fill_price if result.fill_price > 0 else price
        try:
            trade = self.position_manager.close_position(instrument, fill_price, signal.reason)
        except Exception as e:
            # 券商已平倉但記錄失敗 — 強制重置持倉狀態防止重複平倉
            logger.critical(f"[GHOST] [{instrument}] 平倉記錄失敗: {e} — 強制重置持倉")
            from core.position import Position
            self.position_manager.positions[instrument] = Position()
            return

        if trade:
            log_pnl(trade.net_pnl, f"[{instrument}] {signal.reason}")

            if isinstance(self.broker, MockBroker):
                self.broker.update_balance(trade.pnl)
                prices = {inst: p.aggregator.current_price for inst, p in self.pipelines.items()}
                total_pnl = self.position_manager.get_total_unrealized_pnl(prices)
                self.broker.update_pnl(total_pnl)

            self.risk_manager.on_trade_closed(trade.net_pnl)

            if self.performance:
                self.performance.on_trade_closed(trade.to_perf_dict())

            self._broadcast("trade", {
                "time": datetime.now().isoformat(),
                "instrument": instrument,
                "action": "close",
                "price": fill_price,
                "pnl": round(trade.net_pnl, 0),
                "pnl_points": round(trade.pnl_points, 1),
                "reason": signal.reason,
                "side": trade.side,
            })

    def _force_close(self, instrument: str, reason: str):
        """強制平倉指定商品"""
        pipeline = self.pipelines.get(instrument)
        price = pipeline.snapshot.price if pipeline else 0
        signal = Signal(
            direction=SignalDirection.CLOSE,
            strength=1.0,
            stop_loss=0,
            take_profit=0,
            reason=reason,
            source="Engine",
        )
        self._execute_exit(instrument, signal, price)

    def _heartbeat(self):
        for inst in self.instruments:
            pos = self.position_manager.positions.get(inst)
            pipeline = self.pipelines.get(inst)
            price = pipeline.aggregator.current_price if pipeline else 0
            if pos and not pos.is_flat:
                config = get_spec(inst)
                pnl = pos.unrealized_pnl(price, config.point_value)
                logger.debug(f"[Heartbeat] {inst}: {price:.1f} | {pos.side.value} @ {pos.entry_price:.1f} | pnl: {pnl:+.0f}")
            else:
                logger.debug(f"[Heartbeat] {inst}: {price:.1f} | flat")

        # 每 1 分鐘做一次持倉核對（heartbeat 每 60 次空轉 = 60 秒）
        if self._heartbeat_count % 60 == 0 and self.trading_mode == "live":
            self._reconcile_positions()

        # 價格異常偵測
        self._check_price_anomaly()

    def _reconcile_positions(self):
        """定期比對引擎持倉和券商真實持倉"""
        if not hasattr(self.broker, 'get_real_positions'):
            return
        try:
            real_positions = self.broker.get_real_positions()
            for inst in self.instruments:
                engine_pos = self.position_manager.positions.get(inst)
                spec = get_spec(inst)
                contract_code = spec.code

                # 找到券商的真實持倉
                real_qty = 0
                real_side = None
                for rp in real_positions:
                    if rp['code'].startswith(contract_code) and rp['quantity'] > 0:
                        real_qty = rp['quantity']
                        real_side = "long" if "Buy" in rp['direction'] else "short"

                engine_qty = engine_pos.quantity if engine_pos and not engine_pos.is_flat else 0
                engine_side = engine_pos.side.value if engine_pos and not engine_pos.is_flat else "flat"

                if engine_qty != real_qty or (real_qty > 0 and engine_side != real_side):
                    logger.error(
                        f"[RECONCILE] {inst} 持倉不一致！"
                        f" 引擎={engine_side}×{engine_qty}"
                        f" 券商={real_side}×{real_qty}"
                        f" — 請手動檢查！"
                    )
        except Exception as e:
            logger.warning(f"[RECONCILE] 持倉核對失敗: {e}")

    def _check_price_anomaly(self):
        """價格異常偵測 — 超過 5x ATR 自動暫停交易。

        If the previous baseline is stale, treat the first new quote as a data
        gap reset instead of a bad tick. This preserves the emergency guard for
        true short-window jumps while preventing long tick gaps from halting the
        engine incorrectly.
        """
        for inst in self.instruments:
            pipeline = self.pipelines.get(inst)
            if not pipeline or not pipeline.snapshot or pipeline.snapshot.atr <= 0:
                continue
            price = pipeline.aggregator.current_price
            atr = pipeline.snapshot.atr
            now = datetime.now()
            if not hasattr(pipeline, '_last_heartbeat_price'):
                pipeline._last_heartbeat_price = price
                pipeline._last_heartbeat_at = now
                self._last_price_anomaly[inst] = {
                    "status": "baseline",
                    "severity": "normal",
                    "triggered": False,
                    "checked_at": now.isoformat(),
                    "price": round(price, 1),
                    "baseline": round(price, 1),
                    "baseline_at": now.isoformat(),
                    "elapsed_seconds": 0,
                    "hold_until": None,
                    "deviation": 0,
                    "atr": round(atr, 1),
                    "threshold": round(atr * 5, 1),
                }
                continue

            baseline_at = getattr(pipeline, '_last_heartbeat_at', None)
            elapsed = (now - baseline_at).total_seconds() if isinstance(baseline_at, datetime) else 0
            deviation = abs(price - pipeline._last_heartbeat_price)
            if elapsed > self._PRICE_BASELINE_MAX_AGE_SEC:
                previous_price = pipeline._last_heartbeat_price
                hold_until = now + timedelta(seconds=self._DATA_GAP_HOLD_SEC)
                self._market_data_gap_until[inst] = hold_until
                logger.warning(
                    f"[ANOMALY] {inst} baseline stale: elapsed={elapsed:.0f}s, "
                    f"reset baseline {previous_price:.1f} → {price:.1f}; "
                    f"hold new entries until {hold_until.isoformat()}"
                )
                self._last_price_anomaly[inst] = {
                    "status": "data_gap_reset",
                    "severity": "warning",
                    "triggered": False,
                    "checked_at": now.isoformat(),
                    "price": round(price, 1),
                    "baseline": round(previous_price, 1),
                    "new_baseline": round(price, 1),
                    "baseline_at": baseline_at.isoformat() if isinstance(baseline_at, datetime) else None,
                    "elapsed_seconds": round(elapsed, 0),
                    "hold_until": hold_until.isoformat(),
                    "deviation": round(deviation, 1),
                    "atr": round(atr, 1),
                    "threshold": round(atr * 5, 1),
                }
                pipeline._last_heartbeat_price = price
                pipeline._last_heartbeat_at = now
                continue

            if deviation > atr * 5:
                # 嚴重異常：自動觸發熔斷保護帳戶
                reason = f"價格異常: {inst} 偏離 {deviation:.0f} 點"
                logger.error(
                    f"[ANOMALY] {inst} 價格劇烈異常: {pipeline._last_heartbeat_price:.1f} → {price:.1f}"
                    f"（偏離 {deviation:.1f} > 5×ATR {atr * 5:.1f}）— 自動暫停交易！"
                )
                if self.risk_manager:
                    self.risk_manager.circuit_breaker.on_price_anomaly(reason)
                self._last_price_anomaly[inst] = {
                    "status": "triggered",
                    "severity": "danger",
                    "triggered": True,
                    "checked_at": now.isoformat(),
                    "price": round(price, 1),
                    "baseline": round(pipeline._last_heartbeat_price, 1),
                    "baseline_at": baseline_at.isoformat() if isinstance(baseline_at, datetime) else None,
                    "elapsed_seconds": round(elapsed, 0),
                    "hold_until": None,
                    "deviation": round(deviation, 1),
                    "atr": round(atr, 1),
                    "threshold": round(atr * 5, 1),
                }
            elif deviation > atr * 3:
                logger.warning(
                    f"[ANOMALY] {inst} 價格異常波動: {pipeline._last_heartbeat_price:.1f} → {price:.1f}"
                    f"（偏離 {deviation:.1f} > 3×ATR {atr * 3:.1f}）"
                )
                self._last_price_anomaly[inst] = {
                    "status": "warning",
                    "severity": "warning",
                    "triggered": False,
                    "checked_at": now.isoformat(),
                    "price": round(price, 1),
                    "baseline": round(pipeline._last_heartbeat_price, 1),
                    "baseline_at": baseline_at.isoformat() if isinstance(baseline_at, datetime) else None,
                    "elapsed_seconds": round(elapsed, 0),
                    "hold_until": None,
                    "deviation": round(deviation, 1),
                    "atr": round(atr, 1),
                    "threshold": round(atr * 3, 1),
                }
            else:
                self._last_price_anomaly[inst] = {
                    "status": "normal",
                    "severity": "normal",
                    "triggered": False,
                    "checked_at": now.isoformat(),
                    "price": round(price, 1),
                    "baseline": round(pipeline._last_heartbeat_price, 1),
                    "baseline_at": baseline_at.isoformat() if isinstance(baseline_at, datetime) else None,
                    "elapsed_seconds": round(elapsed, 0),
                    "hold_until": None,
                    "deviation": round(deviation, 1),
                    "atr": round(atr, 1),
                    "threshold": round(atr * 5, 1),
                }
            pipeline._last_heartbeat_price = price
            pipeline._last_heartbeat_at = now

    def _broadcast(self, msg_type: str, data: dict):
        if self._ws_broadcast:
            try:
                self._ws_broadcast({"type": msg_type, "data": data})
            except Exception:
                pass

    # ============================================================
    # Intelligence 回調
    # ============================================================

    def _on_intelligence_update(self, snapshot):
        try:
            if self.left_side_engine:
                self.left_side_engine.calculate(snapshot)

            # 注入到所有有 regime_classifier 的策略
            for pipeline in self.pipelines.values():
                if hasattr(pipeline.strategy, 'regime_classifier'):
                    pipeline.strategy.regime_classifier.update_intelligence(
                        vix=snapshot.international.vix,
                        pc_ratio=snapshot.options.pc_ratio_oi,
                        left_side_score=snapshot.left_side_score,
                        left_side_signal=snapshot.left_side_signal,
                        foreign_spot=snapshot.institutional_spot.foreign_buy_sell,
                    )

            self._broadcast("intelligence", snapshot.to_dict())
        except Exception as e:
            logger.error(f"Intelligence 回調錯誤: {e}")

    # ============================================================
    # 狀態查詢
    # ============================================================

    def get_state(self) -> dict:
        """取得完整引擎狀態（多商品版）"""
        prices = {}
        instruments_data = {}

        for inst in self.instruments:
            pipeline = self.pipelines.get(inst)
            price = pipeline.aggregator.current_price if pipeline else 0
            prices[inst] = price

            pos = self.position_manager.positions.get(inst)
            config = get_spec(inst)
            unrealized = pos.unrealized_pnl(price, config.point_value) if pos and not pos.is_flat else 0

            instruments_data[inst] = {
                "name": config.name,
                "price": price,
                "point_value": config.point_value,
                "position": {
                    "side": pos.side.value if pos else "flat",
                    "entry_price": pos.entry_price if pos else 0,
                    "quantity": pos.quantity if pos else 0,
                    "entry_time": pos.entry_time.isoformat() if pos and pos.entry_time else None,
                    "stop_loss": pos.stop_loss if pos else 0,
                    "take_profit": pos.take_profit if pos else 0,
                    "trailing_stop": pos.trailing_stop if pos else 0,
                    "trailing_activated": pos.trailing_activated if pos else False,
                    "breakeven_activated": pos.breakeven_activated if pos else False,
                    "max_unrealized_profit": pos.max_unrealized_profit if pos else 0,
                    "bars_since_entry": pos.bars_since_entry if pos else 0,
                    "unrealized_pnl": round(unrealized, 0),
                },
                "snapshot": {
                    "adx": _safe_round(pipeline.snapshot.adx) if pipeline else 0,
                    "rsi": _safe_round(pipeline.snapshot.rsi, default=50) if pipeline else 50,
                    "atr": _safe_round(pipeline.snapshot.atr) if pipeline else 0,
                    "ema20": _safe_round(pipeline.snapshot.ema20) if pipeline else 0,
                    "ema60": _safe_round(pipeline.snapshot.ema60) if pipeline else 0,
                    "ema200": _safe_round(pipeline.snapshot.ema200) if pipeline and hasattr(pipeline.snapshot, 'ema200') and pipeline.snapshot.ema200 else 0,
                },
                "strategy": pipeline.strategy.get_parameters() if pipeline else {},
            }

        pm = self.position_manager
        total_unrealized = pm.get_total_unrealized_pnl(prices)
        balance = pm.balance
        equity = balance + total_unrealized
        margin_used = pm.get_total_margin_used()

        account_info = {
            "balance": round(balance, 0),
            "equity": round(equity, 0),
            "margin_used": round(margin_used, 0),
            "margin_available": round(equity - margin_used, 0),
            "unrealized_pnl": round(total_unrealized, 0),
        }

        intel_data = {}
        if self.data_collector:
            try:
                intel_snapshot = self.data_collector.snapshot
                if self.left_side_engine:
                    self.left_side_engine.calculate(intel_snapshot)
                intel_data = intel_snapshot.to_dict()
            except Exception:
                pass

        activity = self.performance.get_activity_log(20) if self.performance else []
        paper_signals_count = len(self.performance.paper_signals) if self.performance else 0

        # 向後相容：保留 price / position / snapshot 欄位（指向第一個商品）
        first_inst = self.instruments[0]
        first_data = instruments_data[first_inst]

        return {
            "engine_state": self.state.value,
            "trading_mode": self.trading_mode,
            "risk_profile": self.risk_profile,
            "contract": self.broker.get_contract_name() if self.broker else "",
            "instruments": self.instruments,
            "instruments_data": instruments_data,
            "price": first_data["price"],
            "account": account_info,
            "position": first_data["position"],
            "daily_pnl": pm.get_daily_pnl(),
            "daily_trades": pm.get_daily_trade_count(),
            "paper_signals": paper_signals_count,
            "strategy": first_data["strategy"],
            "risk": self.risk_manager.to_dict() if self.risk_manager else {},
            "snapshot": first_data["snapshot"],
            "intelligence": intel_data,
            "activity": activity,
        }

    def get_trade_history(self) -> list[dict]:
        if not self.position_manager:
            return []
        return [
            {
                "instrument": t.instrument,
                "entry_time": t.entry_time.isoformat(),
                "exit_time": t.exit_time.isoformat(),
                "side": t.side,
                "entry_price": t.entry_price,
                "exit_price": t.exit_price,
                "quantity": t.quantity,
                "pnl": round(t.net_pnl, 0),
                "pnl_points": round(t.pnl_points, 1),
                "reason": t.reason,
                "bars_held": t.bars_held,
            }
            for t in self.position_manager.trades
        ]

    def get_kbars(self, timeframe: int = 1, count: int = 200, instrument: str = "") -> list[dict]:
        """取得 K 棒資料（指定商品）"""
        inst = instrument or self.instruments[0]
        pipeline = self.pipelines.get(inst)
        if not pipeline:
            return []

        bars = list(pipeline.aggregator.get_bars(timeframe, count))
        # 加入當前未完成的 K 棒（即時更新用）
        current = pipeline.aggregator.get_current_bar(timeframe)
        if current and current.close > 0:
            bars.append(current)
        if not bars:
            return []

        closes = [b.close for b in bars]
        ema20_vals = self._calc_ema(closes, 20)
        ema60_vals = self._calc_ema(closes, 60)
        ema200_vals = self._calc_ema(closes, 200)

        def _to_iso(dt):
            """安全轉換 datetime — 處理 pandas Timestamp / int / datetime"""
            if hasattr(dt, 'isoformat'):
                return dt.isoformat()
            if isinstance(dt, (int, float)):
                from datetime import datetime as _dt
                # 可能是秒、毫秒或奈秒
                if dt > 1e18:       # nanoseconds
                    return _dt.fromtimestamp(dt / 1e9).isoformat()
                elif dt > 1e15:     # microseconds
                    return _dt.fromtimestamp(dt / 1e6).isoformat()
                elif dt > 1e12:     # milliseconds
                    return _dt.fromtimestamp(dt / 1e3).isoformat()
                else:               # seconds
                    return _dt.fromtimestamp(dt).isoformat()
            return str(dt)

        # 去重 + 按時間排序 + 移除大 gap 前的舊 session 資料
        seen = {}
        for i, b in enumerate(bars):
            t = _to_iso(b.datetime)
            seen[t] = i
        sorted_times = sorted(seen.keys())
        bars = [bars[seen[t]] for t in sorted_times]
        # 找最後一個大 gap（> timeframe 的 10 倍），只保留 gap 之後的資料
        if len(bars) > 1:
            gap_threshold = max(timeframe * 60 * 10, 3600)  # 至少 1 小時，或 10 根 bar 的時間
            cut = 0
            for i in range(len(bars) - 1, 0, -1):
                dt_curr = bars[i].datetime
                dt_prev = bars[i-1].datetime
                if hasattr(dt_curr, 'to_pydatetime'):
                    dt_curr = dt_curr.to_pydatetime()
                if hasattr(dt_prev, 'to_pydatetime'):
                    dt_prev = dt_prev.to_pydatetime()
                try:
                    gap = (dt_curr - dt_prev).total_seconds()
                except:
                    continue
                if gap > gap_threshold:
                    cut = i
                    break
            if cut > 0:
                bars = bars[cut:]
        closes = [b.close for b in bars]
        ema20_vals = self._calc_ema(closes, 20)
        ema60_vals = self._calc_ema(closes, 60)
        ema200_vals = self._calc_ema(closes, 200)

        return [
            {
                "time": _to_iso(b.datetime),
                "open": b.open,
                "high": b.high,
                "low": b.low,
                "close": b.close,
                "volume": b.volume,
                "ema20": round(ema20_vals[i], 1) if i < len(ema20_vals) and ema20_vals[i] is not None else None,
                "ema60": round(ema60_vals[i], 1) if i < len(ema60_vals) and ema60_vals[i] is not None else None,
                "ema200": round(ema200_vals[i], 1) if i < len(ema200_vals) and ema200_vals[i] is not None else None,
            }
            for i, b in enumerate(bars)
        ]

    @staticmethod
    def _calc_ema(data: list[float], period: int) -> list[float]:
        if len(data) < period:
            return [None] * len(data)
        result = [None] * (period - 1)
        sma = sum(data[:period]) / period
        result.append(sma)
        multiplier = 2.0 / (period + 1)
        for i in range(period, len(data)):
            sma = (data[i] - result[-1]) * multiplier + result[-1]
            result.append(sma)
        return result

    def get_stats(self) -> dict:
        if not self.position_manager:
            return {}
        return self.position_manager.get_stats()
