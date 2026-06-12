"""
UltraTrader 績效記錄器
結構化記錄每日/每週/每月績效數據（JSON）

掛在 TradingEngine 上，自動記錄：
  1. 每筆交易即時存檔（防止斷線丟失）
  2. 每日收盤生成 DailyPerformance
  3. 每週/每月自動彙總
  4. 累計績效持續更新
"""

import json
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

from loguru import logger


# ============================================================
# 績效數據結構
# ============================================================

@dataclass
class DailyPerformance:
    """每日績效摘要"""
    date: str = ""                      # "2026-03-05"
    trading_mode: str = "simulation"    # "simulation" / "paper" / "live"

    # 帳戶
    starting_balance: float = 0.0
    ending_balance: float = 0.0
    daily_pnl: float = 0.0             # 當日損益（扣除手續費+稅）
    daily_return_pct: float = 0.0       # 當日報酬率 %

    # 交易統計
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0               # 勝率 %
    avg_win: float = 0.0                # 平均獲利（元）
    avg_loss: float = 0.0               # 平均虧損（元）
    profit_factor: float = 0.0          # 盈虧比
    largest_win: float = 0.0
    largest_loss: float = 0.0

    # 風險
    max_drawdown: float = 0.0           # 當日最大回撤（元）
    max_drawdown_pct: float = 0.0       # 當日最大回撤 %

    # 策略
    primary_strategy: str = ""
    market_regime_summary: dict = field(default_factory=dict)

    # 交易明細
    trades: list = field(default_factory=list)

    # Paper 訊號（paper 模式用）
    paper_signals: list = field(default_factory=list)

    # meta
    contract: str = "MXF"
    created_at: str = ""


@dataclass
class PeriodPerformance:
    """週/月績效摘要"""
    period_type: str = ""               # "weekly" / "monthly"
    period_label: str = ""              # "2026-W10" / "2026-03"
    start_date: str = ""
    end_date: str = ""

    starting_balance: float = 0.0
    ending_balance: float = 0.0
    total_pnl: float = 0.0
    total_return_pct: float = 0.0

    trading_days: int = 0
    total_trades: int = 0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    max_drawdown: float = 0.0
    max_drawdown_pct: float = 0.0
    best_day_pnl: float = 0.0
    worst_day_pnl: float = 0.0
    avg_daily_pnl: float = 0.0

    # 連勝/連敗
    max_consecutive_wins: int = 0
    max_consecutive_losses: int = 0

    # 每日明細引用
    daily_results: list = field(default_factory=list)


# ============================================================
# 績效記錄器
# ============================================================

class PerformanceTracker:
    """
    績效記錄器 — 掛在 TradingEngine 上，自動記錄。
    """

    def __init__(self, data_dir: str = "data/performance",
                 trading_mode: str = "simulation"):
        self.data_dir = Path(data_dir)
        self.trading_mode = trading_mode
        self.today_trades: list[dict] = []
        self.paper_signals: list[dict] = []  # Paper 模式的訊號記錄
        self.starting_balance: float = 0.0
        self._today: str = ""
        self._activity_log: list[dict] = []  # 即時活動日誌
        self._max_activity_log = 200  # 最多保留 200 筆

        # 確保目錄存在
        for sub in ["daily", "weekly", "monthly"]:
            (self.data_dir / sub).mkdir(parents=True, exist_ok=True)

    # ============================================================
    # 核心方法
    # ============================================================

    def on_trade_closed(self, trade_dict: dict):
        """每筆交易結束時呼叫（engine 呼叫）"""
        today = datetime.now().strftime("%Y-%m-%d")
        if self._today != today:
            self._today = today
            self.today_trades = []
            self.paper_signals = []

        self.today_trades.append(trade_dict)
        self._save_incremental()

        # 加入活動日誌
        pnl = trade_dict.get("net_pnl", trade_dict.get("pnl", 0))
        self._add_activity(
            "trade_closed",
            f"{'獲利' if pnl > 0 else '虧損'} {pnl:+.0f} 元 | {trade_dict.get('reason', '')}",
            {"pnl": pnl, "side": trade_dict.get("side", "")}
        )

    def on_paper_signal(self, signal_dict: dict):
        """Paper 模式訊號記錄"""
        today = datetime.now().strftime("%Y-%m-%d")
        if self._today != today:
            self._today = today
            self.today_trades = []
            self.paper_signals = []

        self.paper_signals.append(signal_dict)
        self._save_incremental()

        # 加入活動日誌
        action = signal_dict.get("action", "?")
        price = signal_dict.get("price", 0)
        strength = signal_dict.get("signal_strength", 0)
        reason = signal_dict.get("reason", "")
        self._add_activity(
            "paper_signal",
            f"[PAPER] {action.upper()} @ {price:.0f} | 強度 {strength:.2f} | {reason}",
            signal_dict
        )

    def on_signal_scan(self, scan_result: dict):
        """策略掃描結果（無論是否觸發訊號）"""
        self._add_activity(
            scan_result.get("type", "scan"),
            scan_result.get("message", ""),
            scan_result.get("data", {})
        )

    def on_session_end(self, ending_balance: float):
        """
        每日收盤時呼叫
        1. 生成 DailyPerformance
        2. 存 daily JSON
        3. 更新 cumulative.json
        4. 若週五 -> 生成 weekly
        5. 若月底 -> 生成 monthly
        """
        today = datetime.now().strftime("%Y-%m-%d")

        daily = self._build_daily_summary(today, ending_balance)
        self._save_daily(daily)
        self._update_cumulative(daily)

        # 週五 → 生成 weekly
        today_date = datetime.now().date()
        if today_date.weekday() == 4:  # 0=Mon, 4=Fri
            self._generate_weekly(today_date)

        # 月底 → 生成 monthly
        tomorrow = today_date + timedelta(days=1)
        if tomorrow.month != today_date.month:
            self._generate_monthly(today_date)

        logger.info(f"[Performance] 日結完成: {today} | PnL: {daily.daily_pnl:+.0f} | "
                    f"交易: {daily.total_trades} 筆 | 勝率: {daily.win_rate:.1f}%")

        self._add_activity(
            "session_end",
            f"日結 {today} | PnL: {daily.daily_pnl:+.0f} | 勝率: {daily.win_rate:.1f}%",
            {"date": today, "pnl": daily.daily_pnl}
        )

    def get_daily_summary(self, date_str: str = None) -> dict:
        """讀取指定日期績效"""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        path = self.data_dir / "daily" / f"{date_str}.json"
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        live_path = self.data_dir / "daily" / f"{date_str}_live.json"
        if live_path.exists():
            with open(live_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            trades = data.get("trades", [])
            paper_signals = data.get("paper_signals", [])
            pnls = [t.get("net_pnl", t.get("pnl", 0)) for t in trades]
            return {
                "date": date_str,
                "trading_mode": data.get("trading_mode", self.trading_mode),
                "trades": trades,
                "paper_signals": paper_signals,
                "total_trades": len(trades),
                "daily_pnl": sum(pnls),
                "updated_at": data.get("updated_at", ""),
                "source": "incremental_live",
            }
        # 如果文件不存在但有當日數據，回傳即時計算結果
        if date_str == datetime.now().strftime("%Y-%m-%d") and (self.today_trades or self.paper_signals):
            daily = self._build_daily_summary(date_str, self.starting_balance)
            return asdict(daily)
        return {}

    def get_cumulative(self) -> dict:
        """讀取累計績效"""
        path = self.data_dir / "cumulative.json"
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def get_weekly_summary(self, week_label: str) -> dict:
        """讀取指定週績效"""
        path = self.data_dir / "weekly" / f"{week_label}.json"
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def get_monthly_summary(self, month_label: str) -> dict:
        """讀取指定月績效"""
        path = self.data_dir / "monthly" / f"{month_label}.json"
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def get_activity_log(self, count: int = 50) -> list[dict]:
        """取得最近的活動日誌"""
        return self._activity_log[-count:]

    def get_post_content(self, date_str: str = None) -> dict:
        """
        生成發文用的結構化數據（給 MindThread API 用）
        """
        daily = self.get_daily_summary(date_str)
        if not daily:
            return {"ready": False, "reason": "no data"}

        pnl = daily.get("daily_pnl", 0)
        win_rate = daily.get("win_rate", 0)
        total_trades = daily.get("total_trades", 0)
        largest_win = daily.get("largest_win", 0)
        largest_loss = daily.get("largest_loss", 0)
        mode = daily.get("trading_mode", "simulation")
        d = daily.get("date", "")

        mode_text = {"simulation": "模擬交易", "paper": "紙上交易", "live": "實單交易"}.get(mode, mode)

        # 生成文案
        sign = "+" if pnl >= 0 else ""
        headline = f"{sign}{pnl:.0f} 元 | 勝率 {win_rate:.0f}% | {total_trades} 筆交易"

        # 月/日
        if d:
            month = d[5:7].lstrip("0")
            day = d[8:10].lstrip("0")
            date_label = f"{month}/{day}"
        else:
            date_label = d

        body_lines = [
            f"[{date_label} 日結]",
            "",
            f"微台指 {mode_text}",
            f"今日 {sign}${abs(pnl):.0f} ({sign}{daily.get('daily_return_pct', 0):.2f}%)",
            "",
            f"{total_trades} 筆交易 / 勝率 {win_rate:.0f}%",
        ]
        if largest_win > 0:
            body_lines.append(f"最大獲利: +${largest_win:.0f}")
        if largest_loss < 0:
            body_lines.append(f"最大虧損: -${abs(largest_loss):.0f}")

        body_lines.append("")
        body_lines.append("以上為模擬績效，非投資建議" if mode != "live" else "以上為實單績效，非投資建議")

        return {
            "headline": headline,
            "body": "\n".join(body_lines),
            "metrics": {
                "pnl": pnl,
                "win_rate": win_rate,
                "total_trades": total_trades,
                "profit_factor": daily.get("profit_factor", 0),
            },
            "hashtags": ["#期貨", "#自動交易", "#UltraTrade", "#微台指"],
            "ready": True,
        }

    # ============================================================
    # 活動日誌
    # ============================================================

    def _add_activity(self, event_type: str, message: str, data: dict = None):
        """加入活動日誌"""
        entry = {
            "time": datetime.now().isoformat(),
            "type": event_type,
            "message": message,
        }
        if data:
            entry["data"] = data
        self._activity_log.append(entry)
        if len(self._activity_log) > self._max_activity_log:
            self._activity_log = self._activity_log[-self._max_activity_log:]

    # ============================================================
    # 內部方法
    # ============================================================

    def _save_incremental(self):
        """每筆交易即時存檔（防止意外斷線丟失）"""
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            path = self.data_dir / "daily" / f"{today}_live.json"
            data = {
                "date": today,
                "trading_mode": self.trading_mode,
                "trades": self.today_trades,
                "paper_signals": self.paper_signals,
                "updated_at": datetime.now().isoformat(),
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        except Exception as e:
            logger.warning(f"[Performance] incremental save failed: {e}")

    def _build_daily_summary(self, date_str: str, ending_balance: float) -> DailyPerformance:
        """建構每日績效摘要"""
        trades = self.today_trades
        daily = DailyPerformance(
            date=date_str,
            trading_mode=self.trading_mode,
            starting_balance=self.starting_balance,
            ending_balance=ending_balance,
            contract="MXF",
            created_at=datetime.now().isoformat(),
            trades=trades,
            paper_signals=self.paper_signals,
        )

        if not trades:
            return daily

        # 交易統計
        pnls = [t.get("net_pnl", t.get("pnl", 0)) for t in trades]
        daily.total_trades = len(trades)
        daily.daily_pnl = sum(pnls)

        if self.starting_balance > 0:
            daily.daily_return_pct = daily.daily_pnl / self.starting_balance * 100

        winners = [p for p in pnls if p > 0]
        losers = [p for p in pnls if p <= 0]

        daily.winning_trades = len(winners)
        daily.losing_trades = len(losers)
        daily.win_rate = len(winners) / len(trades) * 100 if trades else 0
        daily.avg_win = sum(winners) / len(winners) if winners else 0
        daily.avg_loss = abs(sum(losers) / len(losers)) if losers else 0

        total_wins = sum(winners) if winners else 0
        total_losses = abs(sum(losers)) if losers else 0
        daily.profit_factor = total_wins / total_losses if total_losses > 0 else float("inf")

        daily.largest_win = max(pnls) if pnls else 0
        daily.largest_loss = min(pnls) if pnls else 0

        # 最大回撤
        dd, dd_pct = self._calculate_drawdown(pnls)
        daily.max_drawdown = dd
        daily.max_drawdown_pct = dd_pct

        return daily

    def _calculate_drawdown(self, pnls: list) -> tuple[float, float]:
        """計算最大回撤"""
        if not pnls:
            return 0.0, 0.0

        equity = 0.0
        peak = 0.0
        max_dd = 0.0
        max_dd_pct = 0.0

        for pnl in pnls:
            equity += pnl
            peak = max(peak, equity)
            dd = peak - equity
            if dd > max_dd:
                max_dd = dd
                if self.starting_balance > 0:
                    max_dd_pct = dd / self.starting_balance * 100

        return max_dd, max_dd_pct

    def _save_daily(self, daily: DailyPerformance):
        """存儲每日績效 JSON"""
        try:
            path = self.data_dir / "daily" / f"{daily.date}.json"
            data = asdict(daily)
            # 清理 inf
            if data.get("profit_factor") == float("inf"):
                data["profit_factor"] = 999.0
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"[Performance] saved daily: {path}")
        except Exception as e:
            logger.warning(f"[Performance] save daily failed: {e}")

    def _update_cumulative(self, daily: DailyPerformance):
        """更新 cumulative.json（含日期去重保護）"""
        try:
            cum_path = self.data_dir / "cumulative.json"
            if cum_path.exists():
                with open(cum_path, "r", encoding="utf-8") as f:
                    cum = json.load(f)
            else:
                cum = {
                    "first_trade_date": daily.date,
                    "initial_balance": daily.starting_balance,
                    "total_trading_days": 0,
                    "total_trades": 0,
                    "total_pnl": 0,
                    "max_drawdown": 0,
                    "max_drawdown_pct": 0,
                    "best_day": {"date": "", "pnl": -999999},
                    "worst_day": {"date": "", "pnl": 999999},
                    "all_pnls": [],  # 每筆交易 PnL 列表
                    "recorded_dates": [],  # 已記錄的日期（防重複）
                }

            # ---- 去重保護：已記錄的日期不再重複累加 ----
            recorded_dates = cum.get("recorded_dates", [])
            if daily.date in recorded_dates:
                logger.info(f"[Performance] cumulative skip duplicate: {daily.date}")
                return

            recorded_dates.append(daily.date)
            cum["recorded_dates"] = recorded_dates

            cum["last_updated"] = datetime.now().isoformat()
            cum["trading_mode"] = daily.trading_mode
            cum["current_balance"] = daily.ending_balance
            cum["total_trading_days"] = cum.get("total_trading_days", 0) + 1
            cum["total_trades"] = cum.get("total_trades", 0) + daily.total_trades
            cum["total_pnl"] = cum.get("total_pnl", 0) + daily.daily_pnl

            initial = cum.get("initial_balance", daily.starting_balance) or daily.starting_balance
            cum["total_return_pct"] = cum["total_pnl"] / initial * 100 if initial > 0 else 0

            # 更新最佳/最差日
            if daily.daily_pnl > cum.get("best_day", {}).get("pnl", -999999):
                cum["best_day"] = {"date": daily.date, "pnl": daily.daily_pnl}
            if daily.daily_pnl < cum.get("worst_day", {}).get("pnl", 999999):
                cum["worst_day"] = {"date": daily.date, "pnl": daily.daily_pnl}

            # 累計勝率
            all_pnls = cum.get("all_pnls", [])
            for t in daily.trades:
                pnl = t.get("net_pnl", t.get("pnl", 0))
                all_pnls.append(pnl)
            cum["all_pnls"] = all_pnls

            winners = [p for p in all_pnls if p > 0]
            cum["overall_win_rate"] = len(winners) / len(all_pnls) * 100 if all_pnls else 0

            total_wins_sum = sum(p for p in all_pnls if p > 0)
            total_losses_sum = abs(sum(p for p in all_pnls if p <= 0))
            cum["overall_profit_factor"] = total_wins_sum / total_losses_sum if total_losses_sum > 0 else 999.0

            # 累計最大回撤
            dd, dd_pct = self._calculate_drawdown(all_pnls)
            if dd > cum.get("max_drawdown", 0):
                cum["max_drawdown"] = dd
            if dd_pct > cum.get("max_drawdown_pct", 0):
                cum["max_drawdown_pct"] = dd_pct

            # 連勝/連敗
            streak_type = "win" if all_pnls and all_pnls[-1] > 0 else "loss"
            streak_count = 0
            for p in reversed(all_pnls):
                if (streak_type == "win" and p > 0) or (streak_type == "loss" and p <= 0):
                    streak_count += 1
                else:
                    break
            cum["current_streak"] = {"type": streak_type, "count": streak_count}

            with open(cum_path, "w", encoding="utf-8") as f:
                json.dump(cum, f, ensure_ascii=False, indent=2, default=str)

        except Exception as e:
            logger.warning(f"[Performance] update cumulative failed: {e}")

    def _generate_weekly(self, today: date):
        """生成週績效"""
        try:
            # 本週一到今天
            monday = today - timedelta(days=today.weekday())
            week_label = today.strftime("%G-W%V")

            daily_files = []
            daily_pnls = []
            total_trades = 0
            all_trade_pnls = []
            starting_balance = 0
            ending_balance = 0

            for i in range(5):  # 週一到週五
                d = monday + timedelta(days=i)
                path = self.data_dir / "daily" / f"{d.strftime('%Y-%m-%d')}.json"
                if path.exists():
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    daily_files.append(d.strftime("%Y-%m-%d"))
                    daily_pnls.append(data.get("daily_pnl", 0))
                    total_trades += data.get("total_trades", 0)
                    if not starting_balance:
                        starting_balance = data.get("starting_balance", 0)
                    ending_balance = data.get("ending_balance", 0)
                    for t in data.get("trades", []):
                        all_trade_pnls.append(t.get("net_pnl", t.get("pnl", 0)))

            if not daily_files:
                return

            period = PeriodPerformance(
                period_type="weekly",
                period_label=week_label,
                start_date=daily_files[0],
                end_date=daily_files[-1],
                starting_balance=starting_balance,
                ending_balance=ending_balance,
                total_pnl=sum(daily_pnls),
                total_return_pct=sum(daily_pnls) / starting_balance * 100 if starting_balance > 0 else 0,
                trading_days=len(daily_files),
                total_trades=total_trades,
                best_day_pnl=max(daily_pnls) if daily_pnls else 0,
                worst_day_pnl=min(daily_pnls) if daily_pnls else 0,
                avg_daily_pnl=sum(daily_pnls) / len(daily_pnls) if daily_pnls else 0,
                daily_results=daily_files,
            )

            # 勝率、盈虧比
            if all_trade_pnls:
                winners = [p for p in all_trade_pnls if p > 0]
                period.win_rate = len(winners) / len(all_trade_pnls) * 100
                total_wins = sum(winners)
                total_losses = abs(sum(p for p in all_trade_pnls if p <= 0))
                period.profit_factor = total_wins / total_losses if total_losses > 0 else 999.0

            # 連勝/連敗
            max_wins, max_losses = self._calc_streaks(all_trade_pnls)
            period.max_consecutive_wins = max_wins
            period.max_consecutive_losses = max_losses

            path = self.data_dir / "weekly" / f"{week_label}.json"
            data = asdict(period)
            if data.get("profit_factor") == float("inf"):
                data["profit_factor"] = 999.0
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"[Performance] saved weekly: {week_label}")

        except Exception as e:
            logger.warning(f"[Performance] generate weekly failed: {e}")

    def _generate_monthly(self, today: date):
        """生成月績效"""
        try:
            month_label = today.strftime("%Y-%m")
            first_day = today.replace(day=1)

            daily_files = []
            daily_pnls = []
            total_trades = 0
            all_trade_pnls = []
            starting_balance = 0
            ending_balance = 0

            d = first_day
            while d <= today:
                path = self.data_dir / "daily" / f"{d.strftime('%Y-%m-%d')}.json"
                if path.exists():
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    daily_files.append(d.strftime("%Y-%m-%d"))
                    daily_pnls.append(data.get("daily_pnl", 0))
                    total_trades += data.get("total_trades", 0)
                    if not starting_balance:
                        starting_balance = data.get("starting_balance", 0)
                    ending_balance = data.get("ending_balance", 0)
                    for t in data.get("trades", []):
                        all_trade_pnls.append(t.get("net_pnl", t.get("pnl", 0)))
                d += timedelta(days=1)

            if not daily_files:
                return

            period = PeriodPerformance(
                period_type="monthly",
                period_label=month_label,
                start_date=daily_files[0],
                end_date=daily_files[-1],
                starting_balance=starting_balance,
                ending_balance=ending_balance,
                total_pnl=sum(daily_pnls),
                total_return_pct=sum(daily_pnls) / starting_balance * 100 if starting_balance > 0 else 0,
                trading_days=len(daily_files),
                total_trades=total_trades,
                best_day_pnl=max(daily_pnls) if daily_pnls else 0,
                worst_day_pnl=min(daily_pnls) if daily_pnls else 0,
                avg_daily_pnl=sum(daily_pnls) / len(daily_pnls) if daily_pnls else 0,
                daily_results=daily_files,
            )

            if all_trade_pnls:
                winners = [p for p in all_trade_pnls if p > 0]
                period.win_rate = len(winners) / len(all_trade_pnls) * 100
                total_wins = sum(winners)
                total_losses = abs(sum(p for p in all_trade_pnls if p <= 0))
                period.profit_factor = total_wins / total_losses if total_losses > 0 else 999.0

            max_wins, max_losses = self._calc_streaks(all_trade_pnls)
            period.max_consecutive_wins = max_wins
            period.max_consecutive_losses = max_losses

            path = self.data_dir / "monthly" / f"{month_label}.json"
            data = asdict(period)
            if data.get("profit_factor") == float("inf"):
                data["profit_factor"] = 999.0
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"[Performance] saved monthly: {month_label}")

        except Exception as e:
            logger.warning(f"[Performance] generate monthly failed: {e}")

    @staticmethod
    def _calc_streaks(pnls: list) -> tuple[int, int]:
        """計算最大連勝/連敗"""
        max_wins = max_losses = 0
        cur_wins = cur_losses = 0
        for p in pnls:
            if p > 0:
                cur_wins += 1
                cur_losses = 0
                max_wins = max(max_wins, cur_wins)
            else:
                cur_losses += 1
                cur_wins = 0
                max_losses = max(max_losses, cur_losses)
        return max_wins, max_losses

    def get_latest_daily(self) -> dict:
        """取得最新一天的績效"""
        daily_dir = self.data_dir / "daily"
        if not daily_dir.exists():
            return {}

        # 找最新的日期檔案（排除 _live.json）
        files = sorted([
            f for f in daily_dir.glob("*.json")
            if not f.stem.endswith("_live")
        ], reverse=True)

        if files:
            with open(files[0], "r", encoding="utf-8") as f:
                return json.load(f)

        # 嘗試即時數據
        return self.get_daily_summary()
