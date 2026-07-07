"""
UltraTrader Intelligence — 資料模型
定義三大法人、選擇權、國際市場等資料結構
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional


FACTOR_SOURCES = {
    "foreign_futures": {
        "factor": 1,
        "status": "LIVE_NEW",
        "source": "TAIFEX 三大法人區分各期貨契約 CSV",
        "scope": "大台契約",
    },
    "pc_ratio": {
        "factor": 2,
        "status": "LIVE",
        "source": "TAIFEX pcRatioDown POST",
        "scope": "全市場 Put/Call Ratio",
    },
    "vix": {
        "factor": 3,
        "status": "LIVE",
        "source": "yfinance ^VIX",
        "scope": "VIX",
    },
    "foreign_spot": {
        "factor": 4,
        "status": "LIVE",
        "source": "TWSE BFI82U JSON",
        "scope": "外資現貨買賣超",
    },
    "margin": {
        "factor": 5,
        "status": "LIVE_NEW",
        "source": "TWSE OpenAPI MI_MARGN",
        "scope": "全市場加總",
    },
    "large_trader": {
        "factor": 6,
        "status": "LIVE_NEW",
        "source": "TAIFEX 大額交易人 CSV",
        "scope": "TX 所有契約大台等值",
    },
    "us_market": {
        "factor": 7,
        "status": "LIVE",
        "source": "yfinance ES=F / NQ=F",
        "scope": "美股期貨",
    },
    "sox": {
        "factor": 8,
        "status": "LIVE",
        "source": "yfinance ^SOX",
        "scope": "費城半導體指數",
    },
    "trust": {
        "factor": 9,
        "status": "LIVE_NEW",
        "source": "TWSE BFI82U JSON",
        "scope": "投信現貨買賣超",
    },
}


def default_factor_sources() -> dict:
    return {name: dict(meta) for name, meta in FACTOR_SOURCES.items()}


# ============================================================
# 三大法人資料
# ============================================================

@dataclass
class InstitutionalFutures:
    """三大法人期貨部位"""
    date: date = None
    # 外資
    foreign_long: int = 0          # 外資多單口數
    foreign_short: int = 0         # 外資空單口數
    foreign_net: int = 0           # 外資淨部位（正=偏多）
    foreign_oi_net: int = 0        # 外資未平倉淨部位
    # 投信
    trust_long: int = 0
    trust_short: int = 0
    trust_net: int = 0
    trust_oi_net: int = 0
    # 自營
    dealer_long: int = 0
    dealer_short: int = 0
    dealer_net: int = 0
    dealer_oi_net: int = 0
    # 合計
    total_oi_net: int = 0          # 三大法人合計淨OI

    @property
    def foreign_bias(self) -> str:
        """外資偏向：bullish / bearish / neutral"""
        if self.foreign_oi_net > 5000:
            return "bullish"
        elif self.foreign_oi_net < -5000:
            return "bearish"
        return "neutral"


@dataclass
class InstitutionalSpot:
    """三大法人現貨買賣超"""
    date: date = None
    fetched_at: datetime = None
    status: str = "NO_DATA"  # LIVE / STALE_DISPLAY / NO_DATA
    error: str = ""
    source: str = ""
    foreign_buy_sell: float = 0.0   # 外資買賣超（億元）
    foreign_dealer_buy_sell: float = 0.0  # 外資自營商買賣超（億元）
    trust_buy_sell: float = 0.0     # 投信買賣超（億元）
    dealer_buy_sell: float = 0.0    # 自營買賣超（億元）
    total_buy_sell: float = 0.0     # 合計（億元）


# ============================================================
# 選擇權資料
# ============================================================

@dataclass
class OptionsData:
    """選擇權市場資料"""
    date: date = None
    # Put/Call Ratio
    pc_ratio_volume: float = 0.0     # P/C 成交量比
    pc_ratio_oi: float = 0.0        # P/C 未平倉比
    # 外資選擇權
    foreign_call_oi_net: int = 0    # 外資 Call 淨 OI
    foreign_put_oi_net: int = 0     # 外資 Put 淨 OI
    foreign_option_net: int = 0     # 外資選擇權淨部位
    # 散戶 Put/Call（大額交易者以外）
    retail_pc_ratio: float = 0.0
    # 最大 OI 位置（支撐/壓力）
    max_call_oi_strike: float = 0.0   # 最大 Call OI 的履約價（壓力）
    max_put_oi_strike: float = 0.0    # 最大 Put OI 的履約價（支撐）

    @property
    def pc_signal(self) -> str:
        """P/C Ratio 訊號"""
        if self.pc_ratio_oi > 1.5:
            return "extreme_bearish"   # 散戶極度恐慌 → 逆向做多
        elif self.pc_ratio_oi > 1.2:
            return "bearish"
        elif self.pc_ratio_oi < 0.6:
            return "extreme_bullish"   # 散戶極度貪婪 → 逆向做空
        elif self.pc_ratio_oi < 0.8:
            return "bullish"
        return "neutral"


# ============================================================
# 大額交易人 / 十大交易人
# ============================================================

@dataclass
class LargeTraderOI:
    """大額交易人未平倉"""
    date: date = None
    # 前五大
    top5_long: int = 0
    top5_short: int = 0
    top5_net: int = 0
    # 前十大
    top10_long: int = 0
    top10_short: int = 0
    top10_net: int = 0
    # 全市場
    total_oi: int = 0
    # 集中度（前十大佔比）
    concentration_ratio: float = 0.0


# ============================================================
# 融資融券（信用交易）
# ============================================================

@dataclass
class MarginData:
    """融資融券資料"""
    date: date = None
    fetched_at: datetime = None
    status: str = "NO_DATA"
    error: str = ""
    margin_balance: float = 0.0     # 融資今日餘額（交易單位，全市場加總）
    margin_previous_balance: float = 0.0  # 融資前日餘額（交易單位，全市場加總）
    margin_change: float = 0.0      # 融資增減（交易單位）
    margin_change_pct: float = 0.0  # 融資餘額百分比變化
    short_balance: float = 0.0      # 融券今日餘額（交易單位，全市場加總）
    short_previous_balance: float = 0.0  # 融券前日餘額（交易單位，全市場加總）
    short_change: float = 0.0       # 融券增減（交易單位）
    margin_usage_rate: float = 0.0  # 融資使用率 %


# ============================================================
# 國際市場指標
# ============================================================

@dataclass
class InternationalData:
    """國際市場資料"""
    timestamp: datetime = None
    # VIX 恐慌指數
    vix: float = 0.0
    vix_change: float = 0.0
    # 美股期貨
    sp500_futures: float = 0.0     # ES 期貨
    sp500_change_pct: float = 0.0
    nasdaq_futures: float = 0.0    # NQ 期貨
    nasdaq_change_pct: float = 0.0
    # 費城半導體
    sox_index: float = 0.0
    sox_change_pct: float = 0.0
    # 原油
    crude_oil: float = 0.0
    crude_change_pct: float = 0.0
    # 美元指數
    dxy: float = 0.0
    dxy_change_pct: float = 0.0
    # 美國十年期公債殖利率
    us10y_yield: float = 0.0
    us10y_change: float = 0.0

    @property
    def vix_signal(self) -> str:
        """VIX 恐慌訊號"""
        if self.vix > 35:
            return "extreme_fear"    # 極度恐慌 → 左側做多
        elif self.vix > 25:
            return "fear"
        elif self.vix < 12:
            return "extreme_greed"   # 極度貪婪 → 左側做空
        elif self.vix < 16:
            return "greed"
        return "neutral"


# ============================================================
# 綜合情報快照
# ============================================================

@dataclass
class IntelligenceSnapshot:
    """整合所有情報的快照"""
    timestamp: datetime = None

    # 各資料源
    institutional_futures: InstitutionalFutures = field(default_factory=InstitutionalFutures)
    institutional_spot: InstitutionalSpot = field(default_factory=InstitutionalSpot)
    options: OptionsData = field(default_factory=OptionsData)
    large_trader: LargeTraderOI = field(default_factory=LargeTraderOI)
    margin: MarginData = field(default_factory=MarginData)
    international: InternationalData = field(default_factory=InternationalData)

    # 左側評分結果
    left_side_score: float = 0.0        # -1.0（極空）~ +1.0（極多）
    left_side_confidence: float = 0.0   # 0 ~ 1.0 信心度
    left_side_signal: str = "neutral"   # strong_buy / buy / neutral / sell / strong_sell
    factor_scores: list = field(default_factory=list)  # 各因子分數明細

    # 資料新鮮度
    data_freshness: dict = field(default_factory=dict)  # 各資料源最後更新時間
    factor_sources: dict = field(default_factory=default_factor_sources)

    def to_dict(self) -> dict:
        """轉換為 dict（供 API / WebSocket 使用）"""
        factor_sources = default_factor_sources()
        if not self.institutional_futures.date:
            factor_sources["foreign_futures"]["status"] = "STUB"
            factor_sources["foreign_futures"]["scope"] = "資料未更新"
        if not self.large_trader.date:
            factor_sources["large_trader"]["status"] = "STUB"
            factor_sources["large_trader"]["scope"] = "資料未更新"
        if self.institutional_spot.status != "LIVE":
            status = self.institutional_spot.status or "NO_DATA"
            factor_sources["foreign_spot"]["status"] = status
            factor_sources["foreign_spot"]["scope"] = (
                "快取顯示，不參與評分"
                if status == "STALE_DISPLAY"
                else "BFI82U 未更新"
            )
            if self.institutional_spot.source:
                factor_sources["foreign_spot"]["source"] = self.institutional_spot.source
            factor_sources["trust"]["status"] = status
            factor_sources["trust"]["scope"] = (
                "快取顯示，不參與評分"
                if status == "STALE_DISPLAY"
                else "BFI82U 未更新"
            )
            if self.institutional_spot.source:
                factor_sources["trust"]["source"] = self.institutional_spot.source
        if self.margin.status != "LIVE":
            factor_sources["margin"]["status"] = self.margin.status or "NO_DATA"
            factor_sources["margin"]["scope"] = "MI_MARGN 未更新"

        return {
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "left_side": {
                "score": round(self.left_side_score, 3),
                "confidence": round(self.left_side_confidence, 3),
                "signal": self.left_side_signal,
                "factors": self.factor_scores,
            },
            "institutional": {
                "foreign_oi_net": self.institutional_futures.foreign_oi_net,
                "foreign_bias": self.institutional_futures.foreign_bias,
                "trust_oi_net": self.institutional_futures.trust_oi_net,
                "dealer_oi_net": self.institutional_futures.dealer_oi_net,
                "total_oi_net": self.institutional_futures.total_oi_net,
                "foreign_spot_buy_sell": self.institutional_spot.foreign_buy_sell,
                "foreign_dealer_spot_buy_sell": self.institutional_spot.foreign_dealer_buy_sell,
                "trust_spot_buy_sell": self.institutional_spot.trust_buy_sell,
                "dealer_spot_buy_sell": self.institutional_spot.dealer_buy_sell,
                "total_spot_buy_sell": self.institutional_spot.total_buy_sell,
                "spot_date": self.institutional_spot.date.isoformat() if self.institutional_spot.date else None,
                "spot_fetched_at": self.institutional_spot.fetched_at.isoformat() if self.institutional_spot.fetched_at else None,
                "spot_status": self.institutional_spot.status,
                "spot_error": self.institutional_spot.error,
                "spot_source": self.institutional_spot.source,
            },
            "options": {
                "pc_ratio_oi": self.options.pc_ratio_oi,
                "pc_ratio_volume": self.options.pc_ratio_volume,
                "pc_signal": self.options.pc_signal,
                "max_call_strike": self.options.max_call_oi_strike,
                "max_put_strike": self.options.max_put_oi_strike,
            },
            "international": {
                "vix": self.international.vix,
                "vix_signal": self.international.vix_signal,
                "sp500_change_pct": round(self.international.sp500_change_pct, 2),
                "nasdaq_change_pct": round(self.international.nasdaq_change_pct, 2),
                "sox_change_pct": round(self.international.sox_change_pct, 2),
                "us10y_yield": self.international.us10y_yield,
            },
            "large_trader": {
                "top5_net": self.large_trader.top5_net,
                "top10_net": self.large_trader.top10_net,
                "concentration": round(self.large_trader.concentration_ratio, 1),
                "total_oi": self.large_trader.total_oi,
            },
            "margin": {
                "margin_balance": self.margin.margin_balance,
                "margin_previous_balance": self.margin.margin_previous_balance,
                "margin_change": self.margin.margin_change,
                "margin_change_pct": round(self.margin.margin_change_pct, 3),
                "short_balance": self.margin.short_balance,
                "short_previous_balance": self.margin.short_previous_balance,
                "short_change": self.margin.short_change,
                "usage_rate": self.margin.margin_usage_rate,
                "date": self.margin.date.isoformat() if self.margin.date else None,
                "fetched_at": self.margin.fetched_at.isoformat() if self.margin.fetched_at else None,
                "status": self.margin.status,
                "error": self.margin.error,
            },
            "freshness": self.data_freshness,
            "factor_sources": factor_sources,
        }
