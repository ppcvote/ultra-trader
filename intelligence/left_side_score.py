"""
UltraTrader Intelligence — 左側交易評分引擎
9 因子逆向評分系統：當市場極度恐慌時做多，極度貪婪時做空

核心邏輯：左側交易 = 逆向操作
- 所有人都在賣 → 我們準備買
- 所有人都在買 → 我們準備賣
- 關鍵是「極端」— 只在情緒/籌碼到達極端時才觸發
"""

from dataclasses import dataclass
from typing import Optional

from intelligence.models import FACTOR_SOURCES, IntelligenceSnapshot


@dataclass
class FactorResult:
    """單一因子的評分結果"""
    name: str           # 因子名稱
    name_zh: str        # 中文名稱
    score: float        # -1.0（極空）~ +1.0（極多）
    weight: float       # 權重
    confidence: float   # 0~1 信心度（資料品質）
    detail: str         # 說明文字
    level: str          # "extreme" / "strong" / "moderate" / "neutral"
    status: str = ""    # LIVE / LIVE_NEW / STUB
    source: str = ""    # 資料來源
    scope: str = ""     # 資料口徑
    factor: int = 0     # 因子編號

    def __post_init__(self):
        meta = FACTOR_SOURCES.get(self.name, {})
        self.status = self.status or meta.get("status", "LIVE")
        self.source = self.source or meta.get("source", "")
        self.scope = self.scope or meta.get("scope", "")
        self.factor = self.factor or int(meta.get("factor", 0) or 0)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "name_zh": self.name_zh,
            "factor": self.factor,
            "status": self.status,
            "source": self.source,
            "scope": self.scope,
            "score": round(self.score, 3),
            "weight": self.weight,
            "weighted": round(self.score * self.weight, 3),
            "confidence": round(self.confidence, 2),
            "detail": self.detail,
            "level": self.level,
        }


class LeftSideScoreEngine:
    """
    9 因子左側（逆向）交易評分系統

    因子權重分配：
    1. 外資期貨淨OI (20%) — 外資大空 → 左側做多訊號
    2. P/C Ratio (15%) — P/C > 1.5 → 散戶恐慌 → 左側做多
    3. VIX 恐慌指數 (12%) — VIX > 30 → 左側做多
    4. 外資現貨買賣超 (12%) — 外資連續大賣 → 左側做多
    5. 融資融券 (10%) — 散戶斷頭（融資大減）→ 底部訊號
    6. 大額交易人集中度 (8%) — 籌碼極度集中一方
    7. 美股連動 (8%) — 美股暴跌後反彈帶動台股
    8. 費半連動 (8%) — SOX 暴跌 → 台股超跌
    9. 投信動向 (7%) — 投信止血反買 → 轉折訊號

    最終輸出：
    - score: -1.0 ~ +1.0（正 = 看多，負 = 看空）
    - signal: strong_buy / buy / neutral / sell / strong_sell
    """

    WEIGHTS = {
        "foreign_futures": 0.20,
        "pc_ratio": 0.15,
        "vix": 0.12,
        "foreign_spot": 0.12,
        "margin": 0.10,
        "large_trader": 0.08,
        "us_market": 0.08,
        "sox": 0.08,
        "trust": 0.07,
    }

    # 訊號閾值
    STRONG_THRESHOLD = 0.50    # 強烈訊號門檻
    MODERATE_THRESHOLD = 0.25  # 中等訊號門檻

    def __init__(self):
        self._last_factors: list[FactorResult] = []

    def calculate(self, snapshot: IntelligenceSnapshot) -> IntelligenceSnapshot:
        """
        計算左側交易評分並更新 snapshot

        Returns: 更新後的 IntelligenceSnapshot
        """
        factors = [
            self._score_foreign_futures(snapshot),
            self._score_pc_ratio(snapshot),
            self._score_vix(snapshot),
            self._score_foreign_spot(snapshot),
            self._score_margin(snapshot),
            self._score_large_trader(snapshot),
            self._score_us_market(snapshot),
            self._score_sox(snapshot),
            self._score_trust(snapshot),
        ]

        self._last_factors = factors

        # 加權計算總分
        total_weighted = sum(f.score * f.weight for f in factors)
        total_confidence = sum(f.confidence * f.weight for f in factors)

        # 信心度低於 0.3 → 降低訊號強度
        if total_confidence < 0.3:
            total_weighted *= 0.5

        # 裁切到 [-1, 1]
        total_weighted = max(-1.0, min(1.0, total_weighted))

        # 決定訊號
        if total_weighted >= self.STRONG_THRESHOLD:
            signal = "strong_buy"
        elif total_weighted >= self.MODERATE_THRESHOLD:
            signal = "buy"
        elif total_weighted <= -self.STRONG_THRESHOLD:
            signal = "strong_sell"
        elif total_weighted <= -self.MODERATE_THRESHOLD:
            signal = "sell"
        else:
            signal = "neutral"

        # 更新 snapshot
        snapshot.left_side_score = total_weighted
        snapshot.left_side_confidence = total_confidence
        snapshot.left_side_signal = signal
        snapshot.factor_scores = [f.to_dict() for f in factors]

        return snapshot

    @property
    def last_factors(self) -> list[FactorResult]:
        return self._last_factors

    # ============================================================
    # 9 個因子評分函數
    # ============================================================

    def _score_foreign_futures(self, snap: IntelligenceSnapshot) -> FactorResult:
        """
        因子 1：外資期貨淨OI（20%）
        邏輯：外資大幅偏空 → 可能是底部 → 左側做多
        """
        if not snap.institutional_futures.date:
            return FactorResult(
                name="foreign_futures",
                name_zh="外資期貨",
                score=0.0,
                weight=self.WEIGHTS["foreign_futures"],
                confidence=0.0,
                detail="資料未更新: TAIFEX 三大法人大台契約",
                level="neutral",
                status="STUB",
            )

        net_oi = snap.institutional_futures.foreign_oi_net
        confidence = 0.8

        # 正規化：以 ±20000 口為滿分基準
        # 注意是「逆向」：外資大空 → 正分（做多）
        score = 0.0
        level = "neutral"

        if net_oi < -15000:
            score = 0.9   # 外資極度偏空 → 強烈左側做多
            level = "extreme"
        elif net_oi < -10000:
            score = 0.6
            level = "strong"
        elif net_oi < -5000:
            score = 0.3
            level = "moderate"
        elif net_oi > 15000:
            score = -0.9  # 外資極度偏多 → 左側做空
            level = "extreme"
        elif net_oi > 10000:
            score = -0.6
            level = "strong"
        elif net_oi > 5000:
            score = -0.3
            level = "moderate"

        detail = f"外資淨OI {net_oi:+,d} 口"
        if level == "extreme":
            detail += " (極端)"

        return FactorResult(
            name="foreign_futures",
            name_zh="外資期貨",
            score=score,
            weight=self.WEIGHTS["foreign_futures"],
            confidence=confidence,
            detail=detail,
            level=level,
            status="LIVE_NEW",
        )

    def _score_pc_ratio(self, snap: IntelligenceSnapshot) -> FactorResult:
        """
        因子 2：Put/Call Ratio OI（15%）
        邏輯：P/C > 1.5 散戶瘋狂買 Put → 恐慌到頂 → 左側做多
        """
        pc = snap.options.pc_ratio_oi
        confidence = 0.8 if pc > 0 else 0.0

        score = 0.0
        level = "neutral"

        if pc > 1.8:
            score = 1.0    # 極度恐慌
            level = "extreme"
        elif pc > 1.5:
            score = 0.7
            level = "strong"
        elif pc > 1.2:
            score = 0.3
            level = "moderate"
        elif pc < 0.5:
            score = -1.0   # 極度貪婪
            level = "extreme"
        elif pc < 0.65:
            score = -0.7
            level = "strong"
        elif pc < 0.8:
            score = -0.3
            level = "moderate"

        detail = f"P/C={pc:.2f}"
        if pc > 1.5:
            detail += " 散戶恐慌"
        elif pc < 0.65:
            detail += " 散戶貪婪"

        return FactorResult(
            name="pc_ratio",
            name_zh="Put/Call",
            score=score,
            weight=self.WEIGHTS["pc_ratio"],
            confidence=confidence,
            detail=detail,
            level=level,
        )

    def _score_vix(self, snap: IntelligenceSnapshot) -> FactorResult:
        """
        因子 3：VIX 恐慌指數（12%）
        邏輯：VIX > 35 → 全球恐慌到頂 → 左側做多
        """
        vix = snap.international.vix
        confidence = 0.9 if vix > 0 else 0.0

        score = 0.0
        level = "neutral"

        if vix > 40:
            score = 1.0
            level = "extreme"
        elif vix > 30:
            score = 0.7
            level = "strong"
        elif vix > 25:
            score = 0.3
            level = "moderate"
        elif vix < 12:
            score = -0.8
            level = "strong"
        elif vix < 15:
            score = -0.3
            level = "moderate"

        detail = f"VIX={vix:.1f}"
        if vix > 30:
            detail += " 恐慌"
        elif vix < 13:
            detail += " 自滿"

        return FactorResult(
            name="vix",
            name_zh="VIX",
            score=score,
            weight=self.WEIGHTS["vix"],
            confidence=confidence,
            detail=detail,
            level=level,
        )

    def _score_foreign_spot(self, snap: IntelligenceSnapshot) -> FactorResult:
        """
        因子 4：外資現貨買賣超（12%）
        邏輯：外資連續大賣超 → 可能到底 → 左側做多
        """
        if snap.institutional_spot.status != "LIVE" or not snap.institutional_spot.date:
            status = snap.institutional_spot.status or "NO_DATA"
            is_cache = status == "STALE_DISPLAY"
            detail = "BFI82U 快取顯示，不參與評分" if is_cache else "BFI82U 未更新"
            if snap.institutional_spot.date:
                detail += f"；最後資料 {snap.institutional_spot.date}"
            if snap.institutional_spot.error:
                detail += f"；{snap.institutional_spot.error}"
            return FactorResult(
                name="foreign_spot",
                name_zh="外資現貨",
                score=0.0,
                weight=self.WEIGHTS["foreign_spot"],
                confidence=0.0,
                detail=detail,
                level="neutral",
                status=status,
                scope="快取顯示，不參與評分" if is_cache else "BFI82U 未更新",
            )

        buy_sell = snap.institutional_spot.foreign_buy_sell  # 億元
        confidence = 0.7

        score = 0.0
        level = "neutral"

        # 逆向邏輯
        if buy_sell < -200:
            score = 0.9     # 外資瘋狂賣超 → 左側做多
            level = "extreme"
        elif buy_sell < -100:
            score = 0.5
            level = "strong"
        elif buy_sell < -50:
            score = 0.2
            level = "moderate"
        elif buy_sell > 200:
            score = -0.9    # 外資瘋狂買超 → 左側做空
            level = "extreme"
        elif buy_sell > 100:
            score = -0.5
            level = "strong"
        elif buy_sell > 50:
            score = -0.2
            level = "moderate"

        detail = f"外資 {buy_sell:+.1f}億"

        return FactorResult(
            name="foreign_spot",
            name_zh="外資現貨",
            score=score,
            weight=self.WEIGHTS["foreign_spot"],
            confidence=confidence,
            detail=detail,
            level=level,
        )

    def _score_margin(self, snap: IntelligenceSnapshot) -> FactorResult:
        """
        因子 5：融資融券（10%）
        邏輯：融資大幅減少（斷頭）→ 散戶絕望 → 底部接近
        """
        change = snap.margin.margin_change  # 交易單位
        change_pct = snap.margin.margin_change_pct
        has_data = (
            snap.margin.status == "LIVE"
            and (snap.margin.margin_balance != 0 or snap.margin.margin_previous_balance != 0)
        )
        if not has_data:
            status = snap.margin.status or "NO_DATA"
            detail = "MI_MARGN 未更新"
            if snap.margin.date:
                detail += f"；最後資料 {snap.margin.date}"
            if snap.margin.error:
                detail += f"；{snap.margin.error}"
            return FactorResult(
                name="margin",
                name_zh="融資",
                score=0.0,
                weight=self.WEIGHTS["margin"],
                confidence=0.0,
                detail=detail,
                level="neutral",
                status=status,
                scope="MI_MARGN 未更新",
            )

        confidence = 0.6 if has_data else 0.0

        score = 0.0
        level = "neutral"

        if change_pct < -3.0:
            score = 0.8     # 融資大減（斷頭）→ 左側做多
            level = "extreme"
        elif change_pct < -1.5:
            score = 0.4
            level = "strong"
        elif change_pct < -0.5:
            score = 0.1
            level = "moderate"
        elif change_pct > 3.0:
            score = -0.6    # 融資大增（散戶追高）→ 左側做空
            level = "strong"
        elif change_pct > 1.5:
            score = -0.3
            level = "moderate"

        detail = f"融資 {change_pct:+.2f}%"
        if has_data:
            detail += f" 今日{snap.margin.margin_balance:,.0f}"
        if change_pct < -1.5:
            detail += " 斷頭"

        return FactorResult(
            name="margin",
            name_zh="融資",
            score=score,
            weight=self.WEIGHTS["margin"],
            confidence=confidence,
            detail=detail,
            level=level,
        )

    def _score_large_trader(self, snap: IntelligenceSnapshot) -> FactorResult:
        """
        因子 6：大額交易人集中度（8%）
        邏輯：前十大交易人極度偏多/偏空 → 可能反轉
        """
        if not snap.large_trader.date:
            return FactorResult(
                name="large_trader",
                name_zh="大額交易人",
                score=0.0,
                weight=self.WEIGHTS["large_trader"],
                confidence=0.0,
                detail="資料未更新: TAIFEX 大額交易人 TX 所有契約",
                level="neutral",
                status="STUB",
            )

        net = snap.large_trader.top10_net
        confidence = 0.7

        score = 0.0
        level = "neutral"

        # 大額交易人通常是對的，但「極端」時可能過度
        if net < -20000:
            score = 0.5     # 大戶極度偏空 → 可能到底
            level = "strong"
        elif net < -10000:
            score = 0.2
            level = "moderate"
        elif net > 20000:
            score = -0.5    # 大戶極度偏多 → 可能到頂
            level = "strong"
        elif net > 10000:
            score = -0.2
            level = "moderate"

        detail = f"前十大淨 {net:+,d} 口"

        return FactorResult(
            name="large_trader",
            name_zh="大額交易人",
            score=score,
            weight=self.WEIGHTS["large_trader"],
            confidence=confidence,
            detail=detail,
            level=level,
            status="LIVE_NEW",
        )

    def _score_us_market(self, snap: IntelligenceSnapshot) -> FactorResult:
        """
        因子 7：美股連動（8%）
        邏輯：美股暴跌 → 台股隔日低開 → 可能超跌反彈
        """
        sp_change = snap.international.sp500_change_pct
        nq_change = snap.international.nasdaq_change_pct
        avg_change = (sp_change + nq_change) / 2 if sp_change != 0 or nq_change != 0 else 0

        confidence = 0.8 if avg_change != 0 else 0.0

        score = 0.0
        level = "neutral"

        # 逆向邏輯：暴跌 → 做多
        if avg_change < -3.0:
            score = 0.8     # 美股暴跌 → 隔日可能反彈
            level = "extreme"
        elif avg_change < -1.5:
            score = 0.4
            level = "strong"
        elif avg_change < -0.8:
            score = 0.1
            level = "moderate"
        elif avg_change > 3.0:
            score = -0.6    # 美股暴漲 → 隔日可能回檔
            level = "strong"
        elif avg_change > 1.5:
            score = -0.2
            level = "moderate"

        detail = f"SP500 {sp_change:+.1f}% NQ {nq_change:+.1f}%"

        return FactorResult(
            name="us_market",
            name_zh="美股",
            score=score,
            weight=self.WEIGHTS["us_market"],
            confidence=confidence,
            detail=detail,
            level=level,
        )

    def _score_sox(self, snap: IntelligenceSnapshot) -> FactorResult:
        """
        因子 8：費城半導體（8%）
        邏輯：SOX 暴跌 → 台積電/台股連動 → 可能超跌
        """
        sox_change = snap.international.sox_change_pct
        confidence = 0.7 if sox_change != 0 else 0.0

        score = 0.0
        level = "neutral"

        if sox_change < -4.0:
            score = 0.9
            level = "extreme"
        elif sox_change < -2.0:
            score = 0.4
            level = "strong"
        elif sox_change < -1.0:
            score = 0.1
            level = "moderate"
        elif sox_change > 4.0:
            score = -0.5
            level = "strong"
        elif sox_change > 2.0:
            score = -0.2
            level = "moderate"

        detail = f"SOX {sox_change:+.1f}%"

        return FactorResult(
            name="sox",
            name_zh="費半",
            score=score,
            weight=self.WEIGHTS["sox"],
            confidence=confidence,
            detail=detail,
            level=level,
        )

    def _score_trust(self, snap: IntelligenceSnapshot) -> FactorResult:
        """
        因子 9：投信動向（7%）
        邏輯：本版只使用投信現貨買賣超，不把未接的投信期貨 OI 當真資料。
        """
        if snap.institutional_spot.status != "LIVE" or not snap.institutional_spot.date:
            status = snap.institutional_spot.status or "NO_DATA"
            is_cache = status == "STALE_DISPLAY"
            detail = "BFI82U 快取顯示，不參與評分" if is_cache else "BFI82U 未更新"
            if snap.institutional_spot.date:
                detail += f"；最後資料 {snap.institutional_spot.date}"
            if snap.institutional_spot.error:
                detail += f"；{snap.institutional_spot.error}"
            return FactorResult(
                name="trust",
                name_zh="投信",
                score=0.0,
                weight=self.WEIGHTS["trust"],
                confidence=0.0,
                detail=detail,
                level="neutral",
                status=status,
                scope="快取顯示，不參與評分" if is_cache else "BFI82U 未更新",
            )

        trust_spot = snap.institutional_spot.trust_buy_sell
        confidence = 0.5

        score = 0.0
        level = "neutral"

        # 左側邏輯：投信現貨大賣視為偏恐慌，大買視為偏擁擠。
        if trust_spot < -50:
            score = 0.4
            level = "strong"
        elif trust_spot < -20:
            score = 0.2
            level = "moderate"
        elif trust_spot > 50:
            score = -0.4
            level = "strong"
        elif trust_spot > 20:
            score = -0.2
            level = "moderate"

        detail = f"投信現貨 {trust_spot:+.1f}億"

        return FactorResult(
            name="trust",
            name_zh="投信",
            score=score,
            weight=self.WEIGHTS["trust"],
            confidence=confidence,
            detail=detail,
            level=level,
        )
