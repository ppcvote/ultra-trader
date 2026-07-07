"""
UltraTrader Intelligence — 資料收集器
從 TAIFEX（期交所 CSV 下載）、TWSE（JSON API）、yfinance 抓取資料

TAIFEX OpenAPI 端點只是 Swagger UI 文件頁面，不提供 JSON API。
實際資料從 www.taifex.com.tw 的 CSV 下載端點抓取。
"""

import copy
import csv
import io
import json
import threading
import time
from datetime import datetime, date, timedelta
from html.parser import HTMLParser
from pathlib import Path
from typing import Optional, Callable

import requests
from loguru import logger


from intelligence.models import (
    InstitutionalFutures,
    InstitutionalSpot,
    OptionsData,
    LargeTraderOI,
    MarginData,
    InternationalData,
    IntelligenceSnapshot,
)


# 請求標頭（模擬瀏覽器）
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}

# TWSE 端點（JSON API，穩定可用）
TWSE_BASE = "https://www.twse.com.tw/rwd/zh"
TWSE_OPENAPI_BASE = "https://openapi.twse.com.tw/v1"
TWSE_BFI82U_PAGE = "https://www.twse.com.tw/zh/trading/foreign/bfi82u.html"
BFI82U_CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "intelligence_cache"


class _TwseTableParser(HTMLParser):
    """Extract table rows from TWSE report HTML without adding parser dependencies."""

    def __init__(self):
        super().__init__()
        self.rows = []
        self._row = []
        self._cell = []
        self._in_cell = False

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self._row = []
        elif tag in ("td", "th"):
            self._cell = []
            self._in_cell = True

    def handle_data(self, data):
        if self._in_cell:
            text = data.strip()
            if text:
                self._cell.append(text)

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._in_cell:
            self._row.append(" ".join(self._cell).strip())
            self._cell = []
            self._in_cell = False
        elif tag == "tr":
            if self._row:
                self.rows.append(self._row)


class DataCollector:
    """
    資料收集器 — 定時抓取各數據源

    排程邏輯：
    - TAIFEX P/C Ratio：盤後更新，每日抓一次
    - TWSE 外資/投信現貨買賣超、全市場融資餘額：盤後 JSON API
    - 國際市場 yfinance：每 5 分鐘更新
    """

    def __init__(self):
        self._snapshot = IntelligenceSnapshot(timestamp=datetime.now())
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # 各資料源最後更新時間
        self._last_fetch = {
            "taifex": None,
            "twse_spot": None,
            "twse_margin": None,
            "international": None,
        }

        # 更新間隔（秒）
        self.TAIFEX_INTERVAL = 3600       # TAIFEX: 1 小時
        self.TWSE_SPOT_INTERVAL = 3600    # TWSE BFI82U 成功後: 1 小時
        self.TWSE_SPOT_RETRY_INTERVAL = 300  # TWSE BFI82U 失敗後: 5 分鐘
        self.TWSE_MARGIN_INTERVAL = 3600  # TWSE MI_MARGN: 1 小時
        self.INTL_INTERVAL = 300          # 國際市場: 5 分鐘

        # 回調
        self._on_update: Optional[Callable] = None

    @property
    def snapshot(self) -> IntelligenceSnapshot:
        with self._lock:
            return copy.deepcopy(self._snapshot)

    def set_on_update(self, callback: Callable):
        """設定資料更新時的回調"""
        self._on_update = callback

    def start(self):
        """啟動背景資料收集"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._collection_loop, daemon=True)
        self._thread.start()
        logger.info("[Intelligence] data collector started")

    def stop(self):
        """停止資料收集"""
        self._running = False
        logger.info("[Intelligence] data collector stopped")

    def fetch_all(self):
        """手動觸發一次完整資料更新"""
        logger.info("[Intelligence] full data refresh...")
        self._safe_fetch("taifex", self._fetch_taifex_data)
        self._safe_fetch("twse_margin", self._fetch_margin_data)
        self._safe_fetch("twse_spot", self._fetch_foreign_spot)
        self._safe_fetch("international", self._fetch_international_data)
        self._update_timestamp()
        logger.info("[Intelligence] data refresh complete")

    # ============================================================
    # 背景排程
    # ============================================================

    def _collection_loop(self):
        """背景資料收集迴圈"""
        # 首次啟動：立即抓取（國際市場最快，其他延遲）
        self._safe_fetch("international", self._fetch_international_data)
        time.sleep(3)
        self._safe_fetch("taifex", self._fetch_taifex_data)
        time.sleep(2)
        self._safe_fetch("twse_margin", self._fetch_margin_data)
        self._safe_fetch("twse_spot", self._fetch_foreign_spot)
        self._update_timestamp()

        while self._running:
            try:
                now = datetime.now()

                # 國際市場：每 5 分鐘
                if self._should_fetch("international", self.INTL_INTERVAL):
                    self._safe_fetch("international", self._fetch_international_data)

                # TAIFEX：每小時（盤後 15:00 後才有資料）
                if self._should_fetch("taifex", self.TAIFEX_INTERVAL):
                    self._safe_fetch("taifex", self._fetch_taifex_data)

                # TWSE MI_MARGN：每小時。先抓融資，避免 BFI82U 卡住時拖累 Factor 5。
                if self._should_fetch("twse_margin", self.TWSE_MARGIN_INTERVAL):
                    self._safe_fetch("twse_margin", self._fetch_margin_data)

                # TWSE BFI82U：成功後每小時；失敗時每 5 分鐘重試
                spot_interval = (
                    self.TWSE_SPOT_INTERVAL
                    if self.snapshot.institutional_spot.status == "LIVE"
                    else self.TWSE_SPOT_RETRY_INTERVAL
                )
                if self._should_fetch("twse_spot", spot_interval):
                    self._safe_fetch("twse_spot", self._fetch_foreign_spot)

                self._update_timestamp()
                time.sleep(30)

            except Exception as e:
                logger.error(f"[Intelligence] collection loop error: {e}")
                time.sleep(60)

    def _should_fetch(self, source: str, interval: int) -> bool:
        last = self._last_fetch.get(source)
        if last is None:
            return True
        return (datetime.now() - last).total_seconds() >= interval

    def _safe_fetch(self, source: str, fetch_fn: Callable):
        try:
            fetch_fn()
            self._last_fetch[source] = datetime.now()
        except Exception as e:
            logger.warning(f"[Intelligence] {source} fetch failed: {e}")

    def _update_timestamp(self):
        with self._lock:
            self._snapshot.timestamp = datetime.now()
            freshness = {
                k: v.isoformat() if v else None
                for k, v in self._last_fetch.items()
                if k in ("taifex", "international")
            }
            freshness["twse_spot_attempt"] = (
                self._last_fetch["twse_spot"].isoformat()
                if self._last_fetch.get("twse_spot")
                else None
            )
            freshness["twse_margin_attempt"] = (
                self._last_fetch["twse_margin"].isoformat()
                if self._last_fetch.get("twse_margin")
                else None
            )
            if self._snapshot.institutional_spot.fetched_at:
                freshness["twse_spot"] = self._snapshot.institutional_spot.fetched_at.isoformat()
            else:
                freshness["twse_spot"] = None
            freshness["twse_spot_date"] = (
                self._snapshot.institutional_spot.date.isoformat()
                if self._snapshot.institutional_spot.date
                else None
            )
            freshness["twse_spot_status"] = self._snapshot.institutional_spot.status
            if self._snapshot.margin.fetched_at:
                freshness["twse_margin"] = self._snapshot.margin.fetched_at.isoformat()
            else:
                freshness["twse_margin"] = None
            freshness["twse"] = freshness.get("twse_spot") or freshness.get("twse_margin")
            self._snapshot.data_freshness = freshness
        if self._on_update:
            try:
                self._on_update(self._snapshot)
            except Exception:
                pass

    # ============================================================
    # TAIFEX 資料（CSV 下載端點）
    # ============================================================

    def _fetch_taifex_data(self):
        """從 TAIFEX 抓取 P/C Ratio、三大法人期貨、大額交易人（CSV 格式）"""
        self._fetch_put_call_ratio()
        self._fetch_institutional_futures()
        self._fetch_large_trader_oi()

    @staticmethod
    def _decode_taifex_csv(content: bytes) -> str:
        return content.decode("ms950", errors="replace").replace("\ufeff", "")

    @classmethod
    def _csv_rows(cls, content: str) -> list[dict]:
        reader = csv.DictReader(io.StringIO(content))
        rows = []
        for row in reader:
            cleaned = {}
            for key, value in row.items():
                if key is None:
                    continue
                cleaned[str(key).strip().replace("\ufeff", "")] = str(value or "").strip()
            rows.append(cleaned)
        return rows

    def _fetch_put_call_ratio(self):
        """
        P/C Ratio — 從 TAIFEX CSV 下載端點
        POST https://www.taifex.com.tw/cht/3/pcRatioDown
        回傳 MS950 編碼的 CSV
        """
        try:
            # 嘗試今天和前一個交易日
            for days_back in range(0, 5):
                query_date = date.today() - timedelta(days=days_back)
                date_str = query_date.strftime("%Y/%m/%d")

                resp = requests.post(
                    "https://www.taifex.com.tw/cht/3/pcRatioDown",
                    data={"queryDate": date_str, "queryType": "1"},
                    headers=HEADERS,
                    timeout=15,
                )
                resp.raise_for_status()

                # 解碼 MS950 CSV
                content = self._decode_taifex_csv(resp.content)
                lines = content.strip().split("\n")

                # 跳過標頭行，取資料行
                if len(lines) < 2:
                    continue

                # 找到有效數據行
                data_line = None
                for line in lines[1:]:
                    parts = line.strip().split(",")
                    if len(parts) >= 7 and "/" in parts[0]:
                        data_line = parts
                        break

                if not data_line:
                    continue

                opts = OptionsData(date=query_date)
                try:
                    # CSV 格式: 日期,Put成交量,Call成交量,成交量比%,Put未平倉,Call未平倉,未平倉比%
                    put_vol = int(data_line[1].strip())
                    call_vol = int(data_line[2].strip())
                    pc_vol_pct = float(data_line[3].strip())  # 已經是百分比

                    put_oi = int(data_line[4].strip())
                    call_oi = int(data_line[5].strip())
                    pc_oi_pct = float(data_line[6].strip())   # 已經是百分比

                    # 轉換為比率（百分比 / 100）
                    opts.pc_ratio_volume = pc_vol_pct / 100.0
                    opts.pc_ratio_oi = pc_oi_pct / 100.0

                except (ValueError, IndexError):
                    continue

                with self._lock:
                    self._snapshot.options = opts

                logger.info(
                    f"[TAIFEX] P/C Ratio ({query_date}): "
                    f"volume={opts.pc_ratio_volume:.2f} "
                    f"OI={opts.pc_ratio_oi:.2f} "
                    f"signal={opts.pc_signal}"
                )
                return  # 成功拿到資料，跳出

            logger.debug("[TAIFEX] P/C Ratio: no data in recent 5 days")

        except requests.RequestException as e:
            logger.warning(f"[TAIFEX] P/C Ratio request failed: {e}")
        except Exception as e:
            logger.warning(f"[TAIFEX] P/C Ratio parse error: {e}")

    @classmethod
    def _parse_institutional_futures_csv(cls, content: str, query_date: date = None) -> Optional[InstitutionalFutures]:
        rows = cls._csv_rows(content)
        if not rows:
            return None

        futures = InstitutionalFutures(date=query_date or date.today())
        matched = False

        def apply_row(prefix: str, row: dict):
            setattr(futures, f"{prefix}_long", int(cls._parse_number(row.get("多方未平倉口數"))))
            setattr(futures, f"{prefix}_short", int(cls._parse_number(row.get("空方未平倉口數"))))
            setattr(futures, f"{prefix}_net", int(cls._parse_number(row.get("多空交易口數淨額"))))
            setattr(futures, f"{prefix}_oi_net", int(cls._parse_number(row.get("多空未平倉口數淨額"))))

        for row in rows:
            product = row.get("商品名稱", "").strip()
            identity = row.get("身份別", "").strip()
            if product != "臺股期貨":
                continue
            if identity == "外資及陸資":
                apply_row("foreign", row)
                matched = True
            elif identity == "投信":
                apply_row("trust", row)
            elif identity == "自營商":
                apply_row("dealer", row)

        if not matched:
            return None

        futures.total_oi_net = futures.foreign_oi_net + futures.trust_oi_net + futures.dealer_oi_net
        return futures

    def _fetch_institutional_futures(self):
        """
        三大法人區分各期貨契約 — 大台 TXF / 臺股期貨
        POST https://www.taifex.com.tw/cht/3/futContractsDateDown
        """
        try:
            for days_back in range(0, 5):
                query_date = date.today() - timedelta(days=days_back)
                date_str = query_date.strftime("%Y/%m/%d")

                resp = requests.post(
                    "https://www.taifex.com.tw/cht/3/futContractsDateDown",
                    data={
                        "queryStartDate": date_str,
                        "queryEndDate": date_str,
                        "commodityId": "TXF",
                    },
                    headers=HEADERS,
                    timeout=20,
                )
                resp.raise_for_status()

                futures = self._parse_institutional_futures_csv(
                    self._decode_taifex_csv(resp.content),
                    query_date,
                )
                if not futures:
                    continue

                with self._lock:
                    self._snapshot.institutional_futures = futures

                logger.info(
                    f"[TAIFEX] institutional futures ({query_date}): "
                    f"foreign_oi={futures.foreign_oi_net:+,d} "
                    f"trust_oi={futures.trust_oi_net:+,d} "
                    f"dealer_oi={futures.dealer_oi_net:+,d}"
                )
                return

            logger.debug("[TAIFEX] institutional futures: no TXF data in recent 5 days")

        except requests.RequestException as e:
            logger.warning(f"[TAIFEX] institutional futures request failed: {e}")
        except Exception as e:
            logger.warning(f"[TAIFEX] institutional futures parse error: {e}")

    @classmethod
    def _parse_large_trader_csv(cls, content: str, query_date: date = None) -> Optional[LargeTraderOI]:
        rows = cls._csv_rows(content)
        if not rows:
            return None

        for row in rows:
            product = row.get("商品(契約)", "").strip()
            expiry = row.get("到期月份(週別)", "").strip()
            trader_type = row.get("交易人類別", "").strip()
            if product != "TX" or expiry != "999999" or trader_type != "0":
                continue

            top5_long = int(cls._parse_number(row.get("前五大交易人買方")))
            top5_short = int(cls._parse_number(row.get("前五大交易人賣方")))
            top10_long = int(cls._parse_number(row.get("前十大交易人買方")))
            top10_short = int(cls._parse_number(row.get("前十大交易人賣方")))
            total_oi = int(cls._parse_number(row.get("全市場未沖銷部位數")))

            return LargeTraderOI(
                date=query_date or date.today(),
                top5_long=top5_long,
                top5_short=top5_short,
                top5_net=top5_long - top5_short,
                top10_long=top10_long,
                top10_short=top10_short,
                top10_net=top10_long - top10_short,
                total_oi=total_oi,
                concentration_ratio=(top10_long / total_oi * 100.0) if total_oi else 0.0,
            )

        return None

    def _fetch_large_trader_oi(self):
        """
        大額交易人未沖銷部位 — TX 所有契約大台等值總持倉
        POST https://www.taifex.com.tw/cht/3/dlLargeTraderFutDown
        """
        try:
            for days_back in range(0, 5):
                query_date = date.today() - timedelta(days=days_back)
                date_str = query_date.strftime("%Y/%m/%d")

                resp = requests.post(
                    "https://www.taifex.com.tw/cht/3/dlLargeTraderFutDown",
                    data={
                        "queryStartDate": date_str,
                        "queryEndDate": date_str,
                    },
                    headers=HEADERS,
                    timeout=20,
                )
                resp.raise_for_status()

                large = self._parse_large_trader_csv(
                    self._decode_taifex_csv(resp.content),
                    query_date,
                )
                if not large:
                    continue

                with self._lock:
                    self._snapshot.large_trader = large

                logger.info(
                    f"[TAIFEX] large trader ({query_date}): "
                    f"top5={large.top5_net:+,d} "
                    f"top10={large.top10_net:+,d} "
                    f"total_oi={large.total_oi:,d}"
                )
                return

            logger.debug("[TAIFEX] large trader: no TX 999999 data in recent 5 days")

        except requests.RequestException as e:
            logger.warning(f"[TAIFEX] large trader request failed: {e}")
        except Exception as e:
            logger.warning(f"[TAIFEX] large trader parse error: {e}")

    # ============================================================
    # TWSE 資料（JSON API）
    # ============================================================

    def _fetch_twse_data(self):
        """從 TWSE 抓取三大法人現貨買賣超與全市場融資餘額"""
        self._fetch_foreign_spot()
        self._fetch_margin_data()

    @staticmethod
    def _parse_number(value) -> float:
        """解析 TWSE/TAIFEX 常見數字欄位。"""
        if value in (None, "", "--", "-", " "):
            return 0.0
        try:
            return float(str(value).replace(",", "").strip())
        except (ValueError, TypeError):
            return 0.0

    def _fetch_foreign_spot(self):
        """
        三大法人現貨買賣超
        GET https://www.twse.com.tw/fund/BFI82U?response=json&type=day&dayDate=YYYYMMDD
        """
        last_error = ""
        deadline = time.monotonic() + 60
        session = requests.Session()

        try:
            warmup = session.get(
                TWSE_BFI82U_PAGE,
                headers=HEADERS,
                timeout=(5, 20),
            )
            warmup.raise_for_status()
        except requests.RequestException as e:
            last_error = f"TWSE BFI82U official page warmup failed: {e}"
            logger.debug(f"[TWSE] spot warmup failed: {last_error}")

        for days_back in range(0, 5):
            query_date = date.today() - timedelta(days=days_back)
            for source_name, url, response_type in self._bfi82u_urls(query_date):
                if time.monotonic() >= deadline:
                    self._mark_spot_failed_or_cache(last_error or "BFI82U retry budget exceeded")
                    return
                try:
                    remaining = max(1, int(deadline - time.monotonic()))
                    resp = session.get(
                        url,
                        headers=self._bfi82u_headers(),
                        timeout=(5, min(20, remaining)),
                    )
                    resp.raise_for_status()

                    text = self._decode_twse_text(resp.content)
                    if self._is_bfi82u_maintenance(text):
                        last_error = f"{source_name} ({query_date}) returned TWSE maintenance page"
                        logger.debug(f"[TWSE] spot endpoint failed: {last_error}")
                        continue

                    if response_type == "json":
                        spot = self._parse_bfi82u_json(resp.json(), query_date)
                    elif response_type == "csv":
                        spot = self._parse_bfi82u_csv(text, query_date)
                    else:
                        spot = self._parse_bfi82u_html(text, query_date)
                    if not spot:
                        last_error = f"{source_name} ({query_date}) no usable BFI82U rows"
                        logger.debug(f"[TWSE] spot skipped: {last_error}")
                        continue

                    spot.status = "LIVE"
                    spot.error = ""
                    spot.source = source_name
                    spot.fetched_at = datetime.now()

                    with self._lock:
                        self._snapshot.institutional_spot = spot

                    self._save_bfi82u_cache(spot)

                    logger.info(
                        f"[TWSE] spot ({query_date}, {source_name}): "
                        f"foreign={spot.foreign_buy_sell:+.1f}億 "
                        f"trust={spot.trust_buy_sell:+.1f}億 "
                        f"dealer={spot.dealer_buy_sell:+.1f}億"
                    )
                    return

                except requests.RequestException as e:
                    last_error = f"{source_name} ({query_date}) request failed: {e}"
                    logger.debug(f"[TWSE] spot endpoint failed: {last_error}")
                    continue
                except ValueError as e:
                    last_error = f"{source_name} ({query_date}) non-json response: {e}"
                    logger.debug(f"[TWSE] spot endpoint failed: {last_error}")
                    continue
                except Exception as e:
                    last_error = f"{source_name} ({query_date}) parse failed: {e}"
                    logger.debug(f"[TWSE] spot endpoint failed: {last_error}")
                    continue

        self._mark_spot_failed_or_cache(last_error or "BFI82U no usable data in recent 5 days")

    @staticmethod
    def _bfi82u_headers():
        headers = dict(HEADERS)
        headers.update({
            "Accept": "application/json,text/csv,text/html,*/*",
            "Referer": TWSE_BFI82U_PAGE,
        })
        return headers

    @staticmethod
    def _bfi82u_urls(query_date: date):
        date_str = query_date.strftime("%Y%m%d")
        nonce = int(time.time() * 1000)
        return [
            (
                "TWSE BFI82U JSON",
                f"https://www.twse.com.tw/fund/BFI82U?response=json&type=day&dayDate={date_str}&_={nonce}",
                "json",
            ),
            (
                "TWSE BFI82U CSV",
                f"https://www.twse.com.tw/fund/BFI82U?response=csv&type=day&dayDate={date_str}&_={nonce}",
                "csv",
            ),
            (
                "TWSE BFI82U HTML",
                f"https://www.twse.com.tw/fund/BFI82U?response=html&type=day&dayDate={date_str}&_={nonce}",
                "html",
            ),
            (
                "TWSE RWD BFI82U JSON",
                f"{TWSE_BASE}/fund/BFI82U?response=json&type=day&dayDate={date_str}&_={nonce}",
                "json",
            ),
            (
                "TWSE RWD BFI82U CSV",
                f"{TWSE_BASE}/fund/BFI82U?response=csv&type=day&dayDate={date_str}&_={nonce}",
                "csv",
            ),
            (
                "TWSE RWD BFI82U HTML",
                f"{TWSE_BASE}/fund/BFI82U?response=html&type=day&dayDate={date_str}&_={nonce}",
                "html",
            ),
            (
                "TWSE RWD BFI82U date JSON",
                f"{TWSE_BASE}/fund/BFI82U?date={date_str}&response=json&_={nonce}",
                "json",
            ),
        ]

    @staticmethod
    def _decode_twse_text(content: bytes) -> str:
        for encoding in ("utf-8-sig", "utf-8", "ms950", "big5"):
            try:
                return content.decode(encoding)
            except UnicodeDecodeError:
                continue
        return content.decode("utf-8", errors="replace")

    @staticmethod
    def _is_bfi82u_maintenance(text: str) -> bool:
        if not text:
            return False
        markers = ("網站維護中", "系統維護", "暫停服務", "The service is temporarily unavailable")
        return any(marker in text for marker in markers)

    @classmethod
    def _parse_bfi82u_json(cls, payload, query_date: date = None) -> Optional[InstitutionalSpot]:
        if not isinstance(payload, dict):
            return None
        if payload.get("stat") != "OK" or not isinstance(payload.get("data"), list):
            return None
        return cls._parse_bfi82u_rows(payload["data"], query_date)

    @classmethod
    def _parse_bfi82u_csv(cls, text: str, query_date: date = None) -> Optional[InstitutionalSpot]:
        if not text or cls._is_bfi82u_maintenance(text):
            return None
        if "<table" in text.lower() or "<tr" in text.lower():
            return None
        rows = []
        for row in csv.reader(io.StringIO(text)):
            cleaned = [str(cell or "").strip() for cell in row]
            if cleaned:
                rows.append(cleaned)
        return cls._parse_bfi82u_rows(rows, query_date)

    @classmethod
    def _parse_bfi82u_html(cls, html: str, query_date: date = None) -> Optional[InstitutionalSpot]:
        if not html or cls._is_bfi82u_maintenance(html):
            return None
        parser = _TwseTableParser()
        parser.feed(html)
        return cls._parse_bfi82u_rows(parser.rows, query_date)

    @classmethod
    def _parse_bfi82u_rows(cls, rows, query_date: date = None) -> Optional[InstitutionalSpot]:
        """
        Parse BFI82U rows by institution name.

        Official rows are "單位名稱, 買進金額, 賣出金額, 買賣差額".
        Values are NTD and converted to 億元.
        """
        if not rows:
            return None

        spot = InstitutionalSpot(date=query_date)
        found_foreign = False
        found_trust = False
        found_dealer = False
        found_total = False
        dealer_net = 0.0

        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) < 4:
                continue
            name = str(row[0]).strip()
            net = cls._parse_number(row[3])

            if "外資及陸資" in name:
                spot.foreign_buy_sell = round(net / 1e8, 2)
                found_foreign = True
            elif "外資自營商" in name:
                spot.foreign_dealer_buy_sell = round(net / 1e8, 2)
            elif "投信" in name:
                spot.trust_buy_sell = round(net / 1e8, 2)
                found_trust = True
            elif "自營商" in name:
                dealer_net += net
                found_dealer = True
            elif "合計" in name:
                spot.total_buy_sell = round(net / 1e8, 2)
                found_total = True

        if not found_foreign and not found_trust:
            return None

        if found_dealer:
            spot.dealer_buy_sell = round(dealer_net / 1e8, 2)
        if not found_total:
            spot.total_buy_sell = round(
                spot.foreign_buy_sell + spot.trust_buy_sell + spot.dealer_buy_sell,
                2,
            )
        return spot

    @classmethod
    def _bfi82u_cache_dir(cls) -> Path:
        return BFI82U_CACHE_DIR

    @classmethod
    def _spot_to_cache_dict(cls, spot: InstitutionalSpot) -> dict:
        return {
            "date": spot.date.isoformat() if spot.date else None,
            "fetched_at": spot.fetched_at.isoformat() if spot.fetched_at else None,
            "status": "LIVE",
            "source": spot.source,
            "foreign_buy_sell": spot.foreign_buy_sell,
            "foreign_dealer_buy_sell": spot.foreign_dealer_buy_sell,
            "trust_buy_sell": spot.trust_buy_sell,
            "dealer_buy_sell": spot.dealer_buy_sell,
            "total_buy_sell": spot.total_buy_sell,
        }

    @classmethod
    def _spot_from_cache_dict(cls, payload: dict) -> Optional[InstitutionalSpot]:
        if not isinstance(payload, dict) or not payload.get("date"):
            return None
        try:
            spot_date = date.fromisoformat(str(payload["date"]))
        except ValueError:
            return None

        fetched_at = None
        if payload.get("fetched_at"):
            try:
                fetched_at = datetime.fromisoformat(str(payload["fetched_at"]))
            except ValueError:
                fetched_at = None

        return InstitutionalSpot(
            date=spot_date,
            fetched_at=fetched_at,
            status="STALE_DISPLAY",
            source=str(payload.get("source") or "TWSE BFI82U cache"),
            foreign_buy_sell=float(payload.get("foreign_buy_sell") or 0.0),
            foreign_dealer_buy_sell=float(payload.get("foreign_dealer_buy_sell") or 0.0),
            trust_buy_sell=float(payload.get("trust_buy_sell") or 0.0),
            dealer_buy_sell=float(payload.get("dealer_buy_sell") or 0.0),
            total_buy_sell=float(payload.get("total_buy_sell") or 0.0),
        )

    @classmethod
    def _save_bfi82u_cache(cls, spot: InstitutionalSpot):
        if not spot.date:
            return
        cache_dir = cls._bfi82u_cache_dir()
        cache_dir.mkdir(parents=True, exist_ok=True)
        payload = cls._spot_to_cache_dict(spot)
        date_path = cache_dir / f"bfi82u_{spot.date.isoformat()}.json"
        latest_path = cache_dir / "bfi82u_latest.json"
        encoded = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
        date_path.write_text(encoded, encoding="utf-8")
        latest_path.write_text(encoded, encoding="utf-8")

    @classmethod
    def _load_bfi82u_cache(cls) -> Optional[InstitutionalSpot]:
        cache_dir = cls._bfi82u_cache_dir()
        candidates = [cache_dir / "bfi82u_latest.json"]
        if cache_dir.exists():
            candidates.extend(sorted(cache_dir.glob("bfi82u_*.json"), reverse=True))
        seen = set()
        for path in candidates:
            if path in seen or not path.exists():
                continue
            seen.add(path)
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            spot = cls._spot_from_cache_dict(payload)
            if spot:
                return spot
        return None

    def _mark_spot_failed_or_cache(self, error: str):
        cached = self._load_bfi82u_cache()
        if cached:
            cached.error = error
            with self._lock:
                self._snapshot.institutional_spot = cached
            logger.warning(
                f"[TWSE] spot BFI82U warning: {error}; "
                f"using display-only cache from {cached.date}"
            )
            return
        self._mark_spot_failed(error)

    def _mark_spot_failed(self, error: str):
        with self._lock:
            spot = self._snapshot.institutional_spot
            spot.status = "STALE_DISPLAY" if spot.date else "NO_DATA"
            spot.error = error
        logger.warning(f"[TWSE] spot BFI82U warning: {error}")

    @classmethod
    def _parse_margin_payload(cls, payload, query_date: date = None) -> Optional[MarginData]:
        """
        解析 TWSE MI_MARGN 全市場融資融券餘額。

        OpenAPI 回傳為每檔股票一列，這裡依使用者指定口徑做全市場加總。
        """
        rows = []
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict):
            if isinstance(payload.get("data"), list):
                rows = payload["data"]
            elif isinstance(payload.get("tables"), list):
                for table in payload["tables"]:
                    if isinstance(table, dict) and isinstance(table.get("data"), list):
                        rows.extend(table["data"])

        if not rows:
            return None

        margin_prev = 0.0
        margin_today = 0.0
        short_prev = 0.0
        short_today = 0.0

        for row in rows:
            if not isinstance(row, dict):
                continue
            margin_prev += cls._parse_number(row.get("融資前日餘額"))
            margin_today += cls._parse_number(row.get("融資今日餘額"))
            short_prev += cls._parse_number(row.get("融券前日餘額"))
            short_today += cls._parse_number(row.get("融券今日餘額"))

        if margin_prev == 0 and margin_today == 0 and short_prev == 0 and short_today == 0:
            return None

        margin_change = margin_today - margin_prev
        margin_change_pct = (margin_change / margin_prev * 100.0) if margin_prev else 0.0

        return MarginData(
            date=query_date or date.today(),
            margin_balance=margin_today,
            margin_previous_balance=margin_prev,
            margin_change=margin_change,
            margin_change_pct=margin_change_pct,
            short_balance=short_today,
            short_previous_balance=short_prev,
            short_change=short_today - short_prev,
        )

    def _fetch_margin_data(self):
        """
        全市場融資餘額 — 從 TWSE OpenAPI MI_MARGN 取得每檔資料後加總
        GET https://openapi.twse.com.tw/v1/exchangeReport/MI_MARGN
        """
        try:
            resp = requests.get(
                f"{TWSE_OPENAPI_BASE}/exchangeReport/MI_MARGN",
                headers=HEADERS,
                timeout=20,
            )
            resp.raise_for_status()
            margin = self._parse_margin_payload(resp.json(), date.today())

            if not margin:
                logger.debug("[TWSE] margin: no usable data")
                self._mark_margin_failed("MI_MARGN no usable data")
                return

            margin.status = "LIVE"
            margin.error = ""
            margin.fetched_at = datetime.now()

            with self._lock:
                self._snapshot.margin = margin

            logger.info(
                f"[TWSE] margin: today={margin.margin_balance:,.0f} lots, "
                f"prev={margin.margin_previous_balance:,.0f} lots, "
                f"Δ={margin.margin_change_pct:+.2f}%"
            )

        except requests.RequestException as e:
            logger.warning(f"[TWSE] margin request failed: {e}")
            self._mark_margin_failed(str(e))
        except Exception as e:
            logger.warning(f"[TWSE] margin parse error: {e}")
            self._mark_margin_failed(str(e))

    def _mark_margin_failed(self, error: str):
        with self._lock:
            margin = self._snapshot.margin
            margin.status = "STALE" if margin.date else "NO_DATA"
            margin.error = error

    # ============================================================
    # 國際市場（yfinance）
    # ============================================================

    def _fetch_international_data(self):
        """從 yfinance 抓取 VIX、美股、費半等國際市場指標"""
        try:
            import yfinance as yf

            intl = InternationalData(timestamp=datetime.now())

            tickers = {
                "^VIX": "vix",
                "ES=F": "sp500",
                "NQ=F": "nasdaq",
                "^SOX": "sox",
                "CL=F": "crude",
                "DX-Y.NYB": "dxy",
                "^TNX": "us10y",
            }

            for symbol, key in tickers.items():
                try:
                    ticker = yf.Ticker(symbol)
                    hist = ticker.history(period="2d")
                    if hist.empty or len(hist) < 1:
                        continue

                    current = float(hist["Close"].iloc[-1])
                    prev = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else current
                    change_pct = ((current - prev) / prev * 100) if prev != 0 else 0

                    if key == "vix":
                        intl.vix = current
                        intl.vix_change = round(change_pct, 2)
                    elif key == "sp500":
                        intl.sp500_futures = current
                        intl.sp500_change_pct = round(change_pct, 2)
                    elif key == "nasdaq":
                        intl.nasdaq_futures = current
                        intl.nasdaq_change_pct = round(change_pct, 2)
                    elif key == "sox":
                        intl.sox_index = current
                        intl.sox_change_pct = round(change_pct, 2)
                    elif key == "crude":
                        intl.crude_oil = current
                        intl.crude_change_pct = round(change_pct, 2)
                    elif key == "dxy":
                        intl.dxy = current
                        intl.dxy_change_pct = round(change_pct, 2)
                    elif key == "us10y":
                        intl.us10y_yield = current
                        intl.us10y_change = round(current - prev, 3)

                except Exception as e:
                    logger.debug(f"[yfinance] {symbol} failed: {e}")
                    continue

            with self._lock:
                self._snapshot.international = intl

            logger.info(
                f"[International] VIX={intl.vix:.1f} "
                f"SP500={intl.sp500_change_pct:+.1f}% "
                f"NQ={intl.nasdaq_change_pct:+.1f}% "
                f"SOX={intl.sox_change_pct:+.1f}%"
            )

        except ImportError:
            logger.warning("[Intelligence] yfinance not installed, skipping international data")
        except Exception as e:
            logger.warning(f"[Intelligence] international data fetch failed: {e}")
