"""
UltraTrader Intelligence tests.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
import unittest
from datetime import date
from tempfile import TemporaryDirectory
from unittest.mock import patch

from intelligence.data_collector import DataCollector
from intelligence.left_side_score import LeftSideScoreEngine
from intelligence.models import (
    IntelligenceSnapshot,
    InstitutionalFutures,
    InstitutionalSpot,
    LargeTraderOI,
    MarginData,
)


class FakeResponse:
    def __init__(self, json_data=None, content=b"", text="", json_error=None):
        self._json_data = json_data
        self.text = text
        self.content = content or text.encode("utf-8")
        self._json_error = json_error

    def raise_for_status(self):
        return None

    def json(self):
        if self._json_error:
            raise self._json_error
        return self._json_data


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self.responses:
            return FakeResponse(json_data={"stat": "FAIL"})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class TestDataCollectorParsers(unittest.TestCase):
    def test_put_call_ratio_csv_parser(self):
        collector = DataCollector()
        csv = (
            "日期,Put成交量,Call成交量,成交量比%,Put未平倉,Call未平倉,未平倉比%\n"
            "2026/06/02,100,50,200,300,200,150\n"
        ).encode("ms950")

        with patch("intelligence.data_collector.requests.post", return_value=FakeResponse(content=csv)):
            collector._fetch_put_call_ratio()

        snap = collector.snapshot
        self.assertEqual(snap.options.pc_ratio_volume, 2.0)
        self.assertEqual(snap.options.pc_ratio_oi, 1.5)

    def test_bfi82u_spot_parser_extracts_foreign_and_trust(self):
        collector = DataCollector()
        payload = {
            "stat": "OK",
            "data": [
                ["自營商(自行買賣)", "0", "0", "100,000,000"],
                ["自營商(避險)", "0", "0", "-50,000,000"],
                ["投信", "0", "0", "6,450,000,000"],
                ["外資及陸資", "0", "0", "36,800,000,000"],
                ["外資自營商", "0", "0", "0"],
                ["合計", "0", "0", "43,300,000,000"],
            ],
        }

        with TemporaryDirectory() as tmp:
            session = FakeSession([FakeResponse(text="<html></html>"), FakeResponse(json_data=payload)])
            with patch("intelligence.data_collector.requests.Session", return_value=session), \
                 patch("intelligence.data_collector.BFI82U_CACHE_DIR", Path(tmp)):
                collector._fetch_foreign_spot()

        spot = collector.snapshot.institutional_spot
        self.assertEqual(spot.status, "LIVE")
        self.assertEqual(spot.foreign_buy_sell, 368.0)
        self.assertEqual(spot.trust_buy_sell, 64.5)
        self.assertEqual(spot.dealer_buy_sell, 0.5)
        self.assertEqual(spot.foreign_dealer_buy_sell, 0.0)

    def test_bfi82u_csv_parser_extracts_official_rows(self):
        # Use quoted official-style values because CSV commas are part of numbers.
        csv_text = (
            "單位名稱,買進金額,賣出金額,買賣差額\n"
            "自營商(自行買賣),\"11,381,726,498\",\"8,430,164,554\",\"2,951,561,944\"\n"
            "自營商(避險),\"39,332,884,005\",\"39,687,826,199\",\"-354,942,194\"\n"
            "投信,\"67,194,235,881\",\"61,621,573,240\",\"5,572,662,641\"\n"
            "外資及陸資(不含外資自營商),\"540,725,172,582\",\"497,328,836,935\",\"43,396,335,647\"\n"
            "合計,\"658,634,018,966\",\"607,068,400,928\",\"51,565,618,038\"\n"
        )

        spot = DataCollector._parse_bfi82u_csv(csv_text, date(2026, 6, 3))

        self.assertIsNotNone(spot)
        self.assertEqual(spot.foreign_buy_sell, 433.96)
        self.assertEqual(spot.trust_buy_sell, 55.73)
        self.assertEqual(spot.dealer_buy_sell, 25.97)
        self.assertEqual(spot.total_buy_sell, 515.66)

    def test_bfi82u_html_parser_extracts_official_rows(self):
        html = """
        <table>
          <tr><th>單位名稱</th><th>買進金額</th><th>賣出金額</th><th>買賣差額</th></tr>
          <tr><td>自營商(自行買賣)</td><td>11,381,726,498</td><td>8,430,164,554</td><td>2,951,561,944</td></tr>
          <tr><td>自營商(避險)</td><td>39,332,884,005</td><td>39,687,826,199</td><td>-354,942,194</td></tr>
          <tr><td>投信</td><td>67,194,235,881</td><td>61,621,573,240</td><td>5,572,662,641</td></tr>
          <tr><td>外資及陸資(不含外資自營商)</td><td>540,725,172,582</td><td>497,328,836,935</td><td>43,396,335,647</td></tr>
          <tr><td>外資自營商</td><td>0</td><td>0</td><td>0</td></tr>
          <tr><td>合計</td><td>658,634,018,966</td><td>607,068,400,928</td><td>51,565,618,038</td></tr>
        </table>
        """

        spot = DataCollector._parse_bfi82u_html(html, date(2026, 6, 3))

        self.assertIsNotNone(spot)
        self.assertEqual(spot.foreign_buy_sell, 433.96)
        self.assertEqual(spot.trust_buy_sell, 55.73)
        self.assertEqual(spot.dealer_buy_sell, 25.97)
        self.assertEqual(spot.total_buy_sell, 515.66)

    def test_bfi82u_maintenance_page_is_not_usable_data(self):
        html = "<html><title>網站維護中 - 臺灣證券交易所</title><body>網站維護中</body></html>"

        self.assertIsNone(DataCollector._parse_bfi82u_html(html, date(2026, 6, 3)))
        self.assertIsNone(DataCollector._parse_bfi82u_csv(html, date(2026, 6, 3)))

    def test_bfi82u_fetch_continues_after_bad_endpoint(self):
        collector = DataCollector()
        html = """
        <table>
          <tr><td>投信</td><td>0</td><td>0</td><td>5,572,662,641</td></tr>
          <tr><td>外資及陸資(不含外資自營商)</td><td>0</td><td>0</td><td>43,396,335,647</td></tr>
        </table>
        """

        responses = [
            FakeResponse(text="<html></html>"),
            FakeResponse(json_error=ValueError("empty response")),
            FakeResponse(text=""),
            FakeResponse(text=html),
        ]

        with TemporaryDirectory() as tmp:
            session = FakeSession(responses)
            with patch("intelligence.data_collector.requests.Session", return_value=session), \
                 patch("intelligence.data_collector.BFI82U_CACHE_DIR", Path(tmp)):
                collector._fetch_foreign_spot()

        spot = collector.snapshot.institutional_spot
        self.assertEqual(spot.status, "LIVE")
        self.assertEqual(spot.foreign_buy_sell, 433.96)
        self.assertEqual(spot.trust_buy_sell, 55.73)

    def test_bfi82u_failure_marks_spot_no_data_without_fake_zero(self):
        collector = DataCollector()

        with TemporaryDirectory() as tmp:
            session = FakeSession([FakeResponse(text="<html></html>")])
            with patch("intelligence.data_collector.requests.Session", return_value=session), \
                 patch("intelligence.data_collector.BFI82U_CACHE_DIR", Path(tmp)):
                collector._fetch_foreign_spot()

        snap = collector.snapshot
        self.assertEqual(snap.institutional_spot.status, "NO_DATA")
        result = LeftSideScoreEngine().calculate(snap)
        factors = {f["name"]: f for f in result.factor_scores}
        self.assertEqual(factors["foreign_spot"]["status"], "NO_DATA")
        self.assertEqual(factors["foreign_spot"]["confidence"], 0.0)
        self.assertIn("BFI82U 未更新", factors["foreign_spot"]["detail"])
        self.assertNotIn("外資 +0.0億", factors["foreign_spot"]["detail"])
        self.assertEqual(factors["trust"]["status"], "NO_DATA")
        self.assertEqual(factors["trust"]["confidence"], 0.0)
        self.assertNotIn("投信現貨 +0.0億", factors["trust"]["detail"])

    def test_bfi82u_failure_uses_display_only_cache_without_scoring(self):
        collector = DataCollector()
        with TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            cache_dir.mkdir(parents=True, exist_ok=True)
            (cache_dir / "bfi82u_latest.json").write_text(
                json.dumps({
                    "date": "2026-06-03",
                    "fetched_at": "2026-06-03T15:10:00",
                    "status": "LIVE",
                    "source": "TWSE BFI82U JSON",
                    "foreign_buy_sell": 433.96,
                    "trust_buy_sell": 55.73,
                    "dealer_buy_sell": 25.97,
                    "total_buy_sell": 515.66,
                }),
                encoding="utf-8",
            )
            session = FakeSession([FakeResponse(text="<html></html>")])
            with patch("intelligence.data_collector.requests.Session", return_value=session), \
                 patch("intelligence.data_collector.BFI82U_CACHE_DIR", cache_dir):
                collector._fetch_foreign_spot()

        snap = collector.snapshot
        self.assertEqual(snap.institutional_spot.status, "STALE_DISPLAY")
        self.assertEqual(snap.institutional_spot.foreign_buy_sell, 433.96)
        self.assertEqual(snap.institutional_spot.trust_buy_sell, 55.73)

        result = LeftSideScoreEngine().calculate(snap)
        factors = {f["name"]: f for f in result.factor_scores}
        self.assertEqual(factors["foreign_spot"]["status"], "STALE_DISPLAY")
        self.assertEqual(factors["foreign_spot"]["confidence"], 0.0)
        self.assertEqual(factors["foreign_spot"]["weighted"], 0.0)
        self.assertIn("快取顯示", factors["foreign_spot"]["detail"])
        self.assertEqual(factors["trust"]["status"], "STALE_DISPLAY")
        self.assertEqual(factors["trust"]["confidence"], 0.0)

    def test_bfi82u_live_fetch_writes_cache(self):
        collector = DataCollector()
        payload = {
            "stat": "OK",
            "data": [
                ["投信", "0", "0", "5,572,662,641"],
                ["外資及陸資(不含外資自營商)", "0", "0", "43,396,335,647"],
            ],
        }

        with TemporaryDirectory() as tmp:
            session = FakeSession([FakeResponse(text="<html></html>"), FakeResponse(json_data=payload)])
            with patch("intelligence.data_collector.requests.Session", return_value=session), \
                 patch("intelligence.data_collector.BFI82U_CACHE_DIR", Path(tmp)):
                collector._fetch_foreign_spot()
                spot_date = collector.snapshot.institutional_spot.date.isoformat()
                latest = Path(tmp) / "bfi82u_latest.json"
                dated = Path(tmp) / f"bfi82u_{spot_date}.json"
                self.assertTrue(latest.exists())
                self.assertTrue(dated.exists())
                cached = json.loads(latest.read_text(encoding="utf-8"))

        self.assertEqual(cached["status"], "LIVE")
        self.assertEqual(cached["foreign_buy_sell"], 433.96)
        self.assertEqual(cached["trust_buy_sell"], 55.73)

    def test_mi_margn_openapi_payload_is_aggregated(self):
        payload = [
            {
                "股票代號": "0050",
                "融資前日餘額": "1,000",
                "融資今日餘額": "900",
                "融券前日餘額": "50",
                "融券今日餘額": "70",
            },
            {
                "股票代號": "2330",
                "融資前日餘額": "2,000",
                "融資今日餘額": "2,100",
                "融券前日餘額": "20",
                "融券今日餘額": "10",
            },
        ]

        margin = DataCollector._parse_margin_payload(payload)

        self.assertIsNotNone(margin)
        self.assertEqual(margin.margin_previous_balance, 3000)
        self.assertEqual(margin.margin_balance, 3000)
        self.assertEqual(margin.margin_change, 0)
        self.assertEqual(margin.margin_change_pct, 0)
        self.assertEqual(margin.short_previous_balance, 70)
        self.assertEqual(margin.short_balance, 80)
        self.assertEqual(margin.short_change, 10)

    def test_institutional_futures_csv_parser_extracts_txf_foreign_oi(self):
        csv_text = (
            "日期,商品名稱,身份別,多方交易口數,多方交易契約金額(千元),空方交易口數,空方交易契約金額(千元),"
            "多空交易口數淨額,多空交易契約金額淨額(千元),多方未平倉口數,多方未平倉契約金額(千元),"
            "空方未平倉口數,空方未平倉契約金額(千元),多空未平倉口數淨額,多空未平倉契約金額淨額(千元)\n"
            "2026/06/02,臺股期貨,自營商,7546,69314961,7439,68414885,107,900076,7664,70749158,5100,47123735,2564,23625423\n"
            "2026/06/02,臺股期貨,投信,1408,12981051,62,569613,1346,12411438,55075,507824545,5310,48961386,49765,458863159\n"
            "2026/06/02,臺股期貨,外資及陸資,72749,669381423,75092,690930652,-2343,-21549229,15432,142293027,82450,760288172,-67018,-617995145\n"
        )

        futures = DataCollector._parse_institutional_futures_csv(csv_text, date(2026, 6, 2))

        self.assertIsNotNone(futures)
        self.assertEqual(futures.foreign_long, 15432)
        self.assertEqual(futures.foreign_short, 82450)
        self.assertEqual(futures.foreign_net, -2343)
        self.assertEqual(futures.foreign_oi_net, -67018)
        self.assertEqual(futures.trust_oi_net, 49765)
        self.assertEqual(futures.dealer_oi_net, 2564)

    def test_large_trader_csv_parser_uses_tx_all_contracts_type_zero(self):
        csv_text = (
            "日期,商品(契約),商品名稱(契約名稱),到期月份(週別),交易人類別,前五大交易人買方,前五大交易人賣方,"
            "前十大交易人買方,前十大交易人賣方,全市場未沖銷部位數\n"
            "2026/06/02,TX,臺股期貨(TX+MTX/4+TMF/20),202606,0,55886,58420,69326,74036,105291\n"
            "2026/06/02,TX,臺股期貨(TX+MTX/4+TMF/20),999999,1,51898,59274,62747,76219,112001\n"
            "2026/06/02,TX,臺股期貨(TX+MTX/4+TMF/20),999999,0,55,959,59,274,70,194,76,219,112001\n"
        )

        # Keep thousands-free equivalent because CSV commas are field separators in TAIFEX output.
        csv_text = csv_text.replace("55,959,59,274,70,194,76,219", "55959,59274,70194,76219")
        large = DataCollector._parse_large_trader_csv(csv_text, date(2026, 6, 2))

        self.assertIsNotNone(large)
        self.assertEqual(large.top5_long, 55959)
        self.assertEqual(large.top5_short, 59274)
        self.assertEqual(large.top5_net, -3315)
        self.assertEqual(large.top10_long, 70194)
        self.assertEqual(large.top10_short, 76219)
        self.assertEqual(large.top10_net, -6025)
        self.assertEqual(large.total_oi, 112001)


class TestLeftSideFactorStatus(unittest.TestCase):
    def test_stub_factors_have_no_confidence(self):
        snap = IntelligenceSnapshot(
            margin=MarginData(margin_previous_balance=1000, margin_balance=970, margin_change=-30, margin_change_pct=-3.0),
            institutional_spot=InstitutionalSpot(date=date(2026, 6, 2), status="LIVE", trust_buy_sell=64.5),
        )
        result = LeftSideScoreEngine().calculate(snap)
        factors = {f["name"]: f for f in result.factor_scores}

        self.assertEqual(factors["foreign_futures"]["status"], "STUB")
        self.assertEqual(factors["foreign_futures"]["confidence"], 0.0)
        self.assertIn("資料未更新", factors["foreign_futures"]["detail"])
        self.assertEqual(factors["large_trader"]["status"], "STUB")
        self.assertEqual(factors["large_trader"]["confidence"], 0.0)

    def test_live_new_futures_and_large_trader_keep_confidence_for_real_zero(self):
        snap = IntelligenceSnapshot(
            institutional_futures=InstitutionalFutures(date=date(2026, 6, 2), foreign_oi_net=0),
            large_trader=LargeTraderOI(date=date(2026, 6, 2), top10_net=0),
        )
        result = LeftSideScoreEngine().calculate(snap)
        factors = {f["name"]: f for f in result.factor_scores}

        self.assertEqual(factors["foreign_futures"]["status"], "LIVE_NEW")
        self.assertGreater(factors["foreign_futures"]["confidence"], 0.0)
        self.assertEqual(factors["large_trader"]["status"], "LIVE_NEW")
        self.assertGreater(factors["large_trader"]["confidence"], 0.0)

    def test_trust_factor_uses_spot_not_oi_text(self):
        snap = IntelligenceSnapshot(
            institutional_spot=InstitutionalSpot(date=date(2026, 6, 2), status="LIVE", trust_buy_sell=64.5),
        )
        result = LeftSideScoreEngine().calculate(snap)
        trust = {f["name"]: f for f in result.factor_scores}["trust"]

        self.assertEqual(trust["status"], "LIVE_NEW")
        self.assertIn("投信現貨", trust["detail"])
        self.assertNotIn("投信淨OI", trust["detail"])


if __name__ == "__main__":
    unittest.main()
