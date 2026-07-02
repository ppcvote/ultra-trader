# CLAUDE.md — UltraTrader 專案指引

## 你的角色

你是 UltraTrader 的開發者，負責這個開源台灣期貨自動交易系統的維護和功能開發。

---

## 專案概述

UltraTrader 是台灣期貨（TAIFEX）的自動交易系統，完全在本地運行，零雲端依賴。

- **語言**：Python
- **API**：Shioaji（永豐金證券）
- **授權**：MIT（開源）
- **Repo**：ppcvote/ultra-trader

---

## 技術棧

| 項目 | 技術 |
|------|------|
| 語言 | Python 3.11+ |
| 券商 API | Shioaji >= 1.2.0 |
| Web 框架 | FastAPI >= 0.115.0 |
| ASGI | uvicorn[standard] >= 0.34.0 |
| 即時通訊 | websockets >= 13.0 |
| 數據處理 | pandas >= 2.2.0, numpy >= 1.26.0 |
| 日誌 | loguru >= 0.7.0 |
| 歷史數據 | yfinance >= 0.2.36（fallback） |

---

## 架構

```
UltraTrader/
├── core/              # 引擎核心（~4,000 LOC）
│   ├── engine.py      # 主交易循環（1,511 行）：Broker → MarketData → Strategy → Risk → Dashboard
│   ├── broker.py      # 抽象層：MockBroker（模擬）+ ShioajiBroker（實單/模擬盤）
│   ├── market_data.py # Tick 聚合、K bar 生成、指標計算
│   ├── position.py    # 部位追蹤、交易記錄、損益計算
│   ├── performance.py # 績效指標、Sharpe ratio、最大回撤
│   ├── logger.py      # 結構化日誌
│   └── instrument_config.py  # 合約規格（MXF、TMF、TGF 等）
├── strategy/          # 交易策略（~2,200 LOC）
│   ├── adaptive_momentum.py  # 主策略：7 因子進場信號、多時間框架（5m/15m）
│   ├── mean_reversion.py     # 布林通道極端 + RSI 背離
│   ├── gold_trend.py         # 黃金期貨趨勢跟蹤（TGF 專用）
│   ├── signals.py    # 複合信號評分
│   ├── filters.py    # 市場 regime 偵測、盤別判斷、交易過濾器
│   └── base.py       # 策略抽象介面
├── risk/              # 風險管理（~700 LOC）
│   ├── manager.py     # 風控總管
│   ├── circuit_breaker.py    # 連續虧損 / 日損限制自動停機
│   ├── position_sizing.py    # Kelly criterion，3 種風險等級
│   └── persistence.py        # 風控狀態 JSON 持久化
├── backtest/          # 回測引擎（~570 LOC）
│   ├── engine.py      # 歷史數據回放
│   ├── data_loader.py # 歷史 OHLCV 數據載入
│   └── report.py      # 績效報告生成
├── dashboard/         # 即時監控（~2,600 LOC）
│   ├── app.py         # FastAPI server
│   ├── websocket.py   # WebSocket 事件廣播
│   └── static/index.html     # 即時前端 UI
├── intelligence/      # 市場情報（~1,200 LOC）
│   ├── left_side_score.py    # 多時間框架 regime 評分
│   ├── data_collector.py     # 市場數據聚合
│   └── models.py     # 資料模型
├── scripts/           # CLI 工具（21 個）
│   ├── start.py       # 主入口（引擎 + Dashboard）
│   ├── go_live.py     # 切換到實單模式
│   ├── backtest_runner.py    # 回測執行器
│   └── ...            # 診斷、測試、數據抓取工具
├── tests/             # 測試（4 套件）
│   ├── test_broker.py
│   ├── test_risk.py
│   ├── test_strategy.py
│   └── test_fixes.py  # 回歸測試（750 行）
├── data/              # 數據目錄（gitignored）
├── .env               # API 金鑰（gitignored）
├── .env.example       # 環境變數範本
├── requirements.txt   # Python 依賴
└── PERFORMANCE_SPEC.md # 績效追蹤 schema
```

---

## 交易模式

| 模式 | 說明 | 需要 API key | 用途 |
|------|------|---|---|
| `simulation` | 本地 MockBroker + 假 tick | 否 | 開發、測試 |
| `paper` | 永豐模擬盤，真實行情 | 是 | 無風險實盤測試 |
| `live` | 真實下單 | 是 + CA 憑證 | 正式交易 |

---

## 3 種內建策略

1. **Adaptive Momentum** — 主策略，7 因子進場信號，5m/15m 多時間框架確認
2. **Mean Reversion** — 布林通道極端 + RSI 背離，適合盤整行情
3. **Gold Trend** — 黃金期貨（TGF）專用趨勢跟蹤

---

## 風險管理

- **Circuit Breaker**：連續 N 筆虧損 / 日虧損上限 → 自動停機
- **Position Sizing**：Kelly criterion，3 種 profile（conservative / balanced / aggressive）
- **Drawdown Protection**：最大回撤保護
- **盤別邏輯**：盤前/盤中/盤後不同策略行為
- **狀態持久化**：`data/risk_state.json`

---

## 常用指令

```bash
# 啟動（模擬模式）
python scripts/start.py

# 回測
python scripts/backtest_runner.py

# 切換到實單
python scripts/go_live.py

# 跑測試
python -m pytest tests/

# Dashboard
# 引擎啟動後自動開在 http://127.0.0.1:8888
```

---

## 環境變數（.env）

```
SHIOAJI_API_KEY=           # 永豐 API key
SHIOAJI_SECRET_KEY=        # 永豐 secret
SHIOAJI_CA_PATH=           # CA 憑證路徑（實單用）
SHIOAJI_CA_PASSWORD=       # CA 密碼（實單用）
SHIOAJI_PERSON_ID=         # 身分證字號（實單用）
TRADING_MODE=simulation    # simulation | paper | live
CONTRACT_CODE=MXF          # MXF | TMF | TGF 等
RISK_PROFILE=balanced      # conservative | balanced | aggressive
DASHBOARD_HOST=127.0.0.1
DASHBOARD_PORT=8888
```

---

## 開發規範

- 所有策略繼承 `strategy/base.py` 的抽象介面
- 風控狀態 JSON 持久化在 `data/risk_state.json`
- 日誌用 `loguru`，結構化輸出
- 測試用 `pytest`，4 個測試套件覆蓋 broker/risk/strategy/regression
- `data/` 目錄 gitignored，存放歷史數據、回測結果、日誌

---

## 注意事項

- Shioaji API 偶爾會 500 error — 有內建重試機制
- `shioaji.log` 會持續增長（目前 1.4 MB）— 考慮 log rotation
- 模擬模式不需要任何外部依賴，可以直接開發
- CA 憑證只有實單模式需要，模擬和模擬盤不需要
