# Market Data Backend: Detailed Design

Status: design, ready for implementation
Owner: Market Data agent
Related: `PLAN.md` sections 3, 5, 6, 8, 12

This document specifies everything the backend needs for live prices: the shared data model, the price cache, the abstract source interface, the GBM simulator, the Massive (formerly Polygon.io) REST poller, the factory that picks between them, the SSE endpoint, how the rest of the backend uses all of it, and the tests. Code snippets are intended to be close to final; implementers may adjust names but should keep the contracts.

---

## 1. Goals and Non-Goals

**Goals**

- One interface (`MarketDataSource`) with two implementations: `SimulatorSource` (default) and `MassiveSource` (when `MASSIVE_API_KEY` is set).
- One in-memory `PriceCache` that is the single source of truth for "current price" for every other part of the backend (SSE, trades, portfolio valuation, chat context, snapshots).
- Prices for every ticker the user cares about: the union of the watchlist and currently held positions.
- SSE stream at `GET /api/stream/prices` pushing at ~500 ms cadence.
- Fully testable without network access.

**Non-goals**

- Historical bars / candles from the backend. The frontend builds sparklines and the main chart from the SSE stream since page load (PLAN section 2).
- Order books, bid/ask, volume.
- Multi-user fan-out optimisation. The design does not prevent it, but does not build for it.

---

## 2. Module Layout

```
backend/
├── pyproject.toml
└── app/
    ├── main.py                    # FastAPI app, lifespan wires market data in
    └── market/
        ├── __init__.py            # re-exports the public API
        ├── models.py              # PriceUpdate dataclass
        ├── cache.py               # PriceCache
        ├── interface.py           # MarketDataSource ABC
        ├── seeds.py               # seed prices, per-ticker params, sectors
        ├── simulator.py           # GBMEngine (pure math) + SimulatorSource (async loop)
        ├── massive.py             # MassiveClient (HTTP + parsing) + MassiveSource (async loop)
        ├── factory.py             # create_market_source() picks the implementation
        ├── tickers.py             # symbol normalisation / validation
        └── stream.py              # SSE router: GET /api/stream/prices
```

Dependencies to add with `uv add`:

```bash
uv add fastapi "uvicorn[standard]" httpx
uv add --dev pytest pytest-asyncio respx
```

The simulator uses only the standard library (`random`, `math`), so no `numpy` is required.

---

## 3. Data Model

### 3.1 `PriceUpdate`

Every price the system knows about is represented by one immutable record. The same shape is stored in the cache, serialised to SSE, and returned by `GET /api/watchlist`.

```python
# backend/app/market/models.py
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Literal

Direction = Literal["up", "down", "flat"]


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    ticker: str
    price: float              # latest price, rounded to 2 dp
    previous_price: float     # price before this update (equals price on first tick)
    timestamp: float          # unix seconds (float) of the price observation
    session_open: float       # reference price for "daily change %"
    direction: Direction      # vs previous_price

    @property
    def change(self) -> float:
        return round(self.price - self.session_open, 2)

    @property
    def change_percent(self) -> float:
        if self.session_open == 0:
            return 0.0
        return round((self.price - self.session_open) / self.session_open * 100, 2)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["change"] = self.change
        d["change_percent"] = self.change_percent
        return d


def direction_of(new: float, old: float) -> Direction:
    if new > old:
        return "up"
    if new < old:
        return "down"
    return "flat"
```

`session_open` is what the watchlist's "daily change %" is computed against:

| Source | `session_open` value |
|---|---|
| Simulator | The seed price the ticker started at when the process booted (or when it was added). |
| Massive | `prevDay.c` (previous session close) from the snapshot, which is how brokers usually show "today's change". |

### 3.2 JSON wire format

Example of one serialised update (used in SSE and REST):

```json
{
  "ticker": "AAPL",
  "price": 191.37,
  "previous_price": 191.22,
  "timestamp": 1760438400.512,
  "session_open": 190.00,
  "direction": "up",
  "change": 1.37,
  "change_percent": 0.72
}
```

---

## 4. Price Cache

The cache stores the latest `PriceUpdate` per ticker plus a monotonically increasing `version` that bumps on every write. SSE handlers compare versions to decide whether there is anything new to send, which avoids pushing identical payloads when the Massive poller only refreshes every 15 s.

Everything runs on a single asyncio event loop, so plain dict operations are already atomic with respect to other coroutines. A `threading.Lock` is still used so the cache is safe if someone later calls it from a thread (e.g. a sync route running in the threadpool, which FastAPI does for `def` endpoints).

```python
# backend/app/market/cache.py
from __future__ import annotations

import threading
import time

from .models import PriceUpdate, direction_of


class PriceCache:
    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = threading.Lock()
        self._version = 0

    # ---- writes (only called by the active MarketDataSource) ----

    def update(
        self,
        ticker: str,
        price: float,
        *,
        timestamp: float | None = None,
        session_open: float | None = None,
    ) -> PriceUpdate:
        price = round(price, 2)
        ts = timestamp if timestamp is not None else time.time()
        with self._lock:
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price
            open_ = session_open if session_open is not None else (
                prev.session_open if prev else price
            )
            upd = PriceUpdate(
                ticker=ticker,
                price=price,
                previous_price=previous_price,
                timestamp=ts,
                session_open=round(open_, 2),
                direction=direction_of(price, previous_price),
            )
            self._prices[ticker] = upd
            self._version += 1
            return upd

    def remove(self, ticker: str) -> None:
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1

    # ---- reads (anyone) ----

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        upd = self.get(ticker)
        return upd.price if upd else None

    def snapshot(self) -> dict[str, PriceUpdate]:
        with self._lock:
            return dict(self._prices)

    @property
    def version(self) -> int:
        with self._lock:
            return self._version
```

Rules:

- Only the active `MarketDataSource` writes to the cache.
- Trade execution must read the price via `cache.get_price(ticker)` at the moment of the trade. If it returns `None` (ticker not yet priced), the trade is rejected with a clear error: `"No price available for XYZ yet, try again in a moment."`.

---

## 5. Unified Interface

```python
# backend/app/market/interface.py
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable

from .cache import PriceCache


class MarketDataSource(ABC):
    """Produces prices for a dynamic set of tickers and writes them to a PriceCache.

    Lifecycle:  src = Impl(cache); await src.start(initial_tickers); ...; await src.stop()
    Ticker set: add_ticker / remove_ticker may be called at any time after start().
    """

    def __init__(self, cache: PriceCache) -> None:
        self.cache = cache

    @abstractmethod
    async def start(self, tickers: Iterable[str]) -> None:
        """Begin producing prices in a background task. Must return promptly."""

    @abstractmethod
    async def stop(self) -> None:
        """Cancel the background task and release resources. Idempotent."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Start tracking a ticker. Idempotent. Should seed the cache ASAP."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking a ticker and drop it from the cache. Idempotent."""

    @abstractmethod
    def get_tickers(self) -> set[str]:
        """Tickers currently tracked."""

    @property
    @abstractmethod
    def name(self) -> str:
        """'simulator' or 'massive', exposed via /api/health for debugging."""
```

Design notes:

- The interface is push-into-cache, not pull. Consumers never call the source for a price; they read the cache. This keeps SSE, trading and chat identical regardless of source.
- `add_ticker` is `async` because the Massive implementation does an immediate one-off fetch so a newly added ticker shows a price within ~1 s rather than waiting up to 15 s for the next poll.
- `start` does not block. It schedules an `asyncio.Task` and returns.

---

## 6. Ticker Symbols

```python
# backend/app/market/tickers.py
import re

_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


def normalize_ticker(raw: str) -> str:
    """Uppercase, strip whitespace, validate. Raises ValueError on bad input."""
    t = raw.strip().upper()
    if not _TICKER_RE.fullmatch(t):
        raise ValueError(f"Invalid ticker symbol: {raw!r}")
    return t
```

Every entry point (watchlist API, trade API, LLM actions) runs symbols through `normalize_ticker` before touching the cache or the source. Allowed: `AAPL`, `BRK.B`, `RDS-A`. Rejected: `""`, `aapl!`, `123`, anything over 10 chars.

---

## 7. Simulator

### 7.1 Seed data and per-ticker parameters

```python
# backend/app/market/seeds.py
from dataclasses import dataclass


@dataclass(frozen=True)
class TickerParams:
    seed_price: float
    mu: float       # annualised drift
    sigma: float    # annualised volatility
    sector: str


DEFAULT_TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM", "V", "NFLX"]

TICKER_PARAMS: dict[str, TickerParams] = {
    "AAPL":  TickerParams(190.00, 0.08, 0.25, "tech"),
    "GOOGL": TickerParams(175.00, 0.08, 0.28, "tech"),
    "MSFT":  TickerParams(420.00, 0.08, 0.24, "tech"),
    "AMZN":  TickerParams(185.00, 0.09, 0.30, "tech"),
    "TSLA":  TickerParams(250.00, 0.05, 0.60, "auto"),
    "NVDA":  TickerParams(880.00, 0.12, 0.50, "tech"),
    "META":  TickerParams(500.00, 0.09, 0.35, "tech"),
    "JPM":   TickerParams(195.00, 0.06, 0.20, "finance"),
    "V":     TickerParams(275.00, 0.06, 0.18, "finance"),
    "NFLX":  TickerParams(620.00, 0.08, 0.38, "media"),
}

# Correlation structure (factor model, see 7.2)
MARKET_WEIGHT = 0.30   # share of variance explained by the whole market
SECTOR_WEIGHT = 0.35   # share explained by the sector factor (on top of market)


def params_for(ticker: str) -> TickerParams:
    """Known tickers get curated params. Unknown tickers get deterministic
    pseudo-random params derived from the symbol, so the same symbol always
    starts at the same price across restarts."""
    if ticker in TICKER_PARAMS:
        return TICKER_PARAMS[ticker]
    h = sum((i + 1) * ord(c) for i, c in enumerate(ticker))
    seed_price = 20 + (h * 7919) % 480          # $20 .. $500
    sigma = 0.20 + ((h * 104729) % 30) / 100    # 0.20 .. 0.49
    return TickerParams(float(seed_price), 0.07, sigma, f"other:{ticker}")
```

Unknown tickers get a sector unique to themselves, so they only correlate through the market factor.

### 7.2 The math

**Geometric Brownian Motion.** Each tick advances every price by

```
S(t+dt) = S(t) * exp( (mu - sigma^2 / 2) * dt + sigma * sqrt(dt) * Z )
```

where `Z ~ N(0, 1)` and `dt` is the tick length expressed in trading years:

```
dt = 0.5 s / (252 days * 6.5 h * 3600 s) ≈ 8.48e-8
```

With `sigma = 0.25`, one tick moves AAPL by about `190 * 0.25 * sqrt(8.48e-8) ≈ $0.014`, i.e. visible at 2 dp, small enough to look realistic. A `SPEED` multiplier (default `1.0`, env `SIM_SPEED`) scales `dt` for livelier demos.

**Correlated shocks via a factor model.** Instead of a Cholesky decomposition of a full correlation matrix (which must be rebuilt every time a ticker is added), each tick draws one market shock `M`, one shock `F_s` per sector, and one idiosyncratic shock `e_i` per ticker:

```
Z_i = sqrt(wm) * M + sqrt(ws) * F_sector(i) + sqrt(1 - wm - ws) * e_i
```

`Z_i` is still standard normal (the weights sum to 1). The implied correlations are:

| Pair | Correlation |
|---|---|
| Same sector (e.g. AAPL / MSFT) | `wm + ws = 0.65` |
| Different sectors (e.g. AAPL / JPM) | `wm = 0.30` |

This is O(n) per tick and handles dynamic ticker sets for free.

**Random events.** Each ticker, each tick, has probability `EVENT_PROB = 0.0005` of a jump of `±U(2%, 5%)`. With 10 tickers at 2 ticks/s that is roughly one event every 100 s across the watchlist: enough drama without chaos.

**Guard rails.** Prices are clamped to a floor of `$0.01`, so a long simulated crash can never go to zero or negative.

### 7.3 `GBMEngine` (pure, synchronous, deterministic with a seed)

Keeping the math separate from asyncio makes it trivially unit-testable.

```python
# backend/app/market/simulator.py
from __future__ import annotations

import asyncio
import logging
import math
import os
import random
import time
from collections.abc import Iterable

from .cache import PriceCache
from .interface import MarketDataSource
from .seeds import MARKET_WEIGHT, SECTOR_WEIGHT, params_for

log = logging.getLogger(__name__)

TICK_SECONDS = 0.5
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600
EVENT_PROB = 0.0005
EVENT_MIN, EVENT_MAX = 0.02, 0.05
PRICE_FLOOR = 0.01


class GBMEngine:
    def __init__(
        self,
        *,
        tick_seconds: float = TICK_SECONDS,
        speed: float = 1.0,
        event_prob: float = EVENT_PROB,
        rng: random.Random | None = None,
    ) -> None:
        self.dt = tick_seconds * speed / TRADING_SECONDS_PER_YEAR
        self.event_prob = event_prob
        self.rng = rng or random.Random()
        self.prices: dict[str, float] = {}
        self.opens: dict[str, float] = {}

    def add(self, ticker: str) -> None:
        if ticker not in self.prices:
            p = params_for(ticker).seed_price
            self.prices[ticker] = p
            self.opens[ticker] = p

    def remove(self, ticker: str) -> None:
        self.prices.pop(ticker, None)
        self.opens.pop(ticker, None)

    def step(self) -> dict[str, float]:
        """Advance every ticker by one tick. Returns {ticker: new_price}."""
        if not self.prices:
            return {}
        gauss = self.rng.gauss
        market = gauss(0.0, 1.0)
        sector_shocks: dict[str, float] = {}
        wm, ws = MARKET_WEIGHT, SECTOR_WEIGHT
        wi = 1.0 - wm - ws
        sqrt_dt = math.sqrt(self.dt)

        for ticker, s in self.prices.items():
            p = params_for(ticker)
            if p.sector not in sector_shocks:
                sector_shocks[p.sector] = gauss(0.0, 1.0)
            z = (
                math.sqrt(wm) * market
                + math.sqrt(ws) * sector_shocks[p.sector]
                + math.sqrt(wi) * gauss(0.0, 1.0)
            )
            drift = (p.mu - 0.5 * p.sigma**2) * self.dt
            new = s * math.exp(drift + p.sigma * sqrt_dt * z)

            if self.rng.random() < self.event_prob:
                jump = self.rng.uniform(EVENT_MIN, EVENT_MAX)
                sign = 1 if self.rng.random() < 0.5 else -1
                new *= 1 + sign * jump
                log.info("sim event: %s %+.1f%%", ticker, sign * jump * 100)

            self.prices[ticker] = max(new, PRICE_FLOOR)

        return dict(self.prices)
```

### 7.4 `SimulatorSource` (async wrapper)

```python
# backend/app/market/simulator.py  (continued)

class SimulatorSource(MarketDataSource):
    def __init__(
        self,
        cache: PriceCache,
        *,
        engine: GBMEngine | None = None,
        tick_seconds: float = TICK_SECONDS,
    ) -> None:
        super().__init__(cache)
        speed = float(os.getenv("SIM_SPEED", "1.0") or 1.0)
        self.engine = engine or GBMEngine(tick_seconds=tick_seconds, speed=speed)
        self.tick_seconds = tick_seconds
        self._task: asyncio.Task | None = None

    @property
    def name(self) -> str:
        return "simulator"

    async def start(self, tickers: Iterable[str]) -> None:
        for t in tickers:
            await self.add_ticker(t)
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="market-simulator")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def add_ticker(self, ticker: str) -> None:
        if ticker in self.engine.prices:
            return
        self.engine.add(ticker)
        # Seed the cache immediately so the UI and trades see a price right away.
        p = self.engine.prices[ticker]
        self.cache.update(ticker, p, session_open=self.engine.opens[ticker])

    async def remove_ticker(self, ticker: str) -> None:
        self.engine.remove(ticker)
        self.cache.remove(ticker)

    def get_tickers(self) -> set[str]:
        return set(self.engine.prices)

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        next_tick = loop.time()
        while True:
            try:
                now = time.time()
                for ticker, price in self.engine.step().items():
                    self.cache.update(ticker, price, timestamp=now)
            except Exception:  # never let one bad tick kill the feed
                log.exception("simulator tick failed")
            next_tick += self.tick_seconds
            await asyncio.sleep(max(0.0, next_tick - loop.time()))
```

Using a `next_tick` schedule instead of a bare `sleep(0.5)` keeps the cadence steady even if a tick takes a few milliseconds.

---

## 8. Massive API Client

Massive is the rebranded Polygon.io. The REST API is the same; `api.massive.com` and `api.polygon.io` serve it in parallel and existing keys work on both. Base URL is configurable (`MASSIVE_BASE_URL`, default `https://api.massive.com`).

### 8.1 Endpoint

**Primary: full-market snapshot filtered to our tickers (one call for all tickers).**

```
GET {BASE}/v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT,NVDA
Authorization: Bearer <MASSIVE_API_KEY>
```

(`?apiKey=<key>` as a query parameter also works; prefer the header so the key never lands in logs.)

Abbreviated response:

```json
{
  "status": "OK",
  "count": 2,
  "tickers": [
    {
      "ticker": "AAPL",
      "todaysChange": 1.37,
      "todaysChangePerc": 0.72,
      "updated": 1760438400512000000,
      "day":      {"o": 190.10, "h": 191.80, "l": 189.70, "c": 191.37, "v": 41234567, "vw": 190.9},
      "prevDay":  {"o": 188.50, "h": 190.40, "l": 188.00, "c": 190.00, "v": 52345678, "vw": 189.6},
      "min":      {"o": 191.30, "h": 191.40, "l": 191.25, "c": 191.37, "t": 1760438400000},
      "lastTrade": {"p": 191.37, "s": 100, "t": 1760438400512000000, "x": 4},
      "lastQuote": {"P": 191.38, "p": 191.36, "t": 1760438400510000000}
    },
    {"ticker": "MSFT", "...": "..."}
  ]
}
```

Timestamps `updated`, `lastTrade.t` and `lastQuote.t` are **nanoseconds**; `min.t` is milliseconds.

**Fallback: previous close per ticker** (for keys whose plan does not include snapshots):

```
GET {BASE}/v2/aggs/ticker/{TICKER}/prev?adjusted=true
```

```json
{"ticker": "AAPL", "status": "OK", "resultsCount": 1,
 "results": [{"T": "AAPL", "o": 188.5, "h": 190.4, "l": 188.0, "c": 190.0, "v": 52345678, "t": 1760385600000}]}
```

**Plan caveat.** The free "Basic" tier is limited to 5 requests/minute and its data freshness is end-of-day (paid tiers are 15-min delayed or real-time). Whether snapshots are included on the free tier should be verified with a real key. The client therefore:

1. Tries the snapshot endpoint.
2. On `403` (`NOT_AUTHORIZED`), logs a single warning and switches permanently to the per-ticker `prev` endpoint, spreading calls so it stays under 5/min. Prices then only change daily, which is expected for that tier; the UI still works.

### 8.2 Price selection

For each snapshot row, the "current price" is the first present, positive value from:

1. `lastTrade.p`  (most recent trade)
2. `min.c`        (latest minute bar close)
3. `day.c`        (today's running close; can be 0 pre-market)
4. `prevDay.c`    (yesterday's close; weekends/holidays/pre-market)

`session_open = prevDay.c` when present, else the chosen price.
`timestamp = lastTrade.t / 1e9`, else `updated / 1e9`, else `time.time()`.

### 8.3 Parsing (pure functions, easy to test)

```python
# backend/app/market/massive.py
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Iterable
from dataclasses import dataclass

import httpx

from .cache import PriceCache
from .interface import MarketDataSource

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.massive.com"
DEFAULT_POLL_SECONDS = 15.0
MAX_TICKERS_PER_REQUEST = 100   # keep URLs a sane length


@dataclass(frozen=True)
class Quote:
    ticker: str
    price: float
    session_open: float
    timestamp: float


def _pos(x) -> float | None:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def parse_snapshot_row(row: dict) -> Quote | None:
    ticker = row.get("ticker")
    if not ticker:
        return None
    price = (
        _pos((row.get("lastTrade") or {}).get("p"))
        or _pos((row.get("min") or {}).get("c"))
        or _pos((row.get("day") or {}).get("c"))
        or _pos((row.get("prevDay") or {}).get("c"))
    )
    if price is None:
        return None
    prev_close = _pos((row.get("prevDay") or {}).get("c")) or price
    ns = (row.get("lastTrade") or {}).get("t") or row.get("updated")
    ts = ns / 1e9 if isinstance(ns, (int, float)) and ns > 0 else time.time()
    return Quote(ticker=ticker, price=price, session_open=prev_close, timestamp=ts)


def parse_snapshot(payload: dict) -> list[Quote]:
    rows = payload.get("tickers") or []
    return [q for q in (parse_snapshot_row(r) for r in rows) if q is not None]


def parse_prev_close(payload: dict) -> Quote | None:
    results = payload.get("results") or []
    if not results:
        return None
    r = results[0]
    price = _pos(r.get("c"))
    if price is None:
        return None
    ts_ms = r.get("t")
    ts = ts_ms / 1000 if isinstance(ts_ms, (int, float)) else time.time()
    open_ = _pos(r.get("o")) or price
    return Quote(ticker=payload.get("ticker") or r.get("T"), price=price,
                 session_open=open_, timestamp=ts)
```

### 8.4 HTTP client

```python
# backend/app/market/massive.py  (continued)

class MassiveError(Exception):
    pass


class NotAuthorized(MassiveError):
    pass


class RateLimited(MassiveError):
    def __init__(self, retry_after: float | None):
        super().__init__("rate limited")
        self.retry_after = retry_after


class MassiveClient:
    def __init__(self, api_key: str, base_url: str = DEFAULT_BASE_URL,
                 http: httpx.AsyncClient | None = None) -> None:
        self._http = http or httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=httpx.Timeout(10.0, connect=5.0),
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _get(self, path: str, params: dict | None = None) -> dict:
        resp = await self._http.get(path, params=params)
        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            raise RateLimited(float(ra) if ra and ra.isdigit() else None)
        if resp.status_code in (401, 403):
            raise NotAuthorized(resp.text[:200])
        resp.raise_for_status()
        return resp.json()

    async def snapshot(self, tickers: list[str]) -> list[Quote]:
        quotes: list[Quote] = []
        for i in range(0, len(tickers), MAX_TICKERS_PER_REQUEST):
            chunk = tickers[i : i + MAX_TICKERS_PER_REQUEST]
            data = await self._get(
                "/v2/snapshot/locale/us/markets/stocks/tickers",
                {"tickers": ",".join(chunk)},
            )
            quotes.extend(parse_snapshot(data))
        return quotes

    async def prev_close(self, ticker: str) -> Quote | None:
        data = await self._get(f"/v2/aggs/ticker/{ticker}/prev", {"adjusted": "true"})
        return parse_prev_close(data)
```

### 8.5 `MassiveSource` (polling loop)

```python
# backend/app/market/massive.py  (continued)

class MassiveSource(MarketDataSource):
    def __init__(self, cache: PriceCache, api_key: str, *,
                 base_url: str | None = None,
                 poll_seconds: float | None = None,
                 client: MassiveClient | None = None) -> None:
        super().__init__(cache)
        self.client = client or MassiveClient(
            api_key, base_url or os.getenv("MASSIVE_BASE_URL") or DEFAULT_BASE_URL
        )
        self.poll_seconds = poll_seconds or float(
            os.getenv("MASSIVE_POLL_INTERVAL") or DEFAULT_POLL_SECONDS
        )
        self._tickers: set[str] = set()
        self._task: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self._snapshot_allowed = True     # flips to False on 403 -> prev-close mode
        self._backoff = 0.0
        self._closed = False

    @property
    def name(self) -> str:
        return "massive"

    async def start(self, tickers: Iterable[str]) -> None:
        self._tickers.update(tickers)
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="market-massive")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if not self._closed:
            self._closed = True
            await self.client.aclose()

    async def add_ticker(self, ticker: str) -> None:
        if ticker in self._tickers:
            return
        self._tickers.add(ticker)
        self._wake.set()                  # poll now so the new ticker gets a price quickly

    async def remove_ticker(self, ticker: str) -> None:
        self._tickers.discard(ticker)
        self.cache.remove(ticker)

    def get_tickers(self) -> set[str]:
        return set(self._tickers)

    async def _poll_once(self) -> None:
        tickers = sorted(self._tickers)
        if not tickers:
            return
        if self._snapshot_allowed:
            try:
                quotes = await self.client.snapshot(tickers)
            except NotAuthorized:
                log.warning("Massive snapshot not available on this plan; "
                            "falling back to previous-close polling (end-of-day prices).")
                self._snapshot_allowed = False
                return await self._poll_once()
        else:
            quotes = []
            # Only fetch tickers we have never priced; prev close changes once a day.
            for t in tickers:
                if self.cache.get(t) is None:
                    q = await self.client.prev_close(t)
                    if q:
                        quotes.append(q)
        for q in quotes:
            if q.ticker in self._tickers:     # ignore rows for tickers removed mid-flight
                self.cache.update(q.ticker, q.price,
                                  timestamp=q.timestamp, session_open=q.session_open)

    async def _run(self) -> None:
        while True:
            try:
                await self._poll_once()
                self._backoff = 0.0
                delay = self.poll_seconds
            except RateLimited as e:
                self._backoff = min(max(self._backoff * 2, self.poll_seconds), 300)
                delay = e.retry_after or self._backoff
                log.warning("Massive rate limited; retrying in %.0fs", delay)
            except NotAuthorized as e:
                log.error("Massive API key rejected: %s", e)
                delay = 300
            except (httpx.HTTPError, ValueError) as e:
                self._backoff = min(max(self._backoff * 2, 2.0), 60)
                delay = self._backoff
                log.warning("Massive poll failed (%s); retrying in %.0fs", e, delay)
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
```

Behaviour summary:

| Situation | Behaviour |
|---|---|
| Normal | One snapshot call every `MASSIVE_POLL_INTERVAL` s (default 15, good for 5 req/min). Paid tiers can set 2-5 s. |
| Ticker added | Poll immediately (wake event). |
| Unknown ticker | Snapshot simply omits it; cache never gets an entry; watchlist shows "no price" and trades on it are rejected. |
| 429 | Exponential backoff, honours `Retry-After`, capped at 5 min. |
| 401/403 on snapshot | Switch to `prev` endpoint mode once, keep running. |
| Network error | Backoff 2 s → 60 s, keep last known prices in the cache. |
| Market closed | Prices simply stop changing; SSE keeps sending nothing new (version unchanged). |

Note on cadence: Massive updates every ~15 s, but SSE still pushes every 500 ms when anything changed. The frontend's sparklines therefore step rather than wiggle in Massive mode; that is expected.

---

## 9. Factory and Environment

```python
# backend/app/market/factory.py
import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive import MassiveSource
from .simulator import SimulatorSource

log = logging.getLogger(__name__)


def create_market_source(cache: PriceCache) -> MarketDataSource:
    key = (os.getenv("MASSIVE_API_KEY") or "").strip()
    if key:
        log.info("Market data: Massive REST API")
        return MassiveSource(cache, api_key=key)
    log.info("Market data: built-in simulator")
    return SimulatorSource(cache)
```

Environment variables owned by market data (additions beyond `PLAN.md` section 5 are optional with sane defaults):

| Variable | Default | Meaning |
|---|---|---|
| `MASSIVE_API_KEY` | empty | Non-empty selects Massive; empty selects simulator. |
| `MASSIVE_POLL_INTERVAL` | `15` | Seconds between snapshot polls. |
| `MASSIVE_BASE_URL` | `https://api.massive.com` | Override host (e.g. `https://api.polygon.io`). |
| `SIM_SPEED` | `1.0` | Simulator time multiplier (e.g. `20` for a volatile demo). |

---

## 10. Wiring into FastAPI

### 10.1 Which tickers get priced

The tracked set is **watchlist ∪ open positions**. Otherwise a user who removes a held ticker from the watchlist would lose its price and the portfolio value would break.

```python
# backend/app/market/__init__.py
from .cache import PriceCache
from .factory import create_market_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .seeds import DEFAULT_TICKERS
from .tickers import normalize_ticker

__all__ = ["PriceCache", "MarketDataSource", "PriceUpdate", "create_market_source",
           "DEFAULT_TICKERS", "normalize_ticker"]
```

```python
# backend/app/main.py  (market-data relevant parts)
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import PriceCache, create_market_source
from app.market.stream import router as stream_router
from app import db  # owned by the backend agent


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init()                                   # lazy schema + seed
    cache = PriceCache()
    source = create_market_source(cache)
    tickers = set(db.get_watchlist_tickers()) | set(db.get_position_tickers())
    await source.start(tickers)
    app.state.price_cache = cache
    app.state.market = source
    try:
        yield
    finally:
        await source.stop()


app = FastAPI(lifespan=lifespan)
app.include_router(stream_router)
```

Dependency helpers for routes:

```python
# backend/app/deps.py
from fastapi import Request

from app.market import MarketDataSource, PriceCache


def get_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache


def get_market(request: Request) -> MarketDataSource:
    return request.app.state.market
```

### 10.2 Keeping the source in sync

These calls are made by the watchlist, trade and chat code (owned by the backend agent), shown here so the contract is explicit:

```python
# after POST /api/watchlist {ticker} succeeds in the DB
await market.add_ticker(ticker)

# after DELETE /api/watchlist/{ticker} succeeds in the DB
if ticker not in db.get_position_tickers():
    await market.remove_ticker(ticker)

# after a SELL that closes a position (quantity -> 0)
if ticker not in db.get_watchlist_tickers():
    await market.remove_ticker(ticker)

# before executing any trade
price = cache.get_price(ticker)
if price is None:
    raise TradeError(f"No price available for {ticker} yet, try again in a moment.")
```

A buy on a ticker that is in neither set is allowed only if it has a price; in practice trades happen from the watchlist or via the LLM, which adds the ticker to the watchlist first. Recommended rule for the trade route: if the ticker is untracked, call `await market.add_ticker(ticker)`, then reject with the "no price yet" message if the cache is still empty. With the simulator the cache is seeded synchronously, so the trade goes through immediately.

### 10.3 `GET /api/watchlist` shape

```json
[
  {"ticker": "AAPL", "price": 191.37, "previous_price": 191.22, "change": 1.37,
   "change_percent": 0.72, "direction": "up", "timestamp": 1760438400.512},
  {"ticker": "ZZZZ", "price": null, "previous_price": null, "change": null,
   "change_percent": null, "direction": null, "timestamp": null}
]
```

Built by joining DB watchlist rows with `cache.get(ticker)`.

### 10.4 Health

`GET /api/health` should include `{"status": "ok", "market_source": market.name, "tickers": len(market.get_tickers())}` to make debugging trivial.

---

## 11. SSE Endpoint

### 11.1 Protocol

- `GET /api/stream/prices`, `Content-Type: text/event-stream`.
- On connect: one `retry: 2000` line (reconnect after 2 s), then an immediate full snapshot.
- Every 500 ms: if `cache.version` changed, send an event containing **only tickers whose update changed** since the last send for this client. Removed tickers are reported in a `removed` list.
- Every 15 s with nothing to send: a comment line `: ping` as heartbeat, which keeps proxies from closing the idle connection (important in Massive mode).

Event format (one event type, `prices`):

```
retry: 2000

event: prices
data: {"prices":[{"ticker":"AAPL","price":191.37,"previous_price":191.22,"timestamp":1760438400.512,"session_open":190.0,"direction":"up","change":1.37,"change_percent":0.72}, ...],"removed":[]}

: ping

```

Batching all tickers into one event per tick (rather than one event per ticker) keeps the browser to ~2 message callbacks per second and lets React do one state update per tick. Each element still carries ticker, price, previous price, timestamp and direction as required by PLAN section 6.

### 11.2 Implementation

```python
# backend/app/market/stream.py
from __future__ import annotations

import asyncio
import json
import time

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache
from .models import PriceUpdate

router = APIRouter()

PUSH_INTERVAL = 0.5
HEARTBEAT_SECONDS = 15.0


def _event(prices: list[PriceUpdate], removed: list[str]) -> str:
    payload = {"prices": [p.to_dict() for p in prices], "removed": removed}
    return f"event: prices\ndata: {json.dumps(payload, separators=(',', ':'))}\n\n"


async def price_event_stream(request: Request, cache: PriceCache,
                             push_interval: float = PUSH_INTERVAL):
    yield "retry: 2000\n\n"
    sent: dict[str, PriceUpdate] = {}
    last_version = -1
    last_send = time.monotonic()

    while True:
        if await request.is_disconnected():
            break
        version = cache.version
        if version != last_version:
            last_version = version
            snap = cache.snapshot()
            changed = [u for t, u in snap.items() if sent.get(t) is not u]
            removed = [t for t in sent if t not in snap]
            if changed or removed:
                for t in removed:
                    del sent[t]
                for u in changed:
                    sent[u.ticker] = u
                yield _event(changed, removed)
                last_send = time.monotonic()
        if time.monotonic() - last_send >= HEARTBEAT_SECONDS:
            yield ": ping\n\n"
            last_send = time.monotonic()
        await asyncio.sleep(push_interval)


@router.get("/api/stream/prices")
async def stream_prices(request: Request) -> StreamingResponse:
    cache: PriceCache = request.app.state.price_cache
    return StreamingResponse(
        price_event_stream(request, cache),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",   # disable proxy buffering (nginx etc.)
        },
    )
```

The identity check `sent.get(t) is not u` works because `PriceUpdate` objects are immutable and replaced on every cache write; it is cheaper than comparing fields. The first iteration sends every ticker, which is the initial snapshot.

### 11.3 Frontend consumption (reference for the Frontend agent)

```ts
type PriceUpdate = {
  ticker: string; price: number; previous_price: number; timestamp: number;
  session_open: number; direction: "up" | "down" | "flat";
  change: number; change_percent: number;
};

const es = new EventSource("/api/stream/prices");
es.addEventListener("prices", (e) => {
  const { prices, removed } = JSON.parse((e as MessageEvent).data) as
    { prices: PriceUpdate[]; removed: string[] };
  // merge into store; append {time: timestamp, value: price} to per-ticker sparkline buffer
});
es.onopen = () => setStatus("connected");
es.onerror = () => setStatus(es.readyState === EventSource.CLOSED ? "disconnected" : "reconnecting");
```

Sparkline buffers should be capped (e.g. last 600 points ≈ 5 min at 2 Hz) to bound memory.

---

## 12. Interactions with Other Subsystems

| Consumer | What it uses |
|---|---|
| Trade execution | `cache.get_price(ticker)` at fill time. |
| `GET /api/portfolio` | `cache.get_price` per position for current value / unrealised P&L. If a price is missing, fall back to `avg_cost` and flag `"price_stale": true`. |
| Portfolio snapshot task (every 30 s) | Same valuation as above; owned by the backend agent, reads only the cache. |
| Chat context | `cache.snapshot()` for watchlist prices and position valuation. |
| LLM watchlist changes | Same `add_ticker` / `remove_ticker` calls as the REST routes (10.2). |

No subsystem outside `app/market` imports the simulator or Massive classes directly.

---

## 13. Testing

All tests run offline. Use `pytest-asyncio` (`asyncio_mode = "auto"` in `pyproject.toml`).

### 13.1 Cache

```python
def test_cache_tracks_previous_and_direction():
    c = PriceCache()
    a = c.update("AAPL", 100.0)
    assert a.previous_price == 100.0 and a.direction == "flat" and a.session_open == 100.0
    b = c.update("AAPL", 101.234)
    assert b.price == 101.23 and b.previous_price == 100.0 and b.direction == "up"
    assert b.change_percent == 1.23
    v = c.version
    c.remove("AAPL")
    assert c.get("AAPL") is None and c.version == v + 1
```

### 13.2 GBM engine

```python
import math, random, statistics
from app.market.simulator import GBMEngine

def test_prices_positive_and_deterministic():
    e1 = GBMEngine(rng=random.Random(42)); e2 = GBMEngine(rng=random.Random(42))
    for t in ("AAPL", "MSFT"): e1.add(t); e2.add(t)
    for _ in range(1000):
        assert e1.step() == e2.step()
    assert all(p > 0 for p in e1.prices.values())

def test_log_return_volatility_matches_sigma():
    e = GBMEngine(rng=random.Random(1), event_prob=0.0)
    e.add("JPM")  # sigma 0.20
    rets, prev = [], e.prices["JPM"]
    for _ in range(20000):
        cur = e.step()["JPM"]; rets.append(math.log(cur / prev)); prev = cur
    est_sigma = statistics.stdev(rets) / math.sqrt(e.dt)
    assert abs(est_sigma - 0.20) < 0.01

def test_same_sector_more_correlated_than_cross_sector():
    e = GBMEngine(rng=random.Random(7), event_prob=0.0)
    for t in ("AAPL", "MSFT", "JPM"): e.add(t)
    series = {t: [] for t in e.prices}; prev = dict(e.prices)
    for _ in range(20000):
        cur = e.step()
        for t in series: series[t].append(math.log(cur[t] / prev[t]))
        prev = cur
    same = statistics.correlation(series["AAPL"], series["MSFT"])
    cross = statistics.correlation(series["AAPL"], series["JPM"])
    assert 0.55 < same < 0.75 and 0.2 < cross < 0.4

def test_unknown_ticker_gets_stable_seed():
    a, b = GBMEngine(), GBMEngine()
    a.add("ZZZZ"); b.add("ZZZZ")
    assert a.prices["ZZZZ"] == b.prices["ZZZZ"] and 20 <= a.prices["ZZZZ"] <= 500
```

### 13.3 Simulator source

```python
async def test_simulator_seeds_cache_on_add_and_ticks():
    cache = PriceCache()
    src = SimulatorSource(cache, tick_seconds=0.01)
    await src.start(["AAPL"])
    assert cache.get_price("AAPL") == 190.0          # seeded synchronously
    await src.add_ticker("PYPL")
    assert cache.get("PYPL") is not None
    v = cache.version
    await asyncio.sleep(0.05)
    assert cache.version > v
    await src.remove_ticker("PYPL")
    assert cache.get("PYPL") is None
    await src.stop()
```

### 13.4 Massive parsing and polling

```python
SNAP = {"status": "OK", "tickers": [
    {"ticker": "AAPL", "updated": 1_760_438_400_000_000_000,
     "lastTrade": {"p": 191.37, "t": 1_760_438_400_512_000_000},
     "day": {"c": 191.0}, "prevDay": {"c": 190.0}},
    {"ticker": "PRE", "day": {"c": 0}, "prevDay": {"c": 50.0}},   # pre-market: falls back
    {"ticker": "BAD"},                                             # no price: dropped
]}

def test_parse_snapshot():
    q = {x.ticker: x for x in parse_snapshot(SNAP)}
    assert q["AAPL"].price == 191.37 and q["AAPL"].session_open == 190.0
    assert abs(q["AAPL"].timestamp - 1_760_438_400.512) < 1e-3
    assert q["PRE"].price == 50.0
    assert "BAD" not in q

@respx.mock
async def test_massive_source_polls_into_cache():
    respx.get("https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers") \
         .respond(json=SNAP)
    cache = PriceCache()
    src = MassiveSource(cache, api_key="k", poll_seconds=60)
    await src.start(["AAPL"])
    await asyncio.sleep(0.05)
    assert cache.get_price("AAPL") == 191.37
    assert cache.get("PRE") is None          # not tracked, ignored
    await src.stop()

@respx.mock
async def test_massive_falls_back_to_prev_close_on_403():
    respx.get(url__regex=r".*/v2/snapshot/.*").respond(403, json={"status": "NOT_AUTHORIZED"})
    respx.get("https://api.massive.com/v2/aggs/ticker/AAPL/prev").respond(json={
        "ticker": "AAPL", "results": [{"T": "AAPL", "o": 188.5, "c": 190.0, "t": 1760385600000}]})
    cache = PriceCache()
    src = MassiveSource(cache, api_key="k", poll_seconds=60)
    await src.start(["AAPL"])
    await asyncio.sleep(0.05)
    assert cache.get_price("AAPL") == 190.0
    await src.stop()

@respx.mock
async def test_massive_rate_limit_keeps_last_prices():
    route = respx.get(url__regex=r".*/v2/snapshot/.*")
    route.side_effect = [httpx.Response(200, json=SNAP), httpx.Response(429)]
    # first poll fills cache, second raises RateLimited -> cache unchanged, loop alive
```

### 13.5 Interface conformance

```python
@pytest.mark.parametrize("make", [
    lambda c: SimulatorSource(c),
    lambda c: MassiveSource(c, api_key="k", client=FakeMassiveClient()),
])
async def test_source_contract(make):
    cache = PriceCache(); src = make(cache)
    assert isinstance(src, MarketDataSource)
    await src.start(["AAPL"])
    await src.add_ticker("MSFT"); await src.add_ticker("MSFT")       # idempotent
    assert src.get_tickers() == {"AAPL", "MSFT"}
    await src.remove_ticker("MSFT"); await src.remove_ticker("MSFT") # idempotent
    assert src.get_tickers() == {"AAPL"}
    await src.stop(); await src.stop()                                # idempotent
```

`FakeMassiveClient` returns canned `Quote`s from `snapshot()` and has a no-op `aclose()`. `MassiveSource.stop()` tolerates a second call thanks to its `_closed` flag.

### 13.6 Factory and SSE

```python
def test_factory_selects_by_env(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    assert create_market_source(PriceCache()).name == "simulator"
    monkeypatch.setenv("MASSIVE_API_KEY", "  ")
    assert create_market_source(PriceCache()).name == "simulator"
    monkeypatch.setenv("MASSIVE_API_KEY", "abc")
    assert create_market_source(PriceCache()).name == "massive"

async def test_sse_sends_snapshot_then_only_changes():
    cache = PriceCache(); cache.update("AAPL", 1.0); cache.update("MSFT", 2.0)

    class Req:
        async def is_disconnected(self): return False

    gen = price_event_stream(Req(), cache, push_interval=0)
    assert (await anext(gen)).startswith("retry:")
    first = json.loads((await anext(gen)).split("data: ", 1)[1])
    assert {p["ticker"] for p in first["prices"]} == {"AAPL", "MSFT"}
    cache.update("AAPL", 1.5)
    second = json.loads((await anext(gen)).split("data: ", 1)[1])
    assert [p["ticker"] for p in second["prices"]] == ["AAPL"]
    cache.remove("MSFT")
    third = json.loads((await anext(gen)).split("data: ", 1)[1])
    assert third["removed"] == ["MSFT"]
```

### 13.7 E2E hooks

E2E tests (in `test/`) run without `MASSIVE_API_KEY`, so they always use the simulator. Setting `SIM_SPEED=50` in `docker-compose.test.yml` makes price flashes and sparkline growth fast and reliable to assert on.

---

## 14. Implementation Checklist

1. `models.py`, `cache.py`, `tickers.py` + tests (13.1).
2. `interface.py`, `seeds.py`.
3. `GBMEngine` + tests (13.2), then `SimulatorSource` + tests (13.3).
4. `massive.py` parsing + client + source + tests (13.4) using `respx`.
5. `factory.py` + test, conformance tests (13.5).
6. `stream.py` + test (13.6); mount router; wire lifespan (10.1).
7. Hand-off to backend agent: sync calls from watchlist/trade/chat routes (10.2), watchlist response shape (10.3), health fields (10.4).
8. Manual smoke test: `uv run uvicorn app.main:app --reload` then `curl -N localhost:8000/api/stream/prices`.

## 15. Open Questions

- **Massive free tier and snapshots.** Needs confirming with a real free key. The design works either way (fallback in 8.1), but if the free tier is end-of-day only, `README.md` should tell users that real-time-looking data needs a paid plan or the simulator.
- **Simulator persistence across restarts.** Prices restart from seed values on every boot, while positions keep their `avg_cost`. That is acceptable for a demo. If it feels jarring, persist last prices to SQLite on shutdown and reseed from them on startup.
- **Market hours in simulator.** The simulator runs 24/7. That is intentional (demo always looks alive).
