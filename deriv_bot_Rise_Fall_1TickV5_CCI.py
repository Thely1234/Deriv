"""
Deriv RISE/FALL bot - CCI + momentum, ONE high-confidence trade per day.

Session flow (single pass, no loops after the trade):
    connect -> authenticate -> read balance -> scan symbols on a 1-minute data
    window until a setup with confidence >= MIN_CONFIDENCE is found -> place ONE
    RISE/FALL 1-tick contract -> poll the result -> log it -> terminate.

A state file (state/trades_YYYY-MM-DD.json) guarantees at most one trade per
calendar day even if the script is restarted.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

import websocket

# =====================
# CONFIG
# =====================
API_TOKEN = os.environ.get("DERIV_TOKEN", "").strip()

# Personal Access Tokens on the current API belong to an application
# registered at https://developers.deriv.com. With both values the script uses
# the OTP flow; ACCOUNT_ID may be left empty because it is discovered from the
# REST accounts endpoint.
APP_ID = os.environ.get("DERIV_APP_ID", "").strip()
ACCOUNT_ID = os.environ.get("DERIV_ACCOUNT_ID", "").strip()
PREFER_DEMO = os.environ.get("DERIV_PREFER_DEMO", "1") == "1"

WS_PUBLIC_URL = os.environ.get("DERIV_WS_URL", "wss://api.derivws.com/trading/v1/options/ws/public")
WS_TRADING_BASE = "wss://api.derivws.com/trading/v1/options/ws"
REST_BASE = os.environ.get("DERIV_REST_URL", "https://api.derivws.com")

SYMBOLS = [
    "1HZ10V", "1HZ25V", "1HZ50V", "1HZ75V", "1HZ100V",
    "1HZ30V", "1HZ15V", "1HZ90V",
    "R_10", "R_25", "R_50", "R_75", "R_100",
]

# ---- RISK (fixed stake, no progression, one trade per day) ----
STAKE = 0.35
CURRENCY = "USD"
DURATION = 5
DURATION_UNIT = "t"
# -----------------------------------------------------------

# ---- ANALYSIS (1-minute window) ----
ANALYSIS_WINDOW_SECONDS = 60
HISTORY_LOOKBACK_SECONDS = 120
HISTORY_TICK_COUNT = 120

MIN_TICKS = 20
MIN_CONFIDENCE = 80
MIN_AGREEMENT_CHECKS = 4
REQUIRE_UNANIMOUS = True
UNANIMOUS_TOLERANCE = 0.25
CONFLICT_PENALTY = 45.0
MOMENTUM_MIN = 0.5
MIN_EFFICIENCY = 0.40
EFFICIENCY_FULL = 0.75
EFFICIENCY_LOOKBACK = 12
MIN_NET_MOVE_RATIO = 1.5
MAX_SCAN_ROUNDS = 40
SCAN_ROUND_DELAY = 2.0
# Full-scale net weight. Calibrated from the check set below: four agreeing
# checks at full strength sum to ~4.2, so this makes the score span 50..97
# instead of saturating at the clamp for every surviving signal.
NET_RANGE_FULL_SCORE = 4.2
# ----------------------------------

CCI_PERIOD = 14
EMA_FAST = 9
EMA_SLOW = 20
RSI_PERIOD = 7

STATE_DIR = "state"
LOG_DIR = "logs"

RECV_TIMEOUT = 15.0
RATE_LIMIT_BACKOFF = 20.0


# =====================
# MODE FLAGS
# =====================
# Scan-only analyses the market and logs the setup without placing an order.
SCAN_ONLY = os.environ.get("DERIV_SCAN_ONLY", "0") == "1"


# =====================
# LOGGING
# =====================
class Log:
    def __init__(self, path):
        self.path = path
        self.handle = open(path, "a", encoding="utf-8")

    def write(self, message):
        stamp = time.strftime("%H:%M:%S")
        line = f"[{stamp}] {message}"
        print(line, flush=True)
        self.handle.write(line + "\n")
        self.handle.flush()

    def close(self):
        try:
            self.handle.close()
        except OSError:
            pass


# =====================
# WEBSOCKET CLIENT
# =====================
class DerivWS:
    """Thin, retrying wrapper around a Deriv WebSocket connection."""

    def __init__(self, log):
        self.log = log
        self.ws = None
        self.url = None

    def connect(self, url, attempts=5):
        last_error = None
        for attempt in range(1, attempts + 1):
            try:
                self.close()
                self.ws = websocket.create_connection(
                    url,
                    timeout=RECV_TIMEOUT,
                    header=["User-Agent: Mozilla/5.0"],
                    ping_interval=25,
                    ping_timeout=10,
                )
                self.url = url
                self.log.write(f"[WS] connected ({url})")
                return True
            except Exception as exc:  # noqa: BLE001 - report and retry
                last_error = exc
                self.log.write(f"[WS] connect failed (attempt {attempt}/{attempts}): {exc}")
                time.sleep(min(3 * attempt, 10))
        self.log.write(f"[WS] giving up on connection: {last_error}")
        return False

    def close(self):
        if self.ws is not None:
            try:
                self.ws.close()
            except Exception:  # noqa: BLE001 - closing must never raise
                pass
            self.ws = None

    @property
    def alive(self):
        try:
            return self.ws is not None and self.ws.connected
        except Exception:  # noqa: BLE001
            return False

    def _send(self, payload):
        self.ws.send(json.dumps(payload))

    def _answer_ping(self):
        self._send({"pong": 1})

    def request(self, payload, expected, recv_timeout=RECV_TIMEOUT,
                max_retries=3, allow_rate_limit_retry=True):
        """Send one request and return the first response of an expected type.

        Returns the parsed response dict, or None when the request failed.
        Errors are returned as-is so callers can inspect the code/message.
        """
        for attempt in range(1, max_retries + 1):
            if not self.alive:
                self.log.write("[WS] socket down, reconnecting")
                if not self.connect(self.url, attempts=2):
                    return None
            try:
                self.ws.settimeout(recv_timeout)
                self._send(payload)
                deadline = time.time() + recv_timeout
                while time.time() < deadline:
                    self.ws.settimeout(max(1.0, deadline - time.time()))
                    response = json.loads(self.ws.recv())
                    msg_type = response.get("msg_type")
                    if msg_type == "ping":
                        self._answer_ping()
                        continue
                    if msg_type in expected:
                        return response
                    if msg_type == "error":
                        error = response.get("error", {})
                        if (allow_rate_limit_retry
                                and error.get("code") == "RateLimit"
                                and attempt < max_retries):
                            wait = RATE_LIMIT_BACKOFF * attempt
                            self.log.write(f"[WS] rate limited, sleeping {wait:.0f}s")
                            time.sleep(wait)
                            break
                        return response
                else:
                    self.log.write(f"[WS] timeout waiting for {expected}")
                    return None
            except websocket.WebSocketTimeoutException:
                self.log.write(f"[WS] timeout (attempt {attempt}/{max_retries})")
                if attempt == max_retries:
                    return None
                self.connect(self.url, attempts=2)
            except Exception as exc:  # noqa: BLE001 - never let transport kill the run
                self.log.write(f"[WS] error (attempt {attempt}/{max_retries}): {exc}")
                if attempt == max_retries:
                    return None
                self.connect(self.url, attempts=2)
        return None

    def forget(self, sub_id):
        if not sub_id or not self.alive:
            return
        try:
            self.ws.settimeout(3.0)
            self._send({"forget": sub_id})
        except Exception as exc:  # noqa: BLE001
            self.log.write(f"[WS] forget failed: {exc}")

    def stream(self, payload, seconds, on_message):
        """Subscribe and feed live messages to `on_message` for `seconds`."""
        if not self.alive:
            return []
        collected = []
        try:
            self.ws.settimeout(5.0)
            self._send(payload)
            end = time.time() + seconds
            while time.time() < end:
                self.ws.settimeout(max(0.5, min(5.0, end - time.time())))
                try:
                    response = json.loads(self.ws.recv())
                except websocket.WebSocketTimeoutException:
                    continue
                msg_type = response.get("msg_type")
                if msg_type == "ping":
                    self._answer_ping()
                    continue
                if msg_type in ("tick", "history"):
                    collected.append(response)
                    on_message(response)
        except Exception as exc:  # noqa: BLE001 - stream is best effort
            self.log.write(f"[WS] stream ended: {exc}")
        return collected


# =====================
# AUTHENTICATION
# =====================
def rest_get(path):
    """GET a Deriv REST endpoint with the PAT credentials."""
    request = urllib.request.Request(f"{REST_BASE}{path}", method="GET")
    request.add_header("Authorization", f"Bearer {API_TOKEN}")
    request.add_header("Deriv-App-ID", APP_ID)
    request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")[:300]
        raise RuntimeError(f"GET {path} failed ({exc.code}): {body}") from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"GET {path} failed: {exc}") from exc


def discover_account_id(log):
    """Pick an account to trade, preferring demo when configured."""
    if ACCOUNT_ID:
        return ACCOUNT_ID
    payload = rest_get("/trading/v1/options/accounts")
    accounts = payload.get("data") or []
    if not accounts:
        return None
    wanted = "demo" if PREFER_DEMO else "real"
    for account in accounts:
        if account.get("account_type") == wanted:
            log.write(f"[ACCOUNT] using {wanted} account {account['account_id']} "
                      f"({account.get('balance')} {account.get('currency')})")
            return account["account_id"]
    fallback = accounts[0]["account_id"]
    log.write(f"[ACCOUNT] no {wanted} account, using {fallback}")
    return fallback


def rest_otp_url(account_id):
    """Request an authenticated WebSocket URL via the REST OTP endpoint."""
    endpoint = f"{REST_BASE}/trading/v1/options/accounts/{account_id}/otp"
    request = urllib.request.Request(endpoint, data=b"{}", method="POST")
    request.add_header("Authorization", f"Bearer {API_TOKEN}")
    request.add_header("Deriv-App-ID", APP_ID)
    request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")[:300]
        raise RuntimeError(f"OTP request failed ({exc.code}): {body}") from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"OTP request failed: {exc}") from exc
    url = (payload.get("data") or {}).get("url")
    if not url:
        raise RuntimeError(f"OTP response without url: {payload}")
    return url


AUTH_HELP = [
    "This Deriv account cannot be reached with the current credentials:",
    "  - The current API requires a Personal Access Token that belongs to an",
    "    application you registered at https://developers.deriv.com.",
    "  - Provide both values, then run again:",
    "        $env:DERIV_APP_ID = \"<app id from the dashboard>\"",
    "        $env:DERIV_TOKEN = \"<a freshly created PAT>\"",
    "    DERIV_ACCOUNT_ID is optional - it is discovered automatically.",
    "  - To rehearse the analysis without an account, set DERIV_SCAN_ONLY=1.",
]


def authenticate(client, log):
    """Authenticate the socket, preferring the OTP flow when configured."""
    if not APP_ID and not API_TOKEN:
        log.write("[AUTH] no credentials configured")
        for line in AUTH_HELP:
            log.write(f"[AUTH] {line}")
        return False
    if bool(APP_ID) != bool(API_TOKEN):
        missing = "DERIV_TOKEN" if APP_ID else "DERIV_APP_ID"
        log.write(f"[AUTH] {missing} is missing, cannot authenticate")
        for line in AUTH_HELP:
            log.write(f"[AUTH] {line}")
        return False

    if APP_ID and API_TOKEN:
        try:
            account_id = discover_account_id(log)
            if not account_id:
                raise RuntimeError("no account available for this token")
            url = rest_otp_url(account_id)
            if client.connect(url):
                log.write("[AUTH] authenticated via OTP")
                return True
            log.write("[AUTH] could not open the OTP socket")
        except RuntimeError as exc:
            log.write(f"[AUTH] OTP flow failed: {exc}")
            for line in AUTH_HELP:
                log.write(f"[AUTH] {line}")

    if not API_TOKEN:
        log.write("[AUTH] no token configured")
        return False

    response = client.request({"authorize": API_TOKEN}, ["authorize"], recv_timeout=20)
    if not response:
        log.write("[AUTH] no response to authorize")
        return False
    if "error" in response:
        error = response["error"]
        log.write(f"[AUTH] failed: {error.get('code')} - {error.get('message')}")
        for line in AUTH_HELP:
            log.write(f"[AUTH] {line}")
        return False
    account = response["authorize"]
    log.write(f"[AUTH] ok | loginid {account.get('loginid')} | "
              f"{'demo' if account.get('is_virtual') else 'real'} account")
    return True


def get_balance(client, log):
    response = client.request({"balance": 1}, ["balance"])
    if not response or "error" in response:
        return None
    return response.get("balance", {})


# =====================
# TICK COLLECTION
# =====================
def collect_windows(client, symbols, log, watch_seconds=ANALYSIS_WINDOW_SECONDS):
    """Build the analysis window for every symbol from one subscription.

    Subscribing to all symbols in a single request means one 60s wait covers
    the whole watchlist instead of one wait per symbol.
    """
    buffers = {symbol: [] for symbol in symbols}
    pip_sizes = {}
    sub_ids = {}

    def on_message(response):
        tick = response.get("tick") or {}
        symbol = tick.get("symbol")
        if symbol in buffers and tick.get("quote") is not None:
            buffers[symbol].append((float(tick["epoch"]), float(tick["quote"])))
            if tick.get("pip_size") is not None:
                pip_sizes[symbol] = int(tick["pip_size"])

    if client.alive:
        for symbol in symbols:
            response = client.request({
                "ticks_history": symbol,
                "start": int(time.time()) - HISTORY_LOOKBACK_SECONDS,
                "end": "latest",
                "count": HISTORY_TICK_COUNT,
                "style": "ticks",
                "adjust_start_time": 1,
            }, ["history"], recv_timeout=25)
            history = (response or {}).get("history", {})
            for tick in history.get("ticks", []):
                if tick.get("quote") is None:
                    continue
                buffers[symbol].append((float(tick["epoch"]), float(tick["quote"])))
                if tick.get("pip_size") is not None:
                    pip_sizes[symbol] = int(tick["pip_size"])

    history_counts = {symbol: len(buffers[symbol]) for symbol in symbols}

    stream = client.stream({"ticks": list(symbols)}, watch_seconds, on_message)
    for response in stream:
        sub_id = (response.get("subscription") or {}).get("id")
        if sub_id:
            sub_ids.setdefault(sub_id, True)
    for sub_id in sub_ids:
        client.forget(sub_id)

    cutoff = time.time() - ANALYSIS_WINDOW_SECONDS
    windows = {}
    for symbol in symbols:
        samples = sorted(buffers[symbol], key=lambda item: item[0])
        window = [sample for sample in samples if sample[0] >= cutoff]
        windows[symbol] = (window, pip_sizes.get(symbol, 4))
        if not window:
            log.write(f"[DATA] {symbol}: no ticks")
        else:
            log.write(f"[DATA] {symbol}: window {len(window)} ticks "
                      f"(history {history_counts[symbol]})")
    return windows


# =====================
# INDICATORS
# =====================
def sma(values, period):
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def ema_series(values, period):
    if len(values) < period:
        return []
    multiplier = 2.0 / (period + 1)
    seed = sum(values[:period]) / period
    series = [seed]
    for value in values[period:]:
        series.append((value - series[-1]) * multiplier + series[-1])
    return series


def mean_deviation(values, period):
    if len(values) < period:
        return None
    window = values[-period:]
    average = sum(window) / period
    return sum(abs(value - average) for value in window) / period


def cci_series(prices, period=CCI_PERIOD):
    """CCI over a tick stream, computed on the close of each tick."""
    if len(prices) < period + 1:
        return []
    typical = prices
    series = []
    for end in range(period, len(typical) + 1):
        window = typical[end - period:end]
        average = sum(window) / period
        deviation = sum(abs(value - average) for value in window) / period
        if deviation == 0:
            series.append(0.0)
        else:
            series.append((typical[end - 1] - average) / (0.015 * deviation))
    return series


def rsi_series(prices, period=RSI_PERIOD):
    if len(prices) < period + 1:
        return []
    gains, losses = [], []
    for previous, current in zip(prices, prices[1:]):
        change = current - previous
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    values = []
    for index in range(period, len(gains) + 1):
        window_gains = gains[index - period:index]
        window_losses = losses[index - period:index]
        avg_gain = sum(window_gains) / period
        avg_loss = sum(window_losses) / period
        if avg_loss == 0:
            values.append(100.0)
        else:
            rs = avg_gain / avg_loss
            values.append(100.0 - (100.0 / (1.0 + rs)))
    return values


def regression_slope(values):
    """Least-squares slope of `values` against their index."""
    count = len(values)
    if count < 2:
        return 0.0
    mean_x = (count - 1) / 2.0
    mean_y = sum(values) / count
    numerator = sum((i - mean_x) * (value - mean_y) for i, value in enumerate(values))
    denominator = sum((i - mean_x) ** 2 for i in range(count))
    if denominator == 0:
        return 0.0
    return numerator / denominator


def mean_tick_range(prices):
    if len(prices) < 2:
        return 0.0
    moves = [abs(current - previous) for previous, current in zip(prices, prices[1:])]
    return sum(moves) / len(moves)


def efficiency_ratio(prices):
    """Kaufman efficiency ratio: 1.0 = straight line, ~1/sqrt(n) = random walk.

    This separates a genuinely directional window from a noisy drift, which
    the trend-following checks cannot tell apart on their own.
    """
    if len(prices) < 2:
        return 0.0
    path = sum(abs(current - previous)
               for previous, current in zip(prices, prices[1:]))
    if path == 0:
        return 0.0
    return abs(prices[-1] - prices[0]) / path


def to_pips(value, pip_size):
    return value * (10 ** pip_size)


def guess_pip_size(prices):
    """Fallback when the tick payload does not report a pip size."""
    magnitude = max(abs(prices[0]), 1e-9)
    if magnitude < 1:
        return 4
    if magnitude < 100:
        return 2
    return 0


# =====================
# SIGNAL ENGINE
# =====================
@dataclass
class Check:
    name: str
    vote: int          # +1 bullish, -1 bearish, 0 neutral
    strength: float    # 0..1


@dataclass
class Verdict:
    symbol: str
    direction: str = ""
    confidence: int = 0
    reason: str = ""
    checks: list = field(default_factory=list)
    metrics: dict = field(default_factory=dict)


def add_check(checks, name, vote, strength):
    if vote and strength > 0:
        checks.append(Check(name, vote, min(1.0, strength)))


def evaluate_window(symbol, window, pip_size=None):
    """Score a 1-minute tick window; higher confidence means stronger conviction."""
    verdict = Verdict(symbol=symbol)
    prices = [quote for _, quote in window]
    if len(prices) < MIN_TICKS:
        verdict.reason = f"only {len(prices)} ticks in window (need {MIN_TICKS})"
        return verdict

    cci = cci_series(prices)
    rsi = rsi_series(prices)
    fast = ema_series(prices, EMA_FAST)
    slow = ema_series(prices, EMA_SLOW)
    tick_range = mean_tick_range(prices)
    efficiency = efficiency_ratio(prices[-EFFICIENCY_LOOKBACK:])
    efficiency_full = efficiency_ratio(prices)
    slope = regression_slope(prices[-min(len(prices), 20):])
    momentum = slope / tick_range if tick_range > 0 else 0.0
    net_range = abs(prices[-1] - prices[0])
    pip_size = pip_size if pip_size is not None else guess_pip_size(prices)

    verdict.metrics = {
        "last": prices[-1],
        "ticks": len(prices),
        "pip_size": pip_size,
        "net_pips": round(to_pips(net_range, pip_size), 1),
        "range_pips": round(to_pips(tick_range, pip_size), 2),
        "momentum": round(momentum, 2),
        "efficiency": round(efficiency, 3),
        "efficiency_full": round(efficiency_full, 3),
        "cci": round(cci[-1], 1) if cci else None,
        "rsi": round(rsi[-1], 1) if rsi else None,
        "ema_fast": round(fast[-1], 5) if fast else None,
        "ema_slow": round(slow[-1], 5) if slow else None,
    }

    # --- Volatility gate: no edge in a dead or chaotic market -------------
    # Scale-free, so it behaves the same on a 1HZ10V index and on R_100.
    if tick_range <= 0:
        verdict.reason = "flat ticks"
        return verdict
    if net_range < MIN_NET_MOVE_RATIO * tick_range:
        verdict.reason = (f"net move {verdict.metrics['net_pips']} pips below "
                          f"{MIN_NET_MOVE_RATIO}x tick size")
        return verdict
    if efficiency < MIN_EFFICIENCY:
        verdict.reason = (f"no directional structure (efficiency {efficiency:.2f} "
                          f"< {MIN_EFFICIENCY})")
        return verdict

    checks = []

    # 0. Directional quality of the recent leg (orthogonal to trend checks)
    recent = prices[-EFFICIENCY_LOOKBACK:]
    add_check(checks, "efficiency",
              1 if recent[-1] > recent[0] else -1,
              min(1.0, efficiency / EFFICIENCY_FULL))

    # 1. CCI exhaustion / reversal (primary signal)
    if len(cci) >= 3:
        current, previous, oldest = cci[-1], cci[-2], cci[-3]
        verdict.metrics["cci_prev"] = round(previous, 1)
        verdict.metrics["cci_slope"] = round(current - previous, 1)
        if abs(current) >= 100 and abs(current) < abs(previous):
            add_check(checks, "cci-reversal",
                      -1 if current > 0 else 1,
                      min(1.0, abs(current) / 200.0))
        elif current * previous < 0:
            add_check(checks, "cci-zero-cross",
                      1 if current > 0 else -1,
                      min(1.0, abs(current - previous) / 120.0))
        elif abs(current) >= 100 and abs(current) > abs(previous):
            add_check(checks, "cci-trend",
                      1 if current > 0 else -1,
                      min(1.0, abs(current) / 250.0))
        elif abs(current) < 40 and abs(current - oldest) < 20:
            verdict.reason = f"CCI flat near zero ({current:.0f})"
            return verdict

    # 2. Trend filter: EMA fast vs EMA slow and price alignment
    if fast and slow:
        fast_now, slow_now, fast_prev = fast[-1], slow[-1], fast[-2]
        verdict.metrics["ema_slope"] = round(fast_now - fast_prev, 5)
        if fast_now > slow_now and prices[-1] > slow_now:
            add_check(checks, "ema-uptrend", 1, 0.7)
        elif fast_now < slow_now and prices[-1] < slow_now:
            add_check(checks, "ema-downtrend", -1, 0.7)
        if fast_now > fast_prev:
            add_check(checks, "ema-slope", 1, 0.5)
        elif fast_now < fast_prev:
            add_check(checks, "ema-slope", -1, 0.5)

    # 3. Momentum: regression slope relative to typical tick size
    if abs(momentum) >= MOMENTUM_MIN:
        add_check(checks, "momentum", 1 if momentum > 0 else -1,
                  min(1.0, abs(momentum) / (MOMENTUM_MIN * 3)))

    # 4. RSI exhaustion or trend confirmation
    if len(rsi) >= 2:
        current_rsi, previous_rsi = rsi[-1], rsi[-2]
        if current_rsi <= 30 and current_rsi > previous_rsi:
            add_check(checks, "rsi-oversold-recovery", 1,
                      min(1.0, (30 - current_rsi) / 30.0 + 0.4))
        elif current_rsi >= 70 and current_rsi < previous_rsi:
            add_check(checks, "rsi-overbought-fade", -1,
                      min(1.0, (current_rsi - 70) / 30.0 + 0.4))
        elif current_rsi >= 55 and current_rsi > previous_rsi:
            add_check(checks, "rsi-bullish", 1, min(1.0, (current_rsi - 55) / 25.0))
        elif current_rsi <= 45 and current_rsi < previous_rsi:
            add_check(checks, "rsi-bearish", -1, min(1.0, (45 - current_rsi) / 25.0))

    if not checks:
        verdict.reason = "no directional evidence"
        return verdict

    net = sum(check.vote * check.strength for check in checks)
    total = sum(check.strength for check in checks)
    backing = [check for check in checks if check.vote * net > 0]
    opposing = [check for check in checks if check.vote * net < 0]
    backing_weight = sum(check.strength for check in backing)
    opposing_weight = sum(check.strength for check in opposing)
    agreement = backing_weight / total
    saturation = min(1.0, abs(net) / NET_RANGE_FULL_SCORE)
    # Conflicting evidence scales confidence down instead of vetoing outright,
    # so a strong trend is still tradable while a fading CCI caps it.
    conflict_penalty = CONFLICT_PENALTY * (opposing_weight / total)
    confidence = int(round(50 + 50 * agreement * saturation - conflict_penalty))

    verdict.checks = [(check.name, check.vote, round(check.strength, 2)) for check in checks]
    verdict.metrics["opposing"] = [check.name for check in opposing]
    verdict.confidence = max(0, min(97, confidence))

    if len(backing) < MIN_AGREEMENT_CHECKS:
        verdict.reason = f"only {len(backing)} confirmations (need {MIN_AGREEMENT_CHECKS})"
        return verdict
    if REQUIRE_UNANIMOUS and opposing_weight > UNANIMOUS_TOLERANCE:
        verdict.reason = ("conflicting signals: "
                          + ",".join(check.name for check in opposing))
        return verdict
    if not any(check.name.startswith("cci") for check in backing):
        verdict.reason = "no CCI confirmation"
        return verdict
    if verdict.confidence < MIN_CONFIDENCE:
        verdict.reason = f"confidence {verdict.confidence}% < {MIN_CONFIDENCE}%"
        return verdict

    verdict.direction = "CALL" if net > 0 else "PUT"
    verdict.reason = (f"{verdict.confidence}% confidence, "
                      f"{int(round(abs(net) * 100)) / 100} net weight")
    return verdict


def describe(verdict):
    metrics = verdict.metrics or {}
    parts = [
        f"last={metrics.get('last')}",
        f"net={metrics.get('net_pips')}p",
        f"mom={metrics.get('momentum')}",
        f"cci={metrics.get('cci')}",
        f"rsi={metrics.get('rsi')}",
    ]
    if verdict.direction:
        parts.append(f"-> {verdict.direction} {verdict.confidence}%")
    else:
        parts.append(f"-> skip ({verdict.confidence}% {verdict.reason})")
    return " | ".join(parts)


# =====================
# MARKET SCAN
# =====================
def scan_for_setup(client, log):
    """Evaluate the whole watchlist each pass until one clears the filter."""
    for round_number in range(1, MAX_SCAN_ROUNDS + 1):
        log.write(f"[SCAN] pass {round_number}/{MAX_SCAN_ROUNDS}")
        try:
            windows = collect_windows(client, SYMBOLS, log)
        except Exception as exc:  # noqa: BLE001 - transport trouble must not crash the run
            log.write(f"[SCAN] pass failed: {exc}")
            time.sleep(SCAN_ROUND_DELAY)
            continue

        best = None
        for symbol in SYMBOLS:
            window, pip_size = windows.get(symbol) or ([], 4)
            if not window:
                continue
            try:
                verdict = evaluate_window(symbol, window, pip_size)
            except Exception as exc:  # noqa: BLE001 - one bad symbol must not stop the pass
                log.write(f"[SCAN] {symbol} failed: {exc}")
                continue
            log.write(f"[SCAN] {describe(verdict)}")
            if verdict.direction and (best is None
                                      or verdict.confidence > best[1].confidence):
                best = (symbol, verdict)

        if best is not None:
            symbol, verdict = best
            log.write(f"[SCAN] SETUP FOUND on {symbol}: "
                      f"{verdict.direction} at {verdict.confidence}%")
            return verdict
        time.sleep(SCAN_ROUND_DELAY)
    log.write("[SCAN] no setup reached the confidence threshold")
    return None


# =====================
# TRADING
# =====================
def place_risefall_trade(client, symbol, direction, log):
    stake = round(STAKE, 2)
    proposal = client.request({
        "proposal": 1,
        "amount": stake,
        "basis": "stake",
        "contract_type": direction,
        "currency": CURRENCY,
        "duration": DURATION,
        "duration_unit": DURATION_UNIT,
        "underlying_symbol": symbol,
    }, ["proposal"])

    if not proposal or "error" in proposal:
        error = (proposal or {}).get("error", {})
        log.write(f"[TRADE] proposal rejected: {error.get('code')} {error.get('message')}")
        return None
    if "proposal" not in proposal:
        log.write("[TRADE] malformed proposal response")
        return None

    details = proposal["proposal"]
    log.write(f"[TRADE] proposal ok | ask {details.get('ask_price')} "
              f"payout {details.get('payout')}")

    buy = client.request({"buy": proposal["proposal"]["id"], "price": stake}, ["buy"])
    if not buy or "error" in buy or "buy" not in buy:
        error = (buy or {}).get("error", {})
        log.write(f"[TRADE] buy failed: {error.get('code')} {error.get('message')}")
        return None

    contract = buy["buy"]
    log.write(f"[TRADE] BOUGHT {direction} {symbol} "
              f"contract {contract.get('contract_id')} for {stake} {CURRENCY}")
    return {
        "contract_id": contract.get("contract_id"),
        "buy_price": contract.get("buy_price"),
        "payout": contract.get("payout"),
        "longcode": contract.get("longcode"),
    }


def await_result(client, contract_id, log, max_wait=180):
    """Poll the open contract until it is sold."""
    delay = 1.0
    end = time.time() + max_wait
    while time.time() < end:
        response = client.request({
            "proposal_open_contract": 1,
            "contract_id": int(contract_id),
        }, ["proposal_open_contract"], recv_timeout=20)

        if response and "proposal_open_contract" in response:
            contract = response["proposal_open_contract"]
            if contract.get("is_sold"):
                return contract
        time.sleep(delay)
        delay = min(delay * 1.15, 5.0)

    log.write(f"[TRADE] timed out waiting for contract {contract_id}")
    return None


# =====================
# DAILY STATE
# =====================
def trade_state_path(day):
    return os.path.join(STATE_DIR, f"trades_{day}.json")


def load_today_record(day):
    path = trade_state_path(day)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def save_today_record(day, record):
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(trade_state_path(day), "w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=2)


# =====================
# SESSION
# =====================
def banner(log, day):
    log.write("=" * 60)
    log.write("   DERIV RISE/FALL - ONE TRADE PER DAY - CCI STRATEGY")
    log.write("=" * 60)
    log.write(f" Date          : {day}")
    log.write(f" Account       : {'configured' if API_TOKEN and APP_ID else 'NOT CONFIGURED'}")
    log.write(f" Symbols       : {len(SYMBOLS)}")
    log.write(f" Stake         : {STAKE} {CURRENCY} (fixed)")
    log.write(f" Contract      : {DURATION} {DURATION_UNIT} RISE/FALL")
    log.write(f" Window        : {ANALYSIS_WINDOW_SECONDS}s | min ticks {MIN_TICKS}")
    log.write(f" Confidence    : >= {MIN_CONFIDENCE}% "
              f"(CCI{CCI_PERIOD}, EMA{EMA_FAST}/{EMA_SLOW}, RSI{RSI_PERIOD})")
    log.write(f" Scan passes   : {MAX_SCAN_ROUNDS}")
    if SCAN_ONLY:
        log.write(" MODE         : SCAN ONLY (no orders will be placed)")
    log.write("=" * 60)


def run_session(log):
    day = time.strftime("%Y-%m-%d")
    existing = load_today_record(day)
    if existing and existing.get("traded"):
        log.write(f"[DAY] a trade already exists for {day}: "
                  f"{existing.get('direction')} {existing.get('symbol')} "
                  f"profit {existing.get('profit')}")
        log.write("[DAY] one trade per day enforced, nothing to do")
        return 0
    if existing:
        log.write(f"[DAY] previous attempt did not trade ({existing.get('reason')}), "
                  f"scanning again")

    client = DerivWS(log)
    try:
        if not client.connect(WS_PUBLIC_URL):
            log.write("[FATAL] cannot reach Deriv")
            return 1

        # Market data works without an account, so the analysis can still run.
        authenticated = authenticate(client, log)
        if not authenticated:
            if not SCAN_ONLY:
                log.write("[FATAL] authentication failed")
                return 1
            log.write("[MODE] scan-only: analysing and logging, no order will be placed")
        else:
            balance = get_balance(client, log)
            if balance:
                log.write(f"[ACCOUNT] {balance.get('loginid')} | "
                          f"balance {balance.get('balance')} {balance.get('currency')} "
                          f"({'demo' if balance.get('is_virtual') else 'real'})")
                if balance.get("is_virtual") is False and PREFER_DEMO:
                    log.write("[MODE] warning: real account, demo was preferred")

        verdict = scan_for_setup(client, log)
        if not verdict:
            log.write("[DONE] no qualifying trade today - session closed")
            save_today_record(day, {"date": day, "traded": False,
                                    "reason": "no setup above confidence threshold"})
            return 0

        if SCAN_ONLY or not authenticated:
            log.write("=" * 60)
            log.write(f" SETUP (not placed): {verdict.direction} {verdict.symbol}")
            log.write(f" confidence {verdict.confidence}% | {verdict.reason}")
            log.write(f" metrics {verdict.metrics}")
            log.write("=" * 60)
            reason = ("scan-only mode" if SCAN_ONLY
                      else "no valid account credentials")
            save_today_record(day, {
                "date": day, "traded": False, "reason": reason,
                "symbol": verdict.symbol, "direction": verdict.direction,
                "confidence": verdict.confidence,
                "metrics": verdict.metrics, "checks": verdict.checks,
            })
            return 0

        trade = place_risefall_trade(client, verdict.symbol, verdict.direction, log)
        if not trade:
            log.write("[DONE] trade could not be placed - session closed")
            save_today_record(day, {"date": day, "traded": False,
                                    "reason": "order rejected",
                                    "symbol": verdict.symbol,
                                    "direction": verdict.direction})
            return 0

        log.write("[WAIT] polling contract result")
        contract = await_result(client, trade["contract_id"], log)

        record = {
            "date": day,
            "traded": True,
            "symbol": verdict.symbol,
            "direction": verdict.direction,
            "confidence": verdict.confidence,
            "reason": verdict.reason,
            "contract_id": trade["contract_id"],
            "stake": trade["buy_price"],
            "payout": trade["payout"],
            "metrics": verdict.metrics,
            "checks": verdict.checks,
        }

        if contract is None:
            # The trade exists, so the day is still consumed - but the result
            # is unknown, so do not report it as a loss.
            record["outcome"] = "UNKNOWN"
            log.write("[RESULT] unknown - contract still open or unreachable")
            save_today_record(day, record)
            return 3

        profit = float(contract.get("profit", 0.0))
        record["profit"] = profit
        record["outcome"] = "WIN" if profit > 0 else "LOSS"
        log.write("=" * 60)
        log.write(f" RESULT: {record['outcome']} | profit {profit:+.2f} {CURRENCY}")
        log.write(f" {verdict.symbol} {verdict.direction} @ {verdict.confidence}% "
                  f"({verdict.reason})")
        log.write("=" * 60)
        save_today_record(day, record)
        return 0 if profit > 0 else 2
    finally:
        client.close()


def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    day = time.strftime("%Y-%m-%d")
    log = Log(os.path.join(LOG_DIR, f"session_{day}.log"))
    try:
        banner(log, day)
        return run_session(log)
    except KeyboardInterrupt:
        log.write("[!] interrupted by user - session closed without a second trade")
        return 130
    except Exception as exc:  # noqa: BLE001 - report and exit cleanly
        log.write(f"[FATAL] {type(exc).__name__}: {exc}")
        return 1
    finally:
        log.close()


if __name__ == "__main__":
    sys.exit(main())