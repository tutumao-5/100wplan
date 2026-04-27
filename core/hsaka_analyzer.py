import asyncio
import aiohttp
import numpy as np
from datetime import datetime, timezone
from pathlib import Path


PROXY = "http://127.0.0.1:7897"
OKX_BASE = "https://www.okx.com"


async def _fetch_candles(inst_id: str, bar: str = "1H", limit: int = 100) -> list:
    url = f"{OKX_BASE}/api/v5/market/candles"
    params = {"instId": inst_id, "bar": bar, "limit": str(limit)}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, proxy=PROXY, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            return data.get("data", [])


async def _fetch_funding_rate(inst_id: str) -> dict:
    url = f"{OKX_BASE}/api/v5/public/funding-rate"
    params = {"instId": inst_id}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, proxy=PROXY, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            return data.get("data", [{}])[0]


def _parse_candles(raw: list) -> dict:
    if not raw:
        return {}
    arr = np.array([[float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])] for c in raw])
    return {
        "open": arr[:, 0],
        "high": arr[:, 1],
        "low": arr[:, 2],
        "close": arr[:, 3],
        "volume": arr[:, 4],
    }


def _calc_rsi(close: np.ndarray, period: int = 14) -> np.ndarray:
    delta = np.diff(close)
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    avg_gain = np.convolve(gain, np.ones(period) / period, mode="valid")
    avg_loss = np.convolve(loss, np.ones(period) / period, mode="valid")
    rs = np.where(avg_loss == 0, np.inf, avg_gain / avg_loss)
    return 100 - (100 / (1 + rs))


def _calc_bollinger(close: np.ndarray, period: int = 20, std_mult: float = 2.0):
    if len(close) < period:
        return None, None, None
    ma = np.convolve(close, np.ones(period) / period, mode="valid")
    std = np.array([close[i:i + period].std() for i in range(len(close) - period + 1)])
    upper = ma + std_mult * std
    lower = ma - std_mult * std
    return ma, upper, lower


def _calc_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
    tr = np.maximum(high[1:] - low[1:], np.maximum(np.abs(high[1:] - close[:-1]), np.abs(low[1:] - close[:-1])))
    atr = np.convolve(tr, np.ones(period) / period, mode="valid")
    return atr


async def check_sfp(inst_id: str) -> dict:
    raw = await _fetch_candles(inst_id, bar="1H", limit=60)
    if not raw:
        return {"triggered": False, "signal": "neutral", "score": 0.0, "details": {"error": "no data"}}

    c = _parse_candles(raw)
    close = c["close"]
    low = c["low"]
    volume = c["volume"]

    lookback = 20
    recent_low = low[-lookback:-1].min()
    low_idx = np.argmin(low[-lookback:-1])
    vol_at_low = volume[-lookback + low_idx]

    current_close = close[-1]
    current_low = low[-1]
    current_vol = volume[-1]

    price_near_low = current_low <= recent_low * 1.005
    price_recovered = current_close > current_low * 1.002
    vol_contracted = current_vol < vol_at_low * 0.8

    triggered = price_near_low and price_recovered and vol_contracted
    signal = "long" if triggered else "neutral"

    return {
        "triggered": triggered,
        "signal": signal,
        "score": 1.0 if triggered else 0.0,
        "details": {
            "recent_low": float(recent_low),
            "current_low": float(current_low),
            "current_close": float(current_close),
            "price_near_low": price_near_low,
            "price_recovered": price_recovered,
            "vol_contracted": vol_contracted,
            "current_vol": float(current_vol),
            "vol_at_low": float(vol_at_low),
        },
    }


async def check_liquidity_sensing(inst_id: str) -> dict:
    raw = await _fetch_candles(inst_id, bar="1H", limit=60)
    if not raw:
        return {"triggered": False, "signal": "neutral", "score": 0.0, "details": {"error": "no data"}}

    c = _parse_candles(raw)
    close = c["close"]
    volume = c["volume"]

    _, bb_upper, bb_lower = _calc_bollinger(close, period=20)
    if bb_upper is None:
        return {"triggered": False, "signal": "neutral", "score": 0.0, "details": {"error": "insufficient data"}}

    current_close = close[-1]
    current_vol = volume[-1]
    prev_vol = volume[-2]
    vol_spike = current_vol > prev_vol * 1.5

    near_upper = current_close >= bb_upper[-1] * 0.998
    near_lower = current_close <= bb_lower[-1] * 1.002

    reversal_from_upper = near_upper and close[-1] < close[-2] and vol_spike
    reversal_from_lower = near_lower and close[-1] > close[-2] and vol_spike

    triggered = reversal_from_upper or reversal_from_lower
    if reversal_from_upper:
        signal = "short"
    elif reversal_from_lower:
        signal = "long"
    else:
        signal = "neutral"

    return {
        "triggered": triggered,
        "signal": signal,
        "score": 1.0 if triggered else 0.0,
        "details": {
            "bb_upper": float(bb_upper[-1]),
            "bb_lower": float(bb_lower[-1]),
            "current_close": float(current_close),
            "near_upper": near_upper,
            "near_lower": near_lower,
            "vol_spike": vol_spike,
            "current_vol": float(current_vol),
            "prev_vol": float(prev_vol),
        },
    }


async def check_supply_demand_zone(inst_id: str) -> dict:
    raw = await _fetch_candles(inst_id, bar="1H", limit=60)
    if not raw:
        return {"triggered": False, "signal": "neutral", "score": 0.0, "details": {"error": "no data"}}

    c = _parse_candles(raw)
    close = c["close"]
    high = c["high"]
    low = c["low"]

    atr = _calc_atr(high, low, close, period=14)
    if len(atr) < 20:
        return {"triggered": False, "signal": "neutral", "score": 0.0, "details": {"error": "insufficient data"}}

    current_atr = atr[-1]
    atr_mean = atr[-20:].mean()
    low_volatility = current_atr < atr_mean * 0.7

    lookback = 10
    zone_high = high[-lookback:-3].max()
    zone_low = low[-lookback:-3].min()
    zone_width = (zone_high - zone_low) / close[-1]
    narrow_range = zone_width < 0.02

    current_close = close[-1]
    breakout_up = current_close > zone_high * 1.001
    breakout_down = current_close < zone_low * 0.999

    triggered = (low_volatility or narrow_range) and (breakout_up or breakout_down)
    if breakout_up and triggered:
        signal = "long"
    elif breakout_down and triggered:
        signal = "short"
    else:
        signal = "neutral"

    return {
        "triggered": triggered,
        "signal": signal,
        "score": 1.0 if triggered else 0.0,
        "details": {
            "current_atr": float(current_atr),
            "atr_mean": float(atr_mean),
            "low_volatility": low_volatility,
            "zone_high": float(zone_high),
            "zone_low": float(zone_low),
            "zone_width_pct": float(zone_width * 100),
            "narrow_range": narrow_range,
            "breakout_up": breakout_up,
            "breakout_down": breakout_down,
        },
    }


async def check_bollinger_band(inst_id: str) -> dict:
    raw = await _fetch_candles(inst_id, bar="1H", limit=60)
    if not raw:
        return {"triggered": False, "signal": "neutral", "score": 0.0, "details": {"error": "no data"}}

    c = _parse_candles(raw)
    close = c["close"]

    _, bb_upper, bb_lower = _calc_bollinger(close, period=20, std_mult=2.0)
    if bb_upper is None:
        return {"triggered": False, "signal": "neutral", "score": 0.0, "details": {"error": "insufficient data"}}

    rsi = _calc_rsi(close, period=14)
    if len(rsi) == 0:
        return {"triggered": False, "signal": "neutral", "score": 0.0, "details": {"error": "insufficient rsi data"}}

    current_close = close[-1]
    current_rsi = float(rsi[-1])
    current_upper = float(bb_upper[-1])
    current_lower = float(bb_lower[-1])

    above_upper = current_close > current_upper
    below_lower = current_close < current_lower
    rsi_overbought = current_rsi > 70
    rsi_oversold = current_rsi < 30

    long_signal = below_lower and rsi_oversold
    short_signal = above_upper and rsi_overbought
    triggered = long_signal or short_signal

    if long_signal:
        signal = "long"
    elif short_signal:
        signal = "short"
    else:
        signal = "neutral"

    return {
        "triggered": triggered,
        "signal": signal,
        "score": 1.0 if triggered else 0.0,
        "details": {
            "current_close": float(current_close),
            "bb_upper": current_upper,
            "bb_lower": current_lower,
            "rsi": current_rsi,
            "above_upper": above_upper,
            "below_lower": below_lower,
            "rsi_overbought": rsi_overbought,
            "rsi_oversold": rsi_oversold,
        },
    }


async def check_funding_rate(inst_id: str) -> dict:
    fr_data = await _fetch_funding_rate(inst_id)
    if not fr_data:
        return {"triggered": False, "signal": "neutral", "score": 0.0, "details": {"error": "no funding rate data"}}

    funding_rate = float(fr_data.get("fundingRate", 0))
    threshold = 0.003

    bullish_extreme = funding_rate > threshold
    bearish_extreme = funding_rate < -threshold
    triggered = bullish_extreme or bearish_extreme

    if bullish_extreme:
        signal = "short"
    elif bearish_extreme:
        signal = "long"
    else:
        signal = "neutral"

    return {
        "triggered": triggered,
        "signal": signal,
        "score": 1.0 if triggered else 0.0,
        "details": {
            "funding_rate": funding_rate,
            "threshold": threshold,
            "bullish_extreme": bullish_extreme,
            "bearish_extreme": bearish_extreme,
            "next_funding_time": fr_data.get("nextFundingTime", ""),
        },
    }


async def check_range_fakeout(inst_id: str) -> dict:
    raw = await _fetch_candles(inst_id, bar="1H", limit=60)
    if not raw:
        return {"triggered": False, "signal": "neutral", "score": 0.0, "details": {"error": "no data"}}

    c = _parse_candles(raw)
    close = c["close"]
    high = c["high"]
    low = c["low"]

    lookback = 20
    range_high = high[-lookback:-3].max()
    range_low = low[-lookback:-3].min()

    fakeout_up = False
    fakeout_down = False

    for i in range(-3, -1):
        if high[i] > range_high and close[i] < range_high:
            fakeout_up = True
        if low[i] < range_low and close[i] > range_low:
            fakeout_down = True

    current_in_range = range_low <= close[-1] <= range_high
    triggered = (fakeout_up or fakeout_down) and current_in_range

    if fakeout_up and triggered:
        signal = "short"
    elif fakeout_down and triggered:
        signal = "long"
    else:
        signal = "neutral"

    return {
        "triggered": triggered,
        "signal": signal,
        "score": 1.0 if triggered else 0.0,
        "details": {
            "range_high": float(range_high),
            "range_low": float(range_low),
            "current_close": float(close[-1]),
            "fakeout_up": fakeout_up,
            "fakeout_down": fakeout_down,
            "current_in_range": current_in_range,
        },
    }


async def check_high_session(inst_id: str) -> dict:
    now_utc = datetime.now(timezone.utc)
    hour = now_utc.hour

    asia_session = 2 <= hour <= 5
    europe_session = 8 <= hour <= 11
    us_session = 14 <= hour <= 17

    in_high_session = asia_session or europe_session or us_session

    if asia_session:
        session_name = "asia"
    elif europe_session:
        session_name = "europe"
    elif us_session:
        session_name = "us"
    else:
        session_name = "off-hours"

    return {
        "triggered": in_high_session,
        "signal": "neutral",
        "score": 1.0 if in_high_session else 0.0,
        "details": {
            "utc_hour": hour,
            "session": session_name,
            "asia_session": asia_session,
            "europe_session": europe_session,
            "us_session": us_session,
        },
    }


async def run_all_checks(inst_id: str) -> dict:
    results = await asyncio.gather(
        check_sfp(inst_id),
        check_liquidity_sensing(inst_id),
        check_supply_demand_zone(inst_id),
        check_bollinger_band(inst_id),
        check_funding_rate(inst_id),
        check_range_fakeout(inst_id),
        check_high_session(inst_id),
        return_exceptions=True,
    )

    keys = ["sfp", "liquidity_sensing", "supply_demand_zone", "bollinger_band", "funding_rate", "range_fakeout", "high_session"]
    output = {}
    total_score = 0.0

    for key, result in zip(keys, results):
        if isinstance(result, Exception):
            import traceback as tb
            exc_summary = f"[Hsaka Check Exception] inst_id={inst_id} check={key} type={type(result).__name__} msg={str(result)}"
            exc_detail  = "".join(tb.format_exception(type(result), result, result.__traceback__))
            # 写独立日志文件，方便 cron 提取
            log_path = Path(__file__).parent.parent / "logs" / "hsaka_errors.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "a") as f:
                f.write(f"\n{'='*60}\nUTC={datetime.utcnow().isoformat()} | {exc_summary}\n{exc_detail}\n")
            output[key] = {"triggered": False, "signal": "neutral", "score": 0.0, "details": {"error": exc_summary}}
        else:
            output[key] = result
            total_score += result.get("score", 0.0)

    output["total_score"] = total_score
    return output
