"""
OKX永续合约扫描模块 - 漏斗降维扫描引擎
严格按白皮书第四章4.1/4.2节执行七层过滤流程
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
import numpy as np

from core.trade_db import TradeDB, get_db
import core.hsaka_analyzer as hsaka_analyzer
from core.pattern_learner import get_learner, EVOLUTION_THRESHOLD

logger = logging.getLogger(__name__)

# ── 路径配置 ─────────────────────────────────────────────────────────────────
BASE_DIR = Path("/home/jwx/okx-trading-agent-2")
SECTORS_PATH = BASE_DIR / "config" / "sectors.json"
OKX_BASE = "https://www.okx.com"
PROXY = "http://127.0.0.1:7897"

# ── 技术指标参数 ──────────────────────────────────────────────────────────────
RSI_PERIOD = 14
RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 65
VMA_PERIOD = 20
VMA_RATIO_THRESHOLD = 1.8
SCORE_THRESHOLD = 5.5
KLINE_LIMIT = 100

HSAKA_KEYS = [
    "sfp",
    "liquidity_sensing",
    "supply_demand_zone",
    "bollinger_band",
    "funding_rate",
    "range_fakeout",
    "high_session",
]

# ── 129个币种（OKX永续合约USDT本位）─────────────────────────────────────────
SYMBOLS: List[str] = [
    # 主流币
    "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "XRP-USDT-SWAP", "DOGE-USDT-SWAP",
    "ADA-USDT-SWAP", "AVAX-USDT-SWAP", "SHIB-USDT-SWAP", "DOT-USDT-SWAP", "LINK-USDT-SWAP",
    "MATIC-USDT-SWAP", "ATOM-USDT-SWAP", "UNI-USDT-SWAP", "LTC-USDT-SWAP", "ETC-USDT-SWAP",
    "XLM-USDT-SWAP", "NEAR-USDT-SWAP", "APT-USDT-SWAP", "ARB-USDT-SWAP", "OP-USDT-SWAP",
    "FIL-USDT-SWAP", "ICP-USDT-SWAP", "VET-USDT-SWAP", "HBAR-USDT-SWAP", "EGLD-USDT-SWAP",
    "AAVE-USDT-SWAP", "GRT-USDT-SWAP", "ALGO-USDT-SWAP", "FTM-USDT-SWAP", "SAND-USDT-SWAP",
    "MANA-USDT-SWAP", "AXS-USDT-SWAP", "ENJ-USDT-SWAP", "GALA-USDT-SWAP", "THETA-USDT-SWAP",
    "EOS-USDT-SWAP", "XTZ-USDT-SWAP", "CAKE-USDT-SWAP", "FLOW-USDT-SWAP", "SNX-USDT-SWAP",
    "CHZ-USDT-SWAP", "CRV-USDT-SWAP", "MKR-USDT-SWAP", "LDO-USDT-SWAP", "RPL-USDT-SWAP",
    "GMX-USDT-SWAP", "COMP-USDT-SWAP", "SUSHI-USDT-SWAP", "ZEC-USDT-SWAP", "DASH-USDT-SWAP",
    "NEO-USDT-SWAP", "WAVES-USDT-SWAP", "KAVA-USDT-SWAP", "ZIL-USDT-SWAP", "IOTA-USDT-SWAP",
    "KSM-USDT-SWAP", "AXL-USDT-SWAP", "STX-USDT-SWAP", "SUI-USDT-SWAP", "SEI-USDT-SWAP",
    "TIA-USDT-SWAP", "WIF-USDT-SWAP", "PEPE-USDT-SWAP", "BONK-USDT-SWAP", "FLOKI-USDT-SWAP",
    "WLD-USDT-SWAP", "PYTH-USDT-SWAP", "JUP-USDT-SWAP", "ZRO-USDT-SWAP", "NOT-USDT-SWAP",
    "IO-USDT-SWAP", "PENDLE-USDT-SWAP", "ENA-USDT-SWAP", "W-USDT-SWAP", "BNB-USDT-SWAP",
    "OKB-USDT-SWAP", "MNT-USDT-SWAP", "RENDER-USDT-SWAP", "TAO-USDT-SWAP", "FET-USDT-SWAP",
    "AGIX-USDT-SWAP", "OCEAN-USDT-SWAP", "NMR-USDT-SWAP", "INJ-USDT-SWAP", "SUI-USDT-SWAP",
    "BCH-USDT-SWAP", "BSV-USDT-SWAP", "AR-USDT-SWAP", "LUNA-USDT-SWAP", "IMX-USDT-SWAP",
    "DYM-USDT-SWAP", "TIA-USDT-SWAP", "STRD-USDT-SWAP", "OSMO-USDT-SWAP", "CHR-USDT-SWAP",
    "HIGH-USDT-SWAP", "ALPHA-USDT-SWAP", "BAND-USDT-SWAP", "BSW-USDT-SWAP", "TRAC-USDT-SWAP",
    # 额外补充至129个
    "ACH-USDT-SWAP", "CFX-USDT-SWAP", "CYBER-USDT-SWAP", "DODO-USDT-SWAP", "ID-USDT-SWAP",
    "ILV-USDT-SWAP", "JOE-USDT-SWAP", "KEY-USDT-SWAP", "LINA-USDT-SWAP", "LOKA-USDT-SWAP",
    "LOOM-USDT-SWAP", "MAGIC-USDT-SWAP", "MASK-USDT-SWAP", "MC-USDT-SWAP", "MINA-USDT-SWAP",
    "MTL-USDT-SWAP", "OGN-USDT-SWAP", "OXT-USDT-SWAP", "PERP-USDT-SWAP", "PROM-USDT-SWAP",
    "QI-USDT-SWAP", "RAD-USDT-SWAP", "RARE-USDT-SWAP", "RAY-USDT-SWAP", "RIF-USDT-SWAP",
    "RLC-USDT-SWAP", "SXP-USDT-SWAP", "SYS-USDT-SWAP", "T-USDT-SWAP", "TRU-USDT-SWAP",
    "UNFI-USDT-SWAP", "UTK-USDT-SWAP", "VOXEL-USDT-SWAP", "WAXP-USDT-SWAP", "XEM-USDT-SWAP",
    "YFI-USDT-SWAP", "YGG-USDT-SWAP", "ZRX-USDT-SWAP", "ZKSYNC-USDT-SWAP",
]


# ── 公共工具 ──────────────────────────────────────────────────────────────────

def get_available_coins() -> List[str]:
    """从sectors.json加载所有币种，去重后转换为OKX inst_id格式"""
    with open(SECTORS_PATH, "r") as f:
        data = json.load(f)
    seen: Set[str] = set()
    coins: List[str] = []
    for sector_coins in data.get("sectors", {}).values():
        for coin in sector_coins:
            key = coin.upper()
            if key not in seen:
                seen.add(key)
                coins.append(f"{key}-USDT-SWAP")
    return coins


def _get_sector(inst_id: str) -> Optional[str]:
    """从inst_id查找所属板块"""
    try:
        with open(SECTORS_PATH, "r") as f:
            data = json.load(f)
        base = inst_id.split("-")[0].upper()
        for sector_name, coins in data.get("sectors", {}).items():
            if base in [c.upper() for c in coins]:
                return sector_name
    except Exception:
        pass
    return None


# ── K线拉取 ───────────────────────────────────────────────────────────────────

async def _fetch_single(
    session: aiohttp.ClientSession, inst_id: str
) -> Tuple[str, Optional[List]]:
    """单币种K线拉取，指数退避重试 1s→2s→4s，每次超时10s"""
    url = f"{OKX_BASE}/api/v5/market/candles"
    params = {"instId": inst_id, "bar": "1H", "limit": str(KLINE_LIMIT)}
    for delay in [0, 1, 2, 4]:
        if delay:
            await asyncio.sleep(delay)
        try:
            async with session.get(
                url, params=params,
                proxy=PROXY,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                body = await resp.json()
                raw = body.get("data", [])
                if raw:
                    return inst_id, raw
        except Exception:
            pass
    return inst_id, None


async def _fetch_klines_batch(coins: List[str]) -> Dict[str, List]:
    """asyncio.gather并发拉取所有币种K线"""
    async with aiohttp.ClientSession() as session:
        tasks = [_fetch_single(session, c) for c in coins]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    candles: Dict[str, List] = {}
    for r in results:
        if isinstance(r, Exception):
            continue
        inst_id, raw = r
        if raw:
            candles[inst_id] = raw
    return candles


# ── 技术指标 ──────────────────────────────────────────────────────────────────

def _parse_candles(raw: List) -> Optional[Dict[str, np.ndarray]]:
    """将OKX原始K线（最新在前）转换为时间升序numpy数组"""
    if not raw or len(raw) < max(RSI_PERIOD + 5, VMA_PERIOD):
        return None
    try:
        arr = np.array(
            [[float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])]
             for c in raw],
            dtype=float,
        )[::-1]  # 反转为时间升序
        return {
            "open": arr[:, 0],
            "high": arr[:, 1],
            "low": arr[:, 2],
            "close": arr[:, 3],
            "volume": arr[:, 4],
        }
    except Exception:
        return None


def _calc_rsi(close: np.ndarray, period: int = RSI_PERIOD) -> float:
    if len(close) < period + 1:
        return 50.0
    delta = np.diff(close)
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    avg_gain = gain[-period:].mean()
    avg_loss = loss[-period:].mean()
    if avg_loss == 0:
        return 100.0
    return float(100.0 - 100.0 / (1.0 + avg_gain / avg_loss))


def _calc_vma_ratio(volume: np.ndarray, period: int = VMA_PERIOD) -> float:
    if len(volume) < period:
        return 1.0
    vol_ma = volume[-period:].mean()
    if vol_ma == 0:
        return 1.0
    return float(volume[-1] / vol_ma)


def _calc_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> float:
    if len(close) < period + 1:
        return 0.0
    tr = np.maximum(
        high[1:] - low[1:],
        np.maximum(np.abs(high[1:] - close[:-1]), np.abs(low[1:] - close[:-1])),
    )
    return float(tr[-period:].mean())


# ── 漏斗过滤层 ────────────────────────────────────────────────────────────────

def _filter_rsi(candles_dict: Dict[str, List]) -> Dict[str, Dict]:
    """漏斗1：RSI极值初筛（零API成本，纯本地计算）"""
    passed: Dict[str, Dict] = {}
    for inst_id, raw in candles_dict.items():
        parsed = _parse_candles(raw)
        if parsed is None:
            continue
        rsi = _calc_rsi(parsed["close"])
        if rsi < RSI_OVERSOLD or rsi > RSI_OVERBOUGHT:
            passed[inst_id] = {
                "parsed": parsed,
                "rsi": rsi,
                "direction": "long" if rsi < RSI_OVERSOLD else "short",
            }
    return passed


def _filter_vma(candles_dict: Dict[str, Dict]) -> Dict[str, Dict]:
    """漏斗2：VMA倍率次筛"""
    passed: Dict[str, Dict] = {}
    for inst_id, info in candles_dict.items():
        parsed = info["parsed"]
        vma_ratio = _calc_vma_ratio(parsed["volume"])
        if vma_ratio > VMA_RATIO_THRESHOLD:
            info["vma_ratio"] = vma_ratio
            passed[inst_id] = info
    return passed


async def _hsaka_filter(candles_dict: Dict[str, Dict]) -> Dict[str, Dict]:
    """Hsaka七层深度过滤（仅对通过漏斗1+2者）"""
    result: Dict[str, Dict] = {}
    for inst_id, info in candles_dict.items():
        try:
            hsaka_results = await hsaka_analyzer.run_all_checks(inst_id)
            triggered_count = sum(
                1 for key in HSAKA_KEYS
                if hsaka_results.get(key, {}).get("triggered", False)
            )
            hsaka_score = triggered_count / 7 * 7.0
            info["hsaka_results"] = hsaka_results
            info["hsaka_score"] = hsaka_score
            info["triggered_count"] = triggered_count
            result[inst_id] = info
        except Exception as e:
            logger.warning("[Scanner] hsaka_filter %s: %s", inst_id, e)
    return result


async def _score_and_rank(
    hsaka_passed: Dict[str, Dict],
    db: TradeDB,
) -> List[Dict[str, Any]]:
    """AI动态打分 + 综合评分排序"""
    valid_count = await _count_valid_trades(db)
    evolution_active = valid_count >= EVOLUTION_THRESHOLD
    learner = await get_learner() if evolution_active else None

    signals: List[Dict[str, Any]] = []

    for inst_id, info in hsaka_passed.items():
        parsed = info["parsed"]
        rsi = info["rsi"]
        vma_ratio = info["vma_ratio"]
        hsaka_score = info["hsaka_score"]
        hsaka_results = info["hsaka_results"]
        direction = info["direction"]

        close = parsed["close"]
        high = parsed["high"]
        low = parsed["low"]
        current_price = float(close[-1])
        atr = _calc_atr(high, low, close)
        atr_ratio = atr / current_price if current_price > 0 else 0.0

        ai_score = 0.0
        if evolution_active and learner is not None:
            try:
                signal_data = {"rsi": rsi, "vol_ratio": vma_ratio, "atr_ratio": atr_ratio}
                ai_result = await learner.score_signal(signal_data, hsaka_results)
                ai_score = ai_result.get("ai_score", 0.0)
            except Exception as e:
                logger.warning("[Scanner] AI scoring %s: %s", inst_id, e)

        composite_score = hsaka_score + (ai_score if evolution_active else 0.0)

        if direction == "long":
            stop_loss = current_price - 2.0 * atr
            take_profit = current_price + 3.0 * atr
        else:
            stop_loss = current_price + 2.0 * atr
            take_profit = current_price - 3.0 * atr

        sfp = hsaka_results.get("sfp", {})
        liq = hsaka_results.get("liquidity_sensing", {})
        sd = hsaka_results.get("supply_demand_zone", {})
        rf = hsaka_results.get("range_fakeout", {})
        hs = hsaka_results.get("high_session", {})
        fr = hsaka_results.get("funding_rate", {})

        signals.append({
            "inst_id": inst_id,
            "sector": _get_sector(inst_id),
            "direction": direction,
            "rsi": round(rsi, 4),
            "vma_ratio": round(vma_ratio, 4),
            "atr": round(atr, 8),
            "atr_ratio": round(atr_ratio, 8),
            "funding_rate": float(fr.get("details", {}).get("funding_rate", 0.0)),
            "hsaka_score": round(hsaka_score, 4),
            "ai_score": round(ai_score, 4),
            "hsaka_sfp": 1 if sfp.get("triggered") else 0,
            "hsaka_liq": 1 if liq.get("triggered") else 0,
            "supply_demand_zone": 1 if sd.get("triggered") else 0,
            "range_fakeout": 1 if rf.get("triggered") else 0,
            "high_session": 1 if hs.get("triggered") else 0,
            "session_flag": hs.get("details", {}).get("session", "off-hours"),
            "composite_score": round(composite_score, 4),
            "entry_price": round(current_price, 8),
            "stop_loss": round(stop_loss, 8),
            "take_profit": round(take_profit, 8),
            "evolution_active": evolution_active,
        })

    signals.sort(key=lambda x: x["composite_score"], reverse=True)
    return signals


async def _persist_signals(signals: List[Dict[str, Any]], db: TradeDB) -> int:
    """调用trade_db.insert_signal()原子写入，仅写入综合评分≥5.5的信号"""
    written = 0
    now = datetime.utcnow().isoformat()
    expired_at = (datetime.utcnow() + timedelta(hours=4)).isoformat()

    for sig in signals:
        if sig["composite_score"] < SCORE_THRESHOLD:
            continue
        try:
            await db.insert_signal(
                inst_id=sig["inst_id"],
                sector=sig["sector"],
                direction=sig["direction"],
                rsi=sig["rsi"],
                vma_ratio=sig["vma_ratio"],
                atr=sig["atr"],
                atr_ratio=sig["atr_ratio"],
                funding_rate=sig["funding_rate"],
                hsaka_score=sig["hsaka_score"],
                ai_score=sig["ai_score"],
                hsaka_sfp=sig["hsaka_sfp"],
                hsaka_liq=sig["hsaka_liq"],
                supply_demand_zone=sig["supply_demand_zone"],
                range_fakeout=sig["range_fakeout"],
                high_session=sig["high_session"],
                session_flag=sig["session_flag"],
                entry_price=sig["entry_price"],
                stop_loss=sig["stop_loss"],
                take_profit=sig["take_profit"],
                created_at=now,
                expired_at=expired_at,
            )
            written += 1
        except Exception as e:
            logger.error("[Scanner] persist_signal %s: %s", sig["inst_id"], e)

    return written


# ── DB辅助 ────────────────────────────────────────────────────────────────────

async def _load_blocking_lessons(db: TradeDB) -> Set[str]:
    """从pattern_trades读取被AI标记BLOCK的币种"""
    try:
        conn = await db._get_conn()
        cursor = await conn.execute(
            "SELECT DISTINCT inst_id FROM pattern_trades WHERE close_reason = 'BLOCK'"
        )
        rows = await cursor.fetchall()
        return {row[0] for row in rows}
    except Exception as e:
        logger.warning("[Scanner] load_blocking_lessons: %s", e)
        return set()


async def _check_cooldown(db: TradeDB, inst_id: str) -> bool:
    """检查是否在冷却期（True=冷却中，跳过）"""
    try:
        conn = await db._get_conn()
        now = datetime.utcnow().isoformat()
        cursor = await conn.execute(
            "SELECT id FROM signals WHERE inst_id = ? AND cooldown_until > ? AND used = 0 LIMIT 1",
            (inst_id, now),
        )
        row = await cursor.fetchone()
        return row is not None
    except Exception:
        return False


async def _count_valid_trades(db: TradeDB) -> int:
    """统计有效交易数（判断是否达到进化阈值）"""
    try:
        conn = await db._get_conn()
        cursor = await conn.execute(
            "SELECT COUNT(*) FROM pattern_trades WHERE pnl IS NOT NULL AND pnl != 0"
        )
        row = await cursor.fetchone()
        return row[0] if row else 0
    except Exception:
        return 0


# ── 主扫描入口 ────────────────────────────────────────────────────────────────

async def scan_all() -> Dict[str, Any]:
    """
    执行完整漏斗扫描，返回扫描报告。

    流程：blocking_lessons → 并发拉K线 → RSI初筛 → VMA次筛
          → Hsaka七层 → AI打分 → 综合评分≥5.5写入DB
    """
    report: Dict[str, Any] = {
        "total_coins": 0,
        "rsi_filtered": 0,
        "vma_filtered": 0,
        "hsaka_filtered": 0,
        "signals_written": 0,
        "signals": [],
    }

    async with get_db() as db:
        # ── 步骤a：加载blocking_lessons ───────────────────────────────────────
        blocking_coins = await _load_blocking_lessons(db)
        logger.info("[Scanner] blocking_lessons: %d 个币种被屏蔽", len(blocking_coins))

        # ── 步骤b：并发拉取K线（asyncio.gather，代理+指数退避重试）────────────
        all_coins = [c for c in SYMBOLS if c not in blocking_coins]
        report["total_coins"] = len(all_coins)
        logger.info("[Scanner] 开始扫描 %d 个币种", len(all_coins))

        candles_dict = await _fetch_klines_batch(all_coins)
        logger.info("[Scanner] 成功拉取 %d 个币种K线", len(candles_dict))

        # ── 步骤c：漏斗1 - RSI极值初筛（RSI<30 OR RSI>65）───────────────────
        rsi_raw = _filter_rsi(candles_dict)

        # 冷却检查：冷却中的币种在RSI阶段即跳过
        rsi_passed: Dict[str, Dict] = {}
        for inst_id, info in rsi_raw.items():
            if not await _check_cooldown(db, inst_id):
                rsi_passed[inst_id] = info

        report["rsi_filtered"] = len(rsi_passed)
        logger.info("[Scanner] RSI初筛通过: %d 个（含冷却过滤）", len(rsi_passed))

        # ── 步骤d：漏斗2 - VMA倍率次筛（VMA>1.8）────────────────────────────
        vma_passed = _filter_vma(rsi_passed)
        report["vma_filtered"] = len(vma_passed)
        logger.info("[Scanner] VMA次筛通过: %d 个", len(vma_passed))

        if not vma_passed:
            logger.info("[Scanner] 漏斗后无候选币种，扫描结束")
            return report

        # ── 步骤e：Hsaka七层深度过滤（仅对5-8个候选）────────────────────────
        hsaka_passed = await _hsaka_filter(vma_passed)
        report["hsaka_filtered"] = len(hsaka_passed)
        logger.info("[Scanner] Hsaka深度过滤通过: %d 个", len(hsaka_passed))

        if not hsaka_passed:
            return report

        # ── 步骤f：AI动态打分（进化态≥30笔后启用）───────────────────────────
        # ── 步骤g：综合评分≥5.5写入DB ─────────────────────────────────────────
        signals = await _score_and_rank(hsaka_passed, db)
        written = await _persist_signals(signals, db)

        report["signals_written"] = written
        report["signals"] = [
            {k: v for k, v in s.items() if k != "evolution_active"}
            for s in signals
            if s["composite_score"] >= SCORE_THRESHOLD
        ]

        logger.info("[Scanner] 扫描完成，写入信号: %d 个", written)
        return report
