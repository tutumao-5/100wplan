import asyncio
import json
import os
import base64
import logging
from pathlib import Path

import aiohttp

logger = logging.getLogger(__name__)

WEIGHTS_FILE = Path("logs/condition_weights.json")
BATCH_SIZE = 30
EVOLUTION_THRESHOLD = 30

DEFAULT_CONDITION_WEIGHTS = {
    "rsi_weight": 1.0,
    "vma_weight": 1.0,
    "atr_weight": 1.0,
    "funding_rate_weight": 1.0,
    "hsaka_sfp_weight": 1.0,
    "hsaka_liq_weight": 1.0,
    "high_timeframe_weight": 1.0,
}

_learner_instance: "PatternLearner | None" = None


async def get_learner(db=None) -> "PatternLearner":
    global _learner_instance
    if _learner_instance is None:
        if db is None:
            from core.trade_db import TradeDB
            db = TradeDB()
        _learner_instance = PatternLearner(db)
    return _learner_instance
DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"
PROXY = "http://127.0.0.1:7897"

SYSTEM_PROMPT = """你是一个量化交易策略优化专家。根据提供的交易记录，分析哪些技术指标条件对盈利交易最重要，并输出优化后的权重配置。
必须严格返回JSON格式，不要任何其他文字。"""

USER_PROMPT_TEMPLATE = """以下是最近{count}笔交易记录（JSON格式）：

{trades_json}

请分析这些交易数据，找出盈利交易（pnl>0）和亏损交易（pnl<=0）的规律，输出以下JSON：
{{
  "condition_weights": {{
    "rsi_weight": <float, 0.1-3.0>,
    "vma_weight": <float, 0.1-3.0>,
    "atr_weight": <float, 0.1-3.0>,
    "funding_rate_weight": <float, 0.1-3.0>,
    "hsaka_sfp_weight": <float, 0.1-3.0>,
    "hsaka_liq_weight": <float, 0.1-3.0>,
    "high_timeframe_weight": <float, 0.1-3.0>
  }},
  "blocking_lessons": ["从亏损交易中总结的教训1", "教训2"],
  "winning_patterns": "盈利交易的共同特征总结",
  "confidence": <float, 0.0-1.0>
}}"""


class PatternLearner:
    def __init__(self, db):
        self.db = db
        self._trade_count = 0
        self._analysis_in_progress = False
        WEIGHTS_FILE.parent.mkdir(parents=True, exist_ok=True)

    async def record_trade(self, trade_data: dict) -> dict:
        try:
            await self.db.record_pattern_trade(trade_data)
            self._trade_count += 1

            if self._trade_count >= BATCH_SIZE and not self._analysis_in_progress:
                self._trade_count = 0
                self._analysis_in_progress = True
                asyncio.create_task(self._run_analysis_guarded())
                return {"status": "queued_for_analysis"}

            return {"status": "recorded"}
        except Exception as e:
            logger.error(f"record_trade error: {e}")
            return {"status": "recorded"}

    async def _run_analysis_guarded(self):
        try:
            await self.analyze_batch_async()
        except Exception as e:
            logger.error(f"analyze_batch_async error: {e}")
        finally:
            self._analysis_in_progress = False

    async def analyze_batch_async(self) -> dict:
        trades = await self.db.get_recent_pattern_trades(BATCH_SIZE)
        if not trades:
            logger.warning("analyze_batch_async: no trades found")
            return {}

        trades_json = json.dumps(trades, ensure_ascii=False, indent=2)
        prompt = USER_PROMPT_TEMPLATE.format(count=len(trades), trades_json=trades_json)

        result = await self._call_deepseek(prompt)
        if not result:
            return {}

        weights = result.get("condition_weights")
        if weights:
            try:
                WEIGHTS_FILE.write_text(json.dumps(weights, ensure_ascii=False, indent=2))
                logger.info(f"condition_weights updated: confidence={result.get('confidence')}")
            except Exception as e:
                logger.error(f"Failed to write weights file: {e}")

        blocking = result.get("blocking_lessons", [])
        if blocking:
            logger.info(f"Blocking lessons: {blocking}")

        return result

    async def score_signal(self, signal: dict, hsaka_results: dict = None) -> dict:
        weights = self.load_weights()
        score = 0.0
        weight_keys = [
            ("rsi_weight", "rsi"),
            ("vma_weight", "vol_ratio"),
            ("atr_weight", "atr_ratio"),
            ("funding_rate_weight", "funding_rate"),
            ("hsaka_sfp_weight", "hsaka_sfp"),
            ("hsaka_liq_weight", "hsaka_liq"),
            ("high_timeframe_weight", "high_timeframe"),
        ]
        for w_key, s_key in weight_keys:
            w = weights.get(w_key, 1.0)
            v = signal.get(s_key, 0.0)
            score += w * float(v)
        return {"ai_score": score}

    def load_weights(self) -> dict:
        if WEIGHTS_FILE.exists():
            try:
                return json.loads(WEIGHTS_FILE.read_text())
            except Exception as e:
                logger.error(f"Failed to load weights file: {e}")
        return self._get_default_weights()

    def _get_default_weights(self) -> dict:
        return {
            "rsi_weight": 1.0,
            "vma_weight": 1.0,
            "atr_weight": 1.0,
            "funding_rate_weight": 1.0,
            "hsaka_sfp_weight": 1.0,
            "hsaka_liq_weight": 1.0,
            "high_timeframe_weight": 1.0,
        }

    async def _call_deepseek(self, prompt: str) -> dict:
        raw_key = os.environ.get("DEEPSEEK_API_KEY", "")
        if not raw_key:
            import traceback
            logger.error("[DeepSeek API Key Missing] DEEPSEEK_API_KEY not set in env | UTC=%s\n%s", datetime.utcnow().isoformat(), traceback.format_stack())
            return {}

        try:
            api_key = base64.b64decode(raw_key).decode("utf-8").strip()
        except Exception as e:
            import traceback
            logger.error("[DeepSeek API Key Decode Failed] raw_key present but decode failed: %s | UTC=%s\n%s", e, datetime.utcnow().isoformat(), traceback.format_exc())
            api_key = raw_key.strip()

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.3,
            "response_format": {"type": "json_object"},
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    DEEPSEEK_API_URL,
                    headers=headers,
                    json=payload,
                    proxy=PROXY,
                    timeout=aiohttp.ClientTimeout(total=60),
                ) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        logger.error(f"[DeepSeek HTTP Error] status={resp.status} body={text[:500]}")
                        return {}
                    data = await resp.json()
                    content = data["choices"][0]["message"]["content"]
                    return json.loads(content)
        except asyncio.TimeoutError:
            import traceback
            logger.error("[DeepSeek Timeout] UTC=%s | Exception:\n%s", datetime.utcnow().isoformat(), traceback.format_exc())
            return {}
        except Exception as e:
            import traceback
            raw_text = traceback.format_exc()
            logger.error("[DeepSeek API Call Failed] UTC=%s | Exception=%s\nTraceback:\n%s", datetime.utcnow().isoformat(), e, raw_text)
            return {}
