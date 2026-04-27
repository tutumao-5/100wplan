import asyncio
import logging
from datetime import date
from typing import Optional

import aiosqlite
from aiogram import Bot, Dispatcher, Router
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import Command
from aiogram.types import Message
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

PROXY = "http://127.0.0.1:7897"
DB_PATH = "/home/jwx/okx-trading-agent-2/data/trading_2.db"


class TgBot:
    def __init__(self, token: str, chat_id: str, db, executor):
        self._token = token
        self._chat_id = int(chat_id)
        self._db = db
        self._executor = executor
        self._paused = False

        session = AiohttpSession(proxy=PROXY)
        self._bot = Bot(token=token, session=session)
        self._dp = Dispatcher()
        router = Router()
        router.message.register(self.handle_status, Command("status"))
        router.message.register(self.handle_positions, Command("positions"))
        router.message.register(self.handle_pause, Command("pause"))
        router.message.register(self.handle_resume, Command("resume"))
        self._dp.include_router(router)

    async def start(self):
        logger.info("TgBot polling started")
        await self._dp.start_polling(self._bot, allowed_updates=["message"])

    # ── push notifications ────────────────────────────────────────────────────

    async def send_signal_alert(self, signal: dict):
        checks = []
        if signal.get("hsaka_sfp"):
            checks.append("SFP")
        if signal.get("hsaka_liq"):
            checks.append("LIQ")
        if signal.get("supply_demand_zone"):
            checks.append("SDZ")
        if signal.get("range_fakeout"):
            checks.append("FAKEOUT")
        if signal.get("high_session"):
            checks.append("SESSION")

        direction = (signal.get("direction") or "").lower()
        dir_emoji = "🟢" if direction == "buy" else "🔴"
        checks_str = " \\| ".join(checks) if checks else "\\-"

        text = (
            f"*📊 信号预警*\n"
            f"币种: `{signal.get('inst_id', 'N/A')}`\n"
            f"方向: {dir_emoji} `{direction.upper()}`\n"
            f"Hsaka评分: `{signal.get('hsaka_score', 0):.2f}`\n"
            f"触发条件: {checks_str}\n"
            f"入场价: `{signal.get('entry_price', 'N/A')}`\n"
            f"止损: `{signal.get('stop_loss', 'N/A')}` \\| 止盈: `{signal.get('take_profit', 'N/A')}`"
        )
        await self._send(text)
        logger.info("Signal alert sent: %s %s score=%.2f",
                    signal.get("inst_id"), direction, signal.get("hsaka_score", 0))

    async def send_execution_alert(self, order: dict):
        order_type = (order.get("order_type") or "open").lower()
        is_close = order_type in ("close", "平仓", "closing")
        inst = order.get("inst_id", "N/A")
        side = (order.get("side") or "N/A").upper()
        price = order.get("fill_price") or order.get("price", "N/A")
        exec_type = (order.get("exec_type") or "market").upper()

        if is_close:
            pnl = float(order.get("pnl") or 0)
            pnl_pct = float(order.get("pnl_pct") or 0)
            result_emoji = "✅" if pnl >= 0 else "❌"
            text = (
                f"*{result_emoji} 平仓通知*\n"
                f"币种: `{inst}`\n"
                f"方向: `{side}`\n"
                f"平仓价: `{price}`\n"
                f"类型: `{exec_type}`\n"
                f"盈亏: `{pnl:+.4f} USDT` \\(`{pnl_pct:+.2%}`\\)"
            )
        else:
            qty = order.get("quantity") or order.get("fill_qty", "N/A")
            text = (
                f"*🚀 开仓通知*\n"
                f"币种: `{inst}`\n"
                f"方向: `{side}`\n"
                f"开仓价: `{price}`\n"
                f"类型: `{exec_type}`\n"
                f"数量: `{qty}`"
            )

        await self._send(text)
        logger.info("Execution alert sent: %s %s", inst, order_type)

    # ── bot command handlers ──────────────────────────────────────────────────

    async def handle_status(self, message: Message):
        logger.info("/status from user %s", message.from_user.id)
        try:
            stats = await self._query_daily_stats(date.today().isoformat())
            positions = await self._db.get_active_positions()
            cooldown_count = await self._query_cooldown_count()

            pnl = float(stats.get("total_pnl") or 0) if stats else 0.0
            status_emoji = "⏸" if self._paused else "▶️"
            status_text = "已暂停" if self._paused else "运行中"
            pnl_emoji = "🟢" if pnl >= 0 else "🔴"

            text = (
                f"*📈 系统状态*\n"
                f"状态: {status_emoji} `{status_text}`\n"
                f"今日盈亏: {pnl_emoji} `{pnl:+.4f} USDT`\n"
                f"当前持仓: `{len(positions)}` 个\n"
                f"冷却币种: `{cooldown_count}` 个"
            )
        except Exception as e:
            logger.error("handle_status error: %s", e)
            text = f"*❌ 状态查询失败*\n`{e}`"

        await message.answer(text, parse_mode="MarkdownV2")

    async def handle_positions(self, message: Message):
        logger.info("/positions from user %s", message.from_user.id)
        try:
            positions = await self._get_live_positions()
            if not positions:
                await message.answer("*📭 当前无持仓*", parse_mode="MarkdownV2")
                return

            lines = ["*📋 当前持仓*\n"]
            for p in positions:
                inst = p.get("inst_id") or p.get("symbol", "N/A")
                side = (p.get("side") or p.get("position_side") or "N/A").upper()
                qty = p.get("quantity") or p.get("contracts", "N/A")
                price = p.get("fill_price") or p.get("entryPrice", "N/A")
                upnl = float(p.get("unrealizedPnl") or p.get("pnl") or 0)
                pnl_emoji = "🟢" if upnl >= 0 else "🔴"
                lines.append(
                    f"• `{inst}` \\| {side} \\| 量:`{qty}` \\| 价:`{price}` \\| {pnl_emoji}`{upnl:+.4f}`"
                )

            await message.answer("\n".join(lines), parse_mode="MarkdownV2")
        except Exception as e:
            logger.error("handle_positions error: %s", e)
            await message.answer(f"*❌ 持仓查询失败*\n`{e}`", parse_mode="MarkdownV2")

    async def handle_pause(self, message: Message):
        logger.info("/pause from user %s", message.from_user.id)
        self._paused = True
        await message.answer("*⏸ 已暂停交易*\n所有信号将被忽略", parse_mode="MarkdownV2")

    async def handle_resume(self, message: Message):
        logger.info("/resume from user %s", message.from_user.id)
        self._paused = False
        await message.answer("*▶️ 已恢复交易*\n信号处理重新开启", parse_mode="MarkdownV2")

    def is_paused(self) -> bool:
        return self._paused

    # ── internal helpers ──────────────────────────────────────────────────────

    async def _send(self, text: str):
        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=text,
                parse_mode="MarkdownV2",
            )
        except Exception as e:
            logger.error("TG send failed: %s", e)

    async def _get_live_positions(self):
        for attr in ("exchange", "okx", "_exchange", "_okx"):
            exchange = getattr(self._executor, attr, None)
            if exchange is not None:
                try:
                    raw = await exchange.fetch_positions()
                    live = [p for p in raw if float(p.get("contracts") or 0) != 0]
                    if live:
                        return live
                except Exception as e:
                    logger.warning("OKX fetch_positions failed (%s), falling back to DB", e)
                break
        return await self._db.get_active_positions()

    async def _query_daily_stats(self, today: str) -> Optional[dict]:
        db_path = getattr(self._db, "db_path", None) or getattr(self._db, "_db_path", DB_PATH)
        async with aiosqlite.connect(db_path) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(
                "SELECT * FROM daily_stats WHERE date = ?", (today,)
            ) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None

    async def _query_cooldown_count(self) -> int:
        db_path = getattr(self._db, "db_path", None) or getattr(self._db, "_db_path", DB_PATH)
        async with aiosqlite.connect(db_path) as conn:
            async with conn.execute(
                "SELECT COUNT(DISTINCT inst_id) FROM signals "
                "WHERE cooldown_until IS NOT NULL AND cooldown_until > datetime('now')"
            ) as cur:
                row = await cur.fetchone()
                return row[0] if row else 0
