import asyncio
import logging
from datetime import date
from typing import Optional

from aiogram import Bot, Dispatcher, Router
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import Command
from aiogram.types import Message
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

PROXY = "http://127.0.0.1:7897"


class TgBot:
    def __init__(self, token: str, chat_id: str, db, executor):
        self._token = token
        self._chat_id = int(chat_id)
        self._db = db
        self._executor = executor
        self._paused = False
        self._fetch_success = 0
        self._fetch_total = 0

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
            f"<b>📊 信号预警</b>\n"
            f"币种: <code>{signal.get('inst_id', 'N/A')}</code>\n"
            f"方向: {dir_emoji} <code>{direction.upper()}</code>\n"
            f"Hsaka评分: <code>{signal.get('hsaka_score', 0):.2f}</code>\n"
            f"触发条件: {checks_str}\n"
            f"入场价: <code>{signal.get('entry_price', 'N/A')}</code>\n"
            f"止损: <code>{signal.get('stop_loss', 'N/A')}</code> \\| 止盈: <code>{signal.get('take_profit', 'N/A')}</code>"
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
                f"<b>{result_emoji} 平仓通知</b>\n"
                f"币种: <code>{inst}</code>\n"
                f"方向: <code>{side}</code>\n"
                f"平仓价: <code>{price}</code>\n"
                f"类型: <code>{exec_type}</code>\n"
                f"盈亏: <code>{pnl:+.4f} USDT</code> (<code>{pnl_pct:+.2%}</code>)"
            )
        else:
            qty = order.get("quantity") or order.get("fill_qty", "N/A")
            text = (
                f"<b>🚀 开仓通知</b>\n"
                f"币种: <code>{inst}</code>\n"
                f"方向: <code>{side}</code>\n"
                f"开仓价: <code>{price}</code>\n"
                f"类型: <code>{exec_type}</code>\n"
                f"数量: <code>{qty}</code>"
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
                f"<b>📈 系统状态</b>\n"
                f"状态: {status_emoji} <code>{status_text}</code>\n"
                f"今日盈亏: {pnl_emoji} <code>{pnl:+.4f} USDT</code>\n"
                f"当前持仓: <code>{len(positions)}</code> 个\n"
                f"冷却币种: <code>{cooldown_count}</code> 个\n"
                f"本轮K线: <code>{self._fetch_success}/{self._fetch_total}</code> ({(self._fetch_success/self._fetch_total*100) if self._fetch_total > 0 else 0:.0f}%)"
            )
        except Exception as e:
            logger.error("handle_status error: %s", e)
            text = f"<b>❌ 状态查询失败</b>\n<code>{e}</code>"

        await message.answer(text, parse_mode="HTML")

    async def handle_positions(self, message: Message):
        logger.info("/positions from user %s", message.from_user.id)
        try:
            positions = await self._get_live_positions()
            if not positions:
                await message.answer("<b>📭 当前无持仓</b>", parse_mode="HTML")
                return

            lines = ["<b>📋 当前持仓</b>\n"]
            for p in positions:
                inst = p.get("inst_id") or p.get("symbol", "N/A")
                side = (p.get("side") or p.get("position_side") or "N/A").upper()
                qty = p.get("quantity") or p.get("contracts", "N/A")
                price = p.get("fill_price") or p.get("entryPrice", "N/A")
                upnl = float(p.get("unrealizedPnl") or p.get("pnl") or 0)
                pnl_emoji = "🟢" if upnl >= 0 else "🔴"
                lines.append(
                    f"• <code>{inst}</code> \\| {side} \\| 量:<code>{qty}</code> \\| 价:<code>{price}</code> \\| {pnl_emoji}<code>{upnl:+.4f}</code>"
                )

            await message.answer("\n".join(lines), parse_mode="HTML")
        except Exception as e:
            logger.error("handle_positions error: %s", e)
            await message.answer(f"<b>❌ 持仓查询失败</b>\n<code>{e}</code>", parse_mode="HTML")

    async def handle_pause(self, message: Message):
        logger.info("/pause from user %s", message.from_user.id)
        self._paused = True
        await message.answer("<b>⏸ 已暂停交易</b>\n所有信号将被忽略", parse_mode="HTML")

    async def handle_resume(self, message: Message):
        logger.info("/resume from user %s", message.from_user.id)
        self._paused = False
        await message.answer("<b>▶️ 已恢复交易</b>\n信号处理重新开启", parse_mode="HTML")

    def is_paused(self) -> bool:
        return self._paused

    def update_fetch_stats(self, success: int, total: int):
        self._fetch_success = success
        self._fetch_total = total

    async def send_message(self, text: str):
        """对外暴露的发送消息接口，带防御性异常捕获"""
        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=text,
            )
        except Exception as e:
            logger.error("TG send_message failed: %s", e)

    # ── internal helpers ──────────────────────────────────────────────────────

    async def _send(self, text: str):
        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=text,
                parse_mode="HTML",
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
        return await self._db.get_daily_stats(today)

    async def _query_cooldown_count(self) -> int:
        conn = await self._db._get_conn()
        async with conn.execute(
            "SELECT COUNT(DISTINCT inst_id) FROM signals "
            "WHERE cooldown_until IS NOT NULL AND cooldown_until > datetime('now')"
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0
