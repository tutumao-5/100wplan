import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import ccxt.async_support as ccxt

from core.trade_db import TradeDB

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
SETTINGS_PATH = BASE_DIR / "config" / "settings_v2.json"
PROXY_URL = "http://127.0.0.1:7897"


@dataclass
class OrderParams:
    inst_id: str
    side: str
    quantity: float
    price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    position_side: Optional[str] = None


class Executor:
    def __init__(self, api_key: str, secret: str, passphrase: str, db: TradeDB) -> None:
        self.api_key = api_key
        self.secret = secret
        self.passphrase = passphrase
        self.db = db
        self._settings = self._load_settings()
        self._okx: Optional[ccxt.okx] = None

        rc = self._settings.get("risk_control", {})
        self.stop_loss_pct: float = rc.get("stop_loss_pct", 0.02)
        self.take_profit_pct: float = rc.get("take_profit_pct", 0.06)
        self.trailing_stop_pct: float = rc.get("trailing_stop_pct", 0.01)

    # ── Init ───────────────────────────────────────────────────────────────────

    def _load_settings(self) -> Dict[str, Any]:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    async def _get_exchange(self) -> ccxt.okx:
        if self._okx is None:
            self._okx = ccxt.okx({
                "apiKey": self.api_key,
                "secret": self.secret,
                "password": self.passphrase,
                "enableRateLimit": True,
                "options": {"defaultType": "swap"},
                "aiohttp_proxy": PROXY_URL,
                "proxies": {"http": PROXY_URL, "https": PROXY_URL},
            })
        return self._okx

    # ── SL/TP ──────────────────────────────────────────────────────────────────

    def _calc_sl_tp(self, entry_price: float, side: str) -> tuple[float, float]:
        if side == "buy":
            sl = round(entry_price * (1 - self.stop_loss_pct), 8)
            tp = round(entry_price * (1 + self.take_profit_pct), 8)
        else:
            sl = round(entry_price * (1 + self.stop_loss_pct), 8)
            tp = round(entry_price * (1 - self.take_profit_pct), 8)
        return sl, tp

    # ── Core execution methods ─────────────────────────────────────────────────

    async def execute_market_order(
        self,
        inst_id: str,
        side: str,
        quantity: float,
        signal_info: Dict[str, Any],
    ) -> Dict[str, Any]:
        hsaka_score = float(signal_info.get("hsaka_score", 0))
        hsaka_sfp = int(signal_info.get("hsaka_sfp", 0))
        hsaka_liq = int(signal_info.get("hsaka_liq", 0))

        if not (hsaka_score >= 6.5 and (hsaka_sfp == 1 or hsaka_liq == 1)):
            logger.warning(
                "[市价路由拒绝] inst_id=%s score=%.2f sfp=%d liq=%d",
                inst_id, hsaka_score, hsaka_sfp, hsaka_liq,
            )
            return {"status": "rejected", "reason": "routing_condition_not_met"}

        position_side = "long" if side == "buy" else "short"

        db_row_id = await self.db.create_order(
            signal_id=signal_info.get("signal_id"),
            inst_id=inst_id,
            sector=signal_info.get("sector"),
            order_type="market",
            side=side,
            position_side=position_side,
            quantity=quantity,
            price=None,
            status="pending",
        )

        try:
            okx = await self._get_exchange()
            resp = await okx.create_order(
                symbol=inst_id,
                type="market",
                side=side,
                amount=quantity,
                params={"tdMode": "cross", "posSide": position_side},
            )

            ord_id = str(resp.get("id", ""))
            avg_price = float(resp.get("average") or resp.get("price") or 0)
            fill_qty = float(resp.get("filled") or 0)

            sl, tp = self._calc_sl_tp(avg_price, side)
            position_id = await self._fetch_position_id(okx, inst_id, position_side)

            await self._set_row_fields(
                db_row_id,
                ord_id=ord_id,
                status="filled",
                fill_price=avg_price,
                fill_qty=fill_qty,
                stop_loss=sl,
                take_profit=tp,
                position_id=position_id,
            )

            await self._attach_sl_tp(position_id or ord_id, avg_price, side)

            logger.info(
                "[市价开仓成功] inst_id=%s ord_id=%s pos_id=%s price=%.6f sl=%.6f tp=%.6f",
                inst_id, ord_id, position_id, avg_price, sl, tp,
            )
            return {
                "ord_id": ord_id,
                "position_id": position_id or ord_id,
                "status": "filled",
                "entry_price": avg_price,
                "sl": sl,
                "tp": tp,
            }

        except Exception as exc:
            logger.error("[市价开仓失败] inst_id=%s error=%s", inst_id, exc)
            await self._set_row_status(db_row_id, "failed")
            return {"status": "failed", "error": str(exc)}

    async def execute_limit_order(
        self,
        inst_id: str,
        side: str,
        price: float,
        quantity: float,
        signal_info: Dict[str, Any],
    ) -> Dict[str, Any]:
        hsaka_score = float(signal_info.get("hsaka_score", 0))

        if not (5.5 <= hsaka_score < 6.5):
            logger.warning(
                "[限价路由拒绝] inst_id=%s score=%.2f", inst_id, hsaka_score
            )
            return {"status": "rejected", "reason": "routing_condition_not_met"}

        position_side = "long" if side == "buy" else "short"
        sl, tp = self._calc_sl_tp(price, side)

        db_row_id = await self.db.create_order(
            signal_id=signal_info.get("signal_id"),
            inst_id=inst_id,
            sector=signal_info.get("sector"),
            order_type="limit",
            side=side,
            position_side=position_side,
            quantity=quantity,
            price=price,
            stop_loss=sl,
            take_profit=tp,
            status="pending",
        )

        try:
            okx = await self._get_exchange()
            resp = await okx.create_order(
                symbol=inst_id,
                type="limit",
                side=side,
                amount=quantity,
                price=price,
                params={"tdMode": "cross", "posSide": position_side},
            )

            ord_id = str(resp.get("id", ""))

            await self._set_row_fields(db_row_id, ord_id=ord_id, status="pending")

            logger.info(
                "[限价挂单成功] inst_id=%s ord_id=%s price=%.6f sl=%.6f tp=%.6f",
                inst_id, ord_id, price, sl, tp,
            )
            return {
                "ord_id": ord_id,
                "position_id": "",
                "status": "pending",
                "entry_price": price,
                "sl": sl,
                "tp": tp,
            }

        except Exception as exc:
            logger.error("[限价挂单失败] inst_id=%s error=%s", inst_id, exc)
            await self._set_row_status(db_row_id, "failed")
            return {"status": "failed", "error": str(exc)}

    async def close_position(
        self,
        inst_id: str,
        position_id: str,
        reason: str = "manual",
    ) -> Dict[str, Any]:
        db_orders = await self.db.get_orders_by_position(position_id)
        if not db_orders:
            logger.warning("[平仓] 找不到持仓 position_id=%s", position_id)
            return {"status": "failed", "error": "position_not_found"}

        entry = next(
            (o for o in db_orders if o["order_type"] in ("market", "limit") and o["status"] == "filled"),
            db_orders[0],
        )
        position_side = entry.get("position_side", "long")
        quantity = float(entry.get("fill_qty") or entry.get("quantity") or 0)
        entry_price = float(entry.get("fill_price") or entry.get("price") or 0)
        close_side = "sell" if position_side == "long" else "buy"

        db_row_id = await self.db.create_order(
            signal_id=None,
            inst_id=inst_id,
            sector=entry.get("sector"),
            order_type="close",
            side=close_side,
            position_side=position_side,
            quantity=quantity,
            price=None,
            position_id=position_id,
            status="pending",
        )

        try:
            okx = await self._get_exchange()
            resp = await okx.create_order(
                symbol=inst_id,
                type="market",
                side=close_side,
                amount=quantity,
                params={"tdMode": "cross", "posSide": position_side, "reduceOnly": True},
            )

            ord_id = str(resp.get("id", ""))
            close_price = float(resp.get("average") or resp.get("price") or 0)
            fill_qty = float(resp.get("filled") or 0)

            pnl: Optional[float] = None
            pnl_pct: Optional[float] = None
            if entry_price and close_price:
                if position_side == "long":
                    pnl = (close_price - entry_price) * fill_qty
                    pnl_pct = (close_price - entry_price) / entry_price
                else:
                    pnl = (entry_price - close_price) * fill_qty
                    pnl_pct = (entry_price - close_price) / entry_price

            await self._set_row_fields(
                db_row_id,
                ord_id=ord_id,
                status="filled",
                fill_price=close_price,
                fill_qty=fill_qty,
                close_reason=reason,
                close_price=close_price,
                pnl=pnl,
                pnl_pct=pnl_pct,
            )

            entry_ord_id = entry.get("ord_id")
            if entry_ord_id:
                await self.db.update_order_status(
                    entry_ord_id, "closed",
                    close_reason=reason,
                    close_price=close_price,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                )

            await self._record_pattern(entry, position_id, entry_price, close_price, fill_qty,
                                       pnl, pnl_pct, reason)

            logger.info(
                "[平仓成功] inst_id=%s pos_id=%s price=%.6f pnl=%s reason=%s",
                inst_id, position_id, close_price, pnl, reason,
            )
            return {
                "ord_id": ord_id,
                "position_id": position_id,
                "status": "filled",
                "close_price": close_price,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "reason": reason,
            }

        except Exception as exc:
            logger.error("[平仓失败] inst_id=%s pos_id=%s error=%s", inst_id, position_id, exc)
            await self._set_row_status(db_row_id, "failed")
            return {"status": "failed", "error": str(exc)}

    async def cancel_order(self, inst_id: str, ord_id: str) -> Dict[str, Any]:
        try:
            okx = await self._get_exchange()
            resp = await okx.cancel_order(ord_id, symbol=inst_id)
            await self.db.update_order_status(ord_id, "cancelled")
            logger.info("[取消挂单成功] inst_id=%s ord_id=%s", inst_id, ord_id)
            return {"status": "cancelled", "ord_id": ord_id, "details": resp}
        except Exception as exc:
            logger.error("[取消挂单失败] inst_id=%s ord_id=%s error=%s", inst_id, ord_id, exc)
            return {"status": "failed", "error": str(exc)}

    async def get_free_balance(self) -> float:
        try:
            okx = await self._get_exchange()
            balance = await okx.fetch_balance({"type": "swap"})
            free = float((balance.get("USDT") or {}).get("free") or 0)
            logger.info("[余额查询] 可用USDT=%.4f", free)
            return free
        except Exception as exc:
            logger.error("[余额查询失败] error=%s", exc)
            return 0.0

    # ── SL/TP attachment ───────────────────────────────────────────────────────

    async def _attach_sl_tp(
        self,
        position_id: str,
        entry_price: float,
        side: str,
    ) -> None:
        if not entry_price:
            return
        sl, tp = self._calc_sl_tp(entry_price, side)
        position_side = "long" if side == "buy" else "short"
        close_side = "sell" if position_side == "long" else "buy"

        try:
            okx = await self._get_exchange()
            await okx.privatePostTradeOrderAlgo({
                "ordType": "oco",
                "instId": position_id,
                "tdMode": "cross",
                "side": close_side,
                "posSide": position_side,
                "sz": "0",
                "slTriggerPx": str(sl),
                "slOrdPx": "-1",
                "slTriggerPxType": "last",
                "tpTriggerPx": str(tp),
                "tpOrdPx": "-1",
                "tpTriggerPxType": "last",
            })
            logger.info("[附加SL/TP] pos_id=%s sl=%.6f tp=%.6f", position_id, sl, tp)
        except Exception as exc:
            logger.warning("[附加SL/TP失败] pos_id=%s error=%s", position_id, exc)

    # ── DB helpers ─────────────────────────────────────────────────────────────

    async def _update_order_in_db(
        self,
        ord_id: str,
        status: str,
        **kwargs: Any,
    ) -> None:
        await self.db.update_order_status(ord_id, status, **kwargs)

    async def _set_row_fields(self, row_id: int, **fields: Any) -> None:
        conn = await self.db._get_conn()
        fields["updated_at"] = datetime.utcnow().isoformat()
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        await conn.execute(
            f"UPDATE orders SET {set_clause} WHERE id = ?",
            [*fields.values(), row_id],
        )
        await conn.commit()

    async def _set_row_status(self, row_id: int, status: str) -> None:
        await self._set_row_fields(row_id, status=status)

    # ── Exchange helpers ───────────────────────────────────────────────────────

    async def _fetch_position_id(
        self,
        okx: ccxt.okx,
        inst_id: str,
        position_side: str,
    ) -> str:
        try:
            positions = await okx.fetch_positions([inst_id])
            for pos in positions:
                ps = pos.get("side") or pos.get("info", {}).get("posSide", "")
                if ps == position_side:
                    return str(pos.get("id") or pos.get("info", {}).get("posId", ""))
        except Exception as exc:
            logger.warning("[持仓ID查询失败] inst_id=%s error=%s", inst_id, exc)
        return ""

    # ── Pattern trade record ───────────────────────────────────────────────────

    async def _record_pattern(
        self,
        entry_order: Dict[str, Any],
        position_id: str,
        entry_price: float,
        exit_price: float,
        fill_qty: float,
        pnl: Optional[float],
        pnl_pct: Optional[float],
        close_reason: str,
    ) -> None:
        signal_id = entry_order.get("signal_id")
        sig: Dict[str, Any] = {}
        if signal_id:
            sig = (await self.db.get_signal_by_id(signal_id)) or {}

        try:
            await self.db.record_pattern_trade(
                inst_id=entry_order.get("inst_id", ""),
                sector=entry_order.get("sector"),
                rsi=sig.get("rsi"),
                vma_ratio=sig.get("vma_ratio"),
                atr_ratio=sig.get("atr_ratio"),
                funding_rate=sig.get("funding_rate"),
                hsaka_sfp=sig.get("hsaka_sfp"),
                hsaka_liq=sig.get("hsaka_liq"),
                session_flag=sig.get("session_flag"),
                supply_demand_zone=sig.get("supply_demand_zone"),
                range_fakeout=sig.get("range_fakeout"),
                entry_price=entry_price,
                exit_price=exit_price,
                pnl=pnl,
                pnl_pct=pnl_pct,
                close_reason=close_reason,
                duration=None,
                ai_weight=sig.get("ai_weight"),
                order_id=entry_order.get("ord_id"),
                position_id=position_id,
            )
        except Exception as exc:
            logger.warning("[pattern_trade记录失败] pos_id=%s error=%s", position_id, exc)

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def close(self) -> None:
        if self._okx:
            await self._okx.close()
            self._okx = None
