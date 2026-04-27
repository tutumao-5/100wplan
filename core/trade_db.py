import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Optional

import aiosqlite

logger = logging.getLogger(__name__)

DB_PATH = "/home/jwx/okx-trading-agent-2/data/trading_2.db"

_db_instance: Optional["TradeDB"] = None


class TradeDB:

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def _get_conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self._conn = await aiosqlite.connect(self.db_path)
            self._conn.row_factory = aiosqlite.Row
            await self._conn.execute("PRAGMA journal_mode=WAL")
            await self._conn.execute("PRAGMA foreign_keys=ON")
            await self._conn.execute("PRAGMA busy_timeout=10000")
        return self._conn

    # ── init_db ────────────────────────────────────────────────────────────────

    async def init_db(self) -> None:
        conn = await self._get_conn()

        # signals — 27 cols
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                inst_id             TEXT    NOT NULL,
                sector              TEXT,
                direction           TEXT,
                rsi                 REAL,
                vma_ratio           REAL,
                atr                 REAL,
                atr_ratio           REAL,
                funding_rate        REAL,
                hsaka_score         REAL,
                ai_score            REAL,
                hsaka_sfp           INTEGER,
                hsaka_liq           INTEGER,
                supply_demand_zone  INTEGER,
                range_fakeout       INTEGER,
                high_session        INTEGER,
                session_flag        TEXT,
                ai_weight           REAL,
                position_ratio      REAL,
                entry_price         REAL,
                stop_loss           REAL,
                take_profit         REAL,
                created_at          TEXT    NOT NULL,
                cooldown_until      TEXT,
                expired_at          TEXT,
                used                INTEGER DEFAULT 0,
                used_order_id       INTEGER
            )
        """)

        # orders — 22 cols
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id       INTEGER,
                inst_id         TEXT    NOT NULL,
                sector          TEXT,
                order_type      TEXT,
                side            TEXT,
                position_side   TEXT,
                quantity        REAL,
                price           REAL,
                fill_price      REAL,
                fill_qty        REAL,
                ord_id          TEXT,
                position_id     TEXT,
                stop_loss       REAL,
                take_profit     REAL,
                status          TEXT,
                close_reason    TEXT,
                close_price     REAL,
                pnl             REAL,
                pnl_pct         REAL,
                created_at      TEXT    NOT NULL,
                updated_at      TEXT
            )
        """)

        # pattern_trades — 21 cols (excl. id)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS pattern_trades (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                inst_id             TEXT    NOT NULL,
                sector              TEXT,
                rsi                 REAL,
                vma_ratio           REAL,
                atr_ratio           REAL,
                funding_rate        REAL,
                hsaka_sfp           INTEGER,
                hsaka_liq           INTEGER,
                session_flag        TEXT,
                supply_demand_zone  INTEGER,
                range_fakeout       INTEGER,
                entry_price         REAL,
                exit_price          REAL,
                pnl                 REAL,
                pnl_pct             REAL,
                close_reason        TEXT,
                duration            INTEGER,
                ai_weight           REAL,
                order_id            TEXT,
                position_id         TEXT,
                timestamp           TEXT    NOT NULL
            )
        """)

        # daily_stats — 19 cols (excl. id)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                date            TEXT    NOT NULL UNIQUE,
                total_trades    INTEGER DEFAULT 0,
                winning_trades  INTEGER DEFAULT 0,
                losing_trades   INTEGER DEFAULT 0,
                win_rate        REAL,
                total_pnl       REAL,
                avg_pnl         REAL,
                max_drawdown    REAL,
                open_positions  INTEGER DEFAULT 0,
                new_signals     INTEGER DEFAULT 0,
                closing_count   INTEGER DEFAULT 0,
                win_count       INTEGER DEFAULT 0,
                loss_count      INTEGER DEFAULT 0,
                equity_hwm      REAL,
                equity_close    REAL,
                melt_status     TEXT,
                last_melt_time  TEXT,
                created_at      TEXT,
                updated_at      TEXT
            )
        """)

        # config — 5 cols
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS config (
                key         TEXT PRIMARY KEY,
                value       TEXT,
                updated_by  TEXT,
                updated_at  TEXT,
                description TEXT
            )
        """)

        # migrations — add columns that may be missing in existing DBs
        _migrations = [
            ("orders",        "ord_id",       "TEXT"),
            ("orders",        "position_id",  "TEXT"),
            ("orders",        "fill_price",   "REAL"),
            ("orders",        "fill_qty",     "REAL"),
            ("orders",        "close_reason", "TEXT"),
            ("orders",        "close_price",  "REAL"),
            ("orders",        "pnl",          "REAL"),
            ("orders",        "pnl_pct",      "REAL"),
            ("orders",        "sector",       "TEXT"),
            ("orders",        "updated_at",   "TEXT"),
            ("signals",       "used_order_id","INTEGER"),
            ("signals",       "cooldown_until","TEXT"),
            ("signals",       "expired_at",   "TEXT"),
            ("signals",       "ai_score",     "REAL"),
            ("signals",       "hsaka_sfp",    "INTEGER"),
            ("signals",       "hsaka_liq",    "INTEGER"),
            ("signals",       "session_flag", "TEXT"),
            ("pattern_trades","sector",       "TEXT"),
            ("daily_stats",   "win_count",    "INTEGER DEFAULT 0"),
            ("daily_stats",   "loss_count",   "INTEGER DEFAULT 0"),
            ("daily_stats",   "melt_status",  "TEXT"),
            ("daily_stats",   "last_melt_time","TEXT"),
            ("daily_stats",   "updated_at",   "TEXT"),
        ]
        for table, col, col_type in _migrations:
            try:
                await conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
            except Exception:
                pass  # column already exists

        await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_inst_id   ON signals(inst_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_used       ON signals(used)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_created    ON signals(created_at)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_inst_id     ON orders(inst_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_signal_id   ON orders(signal_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_position_id ON orders(position_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_status      ON orders(status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_ord_id      ON orders(ord_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_pattern_inst_id    ON pattern_trades(inst_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_pattern_timestamp  ON pattern_trades(timestamp)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_dailystats_date    ON daily_stats(date)")

        await conn.commit()
        logger.info("Database initialised: %s", self.db_path)

    # ── Signals ────────────────────────────────────────────────────────────────

    async def insert_signal(
        self,
        inst_id: str,
        sector: Optional[str] = None,
        direction: Optional[str] = None,
        rsi: Optional[float] = None,
        vma_ratio: Optional[float] = None,
        atr: Optional[float] = None,
        atr_ratio: Optional[float] = None,
        funding_rate: Optional[float] = None,
        hsaka_score: Optional[float] = None,
        ai_score: Optional[float] = None,
        hsaka_sfp: Optional[int] = None,
        hsaka_liq: Optional[int] = None,
        supply_demand_zone: Optional[int] = None,
        range_fakeout: Optional[int] = None,
        high_session: Optional[int] = None,
        session_flag: Optional[str] = None,
        ai_weight: Optional[float] = None,
        position_ratio: Optional[float] = None,
        entry_price: Optional[float] = None,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        created_at: Optional[str] = None,
        cooldown_until: Optional[str] = None,
        expired_at: Optional[str] = None,
    ) -> int:
        conn = await self._get_conn()
        now = created_at or datetime.utcnow().isoformat()
        cursor = await conn.execute(
            """
            INSERT INTO signals (
                inst_id, sector, direction, rsi, vma_ratio, atr, atr_ratio,
                funding_rate, hsaka_score, ai_score, hsaka_sfp, hsaka_liq,
                supply_demand_zone, range_fakeout, high_session, session_flag,
                ai_weight, position_ratio, entry_price, stop_loss, take_profit,
                created_at, cooldown_until, expired_at, used, used_order_id
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, 0, NULL
            )
            """,
            (
                inst_id, sector, direction, rsi, vma_ratio, atr, atr_ratio,
                funding_rate, hsaka_score, ai_score, hsaka_sfp, hsaka_liq,
                supply_demand_zone, range_fakeout, high_session, session_flag,
                ai_weight, position_ratio, entry_price, stop_loss, take_profit,
                now, cooldown_until, expired_at,
            ),
        )
        await conn.commit()
        return cursor.lastrowid

    async def get_signal_by_id(self, signal_id: int) -> Optional[dict]:
        conn = await self._get_conn()
        cursor = await conn.execute("SELECT * FROM signals WHERE id = ?", (signal_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def mark_signal_used(self, signal_id: int, order_id: int) -> None:
        conn = await self._get_conn()
        await conn.execute(
            "UPDATE signals SET used = 1, used_order_id = ? WHERE id = ?",
            (order_id, signal_id),
        )
        await conn.commit()

    async def get_active_signal(self, inst_id: str) -> Optional[dict]:
        conn = await self._get_conn()
        now = datetime.utcnow().isoformat()
        cursor = await conn.execute(
            """
            SELECT * FROM signals
            WHERE inst_id = ?
              AND used = 0
              AND (expired_at IS NULL OR expired_at > ?)
              AND (cooldown_until IS NULL OR cooldown_until <= ?)
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (inst_id, now, now),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def get_unused_signals_before(self, cutoff_time: str) -> list:
        conn = await self._get_conn()
        cursor = await conn.execute(
            "SELECT * FROM signals WHERE used = 0 AND created_at < ?",
            (cutoff_time,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ── Orders ─────────────────────────────────────────────────────────────────

    async def create_order(
        self,
        signal_id: Optional[int],
        inst_id: str,
        sector: Optional[str],
        order_type: str,
        side: str,
        position_side: str,
        quantity: Optional[float],
        price: Optional[float],
        fill_price: Optional[float] = None,
        fill_qty: Optional[float] = None,
        ord_id: Optional[str] = None,
        position_id: Optional[str] = None,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        status: str = "pending",
        created_at: Optional[str] = None,
    ) -> int:
        conn = await self._get_conn()
        now = created_at or datetime.utcnow().isoformat()
        cursor = await conn.execute(
            """
            INSERT INTO orders (
                signal_id, inst_id, sector, order_type, side, position_side,
                quantity, price, fill_price, fill_qty, ord_id, position_id,
                stop_loss, take_profit, status, close_reason, close_price,
                pnl, pnl_pct, created_at, updated_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, NULL
            )
            """,
            (
                signal_id, inst_id, sector, order_type, side, position_side,
                quantity, price, fill_price, fill_qty, ord_id, position_id,
                stop_loss, take_profit, status, now,
            ),
        )
        await conn.commit()
        return cursor.lastrowid

    async def update_order_status(
        self,
        ord_id: str,
        status: str,
        close_reason: Optional[str] = None,
        close_price: Optional[float] = None,
        pnl: Optional[float] = None,
        pnl_pct: Optional[float] = None,
        fill_price: Optional[float] = None,
        fill_qty: Optional[float] = None,
        position_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        conn = await self._get_conn()
        fields: dict[str, Any] = {"status": status, "updated_at": datetime.utcnow().isoformat()}
        for col, val in (
            ("close_reason", close_reason),
            ("close_price", close_price),
            ("pnl", pnl),
            ("pnl_pct", pnl_pct),
            ("fill_price", fill_price),
            ("fill_qty", fill_qty),
            ("position_id", position_id),
        ):
            if val is not None:
                fields[col] = val
        fields.update(kwargs)
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        await conn.execute(
            f"UPDATE orders SET {set_clause} WHERE ord_id = ?",
            [*fields.values(), ord_id],
        )
        await conn.commit()

    async def get_order_by_id(self, order_id: int) -> Optional[dict]:
        conn = await self._get_conn()
        cursor = await conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def get_order_by_ord_id(self, ord_id: str) -> Optional[dict]:
        conn = await self._get_conn()
        cursor = await conn.execute("SELECT * FROM orders WHERE ord_id = ?", (ord_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def get_active_positions(self) -> list:
        conn = await self._get_conn()
        cursor = await conn.execute(
            """
            SELECT * FROM orders
            WHERE status IN ('filled', 'partially_filled')
              AND position_id IS NOT NULL
            ORDER BY created_at DESC
            """
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_orders_by_position(self, position_id: str) -> list:
        conn = await self._get_conn()
        cursor = await conn.execute(
            "SELECT * FROM orders WHERE position_id = ? ORDER BY created_at ASC",
            (position_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ── Pattern Trades ─────────────────────────────────────────────────────────

    async def record_pattern_trade(
        self,
        inst_id: str,
        sector: Optional[str],
        rsi: Optional[float],
        vma_ratio: Optional[float],
        atr_ratio: Optional[float],
        funding_rate: Optional[float],
        hsaka_sfp: Optional[int],
        hsaka_liq: Optional[int],
        session_flag: Optional[str],
        supply_demand_zone: Optional[int],
        range_fakeout: Optional[int],
        entry_price: Optional[float],
        exit_price: Optional[float],
        pnl: Optional[float],
        pnl_pct: Optional[float],
        close_reason: Optional[str],
        duration: Optional[int],
        ai_weight: Optional[float],
        order_id: Optional[str],
        position_id: Optional[str],
        timestamp: Optional[str] = None,
    ) -> int:
        conn = await self._get_conn()
        ts = timestamp or datetime.utcnow().isoformat()
        cursor = await conn.execute(
            """
            INSERT INTO pattern_trades (
                inst_id, sector, rsi, vma_ratio, atr_ratio, funding_rate,
                hsaka_sfp, hsaka_liq, session_flag, supply_demand_zone,
                range_fakeout, entry_price, exit_price, pnl, pnl_pct,
                close_reason, duration, ai_weight, order_id, position_id, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                inst_id, sector, rsi, vma_ratio, atr_ratio, funding_rate,
                hsaka_sfp, hsaka_liq, session_flag, supply_demand_zone,
                range_fakeout, entry_price, exit_price, pnl, pnl_pct,
                close_reason, duration, ai_weight, order_id, position_id, ts,
            ),
        )
        await conn.commit()
        return cursor.lastrowid

    async def get_pattern_trades(self, limit: int = 100) -> list:
        conn = await self._get_conn()
        cursor = await conn.execute(
            "SELECT * FROM pattern_trades ORDER BY timestamp DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_recent_pattern_trades(self, n: int = 30) -> list:
        conn = await self._get_conn()
        cursor = await conn.execute(
            "SELECT * FROM pattern_trades ORDER BY timestamp DESC LIMIT ?",
            (n,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def count_pattern_trades(self, inst_id: str) -> int:
        conn = await self._get_conn()
        cursor = await conn.execute(
            "SELECT COUNT(*) FROM pattern_trades WHERE inst_id = ?", (inst_id,)
        )
        row = await cursor.fetchone()
        return row[0] if row else 0

    # ── Daily Stats ────────────────────────────────────────────────────────────

    async def get_daily_stats(self, date: str) -> Optional[dict]:
        conn = await self._get_conn()
        cursor = await conn.execute("SELECT * FROM daily_stats WHERE date = ?", (date,))
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def update_daily_stats(self, date: str, **kwargs: Any) -> None:
        if not kwargs:
            return
        conn = await self._get_conn()
        now = datetime.utcnow().isoformat()
        set_clause = ", ".join(f"{k} = ?" for k in kwargs) + ", updated_at = ?"
        values = [*kwargs.values(), now]
        result = await conn.execute(
            f"UPDATE daily_stats SET {set_clause} WHERE date = ?",
            [*values, date],
        )
        if result.rowcount == 0:
            all_cols = list(kwargs.keys()) + ["date", "updated_at"]
            placeholders = ", ".join(["?"] * len(all_cols))
            await conn.execute(
                f"INSERT INTO daily_stats ({', '.join(all_cols)}) VALUES ({placeholders})",
                [*kwargs.values(), date, now],
            )
        await conn.commit()

    async def upsert_daily_stats(self, date: str, data: dict) -> None:
        conn = await self._get_conn()
        now = datetime.utcnow().isoformat()
        payload = {**data, "date": date, "updated_at": now}
        cols = ", ".join(payload.keys())
        placeholders = ", ".join("?" * len(payload))
        update_clause = ", ".join(
            f"{k} = excluded.{k}"
            for k in payload
            if k not in ("id", "date", "created_at")
        )
        await conn.execute(
            f"""
            INSERT INTO daily_stats ({cols}) VALUES ({placeholders})
            ON CONFLICT(date) DO UPDATE SET {update_clause}
            """,
            list(payload.values()),
        )
        await conn.commit()

    # ── Config ─────────────────────────────────────────────────────────────────

    async def get_config(self, key: str) -> Optional[str]:
        conn = await self._get_conn()
        cursor = await conn.execute("SELECT value FROM config WHERE key = ?", (key,))
        row = await cursor.fetchone()
        return row["value"] if row else None

    async def set_config(
        self,
        key: str,
        value: str,
        updated_by: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        conn = await self._get_conn()
        now = datetime.utcnow().isoformat()
        await conn.execute(
            """
            INSERT INTO config (key, value, updated_by, updated_at, description)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value       = excluded.value,
                updated_by  = excluded.updated_by,
                updated_at  = excluded.updated_at,
                description = COALESCE(excluded.description, config.description)
            """,
            (key, value, updated_by, now, description),
        )
        await conn.commit()

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
            logger.info("Database connection closed.")


# ── Module helpers ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def get_db(db_path: str = DB_PATH):
    global _db_instance
    if _db_instance is None:
        _db_instance = TradeDB(db_path)
        await _db_instance.init_db()
    try:
        yield _db_instance
    finally:
        pass


async def close_db() -> None:
    global _db_instance
    if _db_instance is not None:
        await _db_instance.close()
        _db_instance = None
