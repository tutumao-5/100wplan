"""
OKX Auto Trading Agent 2 - Main Entry Point (Hsaka + AI)
调度层：全局初始化、主循环、日志、配置读取
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# 加载 .env 环境变量
load_dotenv(_ROOT / ".env")

from core.scanner import scan_all
from core.executor import Executor
from core.pattern_learner import PatternLearner
from core.trade_db import TradeDB, get_db
from core.tg_bot import TgBot

_ROOT = Path(__file__).resolve().parent

# -------------------------------------------------------------------------
# 日志配置
# -------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            _ROOT / "logs" / f"main_{datetime.now():%Y%m%d}.log",
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger("main")

CONFIG_PATH = _ROOT / "config" / "settings_v2.json"


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _read_api_credentials(config: dict) -> tuple[str, str, str]:
    """优先读 settings_v2.json，回退环境变量。"""
    exchange_cfg = config.get("exchange", {})
    api_key = exchange_cfg.get("okx_api_key") or os.environ.get("OKX_API_KEY", "")
    secret = exchange_cfg.get("okx_secret") or os.environ.get("OKX_SECRET", "")
    passphrase = exchange_cfg.get("okx_passphrase") or os.environ.get("OKX_PASSPHRASE", "")
    return api_key, secret, passphrase


class TradingBot:
    """主交易机器人 — 负责全局调度，不包含业务逻辑。"""

    def __init__(self, tg_bot: TgBot, executor: Executor, db: TradeDB, config: dict) -> None:
        self.config = config
        self.db = db
        self.executor = executor
        self.tg_bot = tg_bot
        # 优先读 SCAN_INTERVAL 环境变量（.env），回退 settings_v2.json
        env_interval = os.environ.get("SCAN_INTERVAL")
        if env_interval is not None:
            self.scan_interval = int(env_interval)
        else:
            self.scan_interval = config.get("scanner", {}).get("scan_interval_seconds", 60)
        self.learner = PatternLearner(db)
        self._cycle = 0

    # ------------------------------------------------------------------
    async def _trading_loop(self) -> None:
        while True:
            self._cycle += 1
            await self._heartbeat()

            # 全局开关监听
            if self.tg_bot.is_paused():
                logger.info("[交易暂停] 跳过本轮扫描")
                await asyncio.sleep(self.scan_interval)
                continue

            try:
                await self._run_once()
            except Exception as exc:
                logger.error("[周期 %d] 未捕获异常: %s", self._cycle, exc, exc_info=True)
                logger.info("60 秒后重试…")
                await asyncio.sleep(60)
                continue

            logger.info("[周期 %d] 本轮完成，休眠 %d 秒", self._cycle, self.scan_interval)
            await asyncio.sleep(self.scan_interval)

    # ------------------------------------------------------------------
    async def _run_once(self) -> None:
        logger.info("[周期 %d] 开始扫描所有标的…", self._cycle)
        report = await scan_all()

        # 更新 tg_bot 拉取统计，供 /status 使用
        self.tg_bot.update_fetch_stats(
            report.get("fetch_success", 0),
            report.get("fetch_total", 0)
        )

        total = report.get("total_coins", 0)
        written = report.get("signals_written", 0)
        signals = report.get("signals", [])

        logger.info(
            "[周期 %d] 扫描完成 — 总标的: %d | RSI筛选: %d | VMA筛选: %d | Hsaka筛选: %d | 写入信号: %d",
            self._cycle,
            total,
            report.get("rsi_filtered", 0),
            report.get("vma_filtered", 0),
            report.get("hsaka_filtered", 0),
            written,
        )

        executed = 0
        for signal in signals:
            inst_id = signal.get("inst_id", "UNKNOWN")

            # 战报推送：推送信号通知
            try:
                await self.tg_bot.send_signal_alert(signal)
            except Exception as exc:
                logger.error("  推送信号通知异常 [%s]: %s", inst_id, exc, exc_info=True)

            try:
                result = await self.executor.execute_signal(signal)
                if result and result.get("success"):
                    executed += 1
                    logger.info("  ✓ 开仓成功: %s | ordId=%s", inst_id, result.get("ord_id", "-"))
                    # 战报推送：开仓成功
                    try:
                        await self.tg_bot.send_execution_alert(result)
                    except Exception as exc:
                        logger.error("  推送执行通知异常 [%s]: %s", inst_id, exc, exc_info=True)
                else:
                    reason = result.get("reason", "-") if result else "无返回"
                    logger.info("  ✗ 跳过/拒绝: %s | 原因: %s", inst_id, reason)
            except Exception as exc:
                logger.error("  执行信号异常 [%s]: %s", inst_id, exc, exc_info=True)

        logger.info("[周期 %d] 本轮执行开仓: %d 笔", self._cycle, executed)

    # ------------------------------------------------------------------
    async def _heartbeat(self) -> None:
        logger.info(
            "[Heartbeat] 系统运行中 | 周期: %d | 时间: %s",
            self._cycle,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------
async def main() -> None:
    # 1. 初始化数据库
    logger.info("初始化数据库…")
    db = TradeDB()
    await db.init_db()
    logger.info("数据库就绪: %s", db.db_path)

    # 2. 加载配置
    logger.info("加载配置: %s", CONFIG_PATH)
    config = load_config()
    logger.info(
        "配置加载完成 — 版本: %s | 项目: %s",
        config.get("version", "?"),
        config.get("system", {}).get("project_name", "?"),
    )

    # 3. 实例化 Executor
    api_key, secret, passphrase = _read_api_credentials(config)
    executor = Executor(api_key=api_key, secret=secret, passphrase=passphrase, db=db)

    # 4. 实例化 TgBot
    bot_token = os.getenv("TG_BOT_TOKEN")
    chat_id = os.getenv("TG_CHAT_ID")
    tg_bot = TgBot(bot_token, chat_id, db, executor)

    # 5. 实例化 TradingBot
    bot = TradingBot(tg_bot=tg_bot, executor=executor, db=db, config=config)

    # 6. 点火仪式
    await tg_bot.send_message(f"🚀 金大帅，okx100w计划已点火成功！当前扫描频率：{bot.scan_interval}s/次。监控中...")

    # 7. asyncio.create_task 启动双核（非阻塞）
    logger.info("=== 系统就绪，启动双核异步任务 ===")
    await asyncio.sleep(0)  # 让事件循环开始
    asyncio.create_task(tg_bot.start())
    asyncio.create_task(bot._trading_loop())

    # 7. 主协程保持运行
    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在退出…")
    finally:
        await executor.close()
        logger.info("资源已释放，程序退出。")


if __name__ == "__main__":
    # 确保 logs/ 目录存在
    (_ROOT / "logs").mkdir(exist_ok=True)
    asyncio.run(main())
