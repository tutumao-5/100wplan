"""
OKX永续合约自动交易2号系统 - Core模块
导出所有核心组件
"""

# Scanner - 币种扫描器
from core.scanner import (
    SYMBOLS as COINS,
    get_available_coins,
    scan_all,
)

# TradeDB - 异步数据库
# 注意: init_db 是 TradeDB.init_db() 方法，非standalone函数
from core.trade_db import (
    TradeDB,
    get_db,
    close_db,
    DB_PATH,
)

# Hsaka 7大技术工具箱
from core.hsaka_analyzer import (
    check_sfp,
    check_liquidity_sensing,
    check_supply_demand_zone,
    check_bollinger_band,
    check_funding_rate,
    check_range_fakeout,
    check_high_session,
    run_all_checks,
)

# Executor - 订单执行器
from core.executor import (
    Executor,
    OrderParams,
)

# PatternLearner - AI进化学习引擎
from core.pattern_learner import (
    PatternLearner,
    get_learner,
    EVOLUTION_THRESHOLD,
    DEFAULT_CONDITION_WEIGHTS,
)

__all__ = [
    # Scanner
    "COINS",
    "get_available_coins",
    "scan_all",
    # TradeDB
    "TradeDB",
    "get_db",
    "close_db",
    "DB_PATH",
    # Hsaka 7大技术工具箱
    "check_sfp",
    "check_liquidity_sensing",
    "check_supply_demand_zone",
    "check_bollinger_band",
    "check_funding_rate",
    "check_range_fakeout",
    "check_high_session",
    "run_all_checks",
    # Executor
    "Executor",
    "OrderParams",
    # PatternLearner
    "PatternLearner",
    "get_learner",
    "EVOLUTION_THRESHOLD",
    "DEFAULT_CONDITION_WEIGHTS",
]
