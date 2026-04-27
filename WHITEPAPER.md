# OKX 自动交易 2号（Hsaka + AI 进化版）
# 系统架构与业务逻辑白皮书

> **版本**：v2.0 正式版（封版）
> **归档日期**：2026-04-27
> **状态**：已封版，未经全体评审不得修改

---

## 一、项目概述

### 1.1 项目定位

OKX 永续合约自动交易 2号是由金大帅（一人公司 CEO）主导的第二代加密货币量化交易系统。核心目标是构建一套融合 Twitter 顶级交易员 Hsaka 实战体系与 AI 持续进化能力的实战型自动交易引擎。

区别于 1号系统的简单规则驱动，2号系统以旁路学习机制为核心，实现从"规则驱动"到"数据驱动"再到"AI进化"的跨越。

### 1.2 核心设计哲学

| 原则 | 说明 |
|------|------|
| **旁路进化** | AI学习独立于主交易链路运行，永不阻塞下单 |
| **漏斗降维** | 多层过滤压缩候选集，杜绝API熔断 |
| **极速响应** | SFP等极速信号触发时允许市价直接入场 |
| **正向学习** | 不仅记住亏损，更提炼盈利条件指导扫描 |
| **冷启动保护** | 30笔交易前纯Hsaka规则，30笔后AI逐步接管 |

### 1.3 三大补丁（v2.0 关键设计决策）

#### 补丁1：漏斗式降维 ✅ 必要，且必须

**问题根源**：129币种全量拉取订单簿+资金费率 → API频率极高 → 触发交易所熔断 → 实盘宕机

**补丁方案**：

```
步骤② 并发拉取K线
    ↓
【漏斗1 - RSI极值初筛】← 极轻量，零API成本
   RSI < 30 OR RSI > 65 → 命中进入下一层
   其他 → 直接淘汰，不发任何API请求
    ↓（假设剩 10-15 个候选）
【漏斗2 - VMA倍率次筛】
   VMA > 1.8 → 命中
   其他 → 淘汰
    ↓（假设剩 5-8 个）
【Hsaka七层深度过滤】← 此时才允许调订单簿+资金费率API
   → 最终 1-3 个信号写入
```

**量化价值**：从 129次深度API调用 → 压缩到 5-8次，节省 **90%+ API开销**

#### 补丁2：大脑引擎 DeepSeek V4 ✅ 必要

**问题根源**：PatternLearner 用规则统计 → 无法发现复杂组合条件 → 进化能力上限很低

**补丁方案**：引入 DeepSeek V4 作为大脑引擎，处理复杂模式发现与条件组合推理（待详细设计）

#### 补丁3：极速动能抢筹（市价单入场）✅ 必要

**问题根源**：SFP等极速反转信号窗口极短，限价单容易踏空错过入场时机

**补丁方案**：
- 当信号同时满足 `hsaka_score >= 6.5` **且** 触发了 SFP 或 流动性抓取 → 强制走市价单通道（Market Order）
- 其他常规信号 → 走限价单通道（Limit Order），等待更优价格成交

---

## 二、项目架构

### 2.1 整体框架图

```
                     OKX 自动交易 2号 - Hsaka + AI 进化版
                        整体框架 v2.0
====================================================================

┌──────────────────────────────────────────────────────────────────────┐
│  📁 config/                                                           │
│     ├── settings_v2.json     全局常量（滑点/仓位/风控参数）            │
│     └── sectors.json         币种板块映射（18板块）                    │
└──────────────────────────────────────────────────────────────────────┘
                                │
┌──────────────────────────────────────────────────────────────────────┐
│  📁 core/                                                              │
│     ├── scanner.py          币种扫描器（129币种漏斗筛选）              │
│     ├── trade_db.py         异步SQLite数据库操作层                    │
│     ├── executor.py         订单执行引擎（路由决策+市价/限价执行）     │
│     ├── hsaka_analyzer.py   Hsaka 7大技术工具箱                       │
│     ├── pattern_learner.py  AI进化学习引擎（PatternLearner）           │
│     └── __init__.py                                                     │
└──────────────────────────────────────────────────────────────────────┘
                                │
┌──────────────────────────────────────────────────────────────────────┐
│  📁 data/                                                             │
│     └── trading_2.db      SQLite数据库                               │
└──────────────────────────────────────────────────────────────────────┘

====================================================================
                           数据流
====================================================================

  Scanner.scan_all()
        │
        ├─ ① 加载 blocking_lessons（PatternLearner）
        │     └─ BLOCK → 从候选池移除
        │
        ├─ ② asyncio.gather 并发拉取 129币种×100根 1h K线
        │     └─ 代理：http://127.0.0.1:7897
        │
        ├─ ③ RSI极值初筛（零API成本）
        │     └─ RSI < 30 OR > 65 → 保留（10-15个）
        │
        ├─ ④ VMA倍率次筛
        │     └─ VMA > 1.8 → 保留（5-8个）
        │
        ├─ ⑤ Hsaka七层深度过滤（仅对通过④者）
        │     ├─ SFP / 流动性 / 供需区 / 布林带
        │     ├─ 资金费率 / 假突破 / 高时段
        │     └─ 最终 1-3 个信号写入 signals 表
        │
        └─ ⑥ AI动态打分（进化态启用，≥30笔后）

  订单执行路由（Executor）
        │
        ├─ 高分极速信号（hsaka_score ≥ 6.5 且触发 SFP/流动性）
        │     └─ → 市价单通道（Market Order）⚡ 极速抢筹
        │
        └─ 常规信号
              └─ → 限价单通道（Limit Order）💰 等待最优价

  平仓流程
        ├─ 触发 pattern_learner.record_trade()    ← 平仓时记录特征
        └─ 触发每日统计更新
```

### 2.2 目录结构

```
okx100w计划/
├── config/
│   ├── settings_v2.json      ✅ 已存在（全局常量）
│   └── sectors.json           ✅ 已存在（18板块映射）
├── core/
│   ├── scanner.py             ✅ 已存在（漏斗扫描器）
│   ├── trade_db.py            ✅ 已存在（异步数据库）
│   ├── executor.py            ✅ 已存在（订单执行引擎）
│   ├── hsaka_analyzer.py      ✅ 已存在（Hsaka7工具箱）
│   ├── pattern_learner.py      ✅ 已存在（AI进化引擎）
│   └── __init__.py            ✅ 已存在
├── data/
│   └── trading_2.db          ✅ 已存在（SQLite）
├── .env.example               ✅ 已存在
└── WHITEPAPER.md             ✅ 已存在（本文档）
```

### 2.3 核心模块

| 模块 | 职责 |
|------|------|
| `Scanner` | 漏斗降维：129币种 → RSI极值 → VMA过滤 → Hsaka七层深度过滤 → 1-3个信号 |
| `Executor` | 订单执行引擎：开仓路由决策、市价/限价执行、止损止盈、余额查询 |
| `HsakaAnalyzer` | 7大技术工具箱：SFP/流动性/供需区/布林带/资金费率/假突破/高时段 |
| `PatternLearner` | AI进化引擎：从历史交易数据提取盈利条件权重，冷启动30笔后激活 |
| `TradeDB` | 异步数据库操作层：5张表（signals/orders/pattern_trades/daily_stats/config） |

---

## 三、数据库设计

### 3.1 signals（信号表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | 主键自增 |
| `inst_id` | TEXT | 币种ID（如 BTC-USDT-SWAP） |
| `sector` | TEXT | 板块名称 |
| `direction` | TEXT | `long` 或 `short` |
| `rsi` | REAL | RSI指标值 |
| `vma_ratio` | REAL | VMA倍率 |
| `atr` | REAL | ATR值 |
| `atr_ratio` | REAL | ATR相对波动率 |
| `funding_rate` | REAL | 资金费率 |
| `hsaka_score` | REAL | Hsaka七层综合得分（≥5.5通过） |
| `ai_score` | REAL | AI动态打分（进化态启用） |
| `hsaka_sfp` | INTEGER | SFP触发标记（0/1） |
| `hsaka_liq` | INTEGER | 流动性触发标记（0/1） |
| `supply_demand_zone` | INTEGER | 供需区信号（0/1） |
| `range_fakeout` | INTEGER | 区间假突破信号（0/1） |
| `high_session` | INTEGER | 高波动时段信号（0/1） |
| `session_flag` | TEXT | 亚/欧/美交易时段标记 |
| `ai_weight` | REAL | PatternLearner动态权重（0~1） |
| `position_ratio` | REAL | 建议仓位比例 |
| `entry_price` | REAL | 建议入场价 |
| `stop_loss` | REAL | 建议止损价 |
| `take_profit` | REAL | 建议止盈价 |
| `created_at` | TEXT | 创建时间 |
| `cooldown_until` | TEXT | 冷却到期时间 |
| `expired_at` | TEXT | 信号失效时间 |
| `used` | INTEGER | 是否已使用（0/1） |
| `used_order_id` | INTEGER | 关联订单ID |

### 3.2 orders（订单表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | 主键自增 |
| `signal_id` | INTEGER | 关联signals表ID |
| `inst_id` | TEXT | 币种ID |
| `sector` | TEXT | 板块名称 |
| `order_type` | TEXT | `entry`/`close_30`/`close_trailing`/`stop_loss`/`close_manual` |
| `side` | TEXT | `buy` 或 `sell` |
| `position_side` | TEXT | `long` 或 `short` |
| `quantity` | REAL | 委托数量（张或U） |
| `price` | REAL | 限价单价（市价单填0） |
| `fill_price` | REAL | 成交价 |
| `fill_qty` | REAL | 成交数量 |
| `ord_id` | TEXT | 交易所订单ID |
| `position_id` | TEXT | 持仓ID |
| `stop_loss` | REAL | 止损价 |
| `take_profit` | REAL | 止盈价 |
| `status` | TEXT | `pending`/`open`/`filled`/`cancelled`/`failed` |
| `close_reason` | TEXT | 平仓原因 |
| `close_price` | REAL | 平仓价格 |
| `pnl` | REAL | 盈亏金额（USD） |
| `pnl_pct` | REAL | 盈亏比例 |
| `created_at` | TEXT | 创建时间 |
| `updated_at` | TEXT | 更新时间 |

### 3.3 pattern_trades（模式交易表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | 主键自增 |
| `inst_id` | TEXT | 币种ID |
| `sector` | TEXT | 板块名称 |
| `rsi` | REAL | 入场RSI值 |
| `vma_ratio` | REAL | 入场VMA倍率 |
| `atr_ratio` | REAL | ATR相对波动率 |
| `funding_rate` | REAL | 入场时资金费率 |
| `hsaka_sfp` | INTEGER | SFP是否触发 |
| `hsaka_liq` | INTEGER | 流动性是否触发 |
| `session_flag` | TEXT | 交易时段 |
| `supply_demand_zone` | INTEGER | 供需区信号 |
| `range_fakeout` | INTEGER | 区间假突破信号 |
| `entry_price` | REAL | 入场价格 |
| `exit_price` | REAL | 出场价格 |
| `pnl` | REAL | 盈亏金额 |
| `pnl_pct` | REAL | 盈亏百分比 |
| `close_reason` | TEXT | 平仓原因 |
| `duration` | INTEGER | 持仓时长（秒） |
| `ai_weight` | REAL | 入场时AI权重 |
| `order_id` | TEXT | 订单ID |
| `position_id` | TEXT | 持仓ID |
| `timestamp` | TEXT | 平仓时间 |

### 3.4 daily_stats（每日统计表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER PK | 主键自增 |
| `date` | TEXT | 日期（YYYY-MM-DD） |
| `total_trades` | INTEGER | 当日交易次数 |
| `winning_trades` | INTEGER | 盈利交易数 |
| `losing_trades` | INTEGER | 亏损交易数 |
| `win_rate` | REAL | 胜率 |
| `total_pnl` | REAL | 总盈亏（USD） |
| `avg_pnl` | REAL | 平均盈亏 |
| `max_drawdown` | REAL | 最大回撤 |
| `open_positions` | INTEGER | 收盘持仓数 |
| `new_signals` | INTEGER | 新信号数 |
| `closing_count` | INTEGER | 平仓次数 |
| `win_count` | INTEGER | 盈利次数 |
| `loss_count` | INTEGER | 亏损次数 |
| `equity_hwm` | REAL | 权益高点 |
| `equity_close` | REAL | 收盘权益 |
| `melt_status` | TEXT | 熔断状态 |
| `last_melt_time` | TEXT | 上次熔断时间 |
| `created_at` | TEXT | 创建时间 |
| `updated_at` | TEXT | 更新时间 |

### 3.5 config（配置表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `key` | TEXT PK | 配置键 |
| `value` | TEXT | 配置值（JSON字符串） |
| `updated_by` | TEXT | 更新者 |
| `updated_at` | TEXT | 更新时间 |
| `description` | TEXT | 配置说明 |

---

## 四、Scanner 扫描漏斗

### 4.1 扫描流程（scan_all）

```
scan_all()
│
├─ ① 加载 blocking_lessons（来自 PatternLearner）
│     └─ 有 BLOCK → 从候选池移除对应币种
│
├─ ② asyncio.gather 并发拉取 129 币种 × 100根 1h K线
│     ├─ 代理：http://127.0.0.1:7897
│     ├─ 超时：每次 10s
│     └─ 重试：指数退避 1s → 2s → 4s（3次）
│
├─ ③ 【漏斗初筛 - RSI极值】（零API成本，纯计算）
│     ├─ rsi < 30 → 候选（超卖，可能反弹）
│     └─ rsi > 65 → 候选（超买，可能回落）
│     └─ 其他 → 直接淘汰，不进入下一步
│
├─ ④ 【次级筛选 - VMA倍率】
│     └─ vma_ratio > 1.8 → 保留
│
├─ ⑤ 【Hsaka七层深度过滤】（仅对漏斗③+④通过者，5-8个币种）
│     │
│     ├─[1] SFP 摆动失败模式
│     ├─[2] 流动性抓取
│     ├─[3] 供需区域识别
│     ├─[4] 布林带区间
│     ├─[5] 资金费率过滤
│     ├─[6] 区间假突破检测
│     └─[7] 高波动时段确认（亚/欧/美）
│
└─ ⑥ 【AI动态打分】（进化态：≥30笔交易后启用）
      └─ ai_score = PatternLearner.score_signal(signal)
```

### 4.2 Hsaka 7大技术工具箱

| # | 技术 | 函数 | 说明 |
|---|------|------|------|
| 1 | SFP（摆动失败模式） | `check_sfp(inst_id)` | 现货Funny Point，量价同步突破 |
| 2 | 流动性抓取 | `check_liquidity_sensing(inst_id)` | 抓机构大单附近的流动性空缺 |
| 3 | 供需区域 | `check_supply_demand_zone(inst_id)` | 识别支撑/压力区域 |
| 4 | 布林带区间 | `check_bollinger_band(inst_id)` | 价格触及布林带极端位置 |
| 5 | 资金费率 | `check_funding_rate(inst_id)` | >0.03%偏多，<-0.03%偏空 |
| 6 | 区间假突破 | `check_range_fakeout(inst_id)` | 假突破区间后的反转信号 |
| 7 | 高波动时段 | `check_high_session(inst_id)` | 亚盘(2-5)/欧盘(8-11)/美盘(14-17) UTC |

每个函数返回统一格式：
```python
{"triggered": bool, "signal": str, "score": float, "details": dict}
```

---

## 五、PatternLearner AI进化引擎

### 5.1 冷启动机制

| 阶段 | 有效交易笔数 | 行为 |
|------|------------|------|
| **冷启动态** | < 30笔 | 纯Hsaka规则打分，AI权重固定为默认值 |
| **进化态** | ≥ 30笔 | PatternLearner从pattern_trades提取条件权重 |

### 5.2 权重数据结构（version 30）

```json
{
  "version": 30,
  "updated_at": "2026-04-27T12:00:00Z",
  "冷启动": false,
  "weights": {
    "rsi_oversold":    { "score": +0.8,  "min": 0,    "max": 30,  "count": 18, "win_rate": 0.78, "avg_pnl": 2.4 },
    "rsi_neutral_low": { "score": -0.4,  "min": 30,   "max": 45,  "count": 12, "win_rate": 0.25, "avg_pnl": -1.8 },
    "vma_strong":      { "score": +0.6,  "min": 1.8,  "max": null, "count": 15, "win_rate": 0.72, "avg_pnl": 1.9 },
    "vma_weak":        { "score": -0.5,  "min": null, "max": 1.5, "count": 5,  "win_rate": 0.20, "avg_pnl": -2.1 }
  }
}
```

### 5.3 正向学习原则

- 统计归纳**盈利交易**的共同特征（RSI区间、VMA倍率、资金费率正负、Hsaka信号组合）
- 对盈利条件赋予**正向分数**，亏损条件赋予**负向分数**
- 动态权重按 `condition_weights` 字典反馈给 Scanner，用于**信号打分排序**
- 不仅记住亏损模式，更提炼**赚钱条件**指导扫描

### 5.4 大脑引擎 DeepSeek V4（补丁2）

引入 DeepSeek V4 作为大脑引擎，处理复杂模式发现与条件组合推理，大幅提升 PatternLearner 发现复杂盈利条件的能力。

---

## 六、Executor 订单执行引擎

### 6.1 核心职责

Executor 是 2号系统的订单执行中枢，负责：
- **开仓路由决策**：判断信号走市价单还是限价单通道
- **订单执行**：市价开仓、限价挂单、平仓、取消挂单
- **止损止盈**：开仓时自动附加 SL/TP，支持追踪止损
- **余额查询**：实时获取可用 USDT 余额

### 6.2 开仓路由逻辑

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Executor 开仓路由决策树                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Scanner 信号触发                                                     │
│       │                                                              │
│       ▼                                                              │
│  ┌─────────────────────────────┐                                    │
│  │ hsaka_score >= 6.5          │                                    │
│  │   AND                        │                                    │
│  │ (hsaka_sfp == 1 OR hsaka_liq == 1)                                │
│  └──────────┬──────────────────┘                                    │
│       YES ──┴── NO                                                    │
│        │        │                                                   │
│        ▼        ▼                                                   │
│  ┌──────────┐ ┌──────────────────────────────┐                     │
│  │市价单通道 │ │限价单通道                     │                     │
│  │⚡Market   │ │💰 Limit Order                │                     │
│  │execute_   │ │execute_limit_order()        │                     │
│  │market_    │ │                              │                     │
│  │order()     │ │等待价格回落到 entry_price    │                     │
│  │            │ │附近成交                      │                     │
│  └──────────┘ └──────────────────────────────┘                     │
│                                                                     │
│  【补丁3 - 极速动能抢筹】                                             │
│  高分极速信号强制市价单，避免踏空                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**路由条件明细**：

| 条件 | 路由 | 触发原因 |
|------|------|----------|
| `hsaka_score >= 6.5` **且** `hsaka_sfp == 1` | ⚡ 市价单 | SFP反转窗口极短，量价同步突破必须抢筹 |
| `hsaka_score >= 6.5` **且** `hsaka_liq == 1` | ⚡ 市价单 | 流动性抓取信号同样要求极速响应 |
| 其他常规信号（`hsaka_score 5.5~6.5`） | 💰 限价单 | 等待价格回落至更优入场价 |
| `hsaka_score < 5.5` | ❌ 淘汰 | 未通过最低门槛，不执行 |

### 6.3 核心方法

| 方法 | 说明 | 返回 |
|------|------|------|
| `execute_market_order(inst_id, side, volume, stop_loss, take_profit)` | 市价开仓，附带止损止盈 | `{"success": bool, "filled_price": float, "position_id": str, ...}` |
| `execute_limit_order(inst_id, side, price, volume)` | 限价挂单 | `{"success": bool, "ord_id": str, ...}` |
| `close_position(position_id, reason)` | 平仓 | `{"success": bool, "pnl": float, "close_price": float, ...}` |
| `cancel_order(ord_id, inst_id)` | 取消挂单 | `{"success": bool, ...}` |
| `get_free_balance()` | 查询可用 USDT | `float` |

### 6.4 市价单执行流程（execute_market_order）

```
1. 创建本地订单记录（status=pending）
2. 发送市价单 → OKX
3. 获取成交结果（filled_price, filled_qty）
4. 计算止损/止盈价格（未指定时按默认比例）
5. 获取持仓ID（fetch_positions）
6. 更新本地订单记录（status=filled）
7. 附加止损止盈（_set_sl_tp）
8. 返回执行结果
```

### 6.5 止损止盈附加逻辑

| 方向 | 止损计算 | 止盈计算 |
|------|----------|----------|
| `buy`（做多） | `entry_price × (1 - stop_loss_pct)` | `entry_price × (1 + take_profit_pct)` |
| `sell`（做空） | `entry_price × (1 + stop_loss_pct)` | `entry_price × (1 - take_profit_pct)` |

默认值：`stop_loss_pct = 2%`，`take_profit_pct = 6%`

---

## 七、风控参数（settings_v2.json）

| 参数分类 | 参数名 | 默认值 | 说明 |
|---------|--------|--------|------|
| **仓位** | `max_position_usdt` | 100 | 单笔最大仓位（USD） |
| **仓位** | `max_position_pct` | 10% | 占余额比例上限 |
| **仓位** | `max_total_positions` | 5 | 最大同时持仓数 |
| **风控** | `stop_loss_pct` | 2% | 止损比例 |
| **风控** | `take_profit_pct` | 6% | 止盈比例 |
| **风控** | `trailing_stop_pct` | 1% | 追踪止损回撤比例 |
| **风控** | `max_sector_exposure` | 30% | 单板块最大暴露 |
| **Scanner** | `min_hsaka_score` | 5.5 | Hsaka最低评分门槛 |
| **Scanner** | `rsi_oversold` | 30 | RSI超卖阈值 |
| **Scanner** | `rsi_overbought` | 65 | RSI超买阈值 |
| **Scanner** | `vma_threshold` | 1.8 | VMA倍率门槛 |
| **Scanner** | `signal_validity_hours` | 2 | 信号有效期（小时） |
| **Scanner** | `cooldown_seconds` | 3600 | 同一币种冷却时间（秒） |
| **Executor** | `fast_track_score` | 6.5 | 极速动能抢筹（市价单）触发门槛 |

---

## 八、开发步骤（11步计划）

| 步骤 | 内容 | 状态 |
|------|------|------|
| 第1步 | 基础目录结构（config/ + core/ + data/） | ✅ 完成 |
| 第2步 | trade_db.py（异步数据库，23个方法，5张表） | ✅ 完成 |
| 第3步 | core库类型文件（executor/hsaka_analyzer/pattern_learner/__init__） | ✅ 完成 |
| 第4步 | 主入口 main.py（整合所有模块） | 📍 待开发 |
| 第5步 | Telegram Bot（信号推送 + 交互控制） | 📍 待开发 |
| 第6步 | 回测引擎（历史数据回测） | 📍 待开发 |
| 第7步 | Dashboard / Web UI | 📍 待开发 |
| 第8步 | *(待补充)* | - |
| 第9步 | *(待补充)* | - |
| 第10步 | *(待补充)* | - |
| 第11步 | *(待补充)* | - |

---

## 九、关键文件清单

```
/home/jwx/okx100w计划/
├── config/
│   ├── settings_v2.json      ✅ 已存在
│   └── sectors.json          ✅ 已存在
├── core/
│   ├── __init__.py           ✅ 已存在
│   ├── scanner.py            ✅ 已存在
│   ├── trade_db.py          ✅ 已存在
│   ├── executor.py           ✅ 已存在
│   ├── hsaka_analyzer.py    ✅ 已存在
│   └── pattern_learner.py    ✅ 已存在
├── data/
│   └── trading_2.db          ✅ 已存在
├── .env.example              ✅ 已存在
└── WHITEPAPER.md            ✅ 已存在（本文档）
```

---

## 十、版本历史

| 版本 | 日期 | 状态 | 说明 |
|------|------|------|------|
| v2.0 正式版 | 2026-04-27 | 封版 | 融合Hsaka7大技术 + PatternLearner AI进化 + 三大补丁 |
| v1.x | 2026-04-21 | 归档 | 1号系统（规则驱动） |
