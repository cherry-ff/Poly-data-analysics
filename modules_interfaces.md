# Poly-15minBTC 模块与接口拆分

本文档基于 [summary.md](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/summary.md) 拆解出第一版可落地的模块、目录和接口契约。

目标不是一次把系统拆成微服务，而是先做一个 **单进程、异步、内存热状态** 的实盘骨架，优先验证：

1. 市场发现与切换是否稳定  
2. 定价模型与 maker-first 报价是否可用  
3. 单腿回补和 close-phase 风控是否可靠  
4. 回报一致性和 ghost fill 修复是否可控

---

## 1. 第一版实现边界

### 1.1 建议技术基线

- 语言：`Python 3.11+`
- 并发模型：`asyncio`
- 运行形态：`单进程 + 多异步任务`
- 热状态：全部内存维护
- 冷路径：异步写文件
- 交易所适配：先只做 `Polymarket + Binance + Chainlink`

### 1.2 第一版不急着拆出去的东西

- 不先拆多进程
- 不先做数据库依赖
- 不先做 UI
- 不先做跨市场扩展
- 不先做高频语言重写

### 1.3 第一版必须做对的东西

- `Market Registry`
- `Lifecycle Manager`
- `Binance / Chainlink / Polymarket` 三路输入
- `Vol Model + Fair Value + Quote Policy`
- `Pair Maker / Selective Taker / Recovery`
- `Risk Engine`
- `Execution Gateway`
- `Order State + Inventory State`
- `Recorder + Replay`

---

## 2. 推荐目录结构

```text
Poly-15minBTC/
  summary.md
  modules_interfaces.md
  app/
    bootstrap.py
    config.py
    main.py
  core/
    clock.py
    event_bus.py
    ids.py
    enums.py
    errors.py
  domain/
    models.py
    events.py
    contracts.py
  market/
    registry.py
    lifecycle.py
    metadata_loader.py
  feeds/
    base.py
    binance_ws.py
    chainlink_feed.py
    polymarket_market_ws.py
    polymarket_user_ws.py
  state/
    book_state.py
    order_state.py
    inventory_state.py
    signal_state.py
  pricing/
    vol_model.py
    fair_value.py
    quote_policy.py
    lead_lag.py
  strategy/
    pair_strategy.py
    recovery_strategy.py
    phase_policy.py
  risk/
    rules.py
    engine.py
  execution/
    intents.py
    order_builder.py
    router.py
    polymarket_gateway.py
    cancel_manager.py
  storage/
    recorder.py
    snapshot_writer.py
  replay/
    player.py
  observability/
    metrics.py
    alerts.py
  tests/
    unit/
    integration/
    replay/
```

---

## 3. 模块总览

| 模块 | 职责 | 主要输入 | 主要输出 |
| --- | --- | --- | --- |
| `app` | 启动、装配、配置注入 | 配置文件、环境变量 | 全局任务与依赖图 |
| `core` | 通用基础设施 | 时间、事件、ID | 基础能力 |
| `domain` | 领域模型与事件定义 | 业务语义 | 数据契约 |
| `market` | 市场发现、元数据、生命周期 | Polymarket 市场事件 | 活跃 market 状态 |
| `feeds` | 三路行情和用户回报接入 | 外部 WS / API | 标准化事件 |
| `state` | 热状态内存存储 | 标准化事件、执行回报 | 最新状态快照 |
| `pricing` | 波动率、理论价、报价策略 | Binance、Chainlink、盘口 | `theo`、quote 建议 |
| `strategy` | pair、recovery、phase 策略 | 定价结果、状态快照 | 订单意图 |
| `risk` | 下单前同步风控 | 订单意图、状态快照 | 放行 / 拒绝 / 降级 |
| `execution` | 下单、撤单、路由、回报处理 | 已放行意图 | 交易所请求与执行回报 |
| `storage` | 冷存储和录制 | 事件流、状态快照 | 回放文件 |
| `replay` | 离线回放 | 录制文件 | 重放事件流 |
| `observability` | 指标与告警 | 全部模块事件 | 监控和告警 |

---

## 4. 核心领域对象

### 4.1 MarketMetadata

```python
@dataclass(slots=True)
class MarketMetadata:
    market_id: str
    condition_id: str
    up_token_id: str
    down_token_id: str
    start_ts_ms: int
    end_ts_ms: int
    tick_size: Decimal
    fee_rate_bps: Decimal
    min_order_size: Decimal
    status: str
```

### 4.2 Price Ticks

```python
@dataclass(slots=True)
class BinanceTick:
    symbol: str
    event_ts_ms: int
    recv_ts_ms: int
    last_price: Decimal
    best_bid: Decimal
    best_ask: Decimal

@dataclass(slots=True)
class ChainlinkTick:
    feed: str
    oracle_ts_ms: int
    recv_ts_ms: int
    price: Decimal
    round_id: str
```

### 4.3 Book Top

```python
@dataclass(slots=True)
class OutcomeBookTop:
    token_id: str
    best_bid_px: Decimal
    best_bid_sz: Decimal
    best_ask_px: Decimal
    best_ask_sz: Decimal
    last_update_ts_ms: int
```

### 4.4 理论价与报价

```python
@dataclass(slots=True)
class TheoSnapshot:
    market_id: str
    ts_ms: int
    sigma_short: Decimal
    theo_up: Decimal
    theo_down: Decimal
    directional_bias: Decimal
    target_full_set_cost: Decimal

@dataclass(slots=True)
class QuotePlan:
    market_id: str
    ts_ms: int
    up_bid_px: Decimal | None
    up_ask_px: Decimal | None
    down_bid_px: Decimal | None
    down_ask_px: Decimal | None
    reason: str
```

### 4.5 订单意图与执行回报

```python
@dataclass(slots=True)
class OrderIntent:
    intent_id: str
    pair_id: str | None
    market_id: str
    token_id: str
    side: str
    price: Decimal
    size: Decimal
    tif: str
    post_only: bool
    role: str
    reason: str

@dataclass(slots=True)
class ExecutionReport:
    client_order_id: str
    pair_id: str | None
    market_id: str
    token_id: str
    status: str
    filled_size: Decimal
    avg_price: Decimal | None
    exchange_order_id: str | None
    event_ts_ms: int
```

### 4.6 Inventory Lots

```python
@dataclass(slots=True)
class InventoryLot:
    lot_id: str
    market_id: str
    token_id: str
    side: str
    avg_cost: Decimal
    size: Decimal
    opened_ts_ms: int
    source: str  # maker / taker / recovery
```

---

## 5. 模块职责与接口

## 5.1 `app`

### 职责

- 读取配置
- 创建模块实例
- 启动异步任务
- 管理 shutdown

### 核心接口

```python
class AppBootstrap(Protocol):
    async def build(self) -> "RuntimeContext": ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
```

---

## 5.2 `core`

### 职责

- 提供单调时钟
- 提供事件总线
- 生成 `intent_id / pair_id / client_order_id`

### 核心接口

```python
class Clock(Protocol):
    def now_ms(self) -> int: ...

class EventBus(Protocol):
    async def publish(self, topic: str, payload: object) -> None: ...
    def subscribe(self, topic: str) -> AsyncIterator[object]: ...

class IdGenerator(Protocol):
    def next_intent_id(self) -> str: ...
    def next_pair_id(self) -> str: ...
    def next_client_order_id(self) -> str: ...
```

---

## 5.3 `domain`

### 职责

- 集中定义 dataclass / enum / 事件类型
- 作为跨模块的数据契约层

### 约束

- 除基础校验外，不放业务逻辑
- 不依赖外部适配器

---

## 5.4 `market`

### `market.registry`

职责：

- 保存当前和下一个 market 的元数据
- 按时间与状态返回 active / prewarm market

接口：

```python
class MarketRegistry(Protocol):
    def upsert(self, market: MarketMetadata) -> None: ...
    def get(self, market_id: str) -> MarketMetadata | None: ...
    def get_active(self, now_ms: int) -> MarketMetadata | None: ...
    def get_next(self, now_ms: int) -> MarketMetadata | None: ...
    def all_markets(self) -> list[MarketMetadata]: ...
```

### `market.lifecycle`

职责：

- 驱动 `DISCOVERED -> PREWARM -> ACTIVE -> FAST_CLOSE -> FINAL_SECONDS -> CLOSED_WAIT_RESOLUTION -> RESOLVED`
- 发布 phase change 事件

接口：

```python
class LifecycleManager(Protocol):
    def on_market_upsert(self, market: MarketMetadata) -> None: ...
    def on_time_tick(self, now_ms: int) -> list["LifecycleTransition"]: ...
    def get_phase(self, market_id: str) -> str: ...
```

### `market.metadata_loader`

职责：

- 拉取 fee、tick、token mapping
- 在 market 被发现时补齐 metadata

接口：

```python
class MetadataLoader(Protocol):
    async def load_market(self, market_id: str) -> MarketMetadata: ...
```

---

## 5.5 `feeds`

统一原则：

- 所有外部适配器都只负责接入和标准化
- 不在 feed 内做策略逻辑
- 产出统一事件，丢给 `event_bus`

### 公共接口

```python
class Feed(Protocol):
    async def connect(self) -> None: ...
    async def subscribe(self) -> None: ...
    async def run(self) -> None: ...
    async def close(self) -> None: ...
```

### `feeds.binance_ws`

职责：

- 接收 BTC 现货/合约实时价格
- 输出 `BinanceTick`

### `feeds.chainlink_feed`

职责：

- 拉取或订阅 Chainlink 价格
- 输出 `ChainlinkTick`

### `feeds.polymarket_market_ws`

职责：

- 接收盘口、市场发现、tick change、market resolved
- 输出标准化 market 事件

### `feeds.polymarket_user_ws`

职责：

- 接收下单回报、撤单回报、成交回报
- 输出 `ExecutionReport` 或用户状态事件

---

## 5.6 `state`

统一原则：

- 全部是热状态
- 只做内存维护和快照读取
- 不发起网络请求

### `state.book_state`

职责：

- 维护 `UP / DOWN` top-of-book
- 计算 `sum_best_ask / sum_best_bid`

接口：

```python
class BookStateStore(Protocol):
    def apply_market_event(self, event: object) -> None: ...
    def get_top(self, market_id: str, token_id: str) -> OutcomeBookTop | None: ...
    def get_pair_top(self, market_id: str) -> tuple[OutcomeBookTop, OutcomeBookTop] | None: ...
```

### `state.order_state`

职责：

- 跟踪本地订单、交易所订单状态
- 标记 ghost fill / state mismatch

接口：

```python
class OrderStateStore(Protocol):
    def on_intent_sent(self, intent: OrderIntent, client_order_id: str) -> None: ...
    def on_execution_report(self, report: ExecutionReport) -> None: ...
    def get_open_orders(self, market_id: str) -> list[ExecutionReport]: ...
    def get_pair_orders(self, pair_id: str) -> list[ExecutionReport]: ...
```

### `state.inventory_state`

职责：

- 跟踪 `UP / DOWN` 库存 lot
- 计算 `avg_cost(UP) + avg_cost(DOWN)`
- 识别未配平腿

接口：

```python
class InventoryStore(Protocol):
    def on_fill(self, report: ExecutionReport) -> None: ...
    def get_inventory(self, market_id: str) -> list[InventoryLot]: ...
    def get_pair_cost(self, market_id: str) -> Decimal | None: ...
    def get_unhedged_exposure(self, market_id: str) -> Decimal: ...
```

### `state.signal_state`

职责：

- 保存最新 `TheoSnapshot`
- 保存最新 quote/recovery 决策

接口：

```python
class SignalStateStore(Protocol):
    def put_theo(self, theo: TheoSnapshot) -> None: ...
    def get_theo(self, market_id: str) -> TheoSnapshot | None: ...
    def put_quote_plan(self, plan: QuotePlan) -> None: ...
```

---

## 5.7 `pricing`

### `pricing.vol_model`

职责：

- 基于 Binance 短窗数据估算 `sigma_short`
- 支持 `1s / 5s / 15s / EWMA`

接口：

```python
class VolModel(Protocol):
    def on_binance_tick(self, tick: BinanceTick) -> None: ...
    def sigma_short(self, now_ms: int) -> Decimal | None: ...
```

### `pricing.lead_lag`

职责：

- 维护 `delta_binance_chainlink`
- 为 recovery 提供方向偏置

接口：

```python
class LeadLagEngine(Protocol):
    def on_binance_tick(self, tick: BinanceTick) -> None: ...
    def on_chainlink_tick(self, tick: ChainlinkTick) -> None: ...
    def current_basis(self) -> Decimal | None: ...
    def directional_bias(self, market_id: str, now_ms: int) -> Decimal | None: ...
```

### `pricing.fair_value`

职责：

- 结合 `P0`、剩余时间、`sigma_short`、basis 产出 `theo_up / theo_down`

接口：

```python
class FairValueEngine(Protocol):
    def compute(self, market: MarketMetadata, now_ms: int) -> TheoSnapshot | None: ...
```

### `pricing.quote_policy`

职责：

- 根据 `TheoSnapshot`、盘口、phase、库存产出双边报价
- maker-first

接口：

```python
class QuotePolicy(Protocol):
    def build(self, market: MarketMetadata, now_ms: int) -> QuotePlan | None: ...
```

---

## 5.8 `strategy`

### `strategy.phase_policy`

职责：

- 不同 phase 下调整 quoting / taking / recovery 激进度

接口：

```python
class PhasePolicy(Protocol):
    def allow_new_quotes(self, phase: str) -> bool: ...
    def allow_selective_taker(self, phase: str) -> bool: ...
    def max_unhedged_exposure(self, phase: str) -> Decimal: ...
```

### `strategy.pair_strategy`

职责：

- 消费 `TheoSnapshot + QuotePlan + BookState`
- 输出 maker 改价、selective taker 意图

接口：

```python
class PairStrategy(Protocol):
    def on_tick(self, market_id: str, now_ms: int) -> list[OrderIntent]: ...
```

### `strategy.recovery_strategy`

职责：

- 对未配平腿做补单、挂单、止损、降风险

接口：

```python
class RecoveryStrategy(Protocol):
    def on_fill(self, report: ExecutionReport) -> list[OrderIntent]: ...
    def on_timer(self, market_id: str, now_ms: int) -> list[OrderIntent]: ...
```

---

## 5.9 `risk`

### 职责

- 下单前同步检查
- 失败时返回明确原因

### 接口

```python
@dataclass(slots=True)
class RiskDecision:
    allowed: bool
    reason: str
    severity: str

class RiskEngine(Protocol):
    def evaluate(self, intent: OrderIntent, now_ms: int) -> RiskDecision: ...
```

### 最小规则集

- 元数据完整性
- 数据新鲜度
- 单笔 size 限制
- 单方向库存限制
- 未配平暴露限制
- close-phase 限制
- open orders 限制
- ghost fill 修复期间禁开新单

---

## 5.10 `execution`

### `execution.intents`

职责：

- 定义 `PLACE / CANCEL / REPLACE` 三类动作

### `execution.order_builder`

职责：

- 将内部 `OrderIntent` 转为 Polymarket 下单 payload
- 处理 tick rounding、fee snapshot、tif

接口：

```python
class OrderBuilder(Protocol):
    def build_place(self, intent: OrderIntent) -> dict: ...
    def build_cancel(self, client_order_id: str) -> dict: ...
```

### `execution.polymarket_gateway`

职责：

- 实际发单 / 撤单
- 只做网络与签名，不做策略

接口：

```python
class ExecutionGateway(Protocol):
    async def place(self, payload: dict) -> str: ...
    async def cancel(self, client_order_id: str) -> None: ...
```

### `execution.router`

职责：

- 统一处理意图队列
- 先走风控，再走 builder/gateway
- 将结果回灌 order_state

接口：

```python
class ExecutionRouter(Protocol):
    async def submit(self, intent: OrderIntent) -> RiskDecision: ...
```

### `execution.cancel_manager`

职责：

- 维护撤单优先级
- 支持 `撤旧单 -> 校验状态 -> 挂新单`

接口：

```python
class CancelManager(Protocol):
    async def cancel_stale_quotes(self, market_id: str, now_ms: int) -> None: ...
```

---

## 5.11 `storage`

### `storage.recorder`

职责：

- 录制原始事件、theo、intent、execution report

接口：

```python
class Recorder(Protocol):
    async def write_event(self, topic: str, payload: object) -> None: ...
```

### `storage.snapshot_writer`

职责：

- 定期写状态快照

接口：

```python
class SnapshotWriter(Protocol):
    async def write_snapshot(self, now_ms: int) -> None: ...
```

---

## 5.12 `replay`

### 职责

- 从录制文件按时间重放三流数据
- 驱动策略离线运行

### 接口

```python
class ReplayPlayer(Protocol):
    async def run(self, path: str) -> None: ...
```

---

## 5.13 `observability`

### 职责

- 指标埋点
- 告警判定

### 接口

```python
class Metrics(Protocol):
    def incr(self, name: str, value: int = 1, **tags: str) -> None: ...
    def gauge(self, name: str, value: float, **tags: str) -> None: ...
    def timing(self, name: str, value_ms: float, **tags: str) -> None: ...

class Alerts(Protocol):
    async def emit(self, level: str, title: str, detail: str) -> None: ...
```

---

## 6. 关键运行流

## 6.1 Quote 更新流

```text
BinanceTick / ChainlinkTick / MarketBookEvent
  -> state 更新
  -> vol_model 更新 sigma_short
  -> fair_value 产出 theo
  -> quote_policy 产出 QuotePlan
  -> pair_strategy 产出 maker intents
  -> risk.evaluate()
  -> execution.router.submit()
```

## 6.2 单腿回补流

```text
ExecutionReport(fill)
  -> order_state 更新
  -> inventory_state 更新
  -> recovery_strategy.on_fill()
  -> 生成 recovery intents
  -> risk.evaluate()
  -> execution.router.submit()
```

## 6.3 收盘阶段风控流

```text
clock tick
  -> lifecycle phase 更新
  -> phase_policy 收紧阈值
  -> cancel_manager 撤旧单
  -> recovery_strategy 优先处理未配平腿
```

## 6.4 市场切换流

```text
new_market event
  -> metadata_loader.load_market()
  -> market_registry.upsert()
  -> lifecycle -> PREWARM
  -> polymarket market ws subscribe
  -> phase 到 ACTIVE 时允许策略开始报价
```

---

## 7. 第一版主循环建议

```python
async def main_loop() -> None:
    while True:
        now_ms = clock.now_ms()

        lifecycle.on_time_tick(now_ms)
        await cancel_manager.cancel_stale_quotes(active_market_id, now_ms)

        theo = fair_value.compute(active_market, now_ms)
        if theo is not None:
            signal_state.put_theo(theo)

        quote_plan = quote_policy.build(active_market, now_ms)
        if quote_plan is not None:
            signal_state.put_quote_plan(quote_plan)

        for intent in pair_strategy.on_tick(active_market.market_id, now_ms):
            await execution_router.submit(intent)

        for intent in recovery_strategy.on_timer(active_market.market_id, now_ms):
            await execution_router.submit(intent)

        await asyncio.sleep(0.01)
```

第一版可以先接受 `10ms` 级 timer loop。  
真正更高频的更新仍然由 feed event 驱动。

---

## 8. MVP 开发顺序

### Phase 1: 基础骨架

- `domain`
- `core`
- `market.registry`
- `market.lifecycle`
- `feeds` 基础框架
- `state.book_state`
- `state.order_state`
- `state.inventory_state`

### Phase 2: 数据与元数据打通

- `binance_ws`
- `chainlink_feed`
- `polymarket_market_ws`
- `polymarket_user_ws`
- `metadata_loader`

### Phase 3: 定价与策略

- `vol_model`
- `lead_lag`
- `fair_value`
- `quote_policy`
- `pair_strategy`
- `recovery_strategy`

### Phase 4: 执行与风控

- `risk.engine`
- `order_builder`
- `polymarket_gateway`
- `router`
- `cancel_manager`

### Phase 5: 旁路能力

- `recorder`
- `snapshot_writer`
- `metrics`
- `alerts`
- `replay.player`

---

## 9. 第一批必须先写的测试

### 单元测试

- `tick rounding`
- `fee 计算`
- `sigma_short` 输出稳定性
- `theo_yes / theo_no` 边界
- `phase_policy`
- `unhedged exposure` 计算
- `ghost fill` 修复状态机

### 集成测试

- `Polymarket user WS` 回报驱动 `order_state`
- 单腿成交后触发 `recovery_strategy`
- `cancel_stale_quotes` 不会误撤 recovery 单

### 回放测试

- 三流合一回放能重现 `theo`
- close-phase 能正确缩库存
- maker adverse selection 能被统计出来

---

## 10. 当前建议的代码实现优先级

如果现在立刻开工，建议顺序是：

1. 先写 `domain/models.py`、`domain/events.py`、`core/enums.py`
2. 再写 `market/registry.py`、`market/lifecycle.py`
3. 再写 `state/book_state.py`、`state/order_state.py`、`state/inventory_state.py`
4. 然后打通 `feeds/binance_ws.py` 和 `feeds/polymarket_market_ws.py`
5. 再接 `feeds/polymarket_user_ws.py`
6. 之后再写 `pricing/*`、`strategy/*`
7. 最后接 `risk/*` 和 `execution/*`

原因很直接：

- 没有 market registry，后面所有模块都会乱
- 没有 state store，策略没有读模型
- 没有 user WS，单腿 recovery 根本不可靠
- 没有 pricing，maker-first 只是空话

---

## 11. 本文档的使用方式

- `summary.md` 负责系统目标、策略原则、风控原则
- `modules_interfaces.md` 负责代码模块边界和接口契约
- 后续如果开始写代码，新增文件应尽量对齐本文件里的模块名

如果后面你决定直接进入编码阶段，第一批我建议先落这 8 个文件：

- `app/config.py`
- `domain/models.py`
- `domain/events.py`
- `market/registry.py`
- `market/lifecycle.py`
- `state/book_state.py`
- `state/order_state.py`
- `state/inventory_state.py`
