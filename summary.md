## 1. 先说结论（最重要的 10 条）

1. 这个系统不是跨交易所双腿套利，也不是单纯方向交易，而是 **Polymarket 15 分钟 BTC 二元市场里的双腿全套套利系统**。  
2. 这个市场的真实结算锚是 **Chainlink BTC/USD**，但想做快，**领先信号必须主要来自 Binance 这类 CEX 的实时价格推送**。  
3. 真正的竞争力不是把本地代码从 3ms 优化到 1ms，而是 **更早拿到有效价格变化、更新 fair value、撤掉旧单并发出新单**。  
4. 系统必须围绕三类数据源设计：  
   - **CEX 快价格**：用于领先判断  
   - **Chainlink 锚价格**：用于结算真值和偏差校准  
   - **Polymarket 盘口与用户回报**：用于执行  
5. 15 分钟市场的核心不是单纯猜方向，而是 **同时盯住 `UP + DOWN` 的全套价格是否偏离 1，以及单腿成交后方向风险如何回补**。  
6. 靠 Chainlink 单独做信号会慢，因为按当前业务假设，Chainlink 价格获取粒度按 **1 秒**处理；因此必须做 **Binance -> Chainlink 的 lead-lag 建模**。  
7. 热路径必须极简，但这里的“快”首先是 **信息快 + 决策快 + 撤单快**，不是为了微秒级而牺牲正确性。  
8. Polymarket 的 `feeRateBps`、`tick_size_change`、订单类型、市场生命周期，必须写进核心逻辑，不能当成外围细节。  
9. 最危险的不是均值延迟，而是 **旧价格下挂着旧单、临近收盘还持有错误库存、市场元数据失步**。  
10. 上线顺序必须是：**历史回放 -> 影子模式 -> 小资金实盘 -> 扩容**。

---

## 2. 先定义清楚这个市场到底是什么

### 2.1 市场本质

- Polymarket 15 分钟 BTC 市场，本质是一个 **短周期二元事件市场**。  
- 你交易的不是 BTC 现货本身，而是一个命题：  
  `本 15 分钟区间结束时，BTC 是否高于区间开始时的结算价格`
- 交易标的通常可抽象成两个 outcome：  
  - `YES / UP`  
  - `NO / DOWN`
- 这个市场不是永续合约，不是现货，也不是典型跨交易所双腿套利。  
- 但它的核心交易方式更准确地说是：  
  - 通过 **共享订单簿上的做市、库存累积、单腿回补**，逐步把 `UP + DOWN` 的平均持仓成本压到 `1` 以下  
  - 若只成交一腿，则进入 **单腿回补 / 单腿止损 / 单腿临时持有** 的恢复流程
- 不应把“直接作为 taker 同时扫两腿拿到 `< 1`”当成稳定主策略；共享订单簿会让这种机会极少且持续时间极短。

### 2.2 结算真值

- 系统必须明确区分两种价格：  
  - **交易领先价格**：Binance 等 CEX 的实时价格  
  - **结算真值价格**：Chainlink BTC/USD
- 对该类市场，策略上要跟踪两个关键锚点：  
  - `P0`：市场开始时的 Chainlink 价格快照  
  - `P1`：市场结束时的 Chainlink 价格快照
- 可以把结算逻辑抽象为：  
  - 若 `P1 >= P0`，则 `UP` 获胜  
  - 若 `P1 < P0`，则 `DOWN` 获胜

### 2.3 这对系统设计意味着什么

- 你不能只看 Polymarket 盘口。  
- 你也不能只看 Binance。  
- 正确架构必须是：  
  **用定价模型和共享订单簿微观结构决定 quote / take，用 Binance 抢单腿回补速度，用 Chainlink 定义最终结算真值。**

---

## 3. 业务目标如何转成工程指标

### 3.1 最重要的系统指标

- **CEX Tick -> Decision 延迟**：Binance 新价格到策略完成 fair value 更新的时间。  
- **Decision -> Order Send 延迟**：信号出现到订单发出的时间。  
- **Decision -> Cancel Send 延迟**：旧单需要撤掉时，从发现失效到发出撤单的时间。  
- **双腿同时成交率**：一次套利触发后，两腿都按预期成交的比例。  
- **单腿回补成功率**：只成交一腿后，能否在可接受成本内补齐另一腿。  
- **Cancel 完成率**：快速行情下旧单是否能及时撤掉。  
- **Book Sync 正确率**：本地 Polymarket 订单簿是否与交易所状态一致。  
- **Binance / Chainlink 偏差监控**：当前 CEX 价格与最近一次 Chainlink 锚价格的偏移。  
- **全套成本获取率**：一段时间内累计获得的 `avg_cost(UP) + avg_cost(DOWN)` 是否持续低于 `1`。  
- **做市 spread capture**：maker 成交后，扣除 adverse selection、fees 后是否还能留下正收益。  
- **收盘前未配平库存风险**：临近结束时，系统是否还持有未补齐的单腿暴露。  
- **净套利命中率**：扣掉 taker fee、maker rebate、滑点后，交易是否还有正期望。  
- **市场发现提前量**：下一个 15 分钟市场能否在开盘前完成订阅、建簿、加载元数据。  

### 3.2 更适合这个系统的延迟预算（示例）

> 这里的预算重点是稳定和正确，不是为了写一个“1.5ms 神话数字”。

- Binance 消息解码与归一化：`<= 1ms`
- fair value 更新与机会判断：`<= 1ms`
- 风控判断：`<= 1ms`
- 下单组包、签名、价格取整：`<= 2ms`
- 撤单决策与重挂决策：`<= 2ms`
- **本地主链路目标：`<= 5ms`**

白话：  
本地处理可以不追求微秒，但必须做到 **收到快价格后几毫秒内完成重定价或撤单动作**。

---

## 4. 推荐总架构：三平面 + 一个市场生命周期管理器

### 4.1 三平面架构

1. **交易平面（热路径）**  
   只包含：`行情接入 -> fair value -> 信号 -> 风控 -> 执行`

2. **市场管理平面（准热路径）**  
   只包含：`新市场发现 -> 元数据加载 -> token 映射 -> 生命周期切换 -> 收盘/结算管理`

3. **旁路平面（冷路径）**  
   只包含：`日志、监控、回放、报表、研究、UI、告警`

### 4.2 必须新增的核心模块

- **Market Registry**  
  保存每个 15 分钟市场的 `condition_id`、token id、开始时间、结束时间、tick size、feeRateBps、状态。

- **Lead-Lag Engine**  
  用 Binance 实时价格判断单腿回补优先级、方向偏置，并和最近一次 Chainlink 锚价格做偏差建模。

- **Pair Arb Engine**  
  管理 full-set 成本、库存配对进度、共享订单簿微观结构，以及双腿回补动作。

- **Lifecycle Manager**  
  把每个市场显式管理为状态机，而不是只把它当作“一个普通订单簿”。

### 4.3 推荐逻辑数据流

`Binance WS + Chainlink 锚价格 + Polymarket Market WS + Polymarket User WS + 市场元数据`
`-> 归一化层`
`-> Market Registry / Book State / Lead-Lag State / Pair Arb State`
`-> Fair Value Engine`
`-> Signal Engine`
`-> Risk Engine`
`-> Execution Gateway`
`-> Polymarket CLOB`

旁路订阅：

`交易事件 + 原始行情 + 决策快照 + 回报流`
`-> 异步队列`
`-> 监控 / 回放 / 报表 / 研究`

---

## 5. 行情系统设计：不是“谁快谁赢”，而是“谁先拿到有效价格变化”

### 5.1 三类输入源的职责分工

#### A. CEX 快价格源

- 主源建议：**Binance 实时 WebSocket 推送**
- 用途：  
  - 毫秒级捕捉 BTC 方向变化  
  - 在 Chainlink 还没更新到最新价格前，提前推断下一次锚价格可能变化方向  
  - 给 Polymarket 的单腿回补、第二腿追单、收盘前方向防守提供领先信号

#### B. Chainlink 锚价格源

- 用途：  
  - 定义 `P0 / P1`  
  - 校准 Binance 价格与结算真值之间的偏差  
  - 在临近收盘时判断当前市场真实结算边界

> 按当前业务假设，Chainlink 价格获取按 1 秒粒度处理。  
> 这意味着它是 **真值锚**，但不是足够快的 **领先信号源**。

#### C. Polymarket 执行与盘口源

- 用途：  
  - 获取 best bid/ask、盘口变化、成交变化  
  - 监听 `tick_size_change`、`new_market`、`market_resolved` 等市场事件  
  - 接收用户订单回报、成交回报、撤单回报

### 5.2 两个时钟必须分开管理

系统里至少有两个“时钟”：

1. **交易时钟**  
   Binance 和 Polymarket WebSocket 按毫秒级不断更新。

2. **结算时钟**  
   Chainlink 锚价格按更粗粒度到来，决定最终 `P0 / P1`。

白话：  
你要做的是在交易时钟里抢先行动，但你的判断必须最终对齐结算时钟。

### 5.3 关键中间量：Basis / Lead-Lag State

系统必须长期维护以下状态：

- `binance_last_price`
- `chainlink_last_price`
- `delta_binance_chainlink = binance_last_price - chainlink_last_price`
- `delta_since_market_open = binance_last_price - P0`
- `time_to_close_ms`
- `binance_short_return_100ms / 500ms / 1s / 3s`
- `binance_realized_vol_1s / 5s / 15s`
- `polymarket_yes_bid/ask`
- `polymarket_no_bid/ask`
- `sum_best_ask = ask_up + ask_down`
- `sum_best_bid = bid_up + bid_down`
- `pair_buy_size_at_best`
- `pair_sell_size_at_best`

### 5.4 新鲜度控制

不同数据源必须分别判定是否过期，不能只看一个总开关。

- `Binance staleness`
- `Chainlink staleness`
- `Polymarket market WS staleness`
- `Polymarket user WS staleness`

建议规则：

- Binance 超过短阈值未更新：停止用它推动新信号  
- Polymarket 盘口过期：停止挂新单  
- User WS 异常：降低攻击性，优先查对订单状态  
- Chainlink 锚异常：进入保守模式，必要时 fail-close

### 5.5 市场发现与预热

15 分钟市场不是一个长期固定 market，而是滚动生成的。

因此系统必须具备：

- 提前发现下一个 market
- 在 market 开盘前完成 token id 加载
- 在开盘前建立订阅
- 在开盘前拿到 feeRateBps、tick size、最小下单单位等元数据

如果做不到这一点，实盘一开盘就会比别人慢一拍。

---

## 6. 策略引擎设计：核心是定价模型 + 做市累积 full-set，单腿恢复和 taker 为辅

### 6.1 策略真正要估计的量

第一层不是“BTC 现在涨了没有”，也不是“屏幕上会不会瞬间出现 `ask_up + ask_down < 1`”，而是：

- `theo_yes`
- `theo_no = 1 - theo_yes`
- `sigma_short`
- `target_quote_yes / target_quote_no`
- `target_full_set_cost`

这里 `theo_yes` 可以从短周期 binary option / digital option 近似开始，关键参数不是利率，而是 **剩余时间内的短期波动率 `sigma_short`**。

第二层才是方向辅助量：

`fair_yes = P(市场结束时 Chainlink_end >= P0 | 当前 Binance 路径、当前波动、剩余时间、当前盘口)`

然后有：

- `directional_bias = fair_yes - market_yes_mid`
- `inventory_pair_cost = avg_cost(UP_inventory) + avg_cost(DOWN_inventory)`

### 6.2 主信号应该是“理论价 + 共享订单簿微观结构 + Lead-Lag 辅助模型”

这是整个系统最关键的一层。

你需要把策略主逻辑写成：

1. 用短期波动率模型持续更新 `theo_yes / theo_no`
2. 以 maker 为主，在共享订单簿上围绕 theo 报双边价格
3. 通过反复成交和库存配对，把 `avg_cost(UP) + avg_cost(DOWN)` 压到 `1` 以下
4. 只在市场价格与 theo 偏离足够大时，才把 taker 作为补充手段
5. 如果只成交一腿，用 Binance lead-lag + `time_to_close` 判断：
   - 是否继续追第二腿
   - 是否把缺口留给 maker quote 慢慢补
   - 是否把已成交腿转为短暂方向持仓
   - 是否直接止损退出
6. 只在 **净 make edge、净 take edge 或净 recovery edge 为正** 时继续动作

白话：  
长期赚钱更像是“模型驱动做市 + 单腿恢复”，不是“等显眼的双腿 `<1` 套利自己送上门”。  
Binance 决定单腿暴露出现后你能不能比别人更快修正。

### 6.3 价格信号不应只看一个点

建议至少组合这些特征：

- Binance 最新成交价与中间价
- Binance 极短期收益率：`100ms / 500ms / 1s / 3s`
- Binance 极短期波动率：`1s / 5s / 15s`
- EWMA / rolling realized volatility
- `delta_binance_chainlink`
- `delta_binance_P0`
- 剩余时间 `time_to_close_ms`
- Polymarket 当前盘口不平衡
- Polymarket 当前 mid 和你自己的 fair value 偏离
- `ask_up + ask_down` 与 `1` 的偏离
- `bid_up + bid_down` 与 `1` 的偏离
- 两腿在当前价位的可成交量是否匹配
- 最近一段时间 maker quote 的 adverse selection 结果

### 6.4 净套利与净回补 edge 计算必须扣掉真实执行成本

示例：

`net_make_edge_yes = fill_prob * (theo_yes - bid_yes + maker_rebate) - adverse_selection_penalty`

`net_take_edge_yes = theo_yes - ask_yes - taker_fee - slippage_buffer - delay_risk_buffer`

`net_recovery_edge = expected_value_after_recovery - current_leg_cost - recovery_cost - directional_risk_buffer`

`target_full_set_cost = avg_cost(UP_inventory) + avg_cost(DOWN_inventory)`

系统永远只对 `net_make_edge`、`net_take_edge`、`net_recovery_edge` 和 `target_full_set_cost` 做决策，不对毛价差做决策。

### 6.5 分阶段交易模式

建议把 15 分钟市场至少拆成 4 个阶段：

1. **Open / Early Phase**  
   新市场开启，先建簿、校准价格、轻仓试探。

2. **Normal Phase**  
   默认以 maker 为主，选择性 taker 为辅。

3. **Fast Close Phase**  
   临近收盘，减少新开 inventory，优先补齐未配平腿，更多使用快撤和择机 taker。

4. **Final Seconds Phase**  
   只允许高置信度回补或清仓；如果订单状态不可确认，优先降风险而不是继续进攻。

> 各阶段的时间边界必须配置化。  
> 例如 `T-30s`、`T-5s`、`T-1s` 只是起点，不是固定真理，要靠回放数据校准。

### 6.6 同类机会去重与节流

- 同一组全套机会要去重
- 同一价格区间不要高频重复发低质量 maker quote
- 如果第二腿环境已变化，优先处理未配平腿，不要继续开新 pair
- 拥塞时宁可放弃旧 pair，也不要保留失真的旧挂单

---

## 7. 风控引擎设计：这个市场最危险的是单腿成交后回补失败

### 7.1 热路径必须同步检查的风控

- 单市场最大净暴露
- 单方向最大持仓
- 最大未配平单腿暴露
- 单笔最大下单金额
- 全局最大未完成订单数
- 收盘前最大允许未配平库存
- 单位时间下单频率上限
- 数据源新鲜度门槛
- 元数据完整性校验  
  例如：`feeRateBps` 未加载、tick size 未同步、token 映射缺失时禁止交易

### 7.2 这个系统必须新增的专用风控

- **Close Phase Inventory Cap**  
  距离结束越近，允许持有的未配平净暴露越小。

- **Oracle Basis Guard**  
  如果 Binance 和最近 Chainlink 锚价格偏差异常大，限制单腿回补攻击性。

- **Book Desync Guard**  
  本地订单簿序列错位、回报不一致时，停止挂新单。

- **Ghost Fill / State Mismatch Guard**  
  本地认为未成交、交易所实际已成交，或 nonce / 状态回报不一致时，立即进入人工可追踪的修复流程。

- **Outstanding Order Guard**  
  撤单未确认时，禁止继续在同价位盲目补单。

- **One-Leg Recovery Guard**  
  单腿成交后必须在限定时间、限定成本内完成回补，否则执行止损或转方向持仓方案。

### 7.3 风控失败策略

- 风控模块异常：默认 fail-close
- Market Registry 状态异常：默认 fail-close
- User WS 断流：默认降级，必要时 cancel-all
- Chainlink 锚状态异常：停止高置信度 close-phase 交易

---

## 8. 执行引擎设计：双腿并发、单腿恢复必须是第一公民

### 8.1 内部订单模型要按 Polymarket 重写

内部统一订单模型至少包含：

- `market_id / condition_id`
- `token_id`
- `side`
- `price`
- `size`
- `time_in_force`
- `post_only`
- `client_order_id`
- `fee_rate_bps_snapshot`
- `tick_size_snapshot`
- `strategy_reason`
- `pair_id`
- `leg_role`  
  例如：`pair_leg_a / pair_leg_b / recovery_leg / unwind_leg`

### 8.2 双腿执行策略

- **Pair Maker 场景（默认）**  
  围绕 `theo_yes / theo_no` 双边报价，逐步累积低成本 full-set 库存，优先 `GTC / GTD + post-only`。

- **Selective Taker 场景（补充）**  
  只有当市场价格与 theo 偏离足够大，且扣掉 fees、delay、slippage 后仍有净 edge，才使用 `FAK / FOK` 或可成交限价单。

- **Recovery 场景**  
  只成交一腿后，按 Binance 领先信号和剩余时间决定第二腿是追价、挂价还是放弃。

注意：

- 不要把“同时扫两腿拿到 `<1`”当成稳定主策略  
- 不要把这个市场当成“有真正市价单”的交易所  
- 所有攻击性成交，本质上也应走 **可成交限价单**
- 任何 taker 侧撮合延迟或 speed bump 都必须靠你自己实测，不要把社区口径里的 `250ms / 500ms` 当成固定协议常数
- `GTD` 不能被当作毫秒级过期工具使用；临近结束时，撤单必须靠本地状态机控制

### 8.3 价格取整和 tick size 不能写死

- Polymarket 价格是离散 tick 的
- `tick size` 可能变化
- 价格 rounding 必须基于当前 market 的最新 tick 配置

任何把 tick 写死成 `0.01` 的实现，到了极端价位都可能直接出问题。

### 8.4 订单幂等与撤单优先级

- 每个订单必须有唯一 `client_order_id`
- 每组双腿必须有唯一 `pair_id`
- 相同信号的 retry 不能变成重复下单
- fair value 快速漂移时，优先级应是：  
  `撤旧单 -> 校验状态 -> 挂新单`

### 8.5 单腿恢复优先级

- 单腿成交后，优先判断第二腿是否还能在可接受成本内补齐
- 如果 Binance 快速朝有利方向运行，可以短暂提高追第二腿的攻击性
- 如果 Binance 快速朝不利方向运行，要更快触发止损或降风险
- 时间越接近结束，越不允许长时间挂着未配平暴露

### 8.6 双腿库存与单腿回补必须单独设计

系统必须支持：

- YES / NO 库存跟踪
- full-set 成本跟踪
- 未配平腿的进入时间、进入价格、目标回补价
- merge / redeem / split 的后处理逻辑
- maker rebate 与 taker fee 的独立核算
- 做市库存与回补库存的分账核算

如果后续同时加入方向性策略，可以复用这套库存层，但不能拿方向逻辑替代双腿回补逻辑。

---

## 9. 市场生命周期管理：这是 15 分钟市场的核心模块，不是附件

### 9.1 推荐状态机

每个 market 建议管理为：

`DISCOVERED -> PREWARM -> ACTIVE -> FAST_CLOSE -> FINAL_SECONDS -> CLOSED_WAIT_RESOLUTION -> RESOLVED -> ARCHIVED`

### 9.2 每个阶段的关键动作

- `DISCOVERED`  
  拉元数据，验证 token id、tick、fee

- `PREWARM`  
  建立 WS 订阅，初始化本地簿和计数器

- `ACTIVE`  
  正常寻找双腿套利和回补机会

- `FAST_CLOSE`  
  缩短挂单寿命，优先处理未配平腿

- `FINAL_SECONDS`  
  强化撤单和高置信度回补，禁止低质量新开 pair

- `CLOSED_WAIT_RESOLUTION`  
  不再开新 pair，等待最终状态或处理剩余 full set

- `RESOLVED`  
  做结算核对、PnL 核对、可赎回处理

### 9.3 下一个 market 的接力

这个系统不能等当前 market 完全结束后，才去初始化下一个 market。

必须做到：

- 当前 market 还在 `FAST_CLOSE` 时，下一个 market 已经进入 `PREWARM`
- 这样才能避免 15 分钟滚动切换时出现空窗

---

## 10. 状态与存储设计：热冷分层，但热状态要更贴近这个市场

### 10.1 热状态（内存）

- 当前活跃 market 的元数据
- Binance 最新价格与短窗统计
- Chainlink 最近锚价格与到达时间
- `P0`
- 剩余时间
- YES / NO 盘口 top levels
- 双腿最优可成交量
- 本地挂单状态
- 本地持仓与风险计数器
- 市场生命周期状态
- 当前 pair edge、recovery edge、directional bias

### 10.2 冷存储（异步）

- 原始 Binance / Polymarket / Chainlink 事件流
- 每次 fair value 快照
- 每次 pair edge 快照
- 每次信号触发原因
- 每次下单、撤单、成交、拒单
- 每个 market 的最终 PnL、费用、滑点、成功率

### 10.3 回放能力必须是三流合一

回放不能只放 Polymarket 订单簿，必须同时回放：

- Binance 快价格
- Chainlink 锚价格
- Polymarket 盘口与用户回报

否则你根本复盘不出为什么当时会误判。

---

## 11. 监控与告警：最该看的不是 CPU，而是信号领先度和收盘阶段风险

### 11.1 必看监控面板

- Binance feed 延迟 / 抖动
- Chainlink 锚价格到达间隔
- Polymarket market WS / user WS 新鲜度
- 本地 fair value 与市场 mid 偏差
- `avg_cost(UP) + avg_cost(DOWN)` 与 `1` 的偏差
- 双腿同时成交率
- 单腿回补成功率与回补耗时
- `delta_binance_chainlink`
- cancel 延迟与 cancel 成功率
- close-phase 未配平暴露
- maker / taker 成交占比
- maker adverse selection 统计
- ghost fill / 状态不一致计数
- fee 与 rebate 贡献
- 下一个 market 的发现提前量
- tick size 变化事件
- 订单拒单原因分布

### 11.2 告警分级（建议）

- `P0`  
  无法下单、订单状态不一致、市场元数据缺失、风控失效、用户回报断流

- `P1`  
  Binance 断流、Polymarket 断流、cancel 成功率突降、close-phase 暴露超限

- `P2`  
  旁路报表延迟、回放写入延迟、非关键面板异常

---

## 12. 技术栈建议：先做对，再做极限

### 12.1 更适合当前阶段的组合

- **Python**
  - 策略研究
  - WebSocket 编排
  - 回放框架
  - 旁路监控

- **Rust 或 Go**
  - 如果后续测到热路径瓶颈，再迁移：
    - 行情归一化
    - fair value 快路径
    - 执行网关

### 12.2 不建议一开始就被“超低延迟语言”绑架

这个系统的首要瓶颈通常不是 CPU 算术，而是：

- 外部 WebSocket 质量
- 订单状态一致性
- 市场生命周期处理
- 撤单与重挂节奏
- 费用与 tick 处理正确性

白话：  
先把 **交易逻辑、状态机、回放、风控** 做对，再决定哪些 20% 热路径要迁移。

---

## 13. 部署与网络策略：公网系统更看重稳定低抖动

### 13.1 网络目标

- 长连接稳定
- 自动重连快
- WS 订阅恢复快
- 网络抖动小
- 系统时钟准确

### 13.2 部署原则

- 不要迷信“同机房”叙事，这不是传统撮合所内 HFT
- 更重要的是：
  - 到 Binance 的路径稳定
  - 到 Polymarket 的路径稳定
  - 长时间运行时连接不抖

### 13.3 系统调优方向

- 关键线程减少无谓上下文切换
- 避免热路径 GC 抖动
- 长连接心跳和重订阅要自动化
- 时间同步必须稳定

---

## 14. 测试与上线流程：必须按这个市场重做

### 14.1 必做测试

1. 单元测试  
   - fair value 计算  
   - pair edge / recovery edge 计算  
   - tick rounding  
   - fee 计算  
   - close-phase 风控

2. 集成测试  
   - Binance WS 接入  
   - Polymarket market/user WS 接入  
   - 下单、撤单、回报一致性

3. 生命周期测试  
   - `new_market`  
   - `tick_size_change`  
   - `market_resolved`  
   - 市场切换接力

4. 三流合一回放  
   - Binance + Chainlink + Polymarket 联合回放

5. 故障注入  
   - Binance 断流  
   - User WS 断流  
   - cancel 回报超时  
   - 收盘前 market desync  
   - ghost fill / 状态不一致

### 14.2 上线节奏

- 先回放
- 再影子模式
- 再小仓位实盘
- 最后逐步增加仓位和市场覆盖

每一步都要有自动回退开关。

---

## 15. 常见反模式（这个项目里尤其要避免）

1. 只盯 Polymarket 盘口，不看 Binance 领先信号。  
2. 只盯 Binance，不对齐 Chainlink 结算真值。  
3. 把“同一 market 内的双腿全套套利”误写成“跨交易所双腿套利”。  
4. 把“直接扫两腿拿到 `<1`”当成稳定主策略。  
5. 把 tick size 写死成固定值。  
6. 不处理 `feeRateBps`，只看毛 edge。  
7. 只算方向 fair value，不建短期波动率模型。  
8. 临近收盘仍盲目买高价 side，忽略最后几秒反转风险。  
9. 单腿成交后没有限时回补和止损机制。  
10. 撤单未确认就继续在同一腿上叠单。  
11. 当前 market 结束前才开始初始化下一个 market。  
12. 回放只记录成交，不记录 fair value、pair edge 和原始 feed。  
13. 只看平均延迟，不看 cancel 延迟、book desync、ghost fill 和 close-phase 风险。  

---

## 16. 一页式落地清单（按这个顺序做）

1. 先实现 `Market Registry + Lifecycle Manager`。  
2. 接入 Binance 实时推送，并把它定义为主领先信号源。  
3. 接入 Chainlink 锚价格，并维护 `P0 / P1 / delta_binance_chainlink`。  
4. 接入 Polymarket market WS 和 user WS，建立可靠订单簿与用户状态机。  
5. 先实现短期波动率模型，产出 `theo_yes / theo_no`。  
6. 再实现 maker-first 的报价引擎，目标是长期把 `avg_cost(UP) + avg_cost(DOWN)` 压到 `1` 以下。  
7. 然后实现 selective taker 与单腿 recovery，引导回补优先级。  
8. 决策统一基于 `net_make_edge`、`net_take_edge` 或 `net_recovery_edge`，必须扣掉 fee、rebate、滑点和撤单风险。  
9. 实现 close-phase 风控：收盘越近，未配平库存越小，挂单寿命越短。  
10. 做三流合一回放，先验证 `vol model`、`lead-lag` 和 maker adverse selection。  
11. 先影子模式，再小资金，再扩容。  

---

## 17. 最后一句话

这个系统的工程本质不是“把代码写得像传统 HFT”，而是：

**先用短期波动率模型定价，在 Polymarket 共享订单簿上做 maker-first 报价，长期把 `UP + DOWN` 的平均持仓成本压到 `1` 以下；再用 Binance 这类 CEX 的快价格处理单腿回补和方向风险，用 Chainlink 定义最终真值，并在 15 分钟滚动生命周期里持续比别人更快地配平、撤单、重挂和完成 full set。**
