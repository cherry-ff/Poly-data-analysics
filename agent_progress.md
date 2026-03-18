# Poly-15minBTC 多 Agent 协同进度文档

本文档用于多 agent 并行开发时的协同控制，不替代：

- [summary.md](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/summary.md)
- [modules_interfaces.md](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/modules_interfaces.md)

前两者负责系统目标和模块设计。  
本文档只负责：

1. 当前进度  
2. agent 分工  
3. 文件边界  
4. 依赖顺序  
5. 交接规则  

---

## 1. 当前状态快照

### 1.1 已完成

- 系统总纲已完成并定稿：
  - [summary.md](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/summary.md)
- 模块与接口文档已完成：
  - [modules_interfaces.md](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/modules_interfaces.md)
- 第一批代码骨架已完成：
  - [app/config.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/config.py)
  - [app/bootstrap.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/bootstrap.py)
  - [app/main.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/main.py)
  - [app/runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/runtime.py)
  - [core/enums.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/core/enums.py)
  - [core/clock.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/core/clock.py)
  - [core/event_bus.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/core/event_bus.py)
  - [core/ids.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/core/ids.py)
  - [domain/models.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/domain/models.py)
  - [domain/events.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/domain/events.py)
  - [market/registry.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/market/registry.py)
  - [market/lifecycle.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/market/lifecycle.py)
  - [market/metadata_loader.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/market/metadata_loader.py)
  - [state/book_state.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/state/book_state.py)
  - [state/order_state.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/state/order_state.py)
  - [state/inventory_state.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/state/inventory_state.py)
  - [feeds/base.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/base.py)
  - [feeds/binance_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/binance_ws.py)
  - [feeds/chainlink_feed.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/chainlink_feed.py)
  - [feeds/polymarket_market_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/polymarket_market_ws.py)
  - [feeds/polymarket_user_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/polymarket_user_ws.py)
  - [pricing/vol_model.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/pricing/vol_model.py)
  - [pricing/lead_lag.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/pricing/lead_lag.py)
  - [pricing/fair_value.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/pricing/fair_value.py)
  - [pricing/quote_policy.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/pricing/quote_policy.py)

### 1.2 已验证

- 已通过基础导入校验：
  - `app`
  - `core`
  - `domain`
  - `market`
  - `state`
  - `feeds`
  - `pricing`
- 已通过最小 bootstrap 实例化校验：
  - `AppBootstrapper().start()`
  - `AppRuntime(context).start()/stop()`
- 已通过 runtime 执行闭环校验：
  - `strategy.order_intents -> ExecutionRouter -> InMemoryOrderStateStore`
  - `risk freshness heartbeat` 已接入 `binance / polymarket_market / chainlink / polymarket_user`
- 已通过定向回归：
  - `tests/unit/test_security.py`
  - `tests/unit/test_main_cli.py`
  - `tests/unit/test_replay_runtime.py`
  - `tests/unit/test_runtime_execution.py`
  - `tests/unit/test_risk_engine.py`
  - `tests/unit/test_strategy.py`
  - `tests/unit/test_imports.py`
  - `tests/unit/test_order_state.py`
  - `tests/unit/test_polymarket_feeds.py`
  - `tests/unit/test_runtime_sidecars.py`
  - `tests/unit/test_feed_resilience.py`
  - `tests/unit/test_metadata_loader.py`
  - `tests/unit/test_depth_feeds.py`
  - `tests/unit/test_database_writer.py`
  - `tests/unit/test_recorder.py`
  - 合计 `117 passed`
- 当前环境说明：
  - `tests/unit/test_cancel_manager.py` 仍依赖 `pytest-asyncio`
  - 本机当前未安装该插件，因此这组 async pytest 用例不能直接在本机 pytest 环境下执行
  - 数据接入层已补上重连退避、配置错误显式失败、metadata 缓存/超时，但真实 feed/auth/schema 联调仍未完成
  - live gateway / live ws 尚未做真实外网联调

### 1.3 未开始

- 无
- 目录与骨架层面已经基本齐全，当前主要缺口是真实接线与联调

### 1.6 Agent B/C 补充（2026-03-08）

- `strategy/phase_policy.py` — PhasePolicy：allow_new_quotes / allow_selective_taker / allow_recovery / max_unhedged_exposure（按 phase 分级）
- `strategy/pair_strategy.py` — PairStrategy：on_tick() → list[OrderIntent]（QuotePlan → maker intents，bids 共享 pair_id，去重已充分报价的 slot）
- `strategy/recovery_strategy.py` — RecoveryStrategy：on_fill() → 紧急 FOK 回补 + on_timer() → 周期 GTC 被动回补，max_concurrent_recovery 节流
- `tests/unit/test_strategy.py` — 28 个用例
- 当前状态：
  - `strategy` 代码已并入主目录
  - 本机已在 `tests/unit/test_strategy.py` 切片内验证通过

### 1.5 Agent C 已完成（2026-03-08）

- `risk/rules.py` — 8 条独立规则：MetadataIntegrity / MarketPhase / Freshness / MinSize / MaxSize / MaxDirectionalInventory / MaxUnhedgedExposure / MaxOpenOrders / GhostFillGuard
- `risk/engine.py` — RiskDecision + RiskConfig + RiskEngine（规则链，首个 hard 拒绝即停止）
- `execution/intents.py` — IntentAction(PLACE/CANCEL/REPLACE) + CancelIntent + ReplaceIntent
- `execution/order_builder.py` — OrderBuilder（tick rounding BUY/SELL maker/taker 四向 + payload 组装）
- `execution/polymarket_gateway.py` — PolymarketGateway（当前保留 dry_run，并已接入官方 `py-clob-client` live adapter）
- `execution/router.py` — ExecutionRouter（risk check → build → gateway → order_state 闭环，含 replace）
- `execution/cancel_manager.py` — CancelManager（stale quote cancel / cancel_all_for_market / cancel_by_pair）
- `tests/unit/test_risk_engine.py` — 16 个风控规则用例
- `tests/unit/test_order_builder.py` — 16 个 tick rounding + payload 用例
- `tests/unit/test_cancel_manager.py` — 9 个 cancel 用例（含 stale/all/by_pair）
- 当前状态：
  - `risk/execution` 代码已并入主目录
  - `test_cancel_manager.py` 需要 `pytest-asyncio`；当前本机未装该插件

### 1.7 Agent Runtime/Execution 集成补充（2026-03-08）

- [app/config.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/config.py) — 新增 `ExecutionConfig`，显式管理 `gateway_base_url / gateway_dry_run / cancel_stale_quotes_ms`
- [app/bootstrap.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/bootstrap.py) — 注入 `RiskRuntime` 与 `ExecutionRuntime`
- [app/runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/runtime.py) — 新增 `strategy.order_intents` consumer，打通 `strategy -> risk -> router -> gateway -> order_state`
- [app/runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/runtime.py) — feed heartbeat 已回灌到 risk，`market_resolved` 会触发 `cancel_all_for_market`，lifecycle loop 会执行 `cancel_stale_quotes`
- [tests/unit/test_runtime_execution.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_runtime_execution.py) — 新增 2 个 plugin-free runtime 测试
- 阶段性实测（runtime/execution 集成完成时）：
  - `tests/unit/test_runtime_execution.py tests/unit/test_risk_engine.py tests/unit/test_strategy.py tests/unit/test_imports.py`
  - `60 passed in 0.23s`

### 1.8 Agent Live-Integration 补充（2026-03-08）

- [state/order_state.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/state/order_state.py) — 新增 `exchange_order_id -> client_order_id` 索引，execution report 现在可按 `exchange_order_id` 回查本地订单
- [execution/router.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/execution/router.py) — 下单后立即保存 `exchange_order_id`；撤单时优先按 exchange order id 发给 gateway
- [execution/cancel_manager.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/execution/cancel_manager.py) — stale/all/by_pair cancel 现在同样优先走 exchange order id
- [execution/polymarket_gateway.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/execution/polymarket_gateway.py) — live 模式改成官方 `py-clob-client` adapter，dry-run 保持不变
- [feeds/base.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/base.py) — websocket 通用 heartbeat 支持，Polymarket `PING` 可直接复用
- [feeds/polymarket_market_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/polymarket_market_ws.py) — 按官方 `market` channel 结构生成 subscription payload
- [feeds/polymarket_user_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/polymarket_user_ws.py) — 新增 auth subscription builder；无 `client_order_id` 时可回退使用 `exchange_order_id`
- [app/config.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/config.py) — 补充 Polymarket auth / markets / assets / chain_id / signature_type / private_key / funder 等配置
- [pyproject.toml](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/pyproject.toml) — 增加官方依赖 `py-clob-client`
- 新增测试：
  - [tests/unit/test_polymarket_feeds.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_polymarket_feeds.py)
  - [tests/unit/test_order_state.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_order_state.py) 补充 exchange id 映射用例
- 本机实测：
  - `tests/unit/test_order_state.py tests/unit/test_polymarket_feeds.py tests/unit/test_runtime_execution.py tests/unit/test_risk_engine.py tests/unit/test_strategy.py tests/unit/test_imports.py`
  - `75 passed in 0.25s`

### 1.9 Agent Runtime Sidecars 补充（2026-03-08）

- [app/config.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/config.py) — 新增 `StorageConfig` 与 `ObservabilityConfig`
- [app/bootstrap.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/bootstrap.py) — 注入 `StorageRuntime` 与 `ObservabilityRuntime`
- [app/runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/runtime.py) — recorder/snapshot/metrics/alerts 已真正接入 runtime
- [app/runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/runtime.py) — 已对 feed latency、metadata load error、gateway submit、ghost fill、snapshot write 等关键路径打点/告警
- [tests/unit/test_runtime_sidecars.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_runtime_sidecars.py) — 新增 sidecar 集成测试，验证 recorder 落盘、snapshot 写入、metrics 与 alerts 更新
- 本机实测：
  - `tests/unit/test_runtime_sidecars.py tests/unit/test_runtime_execution.py tests/unit/test_order_state.py tests/unit/test_polymarket_feeds.py tests/unit/test_risk_engine.py tests/unit/test_strategy.py tests/unit/test_imports.py`
  - `77 passed in 0.44s`

### 1.10 Agent Replay Regression 补充（2026-03-08）

- [replay/registry.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/replay/registry.py) — 新增默认类型注册表，支持 recorder JSONL -> domain models/events 的自动重建
- [replay/runtime_runner.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/replay/runtime_runner.py) — 新增 `ReplayRuntimeRunner` 与 `ReplayReport`，支持目录/文件回放、topic 过滤、回放报告落盘
- [replay/__init__.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/replay/__init__.py) — 导出 replay runtime 入口
- [app/main.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/main.py) — 新增 `replay` 子命令，可直接离线运行 `python -m app.main replay <jsonl-or-dir> --report <path>`
- [app/runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/runtime.py) — lifecycle transition 后会立即 refresh market outputs，避免新 market 刚激活时出现 quote 空窗
- [tests/unit/test_replay_runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_replay_runtime.py) — 新增 recorder -> replay -> runtime 的离线端到端回归测试
- [tests/unit/test_imports.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_imports.py) — 补充 replay registry/runtime runner import smoke tests
- 本机实测：
  - `tests/unit/test_replay_runtime.py tests/unit/test_runtime_sidecars.py tests/unit/test_runtime_execution.py tests/unit/test_order_state.py tests/unit/test_polymarket_feeds.py tests/unit/test_risk_engine.py tests/unit/test_strategy.py tests/unit/test_imports.py`
  - `80 passed in 0.83s`

### 1.11 Agent Secrets / Entry 补充（2026-03-08）

- [security/crypto.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/security/crypto.py) — 新增 PBKDF2 + Fernet 私钥加解密组件
- [security/private_key.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/security/private_key.py) — 新增 `.env` 私钥解析、密码注入与解密缓存
- [app/env.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/env.py) — 新增项目级 `.env` 加载入口
- [app/config.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/config.py) — `RuntimeConfig.from_env()` 现在支持 `decrypt_password / env_path / strict_encrypted_private_key`
- [app/main.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/main.py) — live 启动支持 `python main.py "password"` 风格；新增 `encrypt-secret` 子命令
- [main.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/main.py) — 新增根目录启动入口
- [encrypt_secret.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/encrypt_secret.py) — 新增独立私钥加密脚本入口
- [execution/polymarket_gateway.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/execution/polymarket_gateway.py) — live 下单参数更贴近 `py-clob-client` 当前 `OrderArgs` 结构，补入 `fee_rate_bps`
- [.env.example](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/.env.example) — 新增 `.env` 模板，敏感配置统一收口
- [tests/unit/test_security.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_security.py) — 新增加密/解密与配置解析测试
- [tests/unit/test_main_cli.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_main_cli.py) — 新增 CLI 密码解析测试
- 本机实测：
  - `tests/unit/test_security.py tests/unit/test_main_cli.py tests/unit/test_replay_runtime.py tests/unit/test_runtime_sidecars.py tests/unit/test_runtime_execution.py tests/unit/test_order_state.py tests/unit/test_polymarket_feeds.py tests/unit/test_risk_engine.py tests/unit/test_strategy.py tests/unit/test_imports.py`
  - `90 passed in 1.17s`

### 1.12 Agent Startup Diagnostics 补充（2026-03-08）

- [app/config.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/config.py) — `ObservabilityConfig` 新增启动观察期配置：`startup_diagnostics_enabled / startup_diagnostics_log_interval_ms / startup_diagnostics_window_ms / startup_diagnostics_first_events`
- [app/runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/runtime.py) — 新增启动观察期日志：首批事件延迟日志 + 周期 feed health 摘要，窗口结束后自动收敛
- [app/main.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/main.py) — 启动时若全部 feed 关闭，会显式告警 runtime 处于 idle
- [tests/unit/test_runtime_sidecars.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_runtime_sidecars.py) — 新增 startup diagnostics 日志测试
- 本机实测：
  - `tests/unit/test_security.py tests/unit/test_main_cli.py tests/unit/test_replay_runtime.py tests/unit/test_runtime_sidecars.py tests/unit/test_runtime_execution.py tests/unit/test_order_state.py tests/unit/test_polymarket_feeds.py tests/unit/test_risk_engine.py tests/unit/test_strategy.py tests/unit/test_imports.py`
  - `91 passed in 1.31s`

### 1.13 Agent Feed Failure Visibility 补充（2026-03-08）

- [app/main.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/main.py) — 启动时会显式警告 enabled 但配置不完整的 feed，例如缺失 `POLY15_CHAINLINK_ENDPOINT / POLY15_PM_MARKET_ASSET_IDS / POLY15_PM_USER_MARKET_IDS / POLY15_PM_API_*`
- [app/runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/runtime.py) — feed 后台任务现在会在异常时直接打日志，并在 startup `feed health` 中显示 `error=...`
- [app/runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/runtime.py) — `runtime.stop()` 已避免在停机阶段重复抛出已记录过的 feed 异常
- [tests/unit/test_main_cli.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_main_cli.py) — 新增启动配置 warning 测试
- [tests/unit/test_runtime_sidecars.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_runtime_sidecars.py) — 新增后台 feed 异常可见性测试
- 当前结论：
  - 数据接入层仍是 `IN_PROGRESS`
  - 现阶段可以快速暴露配置缺失和启动失败，但不能把它当成真实接入已完成
- 本机实测：
  - `tests/unit/test_security.py tests/unit/test_main_cli.py tests/unit/test_replay_runtime.py tests/unit/test_runtime_sidecars.py tests/unit/test_runtime_execution.py tests/unit/test_order_state.py tests/unit/test_polymarket_feeds.py tests/unit/test_risk_engine.py tests/unit/test_strategy.py tests/unit/test_imports.py`
  - `93 passed in 1.28s`

### 1.14 Agent Data Ingress Resilience 补充（2026-03-08）

- [app/config.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/config.py) — 为 Binance / Chainlink / Polymarket 补充 `connect_timeout / retry_initial / retry_max / retry_backoff`，为 metadata 补充 `cache_ttl / request_timeout / return_stale_on_error`
- [feeds/base.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/base.py) — websocket feed 现在支持自动重连、指数退避、transport 清理和 `FeedConfigurationError`
- [feeds/binance_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/binance_ws.py) — 接入共享 websocket 重连参数
- [feeds/polymarket_market_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/polymarket_market_ws.py) — 接入共享 websocket 重连参数
- [feeds/polymarket_user_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/polymarket_user_ws.py) — 接入共享 websocket 重连参数
- [feeds/chainlink_feed.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/chainlink_feed.py) — polling feed 现在支持 request timeout、retry backoff、stale tick 过滤
- [market/metadata_loader.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/market/metadata_loader.py) — 增加 TTL 缓存、请求超时、失败时返回 stale cache，以及 `clobTokenIds` list/string 双兼容
- [app/bootstrap.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/bootstrap.py) — metadata loader 已接 runtime config
- [app/main.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/main.py) — `new_market` 启用但缺少 `POLY15_PM_GAMMA_BASE_URL` 时会启动告警
- [tests/unit/test_feed_resilience.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_feed_resilience.py) — 新增 websocket retry、fatal config error、chainlink stale/retry 测试
- [tests/unit/test_metadata_loader.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_metadata_loader.py) — 新增 metadata TTL 缓存和 stale fallback 测试
- 当前结论：
  - 数据接入层仍是 `IN_PROGRESS`
  - 但已从“单次连接骨架”提升到“可重连、可缓存、可显式失败”的阶段
- 本机实测：
  - `tests/unit/test_security.py tests/unit/test_main_cli.py tests/unit/test_replay_runtime.py tests/unit/test_runtime_sidecars.py tests/unit/test_runtime_execution.py tests/unit/test_order_state.py tests/unit/test_polymarket_feeds.py tests/unit/test_feed_resilience.py tests/unit/test_metadata_loader.py tests/unit/test_risk_engine.py tests/unit/test_strategy.py tests/unit/test_imports.py`
  - `100 passed in 1.55s`

### 1.15 Agent Proxy / Dependency Fast-Fail 补充（2026-03-08）

- [app/config.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/config.py) — 新增 `ProxyConfig`，统一通过 `.env` 管理 `POLY15_PROXY_ENABLED / POLY15_PROXY_URL / POLY15_PROXY_NO_PROXY`
- [app/main.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/main.py) — 启动时会把代理注入 `HTTP_PROXY / HTTPS_PROXY / ALL_PROXY / NO_PROXY`；`Ctrl+C` 停机不再抛尾部 traceback
- [feeds/base.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/base.py) — 缺少 `aiohttp` 现在属于 `FeedDependencyError`，会直接失败，不再被误判成可重试网络错误；WS 连接支持显式 `proxy_url`
- [feeds/binance_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/binance_ws.py) — 支持通过统一 proxy config 走本地代理
- [feeds/polymarket_market_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/polymarket_market_ws.py) — 支持通过统一 proxy config 走本地代理
- [feeds/polymarket_user_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/polymarket_user_ws.py) — 支持通过统一 proxy config 走本地代理
- [feeds/chainlink_feed.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/chainlink_feed.py) — HTTP polling 现在支持 proxy 和 `trust_env`
- [market/metadata_loader.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/market/metadata_loader.py) — gamma metadata 拉取现在支持 proxy 和 `trust_env`
- [.env.example](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/.env.example) — 已补代理示例项，方便本地 7890 测试
- [tests/unit/test_main_cli.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_main_cli.py) — 新增代理 env 注入测试
- [tests/unit/test_feed_resilience.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_feed_resilience.py) — 新增 dependency fast-fail 测试
- 当前结论：
  - 本地代理测试现在可以纯靠 `.env` 控制
  - 依赖缺失会直接暴露，不会再无限 backoff 刷屏
- 本机实测：
  - `tests/unit/test_security.py tests/unit/test_main_cli.py tests/unit/test_replay_runtime.py tests/unit/test_runtime_sidecars.py tests/unit/test_runtime_execution.py tests/unit/test_order_state.py tests/unit/test_polymarket_feeds.py tests/unit/test_feed_resilience.py tests/unit/test_metadata_loader.py tests/unit/test_risk_engine.py tests/unit/test_strategy.py tests/unit/test_imports.py`
  - `102 passed in 1.59s`

### 1.16 Agent Market Discovery / Feed Auto-Subscription 补充（2026-03-08）

- [app/config.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/config.py) — Chainlink 新增 `api_url / query_name / feed_id / price_scale`，Gamma metadata 新增 `discovery_enabled / discovery_interval_ms / discovery_max_markets / discovery_tag_slug / discovery_keywords / discovery_exclude_keywords / discovery_min_duration_minutes / discovery_max_duration_minutes`
- [feeds/chainlink_feed.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/chainlink_feed.py) — 兼容 `testChain.py` 的 `query-timescale` 拉取方式；支持 `LIVE_STREAM_REPORTS_QUERY`、`feedId` 和 1e18 price scale；重复 tick 去重
- [market/metadata_loader.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/market/metadata_loader.py) — 新增 Gamma active market discovery，按 `bitcoin` tag + 关键词过滤 + 15m 时长筛选；补了 `Yes/No` 与 `Up/Down` token 映射、标题时间解析、tag id 缓存
- [feeds/polymarket_market_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/polymarket_market_ws.py) — market WS 改成 token 动态订阅；运行中可 `ensure_assets()`；支持 token -> internal market id 映射，不再硬依赖消息里自带 `market_id`
- [feeds/polymarket_user_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/polymarket_user_ws.py) — user WS 改成 condition id 动态订阅；运行中可 `ensure_markets()`；支持 `condition_id -> market_id` 和 `token_id -> market_id` 归一化
- [app/runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/runtime.py) — 新增 discovery loop：周期拉 Gamma，发布 `market.metadata`，并驱动 market/user feed 自动补订阅
- [app/main.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/main.py) — 启动 warning 已改成基于 `query-timescale` 和 Gamma discovery 的真实配置要求，不再错误要求手填 `asset_ids / market_ids`
- [.env.example](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/.env.example) — 已补 Chainlink `query-timescale` 与 Gamma discovery 示例项
- [tests/unit/test_feed_resilience.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_feed_resilience.py) — 新增 `query-timescale` 解析测试
- [tests/unit/test_polymarket_feeds.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_polymarket_feeds.py) — 新增 token/condition 到 internal market id 的映射测试
- [tests/unit/test_metadata_loader.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_metadata_loader.py) — 新增 Gamma discovery + `Yes/No` token 映射测试
- 当前结论：
  - 数据接入层仍是 `IN_PROGRESS`
  - 但已从“必须手填 asset_id / market_id 才能动”提升到“Gamma 自动发现 + feed 动态补订阅”的阶段
  - 仍待真实外网联调验证 Polymarket user auth、market payload schema 和 Chainlink 真网稳定性
- 本机实测：
  - `tests/unit/test_security.py tests/unit/test_main_cli.py tests/unit/test_replay_runtime.py tests/unit/test_runtime_sidecars.py tests/unit/test_runtime_execution.py tests/unit/test_order_state.py tests/unit/test_polymarket_feeds.py tests/unit/test_feed_resilience.py tests/unit/test_metadata_loader.py tests/unit/test_risk_engine.py tests/unit/test_strategy.py tests/unit/test_imports.py`
  - `109 passed in 1.60s`

### 1.17 Agent Feed Health Gating / Discovery Noise Reduction（2026-03-08）

- [feeds/chainlink_feed.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/chainlink_feed.py) — Chainlink 失败日志已改成 `ExceptionType(repr)`，query HTTP 非 2xx 时会打印 status/body 摘要；在本地代理模式下改用 `ssl=False` connector，便于排查 query-timescale 真网失败原因
- [market/metadata_loader.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/market/metadata_loader.py) — Gamma HTTP 拉取在 proxy 模式下也改成 `ssl=False` connector，对齐参考项目里的 aiohttp 用法
- [app/runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/runtime.py) — `feeds.polymarket.market.new_market` 现在只接受看起来像 Gamma market id 的引用；对 `0x...` 这类非 numeric ref 会直接忽略并记 `market_discovery_ws_ignored_count`，不再反复打 `metadata load failed`
- [app/runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/runtime.py) — `pair_strategy` 新增关键 feed 健康门控：`binance / polymarket_market / chainlink / polymarket_user` 中任一路未就绪或过期时，只保留数据计算，不再继续出单；会记 `pair_strategy_blocked_count`
- [tests/unit/test_runtime_sidecars.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_runtime_sidecars.py) — 新增“忽略非 Gamma market discovery ref”测试
- [tests/unit/test_runtime_execution.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_runtime_execution.py) — 新增“required feeds 不健康时禁止出单”测试
- 当前结论：
  - 数据观测链路和出单链路现在被明确切开
  - 在 `Chainlink waiting`、`user auth incomplete` 这类状态下，系统会继续收行情，但不应该再出现大批 dry-run 报单
  - `new_market` 里混入的 `0x...` 引用不会再持续污染 metadata 告警
- 本机实测：
  - `tests/unit/test_security.py tests/unit/test_main_cli.py tests/unit/test_replay_runtime.py tests/unit/test_runtime_sidecars.py tests/unit/test_runtime_execution.py tests/unit/test_order_state.py tests/unit/test_polymarket_feeds.py tests/unit/test_feed_resilience.py tests/unit/test_metadata_loader.py tests/unit/test_risk_engine.py tests/unit/test_strategy.py tests/unit/test_imports.py`
  - `111 passed in 1.75s`

### 1.18 Agent Depth Storage / Chainlink Batch Dedup（2026-03-08）

- [app/config.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/config.py) — Binance 新增 `depth_enabled / depth_ws_url`；Chainlink 默认轮询间隔改为 `10000ms`，默认 `stale_after_ms` 提升到 `20000ms`，更适合 15m 场景下的“慢 oracle、快 CEX”
- [feeds/chainlink_feed.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/chainlink_feed.py) — query-timescale 现在会展开整批 `nodes`，按时间排序后逐条发布唯一样本；同一批/跨批重复点位会通过 bounded recent-key 集合去重；`ChainlinkTick` 也补入了 `bid/ask`
- [feeds/binance_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/binance_ws.py) — 新增 `BinanceDepthFeed`，订阅并归一化 `depth5@100ms`，用于录制 BTC 5 档订单簿
- [feeds/polymarket_market_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/polymarket_market_ws.py) — 在保留 `book_top` 的同时，新增 `book -> 5x5 depth snapshot` 归一化，用于录制 Polymarket 每个 outcome 的 5 档买卖盘
- [domain/models.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/domain/models.py) / [domain/events.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/domain/events.py) — 新增 `BookLevel / BinanceDepthSnapshot / PolymarketDepthSnapshot / BinanceDepthEvent / PolymarketDepthEvent`
- [app/bootstrap.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/bootstrap.py) / [app/runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/runtime.py) — depth feeds 已挂入 runtime；`feeds.binance.depth` 和 `feeds.polymarket.market.depth` 会被 recorder 正常落盘，并补充 `binance_depth_latency_ms / polymarket_market_depth_age_ms`
- [replay/registry.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/replay/registry.py) / [replay/runtime_runner.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/replay/runtime_runner.py) — replay 已认识新 depth 类型和 topic，不会在回放时丢失这两类记录
- [.env.example](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/.env.example) — 已补 `POLY15_BINANCE_DEPTH_ENABLED=1`、`POLY15_CHAINLINK_POLL_MS=10000`、`POLY15_CHAINLINK_STALE_AFTER_MS=20000`、`POLY15_RECORDER_ENABLED=1`
- [tests/unit/test_depth_feeds.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_depth_feeds.py) — 新增 Binance/Polymarket depth 归一化测试
- [tests/unit/test_feed_resilience.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_feed_resilience.py) — Chainlink query-timescale 归一化测试已改成 batch 语义，并验证 `bid/ask` 归一化
- [tests/unit/test_runtime_sidecars.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_runtime_sidecars.py) — recorder 侧已验证 `feeds.binance.depth` 与 `feeds.polymarket.market.depth` 会落盘
- 当前结论：
  - 项目现在除了 top-of-book 之外，也能稳定录制 `Binance 5 档` 和 `Polymarket 5x5` 的标准化深度数据
  - Chainlink 已从“每轮只取最新一条”改成“每轮取整批唯一点位”，更适合做 oracle 对齐样本库
  - 如果目标是以数据采集为主，建议在 `.env` 中显式开启 `POLY15_RECORDER_ENABLED=1`
- 本机实测：
  - `tests/unit/test_security.py tests/unit/test_main_cli.py tests/unit/test_replay_runtime.py tests/unit/test_runtime_execution.py tests/unit/test_risk_engine.py tests/unit/test_strategy.py tests/unit/test_imports.py tests/unit/test_order_state.py tests/unit/test_polymarket_feeds.py tests/unit/test_runtime_sidecars.py tests/unit/test_feed_resilience.py tests/unit/test_metadata_loader.py tests/unit/test_depth_feeds.py`
  - `114 passed in 1.80s`

### 1.19 Agent Threaded DB Storage（2026-03-08）

- [storage/database_writer.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/storage/database_writer.py) — 新增 `ThreadedDatabaseWriter`：使用独立线程持有 SQLite 连接，按 `flush_interval_ms / flush_batch_size` 定期批量落库
- [storage/__init__.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/storage/__init__.py) — 导出 `ThreadedDatabaseWriter`
- [app/config.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/config.py) — `StorageConfig` 新增 `db_enabled / db_path / db_max_queue_size / db_flush_interval_ms / db_flush_batch_size`
- [app/bootstrap.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/bootstrap.py) — `StorageRuntime` 新增 `db_writer`，由 bootstrap 按配置注入
- [app/runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/runtime.py) — runtime 启停时会启动/关闭数据库写线程；每次 `_record_event()` 会同时写入 JSONL recorder 和 SQLite；metrics 新增 `db_writer_dropped_count / db_writer_written_count / db_writer_flush_count`
- [app/main.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/main.py) — 启动摘要新增 `db=` 开关，方便区分“只录 JSONL”还是“同时落库”
- [.env.example](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/.env.example) — 新增 `POLY15_DB_ENABLED / POLY15_DB_PATH / POLY15_DB_FLUSH_INTERVAL_MS / POLY15_DB_FLUSH_BATCH_SIZE`
- [tests/unit/test_database_writer.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_database_writer.py) — 新增线程落库单测，验证独立线程定时 flush 后确实写入 `event_records`
- [tests/unit/test_runtime_sidecars.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_runtime_sidecars.py) — sidecar 集成测试已扩展为 JSONL + SQLite 双落地验证
- 当前结论：
  - 现在项目已经同时支持：
    - `AsyncRecorder`：按 topic 分文件 JSONL 录制
    - `ThreadedDatabaseWriter`：后台线程定期批量写入 SQLite
  - 如果目标是本地持续采集，建议 `POLY15_RECORDER_ENABLED=1` 和 `POLY15_DB_ENABLED=1` 一起开
  - SQLite 表名固定为 `event_records`，适合后续直接做查询、聚合和回放索引
- 本机实测：
  - `tests/unit/test_security.py tests/unit/test_main_cli.py tests/unit/test_replay_runtime.py tests/unit/test_runtime_execution.py tests/unit/test_risk_engine.py tests/unit/test_strategy.py tests/unit/test_imports.py tests/unit/test_order_state.py tests/unit/test_polymarket_feeds.py tests/unit/test_runtime_sidecars.py tests/unit/test_feed_resilience.py tests/unit/test_metadata_loader.py tests/unit/test_depth_feeds.py tests/unit/test_database_writer.py`
  - `116 passed in 1.90s`

### 1.20 Agent Threaded File Recorder（2026-03-08）

- [storage/recorder.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/storage/recorder.py) — `AsyncRecorder` 已从“事件循环内逐条 flush”改成“后台线程 + 定期批量 flush JSONL 文件”
- [storage/recorder.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/storage/recorder.py) — 录制目录已切成 `runtime_data/records/global/*.jsonl` 与 `runtime_data/records/markets/<market_id>/*.jsonl`，更适合单市场解析
- [replay/runtime_runner.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/replay/runtime_runner.py) — replay 文件发现已改成递归扫描，兼容新的分目录 recorder 结构
- [app/config.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/config.py) — `StorageConfig` 新增 `recorder_flush_interval_ms / recorder_flush_batch_size`
- [app/bootstrap.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/bootstrap.py) — bootstrap 现在会把 file recorder 的 flush 参数注入
- [app/runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/runtime.py) — metrics 新增 `recorder_written_count / recorder_flush_count`，便于观察文件落盘线程是否工作正常
- [.env.example](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/.env.example) — 已补 `POLY15_RECORDER_FLUSH_INTERVAL_MS / POLY15_RECORDER_FLUSH_BATCH_SIZE`，并将 `POLY15_DB_ENABLED` 默认改回 `0`
- [tests/unit/test_recorder.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_recorder.py) — 新增文件 recorder 单测，验证后台线程会按批量 flush 写出 JSONL
- [tests/unit/test_runtime_sidecars.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_runtime_sidecars.py) — sidecar 测试已显式使用较短 recorder flush 间隔，验证实时采集时文件能及时落出
- [tests/unit/test_replay_runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests/unit/test_replay_runtime.py) — 录制文件收集改成递归 `rglob("*.jsonl")`，验证分目录后 replay 不会失效
- 当前结论：
  - 当前项目的主采集路径应视为 `JSONL 文件`
  - recorder 已满足“新线程/后台线程定期落文件”的要求
  - 市场相关事件现在会自动落到对应 `market_id` 文件夹；Binance / Chainlink 这类共享源数据则留在 `global/`
  - SQLite 线程写入器仍保留为可选旁路，但默认关闭，不再是推荐主路径
- 本机实测：
  - `tests/unit/test_security.py tests/unit/test_main_cli.py tests/unit/test_replay_runtime.py tests/unit/test_runtime_execution.py tests/unit/test_risk_engine.py tests/unit/test_strategy.py tests/unit/test_imports.py tests/unit/test_order_state.py tests/unit/test_polymarket_feeds.py tests/unit/test_runtime_sidecars.py tests/unit/test_feed_resilience.py tests/unit/test_metadata_loader.py tests/unit/test_depth_feeds.py tests/unit/test_database_writer.py tests/unit/test_recorder.py`
  - `117 passed in 1.84s`

---

## 2. 当前目录状态

### 2.1 已存在的可编辑目录

- [app](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app)
- [core](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/core)
- [domain](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/domain)
- [market](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/market)
- [state](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/state)
- [feeds](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds)
- [pricing](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/pricing)
- [strategy](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/strategy)
- [risk](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/risk)
- [execution](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/execution)
- [storage](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/storage)
- [replay](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/replay)
- [observability](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/observability)
- [tests](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/tests)

### 2.2 下一批要创建的目录

- 无
- 目录层面已经齐全，下一阶段重点是 `真实 feed/auth/gateway` 接线，而不是继续扩目录

### 2.3 不要动的文件

除非明确要改架构，否则不要随意修改：

- [summary.md](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/summary.md)
- [modules_interfaces.md](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/modules_interfaces.md)

---

## 3. 多 Agent 推荐拆分

建议按 4 个 agent 并行，而不是所有人一起改共享文件。

### Agent A: Feed / Metadata

目标：

- 完成 Binance / Chainlink / Polymarket 的真实订阅、认证、重连和 payload 校验
- 完成 `metadata_loader` 的真实 gamma 拉取、缓存和 active/next market 策略

负责文件：

- `feeds/base.py`
- `feeds/binance_ws.py`
- `feeds/polymarket_market_ws.py`
- `feeds/polymarket_user_ws.py`
- `feeds/chainlink_feed.py`
- `market/metadata_loader.py`

依赖：

- 依赖 `domain/models.py`
- 依赖 `domain/events.py`
- 依赖 `app/config.py`
- 依赖 `app/bootstrap.py`
- 依赖 `app/runtime.py`

禁止：

- 不要改 `state/*` 内部数据结构
- 不要改 `market/lifecycle.py` phase 定义
- 不要改 `pricing/*` 算法逻辑

### Agent B: Pricing / Strategy

目标：

- 基于真实 feed 校准短期波动率、theo、lead-lag 和 close-phase 参数
- 校准 `pair_strategy / recovery_strategy / phase_policy`，避免 live 环境下过度报价或回补过慢

负责文件：

- `pricing/vol_model.py`
- `pricing/lead_lag.py`
- `pricing/fair_value.py`
- `pricing/quote_policy.py`
- `strategy/phase_policy.py`
- `strategy/pair_strategy.py`
- `strategy/recovery_strategy.py`

依赖：

- 依赖 `state/book_state.py`
- 依赖 `state/inventory_state.py`
- 依赖 `market/registry.py`
- 依赖 Agent A 提供真实 market/user feed 数据质量结论

禁止：

- 不要直接发单
- 不要改 `execution/*`

### Agent C: Risk / Execution

目标：

- 联调真实下单 / 撤单 / 状态查询
- 确认 `exchange_order_id` 映射、user WS 回报、cancel path 和 gateway 行为一致
- 在 live 条件下验证风控 freshness / ghost fill / close-phase 约束

负责文件：

- `risk/rules.py`
- `risk/engine.py`
- `execution/intents.py`
- `execution/order_builder.py`
- `execution/router.py`
- `execution/polymarket_gateway.py`
- `execution/cancel_manager.py`

依赖：

- 依赖 `domain/models.py`
- 依赖 `state/order_state.py`
- 依赖 `state/inventory_state.py`
- 依赖 Agent A 的 user WS 回报契约
- 依赖 `app/bootstrap.py`
- 依赖 `app/runtime.py`

禁止：

- 不要改 `pricing/*` 算法逻辑

### Agent D: Storage / Replay / Obs / Tests

目标：

- 把 recorder / snapshot / metrics / alerts 真正接到 runtime
- 扩展 replay 和测试，用于离线回放与联调回归

负责文件：

- `storage/recorder.py`
- `storage/snapshot_writer.py`
- `replay/player.py`
- `observability/metrics.py`
- `observability/alerts.py`
- `tests/*`

依赖：

- 依赖所有事件契约稳定
- 依赖 `app/runtime.py`

禁止：

- 不要直接改交易主逻辑

---

## 4. 共享文件锁规则

以下文件视为共享高风险文件，同一时间只允许一个 agent 修改：

- [domain/models.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/domain/models.py)
- [domain/events.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/domain/events.py)
- [app/config.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/config.py)
- [app/bootstrap.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/bootstrap.py)
- [app/runtime.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/app/runtime.py)
- [core/enums.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/core/enums.py)
- [feeds/base.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/base.py)
- [feeds/polymarket_market_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/polymarket_market_ws.py)
- [feeds/polymarket_user_ws.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/feeds/polymarket_user_ws.py)
- [execution/polymarket_gateway.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/execution/polymarket_gateway.py)
- [state/order_state.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/state/order_state.py)
- [pyproject.toml](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/pyproject.toml)
- [agent_progress.md](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/agent_progress.md)

规则：

1. 修改前先在本文档的“工作看板”里标记 `IN_PROGRESS`
2. 修改后立刻补充“接口变化”
3. 如果改动了共享契约，必须说明是否破坏兼容

---

## 5. 当前接口冻结区

以下对象已经进入“尽量不随便改字段”的阶段：

- `MarketMetadata`
- `BinanceTick`
- `ChainlinkTick`
- `OutcomeBookTop`
- `QuotePlan`
- `OrderIntent`
- `ExecutionReport`
- `OrderRecord`
- `InventoryLot`
- `LifecycleTransition`

定义文件：

- [domain/models.py](/Volumes/captain/code/PycharmWorksapce/Poly-15minBTC/domain/models.py)

允许的改动：

- 追加字段
- 补充默认值
- 增加辅助 dataclass

不建议的改动：

- 删除已有字段
- 修改字段语义
- 把 `Decimal` 改成 `float`

---

## 6. 工作看板

状态定义：

- `TODO`
- `IN_PROGRESS`
- `BLOCKED`
- `DONE`

### 6.1 基础层

| 任务 | 状态 | 说明 |
| --- | --- | --- |
| app/bootstrap 基础装配 | DONE | 已完成最小运行时上下文 |
| core 基础设施 | DONE | clock/event_bus/ids 已完成 |
| domain 基础模型 | DONE | 第一版数据契约已落地 |
| market registry | DONE | active/next 查询已实现 |
| lifecycle manager | DONE | 基础 phase 切换已实现 |
| state stores | DONE | book/order/inventory 已实现最小版本 |
| app runtime wiring | DONE | lifecycle loop + pricing consumer + strategy/execution 闭环已接入 |

### 6.2 数据接入层

| 任务 | 状态 | 说明 |
| --- | --- | --- |
| Binance WS | IN_PROGRESS | `bookTicker` 解析已完成；已补自动重连/退避；待真网首包确认、长时间延迟观测和订阅稳定性验证 |
| Chainlink feed | IN_PROGRESS | 现支持 `query-timescale`(`LIVE_STREAM_REPORTS_QUERY + feedId`) 和自定义 endpoint 两条路径；已补 request timeout、retry backoff、stale 过滤、proxy、重复 tick 去重、异常类型可见性；待真网稳定性联调 |
| Polymarket market WS | IN_PROGRESS | 已改成 token 动态订阅 + heartbeat + 自动重连/退避 + proxy；支持 token -> internal market id 映射；待真实 payload schema、订阅恢复和长连稳定性校验 |
| Polymarket user WS | IN_PROGRESS | 已改成 condition id 动态订阅；支持 auth subscription、execution 归一化、condition/token -> market id 映射；待真实认证、schema 校验与订单回报闭环 |
| metadata loader | IN_PROGRESS | 解析逻辑已完成；已补 TTL 缓存、请求超时、stale fallback、proxy、Gamma BTC 15m discovery、Yes/No token 映射；待真实 gamma 拉取、缓存刷新和 active/next 切换策略联调 |

### 6.3 定价与策略层

| 任务 | 状态 | 说明 |
| --- | --- | --- |
| vol_model | DONE | v1 已实现，rolling + EWMA 短波动率 |
| lead_lag | DONE | v1 已实现，basis + micro momentum 偏置 |
| fair_value | DONE | v1 已实现，binary theo + reference price 回退 |
| quote_policy | DONE | v1 已实现，maker-first + inventory skew + pair bid cap |
| pair_strategy | DONE | on_tick() -> maker intents，bids 共享 pair_id，max_quote_depth 节流 |
| recovery_strategy | DONE | on_fill() FOK 紧急回补 + on_timer() GTC 周期回补，max_concurrent 节流 |
| phase_policy | DONE | 各 phase 的 allow_new_quotes / allow_selective_taker / allow_recovery / max_unhedged |

### 6.4 交易与风控层

| 任务 | 状态 | 说明 |
| --- | --- | --- |
| risk rules | DONE | 8 条规则链，metadata/phase/freshness/size/inventory/unhedged/open_orders/ghost_fill |
| risk engine | DONE | RiskDecision + RiskConfig + RiskEngine，首个 hard reject 即停 |
| order builder | DONE | tick rounding 四向（BUY/SELL × maker/taker），payload 组装 |
| gateway | DONE | PolymarketGateway：dry_run 保留，live 路径已切到官方 `py-clob-client` adapter，`OrderArgs` 组装已兼容 `.env` 解密私钥与 fee_rate_bps |
| router | DONE | ExecutionRouter：risk→build→gateway→state，含 replace 原语；已保存 exchange order id |
| cancel manager | DONE | CancelManager：stale quote / cancel_all / cancel_by_pair；撤单优先走 exchange order id |
| runtime execution wiring | DONE | `strategy.order_intents` consumer + feed freshness heartbeat + `market_resolved -> cancel_all` |
| polymarket live adapter | IN_PROGRESS | 官方 `py-clob-client` adapter 已接入代码，但尚未做真实外网联调 |

### 6.5 旁路层

| 任务 | 状态 | 说明 |
| --- | --- | --- |
| recorder | DONE | AsyncRecorder 已挂接 runtime，记录 feed/market/pricing/strategy/lifecycle 事件 |
| snapshot writer | DONE | SnapshotWriter 已挂接周期任务，按配置输出热状态快照 |
| replay player | DONE | ReplayPlayer + TypeRegistry + default registry 已实现 |
| replay runtime regression | DONE | ReplayRuntimeRunner + ReplayReport + CLI 已接通，可用 recorder 文件直接驱动 runtime 回归 |
| secrets / env bootstrap | DONE | `.env` 加载、加密私钥解密、`python main.py \"password\"` 入口已接通 |
| metrics | DONE | InMemoryMetrics 已接入 runtime，覆盖 feed latency / order rejects / snapshot writes / unhedged exposure 等 |
| startup diagnostics | DONE | 启动观察期会打印首批事件延迟和 feed health 摘要，窗口结束后自动降噪 |
| alerts | DONE | LoggingAlerts 已接入 runtime，覆盖 metadata load failure / market resolved / gateway submit failure / ghost fill mismatch |
| tests | DONE | 当前环境已验证 `security + main_cli + replay_runtime + runtime_sidecars + order_state + polymarket_feeds + feed_resilience + metadata_loader + runtime_execution + risk + strategy + imports + depth_feeds + database_writer + recorder = 117 passed`；`cancel_manager` async pytest 需 `pytest-asyncio` |

---

## 7. 推荐集成顺序

多 agent 合并时，建议按这个顺序集成：

1. Agent A 先补 `feeds/*` 的真实订阅报文、认证和重连
2. Agent A 再把 `metadata_loader.py` 接到真实 gamma endpoint 与缓存
3. Agent C 联网验证 `py-clob-client` live path、撤单路径和 order status 查询
4. Agent A / C 联调 `user WS order/trade event -> order_state/inventory_state`
5. Agent B 再在真实 feed 下校准 `pricing/phase/recovery` 参数
6. Agent D 已完成 replay 驱动的离线回放回归入口，后续只需在其上补参数校准脚本或可视化面板

原因：

- 没有 feeds，后面全是假数据
- 没有 user WS，recovery 不可信
- 没有 live 联调，gateway 虽已有官方 adapter 代码但仍未被真实验证
- 已有 replay 驱动的系统化回归入口，但参数校准、事故比对和批量报告仍然偏手工

---

## 8. 每个 Agent 的交接格式

每次提交后都要补 4 行：

```text
模块:
修改文件:
新增接口/变更接口:
验证方式:
```

示例：

```text
模块: feeds.binance_ws
修改文件: feeds/binance_ws.py, domain/events.py
新增接口/变更接口: 新增 BinanceTradeEvent，不破坏现有字段
验证方式: python -c "import feeds.binance_ws"
```

---

## 9. 合并前检查

提交到主线前，至少满足：

1. `python -c "import ..."` 能通过
2. 不修改未声明的共享文件
3. 新增枚举或字段时同步更新本文档
4. 不把 `__pycache__/`、临时文件提交进仓库

---

## 10. 当前建议的下一步

如果现在继续推进，最优先的是：

1. Agent A 补 `feeds/*` 的真实订阅、认证和重连细节
2. Agent C 用真实凭证联调 `execution/polymarket_gateway.py`
3. Agent A / C 用真实 user WS 校验 `exchange_order_id` 对单闭环
4. Agent B 基于真实 feed 校准 `pricing/*` 和 `strategy/*` 参数
5. Agent D 如继续推进，应在现有 replay runtime runner 之上补批量回放、参数对比和回归面板

---

## 11. 决策记录

### 2026-03-08

- 当前系统采用 `单进程 + asyncio + 内存热状态` 作为第一版运行形态
- 当前策略主轴定义为 `maker-first + full-set 成本累积 + 单腿 recovery`
- `summary.md` 与 `modules_interfaces.md` 视为架构主文档，普通开发不应随意改动
- 当前优先级不是极限低延迟，而是先打通数据流、状态一致性和 recovery 闭环
- feed 默认采用显式 `enabled` 开关，避免骨架阶段启动时自动连外网
- pricing v1 已接入 runtime，当前仍需靠真实 metadata/reference price 完善 theo 可信度
