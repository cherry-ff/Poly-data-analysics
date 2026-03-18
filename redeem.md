# PM 资金提取实现方法

## 结论

如果你只是想实现 `Polymarket` 的“资金提取”，核心不是交易策略，也不是事件流程，而是下面这 4 步：

1. 查询用户持仓，找出 `redeemable=true` 的仓位。
2. 把这些仓位按 `conditionId` 整理成提取目标。
3. 构造 `redeemPositions(...)` 的链上调用数据。
4. 通过 PM 官方 `relayer-v2`，用 `PROXY` 或 `SAFE` 方式提交提取请求。

提取成功后，结算份额会被兑换成 PM 账户里的可用 `USDC`。

---

## 一、目标

这里的“提取资金”，指的是：

**把 PM 上已经结算、并且可赎回的条件份额，换回到账户可用余额。**

不是提现吗，也不是转到外部钱包。

---

## 二、必须具备的输入

要实现 PM 提取，至少需要这些信息：

### 1）用户地址

用于查询持仓：

- `PROXY_FUNDER`
- 或你实际在 PM 上持仓的地址

### 2）签名私钥

用于给 relayer 请求签名：

- 对应 `signer_address`
- 项目里通常来自 `poly_rest.private_key`

### 3）Builder 鉴权

提交到 PM 官方 relayer 时需要：

- `POLY_BUILDER_API_KEY`
- `POLY_BUILDER_SECRET`
- `POLY_BUILDER_PASSPHRASE`

### 4）官方接口地址

- 持仓查询：`https://data-api.polymarket.com/positions`
- 提取提交：`https://relayer-v2.polymarket.com`

### 5）链上目标合约地址

普通市场和 `negative risk` 市场的提取目标不一样，所以要准备：

- `collateral_token`：USDC 地址
- `ctf_exchange`
- `neg_risk_adapter`
- `proxy_factory`
- `relay_hub`
- `safe_factory`
- 对应的 `init_code_hash`

---

## 三、实现步骤

## 第 1 步：查询可提取持仓

请求：

- `GET /positions?user=<address>`

重点看返回里的这些字段：

- `redeemable`
- `conditionId`
- `negativeRisk`
- `outcomeIndex`
- `size`

筛选规则很简单：

- 只保留 `redeemable = true`
- `conditionId` 必须合法

白话解释：

这一步就是先问 PM：

**“我有哪些仓位现在已经可以换回钱了？”**

---

## 第 2 步：整理提取目标

不能拿一行持仓就直接提交，因为同一个事件可能会有多条记录。

通常要按下面维度分组：

- `conditionId`
- `negativeRisk`

然后聚合同一个事件下的：

- `outcomeIndex`
- `size`

最终生成提取目标，例如：

- `condition_id`
- `negative_risk`
- `index_sets`
- `amounts_raw`

### 普通市场

普通二元市场可直接按固定：

- `index_sets = [1, 2]`

这是最稳的做法。

### negative risk 市场

这类市场不能直接套普通参数。

需要按每个 `outcomeIndex` 对应的 `size` 去构造：

- `amounts_raw`

---

## 第 3 步：构造链上调用数据

这是整个实现里最关键的一步。

### A. 普通市场

目标函数：

- `redeemPositions(address,bytes32,bytes32,uint256[])`

参数含义：

1. `collateral_token`
2. `parentCollectionId`，通常是 32 字节全 0
3. `conditionId`
4. `index_sets`

也就是：

- 用哪种抵押资产提
- 提哪个条件市场
- 提哪些 outcome 组合

### B. negative risk 市场

目标函数：

- `redeemPositions(bytes32,uint256[])`

参数含义：

1. `conditionId`
2. `amounts_raw`

也就是直接按各 outcome 的份额数量去提。

### 注意

普通市场与 `negative risk` 市场：

- 调用的合约不同
- 函数签名不同
- 参数结构也不同

所以实现时必须先分流，不能混用。

---

## 第 4 步：选择提交通道

PM 官方 relayer 一般支持两种方式：

- `PROXY`
- `SAFE`

### PROXY

适合常见 PM PROXY 账户。

实现方法：

1. 先构造内部调用：
   - 调用目标是 `ctf_exchange` 或 `neg_risk_adapter`
2. 再把这个内部调用包进：
   - `proxy((uint8,address,uint256,bytes)[])`
3. 向 relayer 请求：
   - `GET /relay-payload?address=<signer>&type=PROXY`
4. 拿到：
   - `relay address`
   - `nonce`
5. 构造签名哈希
6. 用私钥签名
7. `POST /submit`

### SAFE

适合 SAFE 账户。

实现方法：

1. 直接把内部调用作为 SAFE 交易内容
2. 请求：
   - `GET /nonce?address=<signer>&type=SAFE`
3. 检查 SAFE 是否已部署：
   - `GET /deployed?address=<safe_address>`
4. 构造 SAFE 交易哈希
5. 用私钥签名
6. 把签名转成 SAFE 需要的打包格式
7. `POST /submit`

### auto 模式

如果你想自动判断：

- `signature_type = 2` 通常走 `SAFE`
- 否则默认走 `PROXY`

---

## 四、提交给 relayer 的关键点

提交时本质上是给 PM 官方 `relayer-v2` 发一个带签名的请求。

关键接口一般包括：

- `GET /relay-payload`
- `GET /nonce`
- `GET /deployed`
- `POST /submit`

### `POST /submit` 里至少要有

- `type`：`PROXY` 或 `SAFE`
- `from`
- `to`
- `proxyWallet`
- `data`
- `nonce`
- `signature`
- `signatureParams`
- `metadata`

### Builder 鉴权头

提交时不能裸发，需要加 Builder 鉴权头。

通常要带：

- `POLY_BUILDER_API_KEY`
- `POLY_BUILDER_PASSPHRASE`
- `POLY_BUILDER_TIMESTAMP`
- `POLY_BUILDER_SIGNATURE`

这里的 `POLY_BUILDER_SIGNATURE` 一般是：

- 用 `builder_secret` 对 `timestamp + method + path + body` 做 HMAC-SHA256

---

## 五、最小实现骨架

你可以把实现理解成下面这个伪代码：

```text
1. positions = GET /positions?user=<address>
2. rows = filter(redeemable == true)
3. targets = group_by(conditionId, negativeRisk)
4. for target in targets:
5.     if negativeRisk:
6.         build redeemPositions(bytes32,uint256[])
7.     else:
8.         build redeemPositions(address,bytes32,bytes32,uint256[])
9. 
10.    tx_type = PROXY or SAFE
11.    if PROXY:
12.        relay_payload = GET /relay-payload
13.        sign proxy struct hash
14.    else:
15.        nonce = GET /nonce
16.        deployed = GET /deployed
17.        sign safe tx hash
18.
19.    POST /submit
20.    read transactionID / transactionHash
```

---

## 六、实现时最容易踩坑的地方

### 1）`redeemable=false` 时不要提交

没到可提取状态就提交，通常没有意义。

### 2）普通市场和 negative risk 不能混用 ABI

这是最容易写错的地方。

### 3）没有 Builder 凭据，提交一定失败

只靠钱包私钥还不够。

### 4）账户类型不同，提交通道也不同

有的账户要走 `PROXY`，有的要走 `SAFE`。

### 5）“已提交” 不等于 “已到账”

提交成功后，仍然要再查余额，确认资金真的回到了可用余额。

---

## 七、如果你只保留最核心的方法

那就记住这一句：

**PM 提取资金的实现方法，就是：查询 `redeemable` 持仓 → 组装 `redeemPositions(...)` 调用 → 通过 PM 官方 `relayer-v2` 用 `PROXY/SAFE` 签名提交。**