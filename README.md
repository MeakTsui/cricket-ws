# 加密交易所实时 Ticker 同步（Cloudflare Workers + Durable Objects）

最小可运行项目：基于 Cloudflare Workers + Durable Objects 实现 Binance Ticker 实时同步、5 秒历史采样（近 1 小时）、动态币对热更新、REST 与 WebSocket 推送。

- 后端：Cloudflare Workers（TypeScript）+ Durable Objects。
- 数据：DO 内存 + state.storage（无外部 DB）。
- 前端：`examples/frontend.html` 最小演示。

## 功能概览
- 实时：与上游（Binance 组合流）建立 WebSocket，维护最新价和 5 秒桶采样。
- 历史：ring buffer 最多 720 条（≈ 1 小时）。
- 热更新：管理接口更新 desiredSymbols，去抖重连上游，不中断服务。
- 推送：WS 支持订阅/退订，1s 节流 ticker、5s 桶闭合推送 bar5s。
- 冷启动：lazy 恢复指定 symbol 的历史；desiredSymbols 持久化。

## 目录结构
- `wrangler.toml`：Workers/DO 配置与迁移
- `package.json`、`tsconfig.json`
- `src/index.ts`：Worker 入口，所有请求委托给 `MARKET_FEED` 单实例（`idFromName("global")`）
- `src/MarketFeedDO.ts`：Durable Object 实现
- `src/types.ts`：公共类型
- `examples/frontend.html`：前端示例
- `README.md`

## 本地开发
1) 安装依赖（需要 Node 18+，已全局或本地安装 Wrangler 均可）

```bash
npm i
```

2) 启动本地开发

```bash
npm run dev
```

默认使用配置：
- `EXCHANGE = binance`
- `EXCHANGE_WS = wss://stream.binance.com:9443/stream`
- `ADMIN_TOKEN = changeme-dev`

若首次启动，请在另一个终端添加所需币对：

```bash
curl -H 'Authorization: Bearer changeme-dev' \
  'http://127.0.0.1:8787/api/symbols'

curl -X POST -H 'Authorization: Bearer changeme-dev' -H 'Content-Type: application/json' \
  -d '{"symbols":["BTCUSDT","ETHUSDT"]}' \
  'http://127.0.0.1:8787/api/symbols/add'
```

然后在浏览器打开 `examples/frontend.html`，或直接通过 curl/WS 客户端验证。

## 部署
- Cloudflare 账户下：

```bash
npm run deploy
```

确保在 Dashboard 或 `wrangler.toml` 中设置环境变量：
- `ADMIN_TOKEN`（强制鉴权用）
- 可选：`EXCHANGE`、`EXCHANGE_WS`

## API 文档

- GET `/api/tickers/latest?symbols=BTCUSDT,ETHUSDT`
  - 响应：`[ { symbol, price, change_percent_24h, ts }, ... ]`

- GET `/api/tickers/history?symbol=BTCUSDT&limit=720`
  - 响应：`[ { ts, price }, ... ]`（按时间升序，最多 720 条）

- 管理接口（需 header：`Authorization: Bearer <ADMIN_TOKEN>`）
  - GET `/api/symbols` → `{ symbols: [...] }`
  - POST `/api/symbols/add` Body: `{ "symbols": ["BTCUSDT", ...] }`
  - POST `/api/symbols/remove` Body: `{ "symbols": ["ETHUSDT", ...] }`

- WebSocket `/ws`
  - 客户端消息
    - 订阅：`{ "op":"sub", "symbols":["BTCUSDT"], "fields":["price","5s"] }`
    - 退订：`{ "op":"unsub", "symbols":["BTCUSDT"] }`
    - 心跳：`{ "op":"ping" }` → `{ "type":"pong" }`
  - 服务端推送
    - Ticker（1s 节流）：`{ "type":"ticker", "symbol":"BTCUSDT", "price":"...", "change_percent_24h":"...", "ts": 1723706869000 }`
    - 5s 点：`{ "type":"bar5s", "symbol":"BTCUSDT", "ts": 1723706870000, "price":"..." }`

## 设计要点（实现细节）
- DO 内存结构（见 `src/types.ts` 与 `src/MarketFeedDO.ts`）：
  - `latest: Map<string, { price, change24h, ts }>`
  - `buckets: Map<string, { currentBucketTs, lastPrice }>`
  - `history: Map<string, Array<{ ts, price }>>`（ring buffer，最多 720）
  - `desiredSymbols: Set<string>`（持久化）
  - `clientSubs, rooms, lastTickerPush`
- 存储：
  - `state.storage.put('history:<symbol>', points[])`
  - `state.storage.put('desiredSymbols', JSON.stringify([...]))`
- 5 秒采样：严格使用桶内“最后价”。桶前移时将 [prevBucketTs+5000, lastPrice] 写入历史。
- 上游适配：`BinanceAdapter.ensureSuperset()` 去抖 700ms 后优雅重连，新连接首条消息到达后切换并延迟 1.5s 关闭旧连接。
- 断线重连：指数退避至 30s。
- GC：无客户端订阅且不在 `desiredSymbols` 的 symbol，设置 30 分钟 TTL，超时清理内存与 `history:<symbol>` 存储。
- 首屏聚合：通过 REST `latest` + `history` 提供。

## 运行示例

- 拉取最新价：
```bash
curl 'http://127.0.0.1:8787/api/tickers/latest?symbols=BTCUSDT,ETHUSDT'
```

- 拉取历史：
```bash
curl 'http://127.0.0.1:8787/api/tickers/history?symbol=BTCUSDT&limit=120'
```

- WebSocket 订阅（浏览器控制台示例）：
```js
const ws = new WebSocket('ws://127.0.0.1:8787/ws');
ws.onmessage = (e) => console.log('msg', e.data);
ws.onopen = () => ws.send(JSON.stringify({ op: 'sub', symbols: ['BTCUSDT'], fields: ['price','5s'] }));
```

## 兼容性与注意事项
- 需要 `compatibility_date: 2024-11-21`；已在 `wrangler.toml` 设置 `durable_object_alarms`。
- Cloudflare 出站 WebSocket 到 Binance：默认允许。
- 组合流 URL：`wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/ethusdt@ticker`。
- 如未来 symbol 数量增加导致 URL 过长，可按 symbol hash 分片到多个 DO，并通过 Router Worker 聚合。本项目先采用单实例 `idFromName('global')`。

## 常见问题（FAQ）
- 看不到数据？
  - 确认已通过管理接口添加了目标 symbol。
  - 观察 Wrangler 控制台日志，确认与上游建连成功（会输出 Upstream open）。
- 历史没有满 1 小时？
  - 仅在 5 秒桶闭合时产生一条历史记录，需等待实际时间积累（或在测试中持续跑一段时间）。
- 精度问题？
  - 用字符串存储价格，避免浮点精度误差。前端自行格式化显示。
