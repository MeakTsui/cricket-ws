import {
  Env,
  LatestEntry,
  BucketEntry,
  Bar5s,
  TickerSnapshot,
  AdminSymbolsResponse,
  ClientMsg,
  ServerMsg,
  UpstreamAdapter,
} from "./types";

// 工具常量与函数
const RING_LIMIT = 720; // 1h @ 5s
const BUCKET_MS = 5000;
const TICKER_PUSH_THROTTLE_MS = 1000;
const GC_TTL_MS = 30 * 60 * 1000; // 30 minutes

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function err(code: string, message: string, status = 400): Response {
  return json({ code, error: message }, status);
}

function toBucketTs(ts: number): number {
  return Math.floor(ts / BUCKET_MS) * BUCKET_MS;
}

function normalizeSymbol(sym: string): string {
  // Binance 组合流 URL 需要小写，但我们在内部逻辑上保留大写符号（例如 BTCUSDT）
  return sym.trim();
}

// Binance 适配器：实现去抖的“超集”重连
class BinanceAdapter implements UpstreamAdapter {
  private doRef: MarketFeedDO;
  private env: Env;
  private baseWs: string;
  private ws: WebSocket | null = null;
  private backoff = 1000; // ms
  private maxBackoff = 30000;
  private reconnecting = false;
  private wanted: Set<string> = new Set();
  private debounceTimer: number | null = null;
  private lastStreams: string = ""; // 记录最近一次连接所用的 streams 串
  private connecting: boolean = false; // 是否正在发起连接

  constructor(doRef: MarketFeedDO, env: Env) {
    this.doRef = doRef;
    this.env = env;
    this.baseWs = env.EXCHANGE_WS || "wss://stream.binance.com:9443/stream";
  }

  async ensureSuperset(symbols: Set<string>): Promise<void> {
    // 合并到目标集合，触发去抖重连
    symbols.forEach((s) => this.wanted.add(s));
    this.debounceReconnect();
  }

  close(reason?: string) {
    try {
      this.ws?.close(1000, reason || "reconnect");
    } catch {}
    this.ws = null;
  }

  private debounceReconnect() {
    if (this.debounceTimer) {
      // @ts-ignore - CF uses number for timers
      clearTimeout(this.debounceTimer);
    }
    // 500–1000ms 去抖窗口
    this.debounceTimer = setTimeout(() => this.connectWithWanted(), 700) as unknown as number;
  }

  private connectWithWanted() {
    const wanted = new Set(Array.from(this.wanted).filter(Boolean));
    // 计算需要订阅的“超集”：desiredSymbols ∪ 各房间的客户端订阅
    const superset = this.doRef.computeNeededSymbols(wanted);
    const streams = Array.from(superset)
      .filter((s) => s)
      .map((s) => s.toLowerCase() + "@ticker")
      .join("/");
    const url = `${this.baseWs}?streams=${encodeURIComponent(streams)}`;

    // 若目标 streams 未变化，且当前已有连接或正在连接，则跳过，避免重复连接与重复日志
    if (streams === this.lastStreams && (this.ws !== null || this.connecting)) {
      return;
    }
    this.lastStreams = streams;

    // 建立新连接；收到第一条消息后切换
    const next = new WebSocket(url);
    this.connecting = true;
    let firstMessage = true;

    next.addEventListener("open", () => {
      this.doRef.log(`上游已连接：${url}`);
      this.backoff = 1000; // reset
    });

    next.addEventListener("message", (ev) => {
      // 组合流消息结构：{ stream: "btcusdt@ticker", data: {...} }
      try {
        const parsed = JSON.parse(typeof ev.data === "string" ? ev.data : new TextDecoder().decode(ev.data)) as any;
        if (firstMessage) {
          firstMessage = false;
          const old = this.ws;
          this.ws = next;
          // 切换为主连接
          this.doRef.onUpstreamReady();
          // 延迟关闭旧连接以尽量避免空窗
          setTimeout(() => {
            try { old?.close(1000, "superseded"); } catch {}
          }, 1500);
          this.connecting = false;
        }
        this.doRef.handleUpstreamTicker(parsed);
      } catch (e) {
        this.doRef.log(`上游消息解析失败：${e}`);
      }
    });

    next.addEventListener("close", (ev) => {
      const isCurrent = this.ws === next;
      if (isCurrent) {
        this.doRef.log(`上游连接关闭 code=${ev.code} reason=${ev.reason || ""}`);
        // 指数退避重连（仅对当前活动连接生效）
        this.ws = null;
        this.scheduleReconnect();
      } else {
        // 旧连接关闭，通常是被新连接替换后的正常行为，避免触发重连和噪声日志
        if (ev.reason && ev.reason !== "superseded") {
          this.doRef.log(`旧上游连接关闭 code=${ev.code} reason=${ev.reason}`);
        }
      }
      this.connecting = false;
    });

    next.addEventListener("error", (ev) => {
      const isCurrent = this.ws === next;
      if (isCurrent) {
        this.doRef.log(`上游连接错误：${JSON.stringify(ev)}`);
        this.ws = null;
        this.scheduleReconnect();
      } else {
        // 忽略旧连接错误
      }
      this.connecting = false;
    });
  }

  private scheduleReconnect() {
    if (this.reconnecting) return;
    this.reconnecting = true;
    const delay = this.backoff;
    this.backoff = Math.min(this.backoff * 2, this.maxBackoff);
    setTimeout(() => {
      this.reconnecting = false;
      this.connectWithWanted();
    }, delay);
  }
}

export class MarketFeedDO implements DurableObject {
  private state: DurableObjectState;
  private env: Env;

  // 内存态
  private latest = new Map<string, LatestEntry>();
  private buckets = new Map<string, BucketEntry>();
  private history = new Map<string, Bar5s[]>();
  private desiredSymbols = new Set<string>();
  private gcDeadline = new Map<string, number>();

  // WS 房间与订阅关系
  private rooms = new Map<string, Set<WebSocket>>();
  private clientSubs = new Map<WebSocket, Set<string>>();
  private lastTickerPush = new Map<string, number>();

  // 上游适配器
  private adapter!: BinanceAdapter;
  private upstreamReady = false;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;

    // 定时闹钟：做 GC 与心跳等维护
    this.state.storage.setAlarm(Date.now() + 30_000);
  }

  private get adminToken() {
    return this.env.ADMIN_TOKEN || "";
  }

  log(msg: string) {
    // eslint-disable-next-line no-console
    console.log(`[市场订阅DO] ${msg}`);
  }

  // 启动时恢复 desiredSymbols
  async loadDesiredSymbols() {
    if (this.desiredSymbols.size > 0) return;
    const raw = await this.state.storage.get<string>("desiredSymbols");
    if (raw) {
      try {
        const list: string[] = JSON.parse(raw);
        list.forEach((s) => this.desiredSymbols.add(normalizeSymbol(s)));
      } catch {}
    }
  }

  computeNeededSymbols(extra: Set<string> = new Set()): Set<string> {
    const sup = new Set<string>();
    this.desiredSymbols.forEach((s) => sup.add(s));
    for (const [sym, sockets] of this.rooms) {
      if (sockets.size > 0) sup.add(sym);
    }
    extra.forEach((s) => sup.add(s));
    return sup;
  }

  async ensureUpstream(): Promise<void> {
    if (!this.adapter) {
      this.adapter = new BinanceAdapter(this, this.env);
    }
    await this.loadDesiredSymbols();
    await this.adapter.ensureSuperset(this.computeNeededSymbols());
  }

  onUpstreamReady() {
    this.upstreamReady = true;
  }

  // 上游 ticker 处理：更新最新价、做 5 秒采样与广播
  async handleUpstreamTicker(msg: any) {
    // 数据示例：{ s: "BTCUSDT", c: "price", P: "change%", E: ts }
    const d = msg?.data;
    if (!d || !d.s || !d.c) return;
    const symbol = normalizeSymbol(d.s);
    const price = String(d.c);
    const change24h = d.P !== undefined ? String(d.P) : "0";
    const ts = Number(d.E || Date.now());

    // 最新快照
    this.latest.set(symbol, { price, change24h, ts });

    // 5 秒采样
    const bucketTs = toBucketTs(ts);
    const b = this.buckets.get(symbol);
    if (!b) {
      this.buckets.set(symbol, { currentBucketTs: bucketTs, lastPrice: price });
    } else {
      if (bucketTs === b.currentBucketTs) {
        b.lastPrice = price; // 桶内保持“最后价”
      } else if (bucketTs > b.currentBucketTs) {
        // 桶前移：将上一个桶最终价固化到历史
        await this.pushHistoryPoint(symbol, { ts: b.currentBucketTs + BUCKET_MS, price: b.lastPrice });
        // 开启新桶
        b.currentBucketTs = bucketTs;
        b.lastPrice = price;
      } else {
        // 乱序，忽略
      }
    }

    // 逐 symbol 对 ticker 推送进行 1s 节流
    const last = this.lastTickerPush.get(symbol) || 0;
    if (ts - last >= TICKER_PUSH_THROTTLE_MS) {
      this.lastTickerPush.set(symbol, ts);
      this.broadcast(symbol, {
        type: "ticker",
        symbol,
        price,
        change_percent_24h: change24h,
        ts,
      });
    }

    // 若该 symbol 无客户端订阅且不在 desiredSymbols，安排 GC
    this.maybeScheduleGc(symbol);
  }

  private async pushHistoryPoint(symbol: string, point: Bar5s) {
    let arr = this.history.get(symbol);
    if (!arr) {
      arr = await this.loadHistory(symbol);
    }
    arr.push(point);
    if (arr.length > RING_LIMIT) arr.splice(0, arr.length - RING_LIMIT);
    this.history.set(symbol, arr);
    await this.state.storage.put(`history:${symbol}`, arr);
    // 通知订阅者
    this.broadcast(symbol, { type: "bar5s", symbol, ts: point.ts, price: point.price });
  }

  private async loadHistory(symbol: string): Promise<Bar5s[]> {
    if (this.history.has(symbol)) return this.history.get(symbol)!;
    const stored = await this.state.storage.get<Bar5s[]>(`history:${symbol}`);
    const arr = Array.isArray(stored) ? stored : [];
    // 确保时间升序并按上限裁剪
    arr.sort((a, b) => a.ts - b.ts);
    if (arr.length > RING_LIMIT) arr.splice(0, arr.length - RING_LIMIT);
    this.history.set(symbol, arr);
    return arr;
  }

  private broadcast(symbol: string, msg: ServerMsg) {
    const room = this.rooms.get(symbol);
    if (!room || room.size === 0) return;
    const data = JSON.stringify(msg);
    for (const ws of room) {
      try { ws.send(data); } catch {}
    }
  }

  private maybeScheduleGc(symbol: string) {
    const desired = this.desiredSymbols.has(symbol);
    const room = this.rooms.get(symbol);
    const hasClients = !!room && room.size > 0;
    if (!desired && !hasClients) {
      const deadline = Date.now() + GC_TTL_MS;
      this.gcDeadline.set(symbol, deadline);
      this.state.storage.setAlarm(Math.min(...Array.from(this.gcDeadline.values())));
    }
  }

  private async runGc() {
    const now = Date.now();
    for (const [sym, ddl] of Array.from(this.gcDeadline.entries())) {
      const desired = this.desiredSymbols.has(sym);
      const hasClients = (this.rooms.get(sym)?.size || 0) > 0;
      if (now >= ddl && !desired && !hasClients) {
        this.latest.delete(sym);
        this.buckets.delete(sym);
        this.history.delete(sym);
        await this.state.storage.delete(`history:${sym}`);
        this.gcDeadline.delete(sym);
        this.log(`GC 已清理符号 ${sym}`);
      }
    }
  }

  // Durable Object required handlers
  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;

    // /ws：升级为 WebSocket
    if (path === "/ws") {
      await this.ensureUpstream();
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair) as [WebSocket, WebSocket];
      this.state.acceptWebSocket(server);
      // 初始订阅为空
      this.clientSubs.set(server, new Set());
      return new Response(null, { status: 101, webSocket: client } as any);
    }

    // REST 接口
    if (path === "/api/tickers/latest" && req.method === "GET") {
      await this.ensureUpstream();
      const symbolsParam = url.searchParams.get("symbols") || "";
      const symbols = symbolsParam
        .split(",")
        .map((s) => normalizeSymbol(s))
        .filter((s) => !!s);
      const result: TickerSnapshot[] = [];
      for (const s of symbols) {
        const l = this.latest.get(s);
        if (l) result.push({ symbol: s, price: l.price, change_percent_24h: l.change24h, ts: l.ts });
      }
      return json(result);
    }

    if (path === "/api/tickers/history" && req.method === "GET") {
      await this.ensureUpstream();
      const symbol = normalizeSymbol(url.searchParams.get("symbol") || "");
      if (!symbol) return err("BadRequest", "symbol required", 400);
      const limit = Math.min(Number(url.searchParams.get("limit") || RING_LIMIT), RING_LIMIT);
      const series = await this.loadHistory(symbol);
      const points = series.slice(Math.max(0, series.length - limit));
      return json(points);
    }

    // 管理接口（需要 Bearer Token）
    if (path === "/api/symbols" && req.method === "GET") {
      const ok = await this.checkAdminAuth(req);
      if (!ok) return err("Unauthorized", "invalid token", 401);
      await this.loadDesiredSymbols();
      const res: AdminSymbolsResponse = { symbols: Array.from(this.desiredSymbols) };
      return json(res);
    }

    if (path === "/api/symbols/add" && req.method === "POST") {
      const ok = await this.checkAdminAuth(req);
      if (!ok) return err("Unauthorized", "invalid token", 401);
      const body = (await req.json().catch(() => undefined)) as any;
      const symbols: string[] = Array.isArray(body?.symbols) ? (body.symbols as string[]) : [];
      symbols.forEach((s) => this.desiredSymbols.add(normalizeSymbol(s)));
      await this.persistDesiredSymbols();
      await this.ensureUpstream();
      return json({ ok: true });
    }

    if (path === "/api/symbols/remove" && req.method === "POST") {
      const ok = await this.checkAdminAuth(req);
      if (!ok) return err("Unauthorized", "invalid token", 401);
      const body = (await req.json().catch(() => undefined)) as any;
      const symbols: string[] = Array.isArray(body?.symbols) ? (body.symbols as string[]) : [];
      symbols.forEach((s) => this.desiredSymbols.delete(normalizeSymbol(s)));
      await this.persistDesiredSymbols();
      await this.ensureUpstream();
      return json({ ok: true });
    }

    return err("NotFound", "route not found", 404);
  }

  async webSocketMessage(ws: WebSocket, msg: string | ArrayBuffer) {
    // 客户端消息：sub / unsub / ping
    let data: ClientMsg | undefined;
    try {
      data = JSON.parse(typeof msg === "string" ? msg : new TextDecoder().decode(msg));
    } catch {
      ws.send(JSON.stringify({ type: "error", code: "BadJSON", error: "invalid json" } satisfies ServerMsg));
      return;
    }

    if (!data) return;

    if (data.op === "ping") {
      ws.send(JSON.stringify({ type: "pong" } satisfies ServerMsg));
      return;
    }

    if (data.op === "sub") {
      await this.ensureUpstream();
      const want = new Set((data.symbols || []).map((s) => normalizeSymbol(s)));
      let set = this.clientSubs.get(ws);
      if (!set) {
        set = new Set();
        this.clientSubs.set(ws, set);
      }
      // 加入房间
      for (const s of want) {
        set.add(s);
        let room = this.rooms.get(s);
        if (!room) {
          room = new Set();
          this.rooms.set(s, room);
        }
        room.add(ws);
      }
      // 重新计算需要的上游订阅集合
      await this.adapter.ensureSuperset(this.computeNeededSymbols());
      return;
    }

    if (data.op === "unsub") {
      const curr = this.clientSubs.get(ws) || new Set<string>();
      for (const s of (data.symbols || []).map((x) => normalizeSymbol(x))) {
        curr.delete(s);
        const room = this.rooms.get(s);
        room?.delete(ws);
        if (room && room.size === 0) this.rooms.delete(s);
      }
      this.clientSubs.set(ws, curr);
      await this.adapter.ensureSuperset(this.computeNeededSymbols());
      return;
    }
  }

  async webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
    // 清理客户端状态
    const subs = this.clientSubs.get(ws) || new Set<string>();
    for (const s of subs) {
      const room = this.rooms.get(s);
      room?.delete(ws);
      if (room && room.size === 0) this.rooms.delete(s);
    }
    this.clientSubs.delete(ws);
  }

  async webSocketError(ws: WebSocket, error: any) {
    try { ws.close(1011, "error"); } catch {}
  }

  async alarm() {
    // 维护任务：GC 与 30s 心跳
    await this.runGc();
    // 每 30s 给客户端发一条 info（示例）
    for (const room of this.rooms.values()) {
      for (const ws of room) {
        try { ws.send(JSON.stringify({ type: "info", msg: "ping" } satisfies ServerMsg)); } catch {}
      }
    }
    // 重新设置闹钟
    this.state.storage.setAlarm(Date.now() + 30_000);
  }

  private async checkAdminAuth(req: Request): Promise<boolean> {
    const auth = req.headers.get("authorization") || req.headers.get("Authorization");
    if (!auth || !auth.startsWith("Bearer ")) return false;
    const token = auth.slice(7);
    return token === this.adminToken;
  }

  private async persistDesiredSymbols() {
    await this.state.storage.put("desiredSymbols", JSON.stringify(Array.from(this.desiredSymbols)));
  }
}
