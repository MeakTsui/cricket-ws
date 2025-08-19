/// <reference types="@cloudflare/workers-types" />
export type MilliTs = number; // UTC 毫秒时间戳

export interface TickerSnapshot {
  symbol: string;
  price: string; // 使用字符串以避免浮点精度漂移
  change_percent_24h: string; // 交易所原样提供的字符串
  ts: MilliTs;
}

export interface Bar5s {
  ts: MilliTs;
  price: string;
  change_percent_24h: string;
}

export interface HistorySeries {
  symbol: string;
  points: Bar5s[]; // 按时间戳升序
}

export interface Env {
  MARKET_FEED: DurableObjectNamespace;
  ADMIN_TOKEN?: string;
  EXCHANGE?: string; // 默认 "binance"
  EXCHANGE_WS?: string; // Binance 组合流的 wss 基地址
  DESIRED_SYMBOLS?: string; // 逗号分隔的默认订阅集合，如 "BTCUSDT,ETHUSDT"
}

export interface AdminSymbolsResponse {
  symbols: string[];
}

export type ClientMsg =
  | { op: "sub"; symbols: string[]; fields?: ("price" | "5s")[] }
  | { op: "unsub"; symbols: string[] }
  | { op: "ping" };

export type ServerMsg =
  | { type: "pong" }
  | { type: "ticker"; symbol: string; price: string; change_percent_24h: string; ts: MilliTs }
  | { type: "bar5s"; symbol: string; ts: MilliTs; price: string; change_percent_24h: string }
  | { type: "info"; msg: string }
  | { type: "error"; code: string; error: string };

// DO 内部使用的数据结构
export interface LatestEntry {
  price: string;
  change24h: string;
  ts: MilliTs;
}

export interface BucketEntry {
  currentBucketTs: MilliTs; // 当前桶起始：floor(ts/5000)*5000
  lastPrice: string; // 当前桶内最新看到的价格
  lastChange24h: string; // 当前桶内最新看到的 24h 涨跌幅
}

export interface UpstreamAdapter {
  ensureSuperset(symbols: Set<string>): Promise<void>;
  close(reason?: string): void;
}
