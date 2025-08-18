import { Env } from "./types";
export { MarketFeedDO } from "./MarketFeedDO";

export default {
  async fetch(req: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(req.url);

    // 将所有请求转发给全局唯一的 Durable Object 实例
    const id = env.MARKET_FEED.idFromName("global");
    const stub = env.MARKET_FEED.get(id);

    // 原样转发；DO 内部处理路由与 WebSocket 升级
    return await stub.fetch(req);
  },
  async scheduled(controller: ScheduledController, env: Env, ctx: ExecutionContext) {
    // 定时唤醒全局 DO，触发内部 warm 路由以确保上游连接与 DO 闹钟链路
    const id = env.MARKET_FEED.idFromName("global");
    const stub = env.MARKET_FEED.get(id);
    const req = new Request("https://internal.cron/internal/warm", { method: "POST" });
    try {
      await stub.fetch(req);
    } catch (e) {
      // 忽略错误，等待下次 cron
    }
  },
};
