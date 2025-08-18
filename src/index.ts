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
};
