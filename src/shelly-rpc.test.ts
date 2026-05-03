import { describe, it, expect, vi } from "vitest";
import { createShellyRpcClient } from "./shelly-rpc.js";
import type { Logger } from "./shelly-plugin.js";

const silentLogger: Logger = {
  child: () => silentLogger,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  debug: () => undefined,
};

function makeFetch(responses: Array<Response | Promise<Response> | (() => Response | Promise<Response>)>): typeof fetch {
  let i = 0;
  return ((_url: RequestInfo | URL, _init?: RequestInit) => {
    if (i >= responses.length) throw new Error("fetch called more times than expected");
    const r = responses[i++];
    return Promise.resolve(typeof r === "function" ? r() : r);
  }) as unknown as typeof fetch;
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("shelly-rpc", () => {
  describe("getRecords", () => {
    it("happy path — returns parsed data_blocks", async () => {
      const f = makeFetch([
        jsonResponse({
          data_blocks: [
            { ts: 1000, period: 60, records: 27 },
            { ts: 3000, period: 60, records: 1846 },
          ],
        }),
      ]);
      const c = createShellyRpcClient({ host: "x.local", fetch: f, logger: silentLogger });
      const blocks = await c.getRecords(0);
      expect(blocks).toHaveLength(2);
      expect(blocks[0]).toEqual({ ts: 1000, period: 60, records: 27 });
      expect(blocks[1]).toEqual({ ts: 3000, period: 60, records: 1846 });
    });

    it("returns [] when device has no data", async () => {
      const f = makeFetch([jsonResponse({ data_blocks: [] })]);
      const c = createShellyRpcClient({ host: "x.local", fetch: f, logger: silentLogger });
      expect(await c.getRecords(0)).toEqual([]);
    });
  });

  describe("getData", () => {
    it("single page (no next_record_ts)", async () => {
      const f = makeFetch([
        jsonResponse({
          keys: ["total_act_energy", "total_act_ret_energy", "max_act_power"],
          data: [
            {
              ts: 1000,
              period: 60,
              values: [
                [10, 1, 500],
                [12, 0, 600],
              ],
            },
          ],
        }),
      ]);
      const c = createShellyRpcClient({ host: "x.local", fetch: f, logger: silentLogger });
      const recs = await c.getData(0, 1000, 1120);
      expect(recs).toEqual([
        { ts: 1000, totalActEnergy: 10, totalActRetEnergy: 1 },
        { ts: 1060, totalActEnergy: 12, totalActRetEnergy: 0 },
      ]);
    });

    it("paginated (3 pages) — concatenates chronologically", async () => {
      const f = makeFetch([
        jsonResponse({
          keys: ["total_act_energy", "total_act_ret_energy"],
          data: [{ ts: 1000, period: 60, values: [[1, 0]] }],
          next_record_ts: 1060,
        }),
        jsonResponse({
          keys: ["total_act_energy", "total_act_ret_energy"],
          data: [{ ts: 1060, period: 60, values: [[2, 0]] }],
          next_record_ts: 1120,
        }),
        jsonResponse({
          keys: ["total_act_energy", "total_act_ret_energy"],
          data: [{ ts: 1120, period: 60, values: [[3, 0]] }],
        }),
      ]);
      const c = createShellyRpcClient({ host: "x.local", fetch: f, logger: silentLogger });
      const recs = await c.getData(0, 1000, 1180);
      expect(recs).toHaveLength(3);
      expect(recs.map((r) => r.totalActEnergy)).toEqual([1, 2, 3]);
    });

    it("excludes records at or beyond end_ts", async () => {
      const f = makeFetch([
        jsonResponse({
          keys: ["total_act_energy", "total_act_ret_energy"],
          data: [
            {
              ts: 1000,
              period: 60,
              values: [
                [1, 0],
                [2, 0],
                [3, 0], // ts=1120, == end_ts → must be excluded
              ],
            },
          ],
        }),
      ]);
      const c = createShellyRpcClient({ host: "x.local", fetch: f, logger: silentLogger });
      const recs = await c.getData(0, 1000, 1120);
      expect(recs).toHaveLength(2);
    });

    it("HTTP 500 then success — retries with backoff", async () => {
      const sleep = vi.fn().mockResolvedValue(undefined);
      const f = makeFetch([
        jsonResponse({}, 500),
        jsonResponse({
          keys: ["total_act_energy", "total_act_ret_energy"],
          data: [{ ts: 1000, period: 60, values: [[1, 0]] }],
        }),
      ]);
      const c = createShellyRpcClient({ host: "x.local", fetch: f, sleep, logger: silentLogger });
      const recs = await c.getData(0, 1000, 1060);
      expect(recs).toHaveLength(1);
      expect(sleep).toHaveBeenCalledOnce();
    });

    it("HTTP 401 with no auth — throws immediately, no retry", async () => {
      const sleep = vi.fn().mockResolvedValue(undefined);
      const f = makeFetch([jsonResponse({}, 401)]);
      const c = createShellyRpcClient({ host: "x.local", fetch: f, sleep, logger: silentLogger });
      await expect(c.getData(0, 1000, 1060)).rejects.toThrow(/401/);
      expect(sleep).not.toHaveBeenCalled();
    });

    it("3 retries exhausted — throws", async () => {
      const sleep = vi.fn().mockResolvedValue(undefined);
      const f = makeFetch([
        jsonResponse({}, 500),
        jsonResponse({}, 500),
        jsonResponse({}, 500),
      ]);
      const c = createShellyRpcClient({
        host: "x.local",
        fetch: f,
        sleep,
        maxRetries: 3,
        logger: silentLogger,
      });
      await expect(c.getRecords(0)).rejects.toThrow(/HTTP 500/);
    });

    it("missing required keys in response — throws (schema guard)", async () => {
      const f = makeFetch([
        jsonResponse({
          keys: ["max_act_power"], // missing total_act_energy
          data: [{ ts: 1000, period: 60, values: [[100]] }],
        }),
      ]);
      const c = createShellyRpcClient({ host: "x.local", fetch: f, logger: silentLogger });
      await expect(c.getData(0, 1000, 1060)).rejects.toThrow(/missing/);
    });

    it("end_ts <= ts — returns [] without calling fetch", async () => {
      const f = vi.fn() as unknown as typeof fetch;
      const c = createShellyRpcClient({ host: "x.local", fetch: f, logger: silentLogger });
      expect(await c.getData(0, 2000, 1000)).toEqual([]);
      expect(f).not.toHaveBeenCalled();
    });
  });
});
