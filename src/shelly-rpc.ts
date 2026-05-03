/**
 * Shelly Pro 3EM EM1Data RPC client.
 *
 * Thin async client over the device's HTTP RPC endpoint. Used by the
 * backfill manager to pull historical minute records out of the device's
 * own ~60-day flash buffer when Sowel's live MQTT stream had a gap.
 *
 * Pure I/O — no caching, no scheduling, no event-bus coupling. The
 * caller decides when to retry overall, when to stop, what to do with
 * the records.
 *
 * Subset of `EM1Data.*` we exercise:
 *   - GetRecords  → the device's internal data_blocks (contiguous record
 *                   windows separated by power-cycles).
 *   - GetData     → minute records inside a [ts, end_ts] window, with
 *                   `next_record_ts` pagination (~27 records per call).
 */
import type { Logger } from "./shelly-plugin.js";

export interface DataBlock {
  /** Epoch seconds — start of the contiguous block. */
  ts: number;
  /** Sample period in seconds; always 60 on the Pro 3EM. */
  period: number;
  /** Number of records held in this block. */
  records: number;
}

export interface MinuteRecord {
  /** Epoch seconds — start of the minute. */
  ts: number;
  /** Forward energy (Wh) over this minute (`total_act_energy`). */
  totalActEnergy: number;
  /** Reverse energy (Wh) over this minute (`total_act_ret_energy`). */
  totalActRetEnergy: number;
}

export interface ShellyRpcClientOptions {
  /** Host or IP — e.g. "shellypro3em-2cbcbbb2cf48.local" or "192.168.0.69". */
  host: string;
  /** Optional auth (only used if the device replies 401). */
  auth?: { user: string; password: string };
  /** Per-call timeout. Default 10s. */
  timeoutMs?: number;
  /** Max retry attempts on transient failure. Default 3. */
  maxRetries?: number;
  /** Injectable fetch for tests. Defaults to global fetch. */
  fetch?: typeof fetch;
  /** Optional sleep override for tests. */
  sleep?: (ms: number) => Promise<void>;
  logger: Logger;
}

export interface ShellyRpcClient {
  getRecords(channelId: number): Promise<DataBlock[]>;
  getData(channelId: number, ts: number, endTs: number): Promise<MinuteRecord[]>;
}

const DEFAULT_TIMEOUT_MS = 10_000;
const DEFAULT_MAX_RETRIES = 3;
const RETRY_BACKOFF_MS = [250, 750, 2000];

interface GetRecordsResponse {
  data_blocks?: Array<{ ts: number; period: number; records: number }>;
}

interface GetDataResponse {
  keys?: string[];
  data?: Array<{ ts: number; period: number; values: number[][] }>;
  next_record_ts?: number;
}

export function createShellyRpcClient(opts: ShellyRpcClientOptions): ShellyRpcClient {
  const fetchImpl = opts.fetch ?? fetch;
  const sleep = opts.sleep ?? ((ms: number) => new Promise((r) => setTimeout(r, ms)));
  const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const maxRetries = opts.maxRetries ?? DEFAULT_MAX_RETRIES;
  const logger = opts.logger.child({ module: "shelly-rpc", host: opts.host });

  /**
   * Perform one RPC HTTP call with timeout + retry. Throws on:
   *  - all retries exhausted
   *  - HTTP 401 with no auth configured (caller can't recover)
   */
  async function callRpc<T>(method: string, query: Record<string, string>): Promise<T> {
    const params = new URLSearchParams(query).toString();
    const url = `http://${opts.host}/rpc/${method}${params ? `?${params}` : ""}`;
    let lastErr: unknown = null;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const headers: Record<string, string> = {};
        if (opts.auth) {
          const tok = Buffer.from(`${opts.auth.user}:${opts.auth.password}`).toString("base64");
          headers["Authorization"] = `Basic ${tok}`;
        }
        const res = await fetchImpl(url, { signal: controller.signal, headers });
        if (res.status === 401) {
          if (!opts.auth) {
            throw new Error(`${method}: 401 unauthorized (auth not configured)`);
          }
          throw new Error(`${method}: 401 unauthorized (credentials rejected)`);
        }
        if (!res.ok) {
          throw new Error(`${method}: HTTP ${res.status}`);
        }
        return (await res.json()) as T;
      } catch (err) {
        lastErr = err;
        const msg = err instanceof Error ? err.message : String(err);
        // Auth errors are non-retriable
        if (msg.includes("401")) throw err;
        const backoff = RETRY_BACKOFF_MS[attempt] ?? RETRY_BACKOFF_MS[RETRY_BACKOFF_MS.length - 1];
        if (attempt + 1 < maxRetries) {
          logger.debug(
            { method, attempt: attempt + 1, maxRetries, backoffMs: backoff, err: msg },
            "RPC call failed, retrying",
          );
          await sleep(backoff);
        }
      } finally {
        clearTimeout(timer);
      }
    }
    throw lastErr instanceof Error ? lastErr : new Error(`${method}: failed after ${maxRetries} attempts`);
  }

  async function getRecords(channelId: number): Promise<DataBlock[]> {
    const r = await callRpc<GetRecordsResponse>("EM1Data.GetRecords", { id: String(channelId) });
    return (r.data_blocks ?? []).map((b) => ({ ts: b.ts, period: b.period, records: b.records }));
  }

  async function getData(channelId: number, ts: number, endTs: number): Promise<MinuteRecord[]> {
    if (endTs <= ts) return [];
    const out: MinuteRecord[] = [];
    let cursor = ts;

    while (cursor < endTs) {
      const r = await callRpc<GetDataResponse>("EM1Data.GetData", {
        id: String(channelId),
        ts: String(cursor),
        end_ts: String(endTs),
      });
      const keys = r.keys ?? [];
      const fwdIdx = keys.indexOf("total_act_energy");
      const revIdx = keys.indexOf("total_act_ret_energy");
      if (fwdIdx < 0 || revIdx < 0) {
        // Schema we don't recognise — abort to avoid silent corruption.
        throw new Error("EM1Data.GetData: missing total_act_energy / total_act_ret_energy keys");
      }

      for (const block of r.data ?? []) {
        const baseTs = block.ts;
        const period = block.period;
        block.values.forEach((row, i) => {
          const recTs = baseTs + i * period;
          if (recTs >= endTs) return;
          out.push({
            ts: recTs,
            totalActEnergy: row[fwdIdx],
            totalActRetEnergy: row[revIdx],
          });
        });
      }

      const next = r.next_record_ts;
      if (typeof next !== "number" || next <= cursor) break;
      cursor = next;
    }

    return out;
  }

  return { getRecords, getData };
}
