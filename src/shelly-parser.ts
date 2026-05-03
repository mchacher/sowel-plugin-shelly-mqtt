/**
 * Shelly MQTT topic & payload parsing.
 *
 * Pure functions — no I/O, easy to unit-test. The plugin engine wires
 * these into the MQTT connector and dispatches the parsed values to
 * Sowel's deviceManager.
 *
 * Pro 3EM in `monophase` profile (a.k.a. EM1 mode) publishes:
 *   <prefix>/<shelly-id>/online                     true|false (LWT)
 *   <prefix>/<shelly-id>/status/em1:N               { id, voltage, current, act_power, aprt_power, pf, freq, calibration }
 *   <prefix>/<shelly-id>/status/em1data:N           { id, total_act_energy, total_act_ret_energy }
 *
 * The legacy 3-phase profile publishes em:0/em:1/em:2 instead and is
 * intentionally ignored in V1 — the plugin will only react to em1:N
 * topics. Spec 086 (iteration 2) tightens role bindings; this iteration
 * just exposes data.
 */

export type ShellyTopicKind = "online" | "em1" | "em1data" | "events_rpc";

export interface ShellyTopicInfo {
  /** Shelly device id, taken as the segment immediately before /online or /status/... */
  shellyId: string;
  /** Channel index for em1 / em1data topics (0..2). null for the online LWT. */
  channel: number | null;
  kind: ShellyTopicKind;
}

/**
 * Recognise the topic shape and pull out the Shelly id + channel.
 * Returns `null` for any topic that doesn't match a supported pattern.
 *
 * Supported patterns (any prefix length, last 3 segments only matter):
 *   .../<shellyId>/online
 *   .../<shellyId>/status/em1:N
 *   .../<shellyId>/status/em1data:N
 */
export function extractDeviceTopic(topic: string): ShellyTopicInfo | null {
  const parts = topic.split("/").filter((s) => s.length > 0);
  if (parts.length < 2) return null;

  const last = parts[parts.length - 1];
  const beforeLast = parts[parts.length - 2];

  // .../shellyId/online
  if (last === "online") {
    return { shellyId: beforeLast, channel: null, kind: "online" };
  }

  // .../shellyId/events/rpc — used to learn the device's mDNS host id
  if (last === "rpc" && beforeLast === "events" && parts.length >= 3) {
    return { shellyId: parts[parts.length - 3], channel: null, kind: "events_rpc" };
  }

  // .../shellyId/status/<component>
  if (parts.length >= 3 && beforeLast === "status") {
    const shellyId = parts[parts.length - 3];
    const m = /^em1(data)?:(\d+)$/.exec(last);
    if (!m) return null;
    return {
      shellyId,
      channel: Number(m[2]),
      kind: m[1] ? "em1data" : "em1",
    };
  }

  return null;
}

/**
 * Extract the `src` (= mDNS host id) from a Shelly events/rpc payload.
 * Returns `null` if the JSON is invalid or `src` is missing.
 */
export function parseEventsRpcSrc(buf: Buffer): string | null {
  try {
    const obj = JSON.parse(buf.toString("utf8")) as { src?: unknown };
    return typeof obj.src === "string" && obj.src.length > 0 ? obj.src : null;
  } catch {
    return null;
  }
}

interface Em1StatusPayload {
  voltage?: number;
  current?: number;
  act_power?: number;
  aprt_power?: number;
  pf?: number;
  freq?: number;
}

interface Em1DataStatusPayload {
  total_act_energy?: number;
  total_act_ret_energy?: number;
}

/**
 * Parse a JSON buffer; throws on invalid JSON. Caller wraps in try/catch.
 */
export function parseJson(buf: Buffer): unknown {
  return JSON.parse(buf.toString("utf8"));
}

/**
 * Convert an em1:N status JSON object into the Sowel data payload.
 * Numeric coercion is strict — anything non-numeric becomes `null` so the
 * deviceManager keeps the previous value rather than poisoning history.
 */
export function parseEm1Status(raw: unknown): {
  power: number | null;
  voltage: number | null;
  current: number | null;
  pf: number | null;
} {
  const o = (typeof raw === "object" && raw !== null ? raw : {}) as Em1StatusPayload;
  return {
    power: numericOrNull(o.act_power),
    voltage: numericOrNull(o.voltage),
    current: numericOrNull(o.current),
    pf: numericOrNull(o.pf),
  };
}

/**
 * Convert an em1data:N status JSON object into the cumulative-energy payload.
 * Both counters are forwarded as separate aliases so the energy aggregator
 * (iteration 2) can distinguish import from export per channel.
 */
export function parseEm1DataStatus(raw: unknown): {
  energy_forward: number | null;
  energy_reverse: number | null;
} {
  const o = (typeof raw === "object" && raw !== null ? raw : {}) as Em1DataStatusPayload;
  return {
    energy_forward: numericOrNull(o.total_act_energy),
    energy_reverse: numericOrNull(o.total_act_ret_energy),
  };
}

/**
 * Parse the LWT payload. Shelly publishes `true`/`false` (no quotes) on the
 * `<id>/online` topic. We accept the JSON-decoded value as well as raw
 * "true"/"false" strings for robustness.
 */
export function parseOnlinePayload(buf: Buffer): boolean | null {
  const s = buf.toString("utf8").trim().toLowerCase();
  if (s === "true") return true;
  if (s === "false") return false;
  return null;
}

function numericOrNull(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  return null;
}
