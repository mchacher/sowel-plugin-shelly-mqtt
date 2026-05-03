import { describe, it, expect, vi, beforeEach } from "vitest";
import { BackfillManager } from "./backfill-manager.js";
import type { DeviceManager, Logger, ShellyChannelGroup } from "./shelly-plugin.js";
import type { ShellyRpcClient, MinuteRecord, DataBlock } from "./shelly-rpc.js";

const silentLogger: Logger = {
  child: () => silentLogger,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  debug: () => undefined,
};

const INTEG = "shelly_mqtt";
const NOW_S = 1_777_800_000; // arbitrary fixed clock for tests

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(NOW_S * 1000);
});

interface Captured {
  sid: string;
  payload: Record<string, unknown>;
  ts: number;
}

function makeDM(opts: {
  lastUpdatedSec?: number | null;          // applied to all channels
  perChannelLastUpdatedSec?: Record<string, number | null>;
  baselines?: Record<string, { fwd?: number; rev?: number }>;
}): { dm: DeviceManager; captured: Captured[] } {
  const captured: Captured[] = [];
  const dm: DeviceManager = {
    upsertFromDiscovery: vi.fn(),
    updateDeviceData: vi.fn((_i, sid, payload, ts) => {
      if (typeof ts === "number") captured.push({ sid, payload, ts });
    }),
    updateDeviceStatus: vi.fn(),
    getDeviceDataValue: (_i, sid, key) => {
      const b = opts.baselines?.[sid];
      if (!b) return null;
      if (key === "energy_forward") return b.fwd ?? null;
      if (key === "energy_reverse") return b.rev ?? null;
      return null;
    },
    getDeviceDataLastUpdated: (_i, sid, _k) => {
      const v = opts.perChannelLastUpdatedSec?.[sid] ?? opts.lastUpdatedSec;
      if (v === null || v === undefined) return null;
      return new Date(v * 1000).toISOString();
    },
  };
  return { dm, captured };
}

function makeRpc(blocksByChannel: Record<number, DataBlock[]>, recordsByChannel: Record<number, MinuteRecord[]>): ShellyRpcClient {
  return {
    getRecords: async (id) => blocksByChannel[id] ?? [],
    getData: async (id, ts, endTs) => {
      return (recordsByChannel[id] ?? []).filter((r) => r.ts >= ts && r.ts < endTs);
    },
  };
}

const ch = (channelId: number, mac = "shellypro3em-aa"): ShellyChannelGroup => ({
  sourceDeviceId: `shelly-pro3em_00-em${channelId}`,
  shellyId: "shelly-pro3em_00",
  deviceMacId: mac,
  channelId,
});

describe("BackfillManager", () => {
  it("backfill_enabled=false → start() is no-op, no RPC calls", () => {
    const { dm } = makeDM({});
    const rpc = makeRpc({}, {});
    const rpcFor = vi.fn(() => rpc);
    const channelsProvider = vi.fn(() => [ch(0)]);
    const setLastCumul = vi.fn();

    const bm = new BackfillManager({
      enabled: false,
      scanHours: 24,
      gapThresholdSec: 300,
      integrationId: INTEG,
      deviceManager: dm,
      channelsProvider,
      rpcFor,
      setLastCumul,
      logger: silentLogger,
    });
    bm.start();
    expect(channelsProvider).not.toHaveBeenCalled();
    expect(rpcFor).not.toHaveBeenCalled();
  });

  it("no channels yet → no-op, RPC never called", async () => {
    const { dm } = makeDM({});
    const rpcFor = vi.fn();
    const bm = new BackfillManager({
      enabled: true,
      scanHours: 24,
      gapThresholdSec: 300,
      integrationId: INTEG,
      deviceManager: dm,
      channelsProvider: () => [],
      rpcFor,
      setLastCumul: () => {},
      logger: silentLogger,
    });
    await bm.runOnce("manual");
    expect(rpcFor).not.toHaveBeenCalled();
  });

  it("lastUpdated < gap threshold → no-op", async () => {
    const { dm, captured } = makeDM({
      lastUpdatedSec: NOW_S - 60, // 1 minute ago, below 5-min threshold
      baselines: { "shelly-pro3em_00-em0": { fwd: 100, rev: 0 } },
    });
    const rpcFor = vi.fn();
    const bm = new BackfillManager({
      enabled: true,
      scanHours: 24,
      gapThresholdSec: 300,
      integrationId: INTEG,
      deviceManager: dm,
      channelsProvider: () => [ch(0)],
      rpcFor,
      setLastCumul: () => {},
      logger: silentLogger,
    });
    await bm.runOnce("manual");
    expect(rpcFor).not.toHaveBeenCalled();
    expect(captured).toHaveLength(0);
  });

  it("lastUpdated 30 min ago, archive holds the full window → 30 minutes emitted on 1 channel", async () => {
    const lastSec = NOW_S - 30 * 60; // 30 min ago
    const recs: MinuteRecord[] = [];
    for (let i = 0; i < 30; i++) {
      recs.push({ ts: lastSec + i * 60, totalActEnergy: 5, totalActRetEnergy: 0 });
    }
    const { dm, captured } = makeDM({
      lastUpdatedSec: lastSec,
      baselines: { "shelly-pro3em_00-em0": { fwd: 1000, rev: 50 } },
    });
    const rpc = makeRpc(
      { 0: [{ ts: lastSec, period: 60, records: 30 }] },
      { 0: recs },
    );
    const setLastCumul = vi.fn();

    const bm = new BackfillManager({
      enabled: true,
      scanHours: 24,
      gapThresholdSec: 300,
      integrationId: INTEG,
      deviceManager: dm,
      channelsProvider: () => [ch(0)],
      rpcFor: () => rpc,
      setLastCumul,
      logger: silentLogger,
    });
    await bm.runOnce("manual");
    expect(captured).toHaveLength(30);
    // First emit: cumul = baseline + first delta
    expect(captured[0].payload).toEqual({
      energy_forward: 1005,
      energy_reverse: 50,
      energy: 5,
    });
    expect(captured[0].ts).toBe(lastSec);
    // Last emit: cumul accumulates 30 × 5 Wh
    expect(captured[29].payload).toEqual({
      energy_forward: 1150,
      energy_reverse: 50,
      energy: 5,
    });
    // setLastCumul called once per channel with the final cumul
    expect(setLastCumul).toHaveBeenCalledOnce();
    expect(setLastCumul).toHaveBeenCalledWith("shelly-pro3em_00-em0", { fwd: 1150, rev: 50 });
  });

  it("3 channels emitted minute-interleaved, channel order 0→1→2", async () => {
    const lastSec = NOW_S - 10 * 60;
    const mkRecs = (n: number, fwdPerMin: number): MinuteRecord[] =>
      Array.from({ length: n }, (_, i) => ({
        ts: lastSec + i * 60,
        totalActEnergy: fwdPerMin,
        totalActRetEnergy: 0,
      }));
    const { dm, captured } = makeDM({
      lastUpdatedSec: lastSec,
      baselines: {
        "shelly-pro3em_00-em0": { fwd: 0, rev: 0 },
        "shelly-pro3em_00-em1": { fwd: 0, rev: 0 },
        "shelly-pro3em_00-em2": { fwd: 0, rev: 0 },
      },
    });
    const rpc = makeRpc(
      {
        0: [{ ts: lastSec, period: 60, records: 10 }],
        1: [{ ts: lastSec, period: 60, records: 10 }],
        2: [{ ts: lastSec, period: 60, records: 10 }],
      },
      { 0: mkRecs(10, 1), 1: mkRecs(10, 2), 2: mkRecs(10, 3) },
    );
    const bm = new BackfillManager({
      enabled: true,
      scanHours: 24,
      gapThresholdSec: 300,
      integrationId: INTEG,
      deviceManager: dm,
      channelsProvider: () => [ch(0), ch(1), ch(2)],
      rpcFor: () => rpc,
      setLastCumul: () => {},
      logger: silentLogger,
    });
    await bm.runOnce("manual");

    expect(captured).toHaveLength(30); // 10 min × 3 channels
    // First three emits: same ts, sids in 0→1→2 order
    expect(captured[0].sid).toBe("shelly-pro3em_00-em0");
    expect(captured[1].sid).toBe("shelly-pro3em_00-em1");
    expect(captured[2].sid).toBe("shelly-pro3em_00-em2");
    expect(captured[0].ts).toBe(captured[1].ts);
    expect(captured[1].ts).toBe(captured[2].ts);
    // Then ts advances and we restart channel order
    expect(captured[3].sid).toBe("shelly-pro3em_00-em0");
    expect(captured[3].ts).toBe(captured[0].ts + 60);
  });

  it("scanHours window clamps an arbitrarily-old lastUpdated", async () => {
    const longAgo = NOW_S - 30 * 86400; // 30 days ago
    const last24hStart = NOW_S - 24 * 3600;
    // Archive holds last 24h only
    const recs = Array.from({ length: 60 }, (_, i) => ({
      ts: last24hStart + i * 60,
      totalActEnergy: 1,
      totalActRetEnergy: 0,
    }));
    const { dm, captured } = makeDM({
      lastUpdatedSec: longAgo,
      baselines: { "shelly-pro3em_00-em0": { fwd: 0, rev: 0 } },
    });
    const getRecords = vi.fn(async () => [{ ts: last24hStart, period: 60, records: 60 }]);
    const getData = vi.fn(async (_id: number, ts: number, end: number) =>
      recs.filter((r) => r.ts >= ts && r.ts < end),
    );
    const rpc: ShellyRpcClient = { getRecords, getData };
    const bm = new BackfillManager({
      enabled: true,
      scanHours: 24,
      gapThresholdSec: 300,
      integrationId: INTEG,
      deviceManager: dm,
      channelsProvider: () => [ch(0)],
      rpcFor: () => rpc,
      setLastCumul: () => {},
      logger: silentLogger,
    });
    await bm.runOnce("manual");
    // The 24h window is clamped: getRecords is called once, getData is called
    // with the intersection of [block_start, block_end] and [windowStart=last24hStart, NOW_S].
    // The block only spans 1h (60 records × 60 s) so the actual fetch range is
    // [last24hStart, last24hStart + 3600]. The window clamp guarantees we never
    // fetch anything older than last24hStart.
    expect(getData).toHaveBeenCalledOnce();
    const call = getData.mock.calls[0];
    expect(call[0]).toBe(0);
    expect(call[1]).toBeGreaterThanOrEqual(last24hStart);
    expect(call[2]).toBeLessThanOrEqual(NOW_S);
    expect(captured).toHaveLength(60);
  });

  it("archive empty for the window → no writes, warn log only", async () => {
    const lastSec = NOW_S - 30 * 60;
    const { dm, captured } = makeDM({
      lastUpdatedSec: lastSec,
      baselines: { "shelly-pro3em_00-em0": { fwd: 0, rev: 0 } },
    });
    const rpc: ShellyRpcClient = {
      getRecords: async () => [],
      getData: async () => [],
    };
    const bm = new BackfillManager({
      enabled: true,
      scanHours: 24,
      gapThresholdSec: 300,
      integrationId: INTEG,
      deviceManager: dm,
      channelsProvider: () => [ch(0)],
      rpcFor: () => rpc,
      setLastCumul: () => {},
      logger: silentLogger,
    });
    await bm.runOnce("manual");
    expect(captured).toHaveLength(0);
  });

  it("device MAC id not yet captured → skip, no RPC call", async () => {
    const { dm } = makeDM({
      lastUpdatedSec: NOW_S - 30 * 60,
      baselines: { "shelly-pro3em_00-em0": { fwd: 0, rev: 0 } },
    });
    const rpcFor = vi.fn();
    const bm = new BackfillManager({
      enabled: true,
      scanHours: 24,
      gapThresholdSec: 300,
      integrationId: INTEG,
      deviceManager: dm,
      channelsProvider: () => [{ ...ch(0), deviceMacId: null }],
      rpcFor,
      setLastCumul: () => {},
      logger: silentLogger,
    });
    await bm.runOnce("manual");
    expect(rpcFor).not.toHaveBeenCalled();
  });

  it("RPC throws on first device → second device still runs", async () => {
    const lastSec = NOW_S - 30 * 60;
    const failingRpc: ShellyRpcClient = {
      getRecords: async () => {
        throw new Error("boom");
      },
      getData: async () => [],
    };
    const recs = [{ ts: lastSec, totalActEnergy: 5, totalActRetEnergy: 0 }];
    const okRpc: ShellyRpcClient = {
      getRecords: async () => [{ ts: lastSec, period: 60, records: 1 }],
      getData: async () => recs,
    };
    const { dm, captured } = makeDM({
      lastUpdatedSec: lastSec,
      baselines: {
        "shelly-pro3em_00-em0": { fwd: 0, rev: 0 },
        "shelly-other-em0": { fwd: 0, rev: 0 },
      },
    });
    const channels: ShellyChannelGroup[] = [
      ch(0, "shellypro3em-fail"),
      {
        sourceDeviceId: "shelly-other-em0",
        shellyId: "shelly-other",
        deviceMacId: "shellypro3em-ok",
        channelId: 0,
      },
    ];
    const bm = new BackfillManager({
      enabled: true,
      scanHours: 24,
      gapThresholdSec: 300,
      integrationId: INTEG,
      deviceManager: dm,
      channelsProvider: () => channels,
      rpcFor: (mac) => (mac === "shellypro3em-fail" ? failingRpc : okRpc),
      setLastCumul: () => {},
      logger: silentLogger,
    });
    await bm.runOnce("manual");
    // First device failed silently, second device wrote its 1 record
    expect(captured.map((c) => c.sid)).toEqual(["shelly-other-em0"]);
  });

  it("baseline fallback to 0 when no value persisted yet", async () => {
    const lastSec = NOW_S - 30 * 60;
    const { dm, captured } = makeDM({
      lastUpdatedSec: lastSec,
      baselines: {}, // none
    });
    const rpc = makeRpc(
      { 0: [{ ts: lastSec, period: 60, records: 1 }] },
      { 0: [{ ts: lastSec, totalActEnergy: 7, totalActRetEnergy: 0 }] },
    );
    const bm = new BackfillManager({
      enabled: true,
      scanHours: 24,
      gapThresholdSec: 300,
      integrationId: INTEG,
      deviceManager: dm,
      channelsProvider: () => [ch(0)],
      rpcFor: () => rpc,
      setLastCumul: () => {},
      logger: silentLogger,
    });
    await bm.runOnce("manual");
    expect(captured).toHaveLength(1);
    expect(captured[0].payload).toEqual({ energy_forward: 7, energy_reverse: 0, energy: 7 });
  });

  it("lastUpdated null → skip silently", async () => {
    const { dm, captured } = makeDM({
      lastUpdatedSec: null,
      baselines: { "shelly-pro3em_00-em0": { fwd: 0, rev: 0 } },
    });
    const rpcFor = vi.fn();
    const bm = new BackfillManager({
      enabled: true,
      scanHours: 24,
      gapThresholdSec: 300,
      integrationId: INTEG,
      deviceManager: dm,
      channelsProvider: () => [ch(0)],
      rpcFor,
      setLastCumul: () => {},
      logger: silentLogger,
    });
    await bm.runOnce("manual");
    expect(rpcFor).not.toHaveBeenCalled();
    expect(captured).toHaveLength(0);
  });

  it("boot run is delayed until at least one channel is discovered", async () => {
    const lastSec = NOW_S - 30 * 60;
    const { dm, captured } = makeDM({
      lastUpdatedSec: lastSec,
      baselines: { "shelly-pro3em_00-em0": { fwd: 0, rev: 0 } },
    });
    const rpc = makeRpc(
      { 0: [{ ts: lastSec, period: 60, records: 1 }] },
      { 0: [{ ts: lastSec, totalActEnergy: 5, totalActRetEnergy: 0 }] },
    );
    // Channels arrive after ~6 s (= MQTT topics dispatched a few seconds after start).
    let channels: ShellyChannelGroup[] = [];
    const bm = new BackfillManager({
      enabled: true,
      scanHours: 24,
      gapThresholdSec: 300,
      integrationId: INTEG,
      deviceManager: dm,
      channelsProvider: () => channels,
      rpcFor: () => rpc,
      setLastCumul: () => {},
      logger: silentLogger,
    });
    bm.start();
    // 1st poll tick: still no channels
    await vi.advanceTimersByTimeAsync(2000);
    expect(captured).toHaveLength(0);
    // Channels arrive — next poll tick at +4 s should trigger the boot run.
    channels = [ch(0)];
    await vi.advanceTimersByTimeAsync(2000);
    // Allow the async runOnce to settle
    await vi.advanceTimersByTimeAsync(0);
    expect(captured).toHaveLength(1);
    bm.stop();
  });

  it("boot run gives up after 60 s if channels never arrive (no leak)", async () => {
    const { dm } = makeDM({});
    const rpcFor = vi.fn();
    const bm = new BackfillManager({
      enabled: true,
      scanHours: 24,
      gapThresholdSec: 300,
      integrationId: INTEG,
      deviceManager: dm,
      channelsProvider: () => [],
      rpcFor,
      setLastCumul: () => {},
      logger: silentLogger,
    });
    bm.start();
    await vi.advanceTimersByTimeAsync(60_000);
    // bootPoll has fired the runOnce (with empty channels → no-op) and cleared itself.
    // Stop must not throw. Also ensure no further polls happen.
    bm.stop();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(rpcFor).not.toHaveBeenCalled();
  });

  it("two concurrent triggers — second one sees inFlight, skips", async () => {
    const lastSec = NOW_S - 30 * 60;
    const { dm, captured } = makeDM({
      lastUpdatedSec: lastSec,
      baselines: { "shelly-pro3em_00-em0": { fwd: 0, rev: 0 } },
    });
    let resolveGetRecords!: () => void;
    const rpc: ShellyRpcClient = {
      getRecords: () => new Promise((res) => {
        resolveGetRecords = () => res([{ ts: lastSec, period: 60, records: 1 }]);
      }),
      getData: async () => [{ ts: lastSec, totalActEnergy: 1, totalActRetEnergy: 0 }],
    };
    const bm = new BackfillManager({
      enabled: true,
      scanHours: 24,
      gapThresholdSec: 300,
      integrationId: INTEG,
      deviceManager: dm,
      channelsProvider: () => [ch(0)],
      rpcFor: () => rpc,
      setLastCumul: () => {},
      logger: silentLogger,
    });
    const p1 = bm.runOnce("manual");
    const p2 = bm.runOnce("manual"); // should be a no-op
    resolveGetRecords();
    await Promise.all([p1, p2]);
    expect(captured).toHaveLength(1); // only the first call's data was emitted
  });
});
