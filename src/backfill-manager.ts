/**
 * Backfill manager — replays missing minute records from the Shelly's
 * own EM1Data flash archive into Sowel's pipeline when the live MQTT
 * stream had a gap.
 *
 * Trigger conditions:
 *   - At plugin start (after the existing baseline hydration).
 *   - On a scheduled interval (default 1h).
 *
 * Gap detection (per spec 088 v2): read `device_data.lastUpdated` for
 * each channel's `energy_forward` value. If `now - lastUpdated >
 * gapThresholdSec` (default 5 min), schedule a replay over the
 * intersection of `[max(lastUpdated, now - scanHours*3600), now]` and
 * the device's available `data_blocks`.
 *
 * Channel interleaving: records are emitted minute by minute across all
 * channels (grid, solar, sub-load) so SelfConsumptionWriter pairs them
 * correctly inside its 30 s match window.
 *
 * Cumul reconstruction: the Shelly archive returns per-minute deltas,
 * but Sowel expects the monotonic `energy_forward` / `energy_reverse`
 * cumuls. Walk forward from the last persisted cumul, summing deltas
 * until the live tick.
 */
import type { DeviceManager, Logger, ShellyChannelGroup } from "./shelly-plugin.js";
import type { MinuteRecord, ShellyRpcClient } from "./shelly-rpc.js";

export interface BackfillManagerOptions {
  enabled: boolean;
  /** Maximum window scanned each run, in hours. Bounded 1..168. */
  scanHours: number;
  /** Lower bound on a "gap". Below this, no-op. Seconds. */
  gapThresholdSec: number;
  /** Interval between scheduled runs (ms). Default 1h. */
  periodicIntervalMs?: number;
  integrationId: string;
  deviceManager: DeviceManager;
  /** Returns the snapshot of channels currently known to the engine. */
  channelsProvider: () => ShellyChannelGroup[];
  /** Build (or look up) an RPC client for a given device MAC id. */
  rpcFor: (deviceMacId: string) => ShellyRpcClient;
  /** Update the engine's in-memory baseline after a replay. */
  setLastCumul: (sourceDeviceId: string, baseline: { fwd?: number; rev?: number }) => void;
  logger: Logger;
}

/** Group channels by `shellyId` so we run one pass per Pro 3EM device. */
function groupByShelly(channels: ShellyChannelGroup[]): Map<string, ShellyChannelGroup[]> {
  const out = new Map<string, ShellyChannelGroup[]>();
  for (const ch of channels) {
    const arr = out.get(ch.shellyId) ?? [];
    arr.push(ch);
    out.set(ch.shellyId, arr);
  }
  return out;
}

export class BackfillManager {
  private readonly logger: Logger;
  private readonly opts: BackfillManagerOptions;
  private timer: ReturnType<typeof setInterval> | null = null;
  private bootPollTimer: ReturnType<typeof setInterval> | null = null;
  /** Set when start() is called and cleared by stop(). Guards reentrance. */
  private running = false;
  /** True while a run is mid-flight; new triggers wait or no-op. */
  private inFlight = false;
  private stopped = false;

  constructor(opts: BackfillManagerOptions) {
    this.opts = opts;
    this.logger = opts.logger.child({ module: "shelly-backfill" });
  }

  start(): void {
    if (!this.opts.enabled) {
      this.logger.info({}, "Backfill disabled by config");
      return;
    }
    if (this.running) return;
    this.running = true;
    this.stopped = false;
    this.logger.info(
      { scanHours: this.opts.scanHours, gapThresholdSec: this.opts.gapThresholdSec },
      "Backfill manager started",
    );

    // Boot run can't fire immediately: MQTT topics arrive a few seconds
    // after start() and the engine populates `known` only then. Poll
    // every 2 s for up to 60 s; fire the boot run as soon as at least
    // one channel is discovered. After it succeeds (or we time out),
    // stop the boot poll and let the periodic interval take over.
    let bootElapsedMs = 0;
    const bootPollIntervalMs = 2_000;
    const bootDeadlineMs = 60_000;
    this.bootPollTimer = setInterval(() => {
      bootElapsedMs += bootPollIntervalMs;
      const channels = this.opts.channelsProvider();
      if (channels.length === 0 && bootElapsedMs < bootDeadlineMs) return;
      if (this.bootPollTimer) {
        clearInterval(this.bootPollTimer);
        this.bootPollTimer = null;
      }
      void this.runOnce("boot");
    }, bootPollIntervalMs);

    const interval = this.opts.periodicIntervalMs ?? 60 * 60 * 1000;
    this.timer = setInterval(() => {
      void this.runOnce("periodic");
    }, interval);
  }

  stop(): void {
    this.stopped = true;
    this.running = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    if (this.bootPollTimer) {
      clearInterval(this.bootPollTimer);
      this.bootPollTimer = null;
    }
  }

  /**
   * One full pass over all known Shelly devices. Per-device errors are
   * isolated — one device failing does not abort the others.
   */
  async runOnce(trigger: "boot" | "periodic" | "manual"): Promise<void> {
    if (this.inFlight) {
      this.logger.debug({ trigger }, "Skip: previous run still in flight");
      return;
    }
    this.inFlight = true;
    try {
      const channels = this.opts.channelsProvider();
      if (channels.length === 0) {
        this.logger.debug({ trigger }, "No channels known yet, skip");
        return;
      }
      const grouped = groupByShelly(channels);
      for (const [shellyId, group] of grouped) {
        if (this.stopped) return;
        try {
          await this.runForDevice(shellyId, group);
        } catch (err) {
          this.logger.warn({ err, shellyId }, "Backfill failed for device");
        }
      }
    } finally {
      this.inFlight = false;
    }
  }

  /** Backfill one Shelly device (= one Pro 3EM, up to 3 channels). */
  private async runForDevice(shellyId: string, group: ShellyChannelGroup[]): Promise<void> {
    // Anchor: the OLDEST `energy_forward.lastUpdated` across all channels.
    // If any channel is stale, refresh the whole group on the same window
    // (simpler, no skew between channels).
    const nowS = Math.floor(Date.now() / 1000);
    const lastUpdates = group.map((ch) => this.lastUpdatedForChannel(ch));
    const oldest = lastUpdates.reduce<number | null>(
      (min, t) => (t === null ? min : min === null ? t : Math.min(min, t)),
      null,
    );
    if (oldest === null) {
      this.logger.debug({ shellyId }, "No persisted lastUpdated yet, skip");
      return;
    }

    const gapSec = nowS - oldest;
    if (gapSec < this.opts.gapThresholdSec) {
      this.logger.debug({ shellyId, gapSec }, "Below gap threshold, skip");
      return;
    }

    // Clamp the scan window.
    const windowStart = Math.max(oldest, nowS - this.opts.scanHours * 3600);
    const windowEnd = nowS;

    const macId = group[0].deviceMacId;
    if (!macId) {
      this.logger.warn(
        { shellyId, gapSec },
        "Gap detected but device mDNS id not yet captured (events/rpc not seen)",
      );
      return;
    }

    const rpc = this.opts.rpcFor(macId);

    // Fetch each channel independently. Per-channel data_blocks may
    // differ if a channel was specifically reset, but on a Pro 3EM they
    // typically match.
    const perChannelRecords: Array<{ ch: ShellyChannelGroup; recs: MinuteRecord[] }> = [];
    for (const ch of group) {
      const blocks = await rpc.getRecords(ch.channelId);
      const recs: MinuteRecord[] = [];
      for (const b of blocks) {
        const blockEnd = b.ts + b.period * b.records;
        const start = Math.max(b.ts, windowStart);
        const end = Math.min(blockEnd, windowEnd);
        if (end <= start) continue;
        const slice = await rpc.getData(ch.channelId, start, end);
        recs.push(...slice);
      }
      perChannelRecords.push({ ch, recs });
    }

    const totalRecords = perChannelRecords.reduce((a, p) => a + p.recs.length, 0);
    if (totalRecords === 0) {
      this.logger.warn(
        { shellyId, gapSec, windowStart, windowEnd },
        "Gap detected but Shelly archive has no records for the window",
      );
      return;
    }

    // Cumul reconstruction baseline — last value the plugin persisted
    // before the gap (= what the live `lastCumul` was tracking).
    const baselines = new Map<string, { fwd: number; rev: number; runFwd: number; runRev: number }>();
    for (const ch of group) {
      const fwd = this.opts.deviceManager.getDeviceDataValue(
        this.opts.integrationId,
        ch.sourceDeviceId,
        "energy_forward",
      );
      const rev = this.opts.deviceManager.getDeviceDataValue(
        this.opts.integrationId,
        ch.sourceDeviceId,
        "energy_reverse",
      );
      baselines.set(ch.sourceDeviceId, {
        fwd: typeof fwd === "number" ? fwd : 0,
        rev: typeof rev === "number" ? rev : 0,
        runFwd: typeof fwd === "number" ? fwd : 0,
        runRev: typeof rev === "number" ? rev : 0,
      });
    }

    // Build a chronological interleaved emission order:
    //   (ch0, T) → (ch1, T) → (ch2, T) → (ch0, T+1) → ...
    // Implementation: index per-minute records by ts on each channel,
    // walk the union of timestamps in ascending order.
    const byTs = new Map<number, Array<{ ch: ShellyChannelGroup; rec: MinuteRecord }>>();
    for (const { ch, recs } of perChannelRecords) {
      for (const rec of recs) {
        const arr = byTs.get(rec.ts) ?? [];
        arr.push({ ch, rec });
        byTs.set(rec.ts, arr);
      }
    }
    const sortedTs = [...byTs.keys()].sort((a, b) => a - b);

    let writtenPerChannel = new Map<string, number>();
    for (const ts of sortedTs) {
      if (this.stopped) return;
      const tickEntries = byTs.get(ts)!;
      // Emit in stable channel order (0 → 1 → 2)
      tickEntries.sort((a, b) => a.ch.channelId - b.ch.channelId);
      for (const { ch, rec } of tickEntries) {
        const b = baselines.get(ch.sourceDeviceId)!;
        b.runFwd += rec.totalActEnergy;
        b.runRev += rec.totalActRetEnergy;
        const energy = rec.totalActEnergy - rec.totalActRetEnergy;
        this.opts.deviceManager.updateDeviceData(
          this.opts.integrationId,
          ch.sourceDeviceId,
          {
            energy_forward: b.runFwd,
            energy_reverse: b.runRev,
            energy,
          },
          ts,
        );
        writtenPerChannel.set(ch.sourceDeviceId, (writtenPerChannel.get(ch.sourceDeviceId) ?? 0) + 1);
      }
    }

    // Update in-memory baseline so the next live tick computes the
    // delta from the last replayed value, not from before the gap.
    for (const ch of group) {
      const b = baselines.get(ch.sourceDeviceId)!;
      this.opts.setLastCumul(ch.sourceDeviceId, { fwd: b.runFwd, rev: b.runRev });
    }

    this.logger.info(
      {
        shellyId,
        windowStart,
        windowEnd,
        gapSec,
        recordsWritten: totalRecords,
        perChannel: Object.fromEntries(writtenPerChannel),
      },
      "Backfill replayed",
    );
  }

  /**
   * Read the lastUpdated timestamp for a channel's `energy_forward` and
   * convert to epoch seconds. Returns null when no value is persisted
   * or when the host's plugin API doesn't expose the accessor.
   */
  private lastUpdatedForChannel(ch: ShellyChannelGroup): number | null {
    const dm = this.opts.deviceManager;
    if (typeof dm.getDeviceDataLastUpdated !== "function") return null;
    const iso = dm.getDeviceDataLastUpdated(this.opts.integrationId, ch.sourceDeviceId, "energy_forward");
    if (!iso) return null;
    const t = Date.parse(iso);
    return Number.isFinite(t) ? Math.floor(t / 1000) : null;
  }
}
