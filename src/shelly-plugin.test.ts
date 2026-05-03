import { describe, it, expect, beforeEach } from "vitest";
import { ShellyEngine } from "./shelly-plugin.js";
import type { DeviceManager, Logger } from "./shelly-plugin.js";
import type { MqttConnector } from "./mqtt-connector.js";

const SID = "shelly-pro3em_00-em0";
const INTEG = "shelly_mqtt";

const silentLogger: Logger = {
  child: () => silentLogger,
  info: () => {},
  warn: () => {},
  error: () => {},
  debug: () => {},
};

interface UpdateCall {
  sid: string;
  payload: Record<string, unknown>;
}

class StubDeviceManager implements DeviceManager {
  /** key → typed value */
  store = new Map<string, string | number | boolean>();
  updates: UpdateCall[] = [];
  discoveries: { integrationId: string; source: string; discovered: unknown }[] = [];
  statuses: { sid: string; status: string }[] = [];

  upsertFromDiscovery(integrationId: string, source: string, discovered: unknown): void {
    this.discoveries.push({ integrationId, source, discovered });
  }
  updateDeviceData(_integ: string, sid: string, payload: Record<string, unknown>): void {
    this.updates.push({ sid, payload });
    for (const [k, v] of Object.entries(payload)) {
      if (typeof v === "number" || typeof v === "boolean" || typeof v === "string") {
        this.store.set(`${sid}:${k}`, v);
      }
    }
  }
  updateDeviceStatus(_integ: string, sid: string, status: string): void {
    this.statuses.push({ sid, status });
  }
  getDeviceDataValue(
    _integ: string,
    sid: string,
    key: string,
  ): string | number | boolean | null {
    return this.store.get(`${sid}:${key}`) ?? null;
  }
}

/** Minimal MqttConnector stub — the engine never calls anything but subscribe(). */
const noopMqtt: MqttConnector = {
  subscribe: () => {},
  // The real connector has more methods but the engine doesn't use them.
} as unknown as MqttConnector;

function emit(engine: ShellyEngine, channel: number, body: Record<string, number>): void {
  // Bypass MQTT — call the dispatch path directly via a private accessor.
  const payload = Buffer.from(JSON.stringify({ id: channel, ...body }));
  // @ts-expect-error — exercising the private dispatch keeps tests close to reality.
  engine.dispatch(`shelly/shelly-pro3em_00/status/em1data:${channel}`, payload);
}

function lastUpdate(dm: StubDeviceManager): UpdateCall | undefined {
  return dm.updates[dm.updates.length - 1];
}

describe("ShellyEngine — energy delta synthesiser", () => {
  let dm: StubDeviceManager;
  let engine: ShellyEngine;

  beforeEach(() => {
    dm = new StubDeviceManager();
    engine = new ShellyEngine(INTEG, noopMqtt, dm, silentLogger);
  });

  it("first em1data event for a new channel emits energy = 0 and persists baseline", () => {
    emit(engine, 0, { total_act_energy: 6105.7, total_act_ret_energy: 4690.3 });

    const u = lastUpdate(dm)!;
    expect(u.sid).toBe(SID);
    expect(u.payload.energy_forward).toBe(6105.7);
    expect(u.payload.energy_reverse).toBe(4690.3);
    expect(u.payload.energy).toBe(0);
  });

  it("steady-state — fwd grows, rev unchanged → energy = +deltaFwd", () => {
    emit(engine, 0, { total_act_energy: 6100, total_act_ret_energy: 4685 });
    emit(engine, 0, { total_act_energy: 6105.5, total_act_ret_energy: 4685 });

    expect(lastUpdate(dm)!.payload.energy).toBeCloseTo(5.5, 9);
  });

  it("steady-state — rev grows, fwd unchanged → energy = -deltaRev", () => {
    emit(engine, 0, { total_act_energy: 6100, total_act_ret_energy: 4685 });
    emit(engine, 0, { total_act_energy: 6100, total_act_ret_energy: 4690 });

    expect(lastUpdate(dm)!.payload.energy).toBeCloseTo(-5, 9);
  });

  it("both grow within the same window → energy = deltaFwd - deltaRev", () => {
    emit(engine, 0, { total_act_energy: 6100, total_act_ret_energy: 4685 });
    emit(engine, 0, { total_act_energy: 6105.5, total_act_ret_energy: 4689.9 });

    expect(lastUpdate(dm)!.payload.energy).toBeCloseTo(0.6, 9);
  });

  it("counter reset (current fwd < last) → energy = 0, baseline refreshed", () => {
    emit(engine, 0, { total_act_energy: 6100, total_act_ret_energy: 4685 });
    emit(engine, 0, { total_act_energy: 10, total_act_ret_energy: 4685 });

    expect(lastUpdate(dm)!.payload.energy).toBe(0);
    // Next event after reset uses the new baseline (10), not 6100
    emit(engine, 0, { total_act_energy: 12, total_act_ret_energy: 4685 });
    expect(lastUpdate(dm)!.payload.energy).toBeCloseTo(2, 9);
  });

  it("out-of-order delivery (older payload after newer) → energy = 0, no negative emission", () => {
    emit(engine, 0, { total_act_energy: 6100, total_act_ret_energy: 4685 });
    emit(engine, 0, { total_act_energy: 6105, total_act_ret_energy: 4685 });
    // Now an older event arrives
    emit(engine, 0, { total_act_energy: 6098, total_act_ret_energy: 4685 });

    expect(lastUpdate(dm)!.payload.energy).toBe(0);
  });

  it("hydrates baseline from device_data on plugin restart", () => {
    // Simulate a previous Sowel session that persisted last counters
    dm.store.set(`${SID}:energy_forward`, 6100);
    dm.store.set(`${SID}:energy_reverse`, 4685);

    // Fresh engine, empty in-memory map
    emit(engine, 0, { total_act_energy: 6105.5, total_act_ret_energy: 4685 });

    // First event after restart uses the persisted baseline → small delta
    expect(lastUpdate(dm)!.payload.energy).toBeCloseTo(5.5, 9);
  });

  it("plugin restart with no prior data → first event treats current as baseline (energy = 0)", () => {
    // device_data is empty (fresh install) — already covered by first test, kept here
    // for the explicit "no prior data" scenario.
    emit(engine, 0, { total_act_energy: 12345, total_act_ret_energy: 0 });

    expect(lastUpdate(dm)!.payload.energy).toBe(0);
  });

  it("partial payload (only fwd present) → delta on the field that arrived; the other is unchanged", () => {
    emit(engine, 0, { total_act_energy: 6100, total_act_ret_energy: 4685 });

    // Now only forward arrives (parser still emits both fields, simulate by
    // omitting total_act_ret_energy — the parser yields null for the missing key)
    const payload = Buffer.from(JSON.stringify({ id: 0, total_act_energy: 6105 }));
    // @ts-expect-error — private dispatch
    engine.dispatch(`shelly/shelly-pro3em_00/status/em1data:0`, payload);

    const u = lastUpdate(dm)!;
    expect(u.payload.energy_forward).toBe(6105);
    expect(u.payload.energy_reverse).toBeUndefined();
    expect(u.payload.energy).toBeCloseTo(5, 9);
  });

  it("ignores em1data with no energy fields at all", () => {
    const before = dm.updates.length;
    const payload = Buffer.from(JSON.stringify({ id: 0 }));
    // @ts-expect-error — private dispatch
    engine.dispatch(`shelly/shelly-pro3em_00/status/em1data:0`, payload);
    expect(dm.updates.length).toBe(before);
  });
});
