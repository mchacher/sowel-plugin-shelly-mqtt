import { describe, it, expect } from "vitest";
import {
  extractDeviceTopic,
  parseEm1Status,
  parseEm1DataStatus,
  parseOnlinePayload,
  parseJson,
} from "./shelly-parser.js";

describe("extractDeviceTopic", () => {
  it("matches .../shellyId/status/em1:N", () => {
    expect(extractDeviceTopic("shelly/shelly-pro3em_00/status/em1:0")).toEqual({
      shellyId: "shelly-pro3em_00",
      channel: 0,
      kind: "em1",
    });
  });
  it("matches .../shellyId/status/em1data:N", () => {
    expect(extractDeviceTopic("shelly/shelly-pro3em_00/status/em1data:1")).toEqual({
      shellyId: "shelly-pro3em_00",
      channel: 1,
      kind: "em1data",
    });
  });
  it("matches .../shellyId/online (LWT)", () => {
    expect(extractDeviceTopic("shelly/shelly-pro3em_00/online")).toEqual({
      shellyId: "shelly-pro3em_00",
      channel: null,
      kind: "online",
    });
  });
  it("ignores 3-phase em:0 (V1 only supports EM1 mode)", () => {
    expect(extractDeviceTopic("shelly/shelly-pro3em_00/status/em:0")).toBeNull();
  });
  it("ignores unrelated topics", () => {
    expect(extractDeviceTopic("zigbee2mqtt/somedevice/status")).toBeNull();
    expect(extractDeviceTopic("totally/random")).toBeNull();
  });
  it("works regardless of prefix depth", () => {
    expect(extractDeviceTopic("home/floor1/shelly-pro3em_00/status/em1:2")?.channel).toBe(2);
    expect(extractDeviceTopic("a/b/c/d/shelly-x/online")?.shellyId).toBe("shelly-x");
  });
});

describe("parseEm1Status", () => {
  it("extracts numeric fields", () => {
    const r = parseEm1Status({
      id: 0, voltage: 230.4, current: 4.16, act_power: 346.3, aprt_power: 959.5, pf: 0.36, freq: 50,
    });
    expect(r.voltage).toBe(230.4);
    expect(r.current).toBe(4.16);
    expect(r.power).toBe(346.3);
    expect(r.pf).toBe(0.36);
  });
  it("returns null for missing fields", () => {
    const r = parseEm1Status({ voltage: 230 });
    expect(r.voltage).toBe(230);
    expect(r.power).toBeNull();
    expect(r.current).toBeNull();
    expect(r.pf).toBeNull();
  });
  it("returns null for non-numeric values (defensive)", () => {
    const r = parseEm1Status({ act_power: "oops", voltage: NaN, current: null });
    expect(r.power).toBeNull();
    expect(r.voltage).toBeNull();
    expect(r.current).toBeNull();
  });
  it("preserves negative power (export side)", () => {
    expect(parseEm1Status({ act_power: -850 }).power).toBe(-850);
  });
});

describe("parseEm1DataStatus", () => {
  it("extracts the two cumulative counters", () => {
    const r = parseEm1DataStatus({ total_act_energy: 12.64, total_act_ret_energy: 0 });
    expect(r.energy_forward).toBe(12.64);
    expect(r.energy_reverse).toBe(0);
  });
  it("returns null for missing fields", () => {
    expect(parseEm1DataStatus({}).energy_forward).toBeNull();
    expect(parseEm1DataStatus({}).energy_reverse).toBeNull();
  });
});

describe("parseOnlinePayload", () => {
  it("accepts true / false", () => {
    expect(parseOnlinePayload(Buffer.from("true"))).toBe(true);
    expect(parseOnlinePayload(Buffer.from("false"))).toBe(false);
  });
  it("ignores anything else", () => {
    expect(parseOnlinePayload(Buffer.from("yes"))).toBeNull();
    expect(parseOnlinePayload(Buffer.from(""))).toBeNull();
  });
});

describe("parseJson", () => {
  it("parses valid JSON", () => {
    expect(parseJson(Buffer.from('{"a":1}'))).toEqual({ a: 1 });
  });
  it("throws on invalid", () => {
    expect(() => parseJson(Buffer.from("nope"))).toThrow();
  });
});
