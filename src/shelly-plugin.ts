/**
 * Shelly MQTT plugin engine.
 *
 * Responsibilities:
 *  - Subscribe to a configurable wildcard MQTT topic (default `shelly/#`)
 *  - Recognise Pro 3EM em1:N / em1data:N / online topics
 *  - On first sight of a channel, register a Sowel device via upsertFromDiscovery
 *  - Forward subsequent values via updateDeviceData
 *  - Track the LWT to flip device statuses online/offline
 *
 * The plugin is read-only: Pro 3EM has no orders. executeOrder is
 * implemented as a no-op throw so a stale binding doesn't crash Sowel.
 */

import type { MqttConnector } from "./mqtt-connector.js";
import {
  extractDeviceTopic,
  parseEm1Status,
  parseEm1DataStatus,
  parseOnlinePayload,
  parseJson,
} from "./shelly-parser.js";

// ── Local Sowel surface (mirror of the IntegrationPlugin contract) ──────

export interface Logger {
  child(bindings: Record<string, unknown>): Logger;
  info(obj: Record<string, unknown> | string, msg?: string): void;
  warn(obj: Record<string, unknown> | string, msg?: string): void;
  error(obj: Record<string, unknown> | string, msg?: string): void;
  debug(obj: Record<string, unknown> | string, msg?: string): void;
}

export interface DeviceManager {
  upsertFromDiscovery(integrationId: string, source: string, discovered: unknown): void;
  updateDeviceData(
    integrationId: string,
    sourceDeviceId: string,
    payload: Record<string, unknown>,
  ): void;
  updateDeviceStatus(integrationId: string, sourceDeviceId: string, status: string): void;
}

interface ChannelData {
  key: string;
  type: "boolean" | "number" | "enum" | "text" | "json";
  category: string;
  unit?: string;
  enumValues?: string[];
}

interface DiscoveredDevice {
  friendlyName: string;
  manufacturer?: string;
  model?: string;
  data: ChannelData[];
  orders: never[];
}

const SOURCE = "mqtt";

/**
 * Per-channel data definition exposed to Sowel. Energy counters use the
 * generic `energy` category in V1 — iteration 2 (spec 086) will add a
 * pool_-style override or alias resolution to feed the EnergyAggregator
 * with proper forward/reverse bindings.
 */
const CHANNEL_DATA_DEF: ChannelData[] = [
  { key: "power",          type: "number", category: "power",   unit: "W" },
  { key: "voltage",        type: "number", category: "voltage", unit: "V" },
  { key: "current",        type: "number", category: "current", unit: "A" },
  { key: "energy_forward", type: "number", category: "energy",  unit: "Wh" },
  { key: "energy_reverse", type: "number", category: "energy",  unit: "Wh" },
];

export class ShellyEngine {
  private readonly integrationId: string;
  private readonly mqtt: MqttConnector;
  private readonly deviceManager: DeviceManager;
  private readonly logger: Logger;
  private readonly known = new Set<string>(); // sourceDeviceIds we've already discovered

  constructor(
    integrationId: string,
    mqtt: MqttConnector,
    deviceManager: DeviceManager,
    logger: Logger,
  ) {
    this.integrationId = integrationId;
    this.mqtt = mqtt;
    this.deviceManager = deviceManager;
    this.logger = logger.child({ module: "shelly-mqtt" });
  }

  start(topicFilter: string): void {
    this.mqtt.subscribe(topicFilter, (topic, payload) => this.dispatch(topic, payload));
    this.logger.info({ topicFilter }, "Subscribed");
  }

  stop(): void {
    // The MqttConnector is disposed by the plugin wrapper.
    this.known.clear();
  }

  // ── Internals ──────────────────────────────────────────────────────────

  private dispatch(topic: string, payload: Buffer): void {
    let info;
    try {
      info = extractDeviceTopic(topic);
    } catch (err) {
      this.logger.error({ err, topic }, "Topic parse error");
      return;
    }
    if (!info) return;

    const sid = sourceDeviceId(info.shellyId, info.channel);

    try {
      switch (info.kind) {
        case "online":
          this.handleOnline(info.shellyId, payload);
          break;
        case "em1":
          this.ensureDevice(info.shellyId, info.channel!);
          this.handleEm1Status(sid, payload);
          break;
        case "em1data":
          this.ensureDevice(info.shellyId, info.channel!);
          this.handleEm1DataStatus(sid, payload);
          break;
      }
    } catch (err) {
      this.logger.error({ err, topic }, "Dispatch error");
    }
  }

  private ensureDevice(shellyId: string, channel: number): void {
    const sid = sourceDeviceId(shellyId, channel);
    if (this.known.has(sid)) return;

    const discovered: DiscoveredDevice = {
      friendlyName: `${shellyId} · ch ${channel}`,
      manufacturer: "Shelly",
      model: `Pro 3EM channel ${channel}`,
      data: CHANNEL_DATA_DEF,
      orders: [],
    };
    this.deviceManager.upsertFromDiscovery(this.integrationId, SOURCE, {
      ...discovered,
      friendlyName: sid, // friendlyName drives sourceDeviceId in upsertFromDiscovery
    });
    this.known.add(sid);
    this.logger.info({ shellyId, channel, sid }, "Channel discovered");
  }

  private handleOnline(shellyId: string, payload: Buffer): void {
    const online = parseOnlinePayload(payload);
    if (online === null) return;
    // Flip every known channel under that Shelly id
    for (const sid of this.known) {
      if (!sid.startsWith(`${shellyId}-em`)) continue;
      this.deviceManager.updateDeviceStatus(
        this.integrationId,
        sid,
        online ? "online" : "offline",
      );
    }
  }

  private handleEm1Status(sid: string, payload: Buffer): void {
    let raw: unknown;
    try {
      raw = parseJson(payload);
    } catch {
      return;
    }
    const v = parseEm1Status(raw);
    const data: Record<string, unknown> = {};
    if (v.power !== null) data.power = v.power;
    if (v.voltage !== null) data.voltage = v.voltage;
    if (v.current !== null) data.current = v.current;
    if (Object.keys(data).length === 0) return;
    this.deviceManager.updateDeviceData(this.integrationId, sid, data);
  }

  private handleEm1DataStatus(sid: string, payload: Buffer): void {
    let raw: unknown;
    try {
      raw = parseJson(payload);
    } catch {
      return;
    }
    const v = parseEm1DataStatus(raw);
    const data: Record<string, unknown> = {};
    if (v.energy_forward !== null) data.energy_forward = v.energy_forward;
    if (v.energy_reverse !== null) data.energy_reverse = v.energy_reverse;
    if (Object.keys(data).length === 0) return;
    this.deviceManager.updateDeviceData(this.integrationId, sid, data);
  }
}

/** Stable source id for a (shelly device, channel) pair. */
function sourceDeviceId(shellyId: string, channel: number | null): string {
  return channel === null ? shellyId : `${shellyId}-em${channel}`;
}
