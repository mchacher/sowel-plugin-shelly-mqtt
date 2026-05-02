/**
 * Sowel plugin: Shelly MQTT.
 *
 * Subscribes to a Mosquitto broker, recognises Shelly Pro 3EM EM1 channels,
 * and exposes them as Sowel devices (one device per CT channel) with
 * `power`, `voltage`, `current`, `energy_forward`, `energy_reverse`.
 */

import { MqttConnector } from "./mqtt-connector.js";
import { ShellyEngine } from "./shelly-plugin.js";
import type { DeviceManager, Logger } from "./shelly-plugin.js";

interface SettingsManager {
  get(key: string): string | undefined;
}

interface EventBus {
  emit(event: { type: string; integrationId?: string }): void;
}

interface Device {
  id: string;
  integrationId: string;
  sourceDeviceId: string;
  name: string;
}

interface PluginDeps {
  logger: Logger;
  eventBus: EventBus;
  settingsManager: SettingsManager;
  deviceManager: DeviceManager;
  pluginDir: string;
}

type IntegrationStatus = "connected" | "disconnected" | "not_configured" | "error";

interface IntegrationSettingDef {
  key: string;
  label: string;
  type: "text" | "password" | "number" | "boolean";
  required: boolean;
  placeholder?: string;
  defaultValue?: string;
}

interface IntegrationPlugin {
  readonly id: string;
  readonly name: string;
  readonly description: string;
  readonly icon: string;
  readonly apiVersion?: number;
  getStatus(): IntegrationStatus;
  isConfigured(): boolean;
  getSettingsSchema(): IntegrationSettingDef[];
  start(options?: { pollOffset?: number }): Promise<void>;
  stop(): Promise<void>;
  executeOrder(
    device: Device,
    orderKeyOrDispatchConfig: string | Record<string, unknown>,
    value: unknown,
  ): Promise<void>;
}

const INTEGRATION_ID = "shelly_mqtt";
const SETTINGS_PREFIX = `integration.${INTEGRATION_ID}.`;

class ShellyMqttPlugin implements IntegrationPlugin {
  readonly id = INTEGRATION_ID;
  readonly name = "Shelly MQTT";
  readonly description =
    "Shelly Pro / Plus devices over MQTT (live + cumulative energy from EM channels)";
  readonly icon = "Zap";
  readonly apiVersion = 2;

  private logger: Logger;
  private eventBus: EventBus;
  private settingsManager: SettingsManager;
  private deviceManager: DeviceManager;
  private mqtt: MqttConnector | null = null;
  private engine: ShellyEngine | null = null;
  private status: IntegrationStatus = "disconnected";

  constructor(deps: PluginDeps) {
    this.logger = deps.logger;
    this.eventBus = deps.eventBus;
    this.settingsManager = deps.settingsManager;
    this.deviceManager = deps.deviceManager;
  }

  getStatus(): IntegrationStatus {
    if (!this.isConfigured()) return "not_configured";
    if (this.status === "connected" && this.mqtt && !this.mqtt.isConnected()) return "error";
    return this.status;
  }

  isConfigured(): boolean {
    return this.getSetting("mqtt_url") !== undefined;
  }

  getSettingsSchema(): IntegrationSettingDef[] {
    return [
      { key: "mqtt_url",       label: "MQTT Broker URL", type: "text",     required: true,  placeholder: "mqtt://localhost:1883" },
      { key: "mqtt_username",  label: "MQTT Username",   type: "text",     required: false },
      { key: "mqtt_password",  label: "MQTT Password",   type: "password", required: false },
      { key: "mqtt_client_id", label: "MQTT Client ID",  type: "text",     required: false, defaultValue: "sowel-shelly" },
      { key: "topic_filter",   label: "Topic filter",    type: "text",     required: false, defaultValue: "shelly/#",
        placeholder: "Wildcard the plugin subscribes to" },
    ];
  }

  async start(): Promise<void> {
    if (!this.isConfigured()) {
      this.status = "not_configured";
      return;
    }

    const url = this.getSetting("mqtt_url")!;
    const username = this.getSetting("mqtt_username") || undefined;
    const password = this.getSetting("mqtt_password") || undefined;
    const baseClientId = this.getSetting("mqtt_client_id") ?? "sowel-shelly";
    // Random suffix to avoid clientId collisions across restarts
    const clientId = `${baseClientId}-${Math.random().toString(36).slice(2, 8)}`;
    const topicFilter = this.getSetting("topic_filter") ?? "shelly/#";

    try {
      this.mqtt = new MqttConnector(
        url,
        { username, password, clientId },
        this.eventBus,
        this.logger,
        INTEGRATION_ID,
      );
      await this.mqtt.connect();

      this.engine = new ShellyEngine(
        INTEGRATION_ID,
        this.mqtt,
        this.deviceManager,
        this.logger,
      );
      this.engine.start(topicFilter);

      this.status = this.mqtt.isConnected() ? "connected" : "disconnected";
      if (this.status === "connected") {
        this.eventBus.emit({ type: "system.integration.connected", integrationId: this.id });
      }
      this.logger.info({ topicFilter }, "Shelly MQTT plugin started");
    } catch (err) {
      this.status = "error";
      this.logger.error({ err } as Record<string, unknown>, "Failed to start Shelly MQTT plugin");
    }
  }

  async stop(): Promise<void> {
    if (this.mqtt) {
      await this.mqtt.disconnect();
      this.mqtt = null;
    }
    if (this.engine) {
      this.engine.stop();
      this.engine = null;
    }
    this.status = "disconnected";
    this.eventBus.emit({ type: "system.integration.disconnected", integrationId: this.id });
    this.logger.info({}, "Shelly MQTT plugin stopped");
  }

  async executeOrder(_device: Device, _orderKey: unknown, _value: unknown): Promise<void> {
    // Pro 3EM is read-only — no orders.
    throw new Error("Shelly MQTT plugin: device is read-only");
  }

  private getSetting(key: string): string | undefined {
    return this.settingsManager.get(`${SETTINGS_PREFIX}${key}`);
  }
}

export function createPlugin(deps: PluginDeps): IntegrationPlugin {
  return new ShellyMqttPlugin(deps);
}
