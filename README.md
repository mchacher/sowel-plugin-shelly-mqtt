# sowel-plugin-shelly-mqtt

Sowel plugin for Shelly Pro / Plus devices over MQTT.

## V1 — Pro 3EM only

Subscribes to a Mosquitto broker and exposes each EM channel of a Shelly
Pro 3EM (in `monophase` profile) as a Sowel device with five data points:

| Sowel data       | Source                  |
|------------------|-------------------------|
| `power`          | `act_power` (W)         |
| `voltage`        | `voltage` (V)           |
| `current`        | `current` (A)           |
| `energy_forward` | `total_act_energy` (Wh) |
| `energy_reverse` | `total_act_ret_energy` (Wh) |

The plugin recognises three topic shapes:

- `<prefix>/<device-id>/online`
- `<prefix>/<device-id>/status/em1:N` (1 Hz live)
- `<prefix>/<device-id>/status/em1data:N` (1/min cumulative)

Topic prefix is configurable (default subscription `shelly/#`), so the
plugin matches whatever convention you set in the Shelly app.

Read-only — Pro 3EM has no orders.

## Settings (configured from the Sowel UI)

| Key                    | Default         | Notes                                                                   |
|------------------------|-----------------|-------------------------------------------------------------------------|
| `mqtt_url`             | required        | e.g. `mqtt://192.168.0.230:1883`                                        |
| `mqtt_username`        | optional        |                                                                         |
| `mqtt_password`        | optional        |                                                                         |
| `mqtt_client_id`       | `sowel-shelly`  | random suffix added at connect                                          |
| `topic_filter`         | `shelly/#`      | wildcard subscription                                                   |
| `backfill_enabled`     | `true`          | replay missing minutes from Shelly's flash on boot + every hour         |
| `backfill_hours`       | `24`            | scan window in hours (1..168). Past 60 d the device's flash is empty.   |
| `shelly_auth_user`     | optional        | only set if the Shelly's HTTP UI requires auth                          |
| `shelly_auth_password` | optional        |                                                                         |

## Backfill (v1.2+)

When Sowel was offline for several minutes, the live MQTT stream missed
the corresponding `em1data:N` events. On the next live tick the plugin
would emit the entire gap delta as a single point at the restart
timestamp — daily totals stay correct, but the hourly granularity
collapses and HP/HC classification becomes wrong if the gap straddles a
tariff boundary.

The Pro 3EM stores at least 60 days of 1-minute records in its own
flash (`EM1Data.GetData` RPC). At plugin start (and every hour after),
the plugin checks the persisted `lastUpdated` timestamp on each
channel's `energy_forward`. If it's older than 5 minutes, the plugin
queries the device over HTTP RPC, replays the missing minutes through
the same `updateDeviceData` path the live handler uses (with the
original `sourceTimestamp`), and updates the live `lastCumul` baseline
so the next live tick computes the right delta from the freshly-replayed
last record. The replay interleaves channels minute-by-minute so
`SelfConsumptionWriter` keeps pairing grid + solar correctly.

mDNS resolution: the plugin learns each Shelly's host id from MQTT
`events/rpc` messages (`src` field) and queries
`http://<host-id>.local/rpc/EM1Data.*`. If your network drops mDNS,
shipping a static IP via DHCP-reserved hostnames usually works.

Idempotency: replaying minutes already in InfluxDB is safe — points key
on `(measurement, tag set, timestamp)` so re-runs upsert in place.

## Build / test

```bash
npm install
npm run build
npm test
```
