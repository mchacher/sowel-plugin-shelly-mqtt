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

| Key             | Default         | Notes                                     |
|-----------------|-----------------|-------------------------------------------|
| `mqtt_url`      | required        | e.g. `mqtt://192.168.0.230:1883`          |
| `mqtt_username` | optional        |                                           |
| `mqtt_password` | optional        |                                           |
| `mqtt_client_id`| `sowel-shelly`  | random suffix added at connect            |
| `topic_filter`  | `shelly/#`      | wildcard subscription                     |

## Build / test

```bash
npm install
npm run build
npm test
```
