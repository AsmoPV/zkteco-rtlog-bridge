# ZKTeco C3 RTLog Bridge

A Home Assistant add-on that reads real-time access events from a ZKTeco C3 controller and publishes them to MQTT. It uses the read-only RTLog interface; it does not configure the controller or operate door locks.

## Install

Add this repository to **Home Assistant → Settings → Add-ons → Add-on Store → ⋮ → Repositories**:

```text
https://github.com/AsmoPV/zkteco-rtlog-bridge
```

Install **ZKTeco C3 RTLog Bridge**, configure the controller address and timezone, then start the add-on. The default controller address is a documentation-only placeholder (`192.0.2.10`); replace it with the address on your own network.

## Configuration

| Option | Purpose |
| --- | --- |
| `DEVICE_IP` | Controller address |
| `DEVICE_PORT` | Controller port (default `4370`) |
| `DEVICE_PASSWORD` | Optional controller password |
| `MQTT_TOPIC` | Topic for access events |
| `POLLING_INTERVAL_SECONDS` | RTLog polling interval |
| `TIME_ZONE` | Timezone used for controller timestamps |

MQTT broker credentials are obtained from the Home Assistant Supervisor MQTT service. Do not commit credentials, real access-card identifiers, event databases, or local network details.

Events are published as JSON with `timestamp`, `event`, `card`, and `reader` fields. The bridge queues pending publications in SQLite under `/data` and retries them if MQTT is temporarily unavailable.

## Development

The add-on source is in [`zkteco_rtlog_bridge/`](./zkteco_rtlog_bridge/). Dependencies are pinned in its `requirements.txt`; the container image is built from its `Dockerfile`.
