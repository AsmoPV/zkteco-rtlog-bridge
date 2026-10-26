import json
import logging
import os
import signal
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import paho.mqtt.client as mqtt
from c3 import C3


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("zkteco_rtlog_bridge")
STOP = False


def stop_handler(_signum, _frame):
    global STOP
    STOP = True


def load_options():
    options_path = Path("/data/options.json")
    try:
        with options_path.open(encoding="utf-8") as options_file:
            options = json.load(options_file)
    except (OSError, json.JSONDecodeError) as err:
        raise RuntimeError(f"Could not load add-on options from {options_path}") from err
    if not isinstance(options, dict):
        raise RuntimeError(f"Add-on options in {options_path} must be a JSON object")
    return options


def required_option(options, name):
    value = str(options.get(name, "")).strip()
    if not value:
        raise RuntimeError(f"Required add-on option {name} is missing")
    return value


def positive_int_option(options, name, default):
    raw = options.get(name, default)
    try:
        value = int(raw)
    except (TypeError, ValueError) as err:
        raise ValueError(f"Add-on option {name} must be an integer") from err
    if value < 1:
        raise ValueError(f"Add-on option {name} must be at least 1")
    return value


def event_timestamp(value, timezone):
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone)
    else:
        parsed = parsed.astimezone(timezone)
    return parsed.isoformat()


def database_connection():
    data_dir = Path("/data")
    data_dir.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(data_dir / "pending-events.sqlite3")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS pending_events (
            event_key TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            event TEXT NOT NULL,
            card TEXT NOT NULL,
            reader TEXT NOT NULL,
            event_code INTEGER,
            verify_mode TEXT,
            entry_exit TEXT,
            created_at REAL NOT NULL,
            published INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    connection.commit()
    return connection


def queue_event(connection, record, timezone):
    card = str(record.card_no).strip()
    if card in ("", "0"):
        return False

    reader = str(record.port_nr).strip()
    timestamp = event_timestamp(record.time_second, timezone)
    event_type = record.event_type
    event_code = int(event_type.value)
    event_name = str(event_type.description or event_code)
    key = "|".join((timestamp, str(event_code), card, reader))

    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO pending_events
            (event_key, timestamp, event, card, reader, event_code,
             verify_mode, entry_exit, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            key,
            timestamp,
            event_name,
            card,
            reader,
            event_code,
            str(record.verified),
            str(record.in_out_state),
            time.time(),
        ),
    )
    connection.commit()
    return cursor.rowcount > 0


def publish_pending(connection, client, topic):
    if not client.is_connected():
        return

    rows = connection.execute(
        """
        SELECT event_key, timestamp, event, card, reader, event_code,
               verify_mode, entry_exit
        FROM pending_events
        WHERE published = 0
        ORDER BY created_at, event_key
        """
    ).fetchall()

    for row in rows:
        if STOP or not client.is_connected():
            return
        event_key, timestamp, event, card, reader, code, verify_mode, entry_exit = row
        payload = {
            "timestamp": timestamp,
            "event": event,
            "card": card,
            "reader": reader,
            "event_code": code,
            "verify_mode": verify_mode,
            "entry_exit": entry_exit,
        }
        try:
            result = client.publish(
                topic,
                json.dumps(payload, ensure_ascii=True),
                qos=1,
                retain=False,
            )
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                LOG.error("MQTT publish failed with result code %s", result.rc)
                return
            result.wait_for_publish(timeout=10)
            if not result.is_published():
                LOG.error("MQTT did not confirm event publication")
                return
        except (ValueError, RuntimeError, OSError):
            LOG.exception("Could not publish a queued access event")
            return

        connection.execute(
            "UPDATE pending_events SET published = 1 WHERE event_key = ?",
            (event_key,),
        )
        connection.commit()

    connection.execute(
        "DELETE FROM pending_events WHERE published = 1 AND created_at < ?",
        (time.time() - 7 * 24 * 60 * 60,),
    )
    connection.commit()


def connect_controller(host, port, password):
    panel = C3(host, port)
    connected = panel.connect(password) if password else panel.connect()
    if not connected:
        panel.disconnect()
        raise ConnectionError(f"Could not connect to controller at {host}:{port}")
    LOG.info("Connected to the ZKTeco controller")
    return panel


def connect_mqtt(options):
    token = os.environ.get("SUPERVISOR_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Required SUPERVISOR_TOKEN environment variable is missing")

    request = Request(
        "http://supervisor/services/mqtt",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urlopen(request, timeout=10) as response:
            service_response = json.load(response)
    except HTTPError as err:
        raise RuntimeError(
            f"Supervisor MQTT service lookup failed with HTTP {err.code}"
        ) from err
    except (URLError, TimeoutError, OSError, json.JSONDecodeError) as err:
        raise RuntimeError("Could not read the Supervisor MQTT service details") from err

    if not isinstance(service_response, dict) or service_response.get("result") != "ok":
        raise RuntimeError("Supervisor did not return valid MQTT service details")
    service = service_response.get("data")
    if not isinstance(service, dict):
        raise RuntimeError("Supervisor MQTT service details are missing")

    host = str(service.get("host") or required_option(options, "MQTT_HOST"))
    port = positive_int_option(
        {"MQTT_PORT": service.get("port") or options.get("MQTT_PORT", 1883)},
        "MQTT_PORT",
        1883,
    )
    username = str(
        service.get("username") or options.get("MQTT_USERNAME", "")
    ).strip()
    password = str(service.get("password") or options.get("MQTT_PASSWORD", ""))
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="zkteco_rtlog_bridge",
    )
    if username or password:
        client.username_pw_set(username or None, password or None)
    client.connect(host, port, keepalive=60)
    client.loop_start()

    deadline = time.monotonic() + 15
    while not client.is_connected() and time.monotonic() < deadline:
        if STOP:
            break
        time.sleep(0.2)
    if not client.is_connected():
        client.loop_stop()
        raise ConnectionError(f"Could not connect to MQTT broker at {host}:{port}")
    LOG.info("Connected to the MQTT broker")
    return client


def main():
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    options = load_options()
    host = required_option(options, "DEVICE_IP")
    port = positive_int_option(options, "DEVICE_PORT", 4370)
    password = str(options.get("DEVICE_PASSWORD", ""))
    topic = required_option(options, "MQTT_TOPIC")
    interval = positive_int_option(options, "POLLING_INTERVAL_SECONDS", 2)
    timezone = ZoneInfo(required_option(options, "TIME_ZONE"))
    connection = database_connection()
    client = connect_mqtt(options)
    panel = None
    stats_started = time.monotonic()
    poll_count = 0
    record_count = 0
    event_count = 0
    queued_count = 0

    try:
        while not STOP:
            publish_pending(connection, client, topic)
            if panel is None:
                try:
                    panel = connect_controller(host, port, password)
                except Exception:
                    LOG.exception("Controller connection failed; retrying")
                    time.sleep(interval)
                    continue

            try:
                records = list(panel.get_rt_log() or [])
                events = [record for record in records if record.is_event()]
                queued = sum(
                    queue_event(connection, record, timezone) for record in events
                )
                poll_count += 1
                record_count += len(records)
                event_count += len(events)
                queued_count += queued
                publish_pending(connection, client, topic)
                now = time.monotonic()
                if now - stats_started >= 30:
                    LOG.info(
                        "RTLog poll summary: %d polls, %d records, %d events, "
                        "%d newly queued",
                        poll_count,
                        record_count,
                        event_count,
                        queued_count,
                    )
                    stats_started = now
                    poll_count = 0
                    record_count = 0
                    event_count = 0
                    queued_count = 0
            except Exception:
                LOG.exception("Controller polling failed; reconnecting")
                try:
                    panel.disconnect()
                except Exception:
                    LOG.exception("Could not close the controller connection")
                panel = None

            time.sleep(interval)
    finally:
        if panel is not None:
            try:
                panel.disconnect()
            except Exception:
                LOG.exception("Could not close the controller connection")
        client.disconnect()
        client.loop_stop()
        connection.close()
        LOG.info("Bridge stopped")


if __name__ == "__main__":
    main()
