import json
from datetime import datetime, timezone

import appdaemon.plugins.hass.hassapi as hass
import psycopg


class RainTipIngest(hass.Hass):
    def initialize(self):
        self.topic = self.args["topic"]
        self.mqtt_namespace = self.args.get("mqtt_namespace", "mqtt")

        self.db_config = {
            "host": self.args["db_host"],
            "port": int(self.args.get("db_port", 5432)),
            "dbname": self.args["db_name"],
            "user": self.args["db_user"],
            "password": self.args["db_password"],
        }

        self.log(f"RainTipIngest startar. Lyssnar på MQTT topic: {self.topic}")

        self.ensure_database()

        # Lyssna på inkommande MQTT-meddelanden
        self.listen_event(
            self.on_mqtt_message,
            "MQTT_MESSAGE",
            namespace=self.mqtt_namespace,
            topic=self.topic,
        )

    def get_connection(self):
        return psycopg.connect(**self.db_config)

    def ensure_database(self):
        sql = """
        CREATE TABLE IF NOT EXISTS rain_tip_events (
            time timestamptz NOT NULL,
            received_at timestamptz NOT NULL DEFAULT now(),

            source text NOT NULL,
            sensor_id text NOT NULL,
            event text NOT NULL,

            mm double precision NOT NULL,
            pulse_total bigint,
            uptime_ms bigint,

            time_valid boolean,
            epoch_s bigint,

            topic text NOT NULL,
            payload jsonb NOT NULL
        );

        SELECT create_hypertable(
            'rain_tip_events',
            'time',
            if_not_exists => TRUE
        );

        CREATE INDEX IF NOT EXISTS idx_rain_tip_events_sensor_time
            ON rain_tip_events (sensor_id, time DESC);

        CREATE INDEX IF NOT EXISTS idx_rain_tip_events_source_time
            ON rain_tip_events (source, time DESC);
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()

        self.log("Databastabell rain_tip_events är kontrollerad/skapar vid behov.")

    def on_mqtt_message(self, event_name, data, kwargs):
        try:
            topic = data.get("topic")
            payload_raw = data.get("payload")

            if topic != self.topic:
                return

            if isinstance(payload_raw, dict):
                payload = payload_raw
            else:
                payload = json.loads(payload_raw)

            if payload.get("event") != "rain_tip":
                self.log(f"Ignorerar MQTT-event utan rain_tip: {payload}", level="DEBUG")
                return

            time_valid = bool(payload.get("time_valid", False))
            epoch_s = payload.get("epoch_s")

            if time_valid and epoch_s:
                event_time = datetime.fromtimestamp(int(epoch_s), tz=timezone.utc)
            else:
                event_time = datetime.now(timezone.utc)

            received_at = datetime.now(timezone.utc)

            row = {
                "time": event_time,
                "received_at": received_at,
                "source": payload.get("source", "unknown"),
                "sensor_id": payload.get("sensor_id", "unknown"),
                "event": payload.get("event", "rain_tip"),
                "mm": float(payload["mm"]),
                "pulse_total": payload.get("pulse_total"),
                "uptime_ms": payload.get("uptime_ms"),
                "time_valid": time_valid,
                "epoch_s": epoch_s,
                "topic": topic,
                "payload": json.dumps(payload),
            }

            self.insert_tip(row)

            self.log(
                f"Regnpuls sparad: {row['sensor_id']} "
                f"{row['mm']} mm, pulse_total={row['pulse_total']}"
            )

        except Exception as err:
            self.log(f"Fel vid hantering av regnpuls: {err}", level="ERROR")

    def insert_tip(self, row):
        sql = """
        INSERT INTO rain_tip_events (
            time,
            received_at,
            source,
            sensor_id,
            event,
            mm,
            pulse_total,
            uptime_ms,
            time_valid,
            epoch_s,
            topic,
            payload
        )
        VALUES (
            %(time)s,
            %(received_at)s,
            %(source)s,
            %(sensor_id)s,
            %(event)s,
            %(mm)s,
            %(pulse_total)s,
            %(uptime_ms)s,
            %(time_valid)s,
            %(epoch_s)s,
            %(topic)s,
            %(payload)s::jsonb
        );
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, row)
            conn.commit()