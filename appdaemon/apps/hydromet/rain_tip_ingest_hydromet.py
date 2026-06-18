import json
from datetime import datetime, timezone

import appdaemon.plugins.hass.hassapi as hass
import psycopg


class RainTipIngestHydromet(hass.Hass):
    """MQTT rain-tip ingest for the hydromet core data model.

    First intended use:
    - test logger only
    - writes to hydromet.event_observations
    - leaves public.rain_tip_events untouched
    """

    def initialize(self):
        self.topic = self.args["topic"]
        self.mqtt_namespace = self.args.get("mqtt_namespace", "mqtt")

        self.series_key_prefix = self.args.get("series_key_prefix", "rain")
        self.location_key = self.args.get("location_key")
        self.location_type = self.args.get("location_type", "test")
        self.instrument_model = self.args.get("instrument_model")
        self.sensor_type = self.args.get("sensor_type", "tipping_bucket")
        self.logger_type = self.args.get("logger_type")
        self.firmware = self.args.get("firmware")

        self.db_config = {
            "host": self.args["db_host"],
            "port": int(self.args.get("db_port", 5432)),
            "dbname": self.args["db_name"],
            "user": self.args["db_user"],
            "password": self.args["db_password"],
        }

        self.log(
            f"RainTipIngestHydromet startar. "
            f"Lyssnar på MQTT topic: {self.topic}"
        )

        self.ensure_core_tables_available()

        self.listen_event(
            self.on_mqtt_message,
            "MQTT_MESSAGE",
            namespace=self.mqtt_namespace,
            topic=self.topic,
        )

    def get_connection(self):
        return psycopg.connect(**self.db_config)

    def ensure_core_tables_available(self):
        """Check that migration 001_core_observations.sql has been run."""
        sql = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'hydromet'
            AND table_name = 'event_observations'
        );
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                exists = cur.fetchone()[0]

        if not exists:
            raise RuntimeError(
                "hydromet.event_observations saknas. "
                "Kör sql/hydromet/001_core_observations.sql först."
            )

        self.log("Hydromet core-tabeller finns.")

    def on_mqtt_message(self, event_name, data, kwargs):
        try:
            topic = data.get("topic")
            payload_raw = data.get("payload")

            if topic != self.topic:
                return

            payload = payload_raw if isinstance(payload_raw, dict) else json.loads(payload_raw)

            if payload.get("event") != "rain_tip":
                self.log(f"Ignorerar MQTT-event utan rain_tip: {payload}", level="DEBUG")
                return

            row = self.parse_payload(topic, payload)
            self.insert_tip(row)

            self.log(
                f"Hydromet rain_tip sparad: "
                f"series_key={row['series_key']}, "
                f"value={row['value']} {row['unit']}, "
                f"counter={row['counter']}"
            )

        except Exception as err:
            self.log(f"Fel vid hydromet-hantering av regnpuls: {err}", level="ERROR")

    def parse_payload(self, topic, payload):
        time_valid = bool(payload.get("time_valid", False))
        epoch_s = payload.get("epoch_s")

        if time_valid and epoch_s:
            event_time = datetime.fromtimestamp(int(epoch_s), tz=timezone.utc)
        else:
            event_time = datetime.now(timezone.utc)

        received_at = datetime.now(timezone.utc)

        source = payload.get("source", "unknown")
        sensor_id = payload.get("sensor_id", "unknown")

        # Stable human-readable key. This is not a UUID.
        # Example: rain.test.tb4_test
        series_key = f"{self.series_key_prefix}.{source}.{sensor_id}"

        metadata = {
            "topic": topic,
            "source": source,
            "sensor_id": sensor_id,
            "time_valid": time_valid,
            "epoch_s": epoch_s,
            "uptime_ms": payload.get("uptime_ms"),
            "raw_pulse_total": payload.get("raw_pulse_total"),
            "ignored_pulse_total": payload.get("ignored_pulse_total"),
            "interval_ms": payload.get("interval_ms"),
            "gpio": payload.get("gpio"),
        }

        return {
            "time": event_time,
            "received_at": received_at,
            "series_key": series_key,
            "source": source,
            "sensor_id": sensor_id,
            "event_type": "rain_tip",
            "value": float(payload["mm"]),
            "unit": "mm",
            "counter": payload.get("pulse_total"),
            "raw_payload": json.dumps(payload),
            "metadata": json.dumps(metadata),
        }

    def insert_tip(self, row):
        sql = """
        WITH series AS (
            INSERT INTO hydromet.observation_series (
                series_key,
                observed_property,
                medium,
                location_key,
                location_type,
                unit,
                source_type,
                resolution_type,
                is_primary,
                is_active,
                is_test,
                metadata
            )
            VALUES (
                %(series_key)s,
                'precipitation_tip',
                'precipitation',
                %(location_key)s,
                %(location_type)s,
                'mm',
                %(source_type)s,
                'event',
                false,
                true,
                true,
                %(series_metadata)s::jsonb
            )
            ON CONFLICT (series_key) DO UPDATE
            SET
                is_active = true,
                metadata = hydromet.observation_series.metadata || EXCLUDED.metadata
            RETURNING series_id
        ),
        active_series AS (
            SELECT series_id FROM series
            UNION ALL
            SELECT series_id
            FROM hydromet.observation_series
            WHERE series_key = %(series_key)s
            LIMIT 1
        ),
        setup AS (
            INSERT INTO hydromet.measurement_setups (
                series_id,
                valid_from,
                instrument_model,
                sensor_type,
                logger_type,
                firmware,
                notes,
                metadata
            )
            SELECT
                series_id,
                '2026-01-01 00:00:00+00'::timestamptz,
                %(instrument_model)s,
                %(sensor_type)s,
                %(logger_type)s,
                %(firmware)s,
                'Auto-created by RainTipIngestHydromet for test ingest.',
                '{}'::jsonb
            FROM active_series
            WHERE NOT EXISTS (
                SELECT 1
                FROM hydromet.measurement_setups ms
                WHERE ms.series_id = active_series.series_id
                AND ms.valid_to IS NULL
            )
            RETURNING setup_id, series_id
        ),
        active_setup AS (
            SELECT setup_id, series_id FROM setup
            UNION ALL
            SELECT ms.setup_id, ms.series_id
            FROM hydromet.measurement_setups ms
            JOIN active_series s ON s.series_id = ms.series_id
            WHERE ms.valid_to IS NULL
            LIMIT 1
        )
        INSERT INTO hydromet.event_observations (
            time,
            received_at,
            series_id,
            setup_id,
            event_type,
            value,
            unit,
            counter,
            quality_flag,
            raw_payload,
            metadata
        )
        SELECT
            %(time)s,
            %(received_at)s,
            active_setup.series_id,
            active_setup.setup_id,
            %(event_type)s,
            %(value)s,
            %(unit)s,
            %(counter)s,
            'unchecked',
            %(raw_payload)s::jsonb,
            %(metadata)s::jsonb
        FROM active_setup
        ON CONFLICT (time, series_id, event_type) DO NOTHING;
        """

        params = {
            **row,
            "location_key": self.location_key,
            "location_type": self.location_type,
            "source_type": row["source"],
            "instrument_model": self.instrument_model,
            "sensor_type": self.sensor_type,
            "logger_type": self.logger_type,
            "firmware": self.firmware,
            "series_metadata": json.dumps({
                "auto_created_by": "RainTipIngestHydromet",
                "source": row["source"],
                "sensor_id": row["sensor_id"],
                "topic": self.topic,
            }),
        }

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()
