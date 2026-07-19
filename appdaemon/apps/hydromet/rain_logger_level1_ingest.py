"""Generic Level 1 rain logger MQTT -> Hydromet event ingest.

One class is instantiated twice:
- test:       hydromet.rain_logger_test_events
- production: hydromet.event_observations

The definitions are identical apart from ``target_table``.

Idempotency and receiver state are stored in PostgreSQL. Event row,
idempotency key and receiver state are committed transactionally.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from uuid import UUID, uuid4

import appdaemon.plugins.mqtt.mqttapi as mqtt
import psycopg
from psycopg import sql


class RainLoggerLevel1Ingest(mqtt.Mqtt):
    APP_VERSION = "rain-logger-level1-ingest-0.3.0"
    TIP_SCHEMA = "rainlens.logger.tip_event.v1"
    STATE_SCHEMA = "rainlens.logger.channel_state.v1"

    ALLOWED_TARGETS = {
        "hydromet.event_observations",
        "hydromet.rain_logger_test_events",
    }

    def initialize(self) -> None:
        self.mqtt_namespace = str(self.args.get("mqtt_namespace", "mqtt"))
        self.event_name = str(self.args.get("event_name", "MQTT_MESSAGE"))

        self.topic_root = str(self.args["topic_root"]).rstrip("/")
        self.tip_topic = f"{self.topic_root}/tip"
        self.state_topic = f"{self.topic_root}/state"
        self.topic_wildcard = f"{self.topic_root}/#"

        self.site_id = str(self.args["site_id"])
        self.logger_id = str(self.args["logger_id"])
        self.channel_id = str(self.args["channel_id"])
        self.sensor_id = str(self.args["sensor_id"])
        self.series_key = str(self.args["series_key"])

        self.mm_per_tip = float(self.args.get("mm_per_tip", 0.2))
        self.state_grace_seconds = float(
            self.args.get("state_grace_seconds", 2.0)
        )

        self.target_table = str(self.args["target_table"])
        if self.target_table not in self.ALLOWED_TARGETS:
            raise ValueError(
                f"Otillåten target_table: {self.target_table!r}"
            )

        target_schema, target_name = self.target_table.split(".", 1)
        self._target_identifier = sql.Identifier(target_schema, target_name)

        self.db_config = {
            "host": self.args["db_host"],
            "port": int(self.args.get("db_port", 5432)),
            "dbname": self.args["db_name"],
            "user": self.args["db_user"],
            "password": self.args["db_password"],
            "connect_timeout": int(self.args.get("db_connect_timeout", 5)),
            "application_name": str(
                self.args.get(
                    "db_application_name",
                    "appdaemon-rain-logger-level1-ingest",
                )
            ),
        }

        self._lock = threading.RLock()
        self._pending_state_payload: Optional[Dict[str, Any]] = None
        self._pending_state_timer: Optional[str] = None

        self._series_id, self._setup_id = self._resolve_registry()
        self._ensure_target_ready()
        self._last_seen_total, self._last_boot_count = (
            self._load_ingest_state()
        )

        # Register exact event filters before subscribing. This ordering is
        # deliberate: a retained state can be delivered immediately when the
        # MQTT subscription is created and must not arrive before the listener.
        self._tip_listener = self.listen_event(
            self._on_mqtt_message,
            self.event_name,
            topic=self.tip_topic,
            namespace=self.mqtt_namespace,
        )
        self._state_listener = self.listen_event(
            self._on_mqtt_message,
            self.event_name,
            topic=self.state_topic,
            namespace=self.mqtt_namespace,
        )

        # The operational runbook uses the channel wildcard. The callbacks
        # still filter on the two canonical exact topics above.
        self.mqtt_subscribe(
            self.topic_wildcard,
            namespace=self.mqtt_namespace,
        )

        self.log(
            "RainLoggerLevel1Ingest startad: version=%s target=%s "
            "series_id=%s setup_id=%s last_seen=%s mqtt_namespace=%s "
            "subscription=%s mqtt_connected=%s",
            self.APP_VERSION,
            self.target_table,
            self._series_id,
            self._setup_id,
            self._last_seen_total,
            self.mqtt_namespace,
            self.topic_wildcard,
            self.is_client_connected(namespace=self.mqtt_namespace),
        )

    def get_connection(self):
        return psycopg.connect(**self.db_config)

    def _resolve_registry(self) -> Tuple[UUID, UUID]:
        query = """
        SELECT s.series_id, ms.setup_id
        FROM hydromet.observation_series s
        JOIN hydromet.measurement_setups ms
          ON ms.series_id = s.series_id
         AND ms.valid_to IS NULL
        WHERE s.series_key = %s
        ORDER BY ms.valid_from DESC
        LIMIT 1;
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (self.series_key,))
                row = cur.fetchone()

        if row is None:
            raise RuntimeError(
                "Loggerns aktiva series/setup saknas för series_key="
                f"{self.series_key!r}."
            )

        return row[0], row[1]

    def _ensure_target_ready(self) -> None:
        query = """
        SELECT
            to_regclass(%s),
            to_regclass('hydromet.event_ingest_keys'),
            to_regclass('hydromet.event_ingest_state');
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (self.target_table,))
                target, ledger, ingest_state = cur.fetchone()

        if target is None:
            raise RuntimeError(f"Måltabellen saknas: {self.target_table}")
        if ledger is None or ingest_state is None:
            raise RuntimeError(
                "event_ingest_keys/event_ingest_state saknas. "
                "Kör korrigerad 002 först."
            )

    def _load_ingest_state(self) -> Tuple[Optional[int], Optional[int]]:
        query = """
        SELECT last_seen_pulse_total, last_boot_count
        FROM hydromet.event_ingest_state
        WHERE target_table = %s
          AND series_id = %s;
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    (self.target_table, self._series_id),
                )
                row = cur.fetchone()

        if row is None:
            return None, None

        return (
            self._optional_nonnegative_int(row[0]),
            self._optional_nonnegative_int(row[1]),
        )

    def _on_mqtt_message(
        self,
        event_name: str,
        data: Dict[str, Any],
        kwargs: Dict[str, Any],
    ) -> None:
        topic = data.get("topic")
        payload_raw = data.get("payload")

        if not isinstance(topic, str) or payload_raw is None:
            return

        try:
            payload = self._parse_payload(payload_raw)

            if topic == self.tip_topic:
                self._validate_tip_payload(payload)
                self._handle_tip(payload)
            elif topic == self.state_topic:
                self._validate_state_payload(payload)
                self._handle_state(payload)

        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            self.log(
                "Ogiltigt logger-meddelande på %s: %s",
                topic,
                exc,
                level="ERROR",
            )
        except Exception as exc:
            self.log(
                "Fel i Level 1-ingest target=%s topic=%s: %s",
                self.target_table,
                topic,
                exc,
                level="ERROR",
            )

    @staticmethod
    def _parse_payload(payload_raw: Any) -> Dict[str, Any]:
        payload = (
            dict(payload_raw)
            if isinstance(payload_raw, dict)
            else json.loads(payload_raw)
        )
        if not isinstance(payload, dict):
            raise ValueError("payload är inte ett JSON-objekt")
        return payload

    def _validate_identity(self, payload: Dict[str, Any]) -> None:
        expected = {
            "site_id": self.site_id,
            "logger_id": self.logger_id,
            "channel_id": self.channel_id,
            "sensor_id": self.sensor_id,
        }
        mismatches = {
            field: (payload.get(field), expected_value)
            for field, expected_value in expected.items()
            if payload.get(field) is None
            or str(payload[field]) != expected_value
        }
        if mismatches:
            raise ValueError(f"identitetsavvikelse: {mismatches}")

    def _validate_tip_payload(self, payload: Dict[str, Any]) -> None:
        self._validate_identity(payload)

        if payload.get("schema") != self.TIP_SCHEMA:
            raise ValueError(
                f"fel tip-schema: {payload.get('schema')!r}"
            )
        if payload.get("event") != "rain_tip":
            raise ValueError(
                f"fel tip-event: {payload.get('event')!r}"
            )

        pulse_total = self._required_nonnegative_int(
            payload,
            "pulse_total",
        )
        self._required_nonnegative_int(payload, "uptime_ms")
        self._required_bool(payload, "time_valid")

        mm = self._required_nonnegative_number(payload, "mm")
        if not self._numbers_equal(mm, self.mm_per_tip):
            raise ValueError(
                f"tip.mm={mm} stämmer inte med mm_per_tip="
                f"{self.mm_per_tip}"
            )

        # Parsing here gives a precise validation error before DB handling.
        if pulse_total < 0:
            raise ValueError("pulse_total får inte vara negativ")

    def _validate_state_payload(self, payload: Dict[str, Any]) -> None:
        self._validate_identity(payload)

        if payload.get("schema") != self.STATE_SCHEMA:
            raise ValueError(
                f"fel state-schema: {payload.get('schema')!r}"
            )

        pulse_total = self._required_nonnegative_int(
            payload,
            "pulse_total",
        )
        self._required_nonnegative_int(payload, "uptime_ms")
        self._required_bool(payload, "time_valid")

        payload_mm_per_tip = self._required_nonnegative_number(
            payload,
            "mm_per_tip",
        )
        if not self._numbers_equal(
            payload_mm_per_tip,
            self.mm_per_tip,
        ):
            raise ValueError(
                f"state.mm_per_tip={payload_mm_per_tip} stämmer inte "
                f"med konfigurationen {self.mm_per_tip}"
            )

        if "rain_total_mm" in payload:
            rain_total_mm = self._required_nonnegative_number(
                payload,
                "rain_total_mm",
            )
            expected_total = pulse_total * self.mm_per_tip
            if not self._numbers_equal(
                rain_total_mm,
                expected_total,
                abs_tol=1e-6,
            ):
                raise ValueError(
                    f"state.rain_total_mm={rain_total_mm} stämmer inte "
                    f"med pulse_total×mm_per_tip={expected_total}"
                )

    def _handle_tip(self, payload: Dict[str, Any]) -> None:
        pulse_total = self._required_nonnegative_int(
            payload,
            "pulse_total",
        )

        with self._lock:
            previous = self._last_seen_total

            if previous is not None and pulse_total < previous:
                self.log(
                    "COUNTER REGRESSION target=%s previous=%s reported=%s",
                    self.target_table,
                    previous,
                    pulse_total,
                    level="ERROR",
                )
                return

            if previous is not None and pulse_total == previous:
                return

            missing_before = (
                0
                if previous is None
                else max(0, pulse_total - previous - 1)
            )

            self._store_tip_transaction(
                payload,
                previous,
                pulse_total,
                missing_before,
            )
            self._last_seen_total = pulse_total
            self.log(
                "Rain tip lagrad: target=%s counter=%s value=%s mm "
                "gap_before=%s",
                self.target_table,
                pulse_total,
                self.mm_per_tip,
                missing_before,
            )

    def _handle_state(self, payload: Dict[str, Any]) -> None:
        pulse_total = self._required_nonnegative_int(
            payload,
            "pulse_total",
        )
        boot_count = self._optional_nonnegative_int(
            payload.get("boot_count")
        )

        with self._lock:
            previous = self._last_seen_total

            if previous is not None and pulse_total < previous:
                self.log(
                    "COUNTER REGRESSION i state target=%s "
                    "previous=%s reported=%s",
                    self.target_table,
                    previous,
                    pulse_total,
                    level="ERROR",
                )
                return

            if previous is not None and pulse_total == previous:
                self._store_state_only(
                    pulse_total,
                    boot_count,
                    "state_equal",
                    payload,
                )
                self._last_boot_count = boot_count
                return

            self._pending_state_payload = dict(payload)

            if self._pending_state_timer is not None:
                self.cancel_timer(
                    self._pending_state_timer,
                    silent=True,
                )

            self._pending_state_timer = self.run_in(
                self._reconcile_pending_state,
                self.state_grace_seconds,
            )

    def _reconcile_pending_state(self, kwargs: Dict[str, Any]) -> None:
        with self._lock:
            payload = self._pending_state_payload
            self._pending_state_payload = None
            self._pending_state_timer = None

            if payload is None:
                return

            pulse_total = self._optional_nonnegative_int(
                payload.get("pulse_total")
            )
            if pulse_total is None:
                return

            boot_count = self._optional_nonnegative_int(
                payload.get("boot_count")
            )
            previous = self._last_seen_total

            if previous is None:
                self._store_state_only(
                    pulse_total,
                    boot_count,
                    "initial_baseline",
                    payload,
                )
                self._last_seen_total = pulse_total
                self._last_boot_count = boot_count
                self.log(
                    "Retained baseline satt: target=%s pulse_total=%s "
                    "boot_count=%s",
                    self.target_table,
                    pulse_total,
                    boot_count,
                )
                return

            if pulse_total < previous:
                self.log(
                    "COUNTER REGRESSION vid reconciliation target=%s "
                    "previous=%s reported=%s",
                    self.target_table,
                    previous,
                    pulse_total,
                    level="ERROR",
                )
                return

            if pulse_total == previous:
                self._store_state_only(
                    pulse_total,
                    boot_count,
                    "state_caught_up",
                    payload,
                )
                self._last_boot_count = boot_count
                return

            self._store_recovery_transaction(
                payload,
                previous,
                pulse_total,
                pulse_total - previous,
                boot_count,
            )
            self._last_seen_total = pulse_total
            self._last_boot_count = boot_count
            self.log(
                "Rain recovery lagrad: target=%s previous=%s "
                "pulse_total=%s recovered_tips=%s recovered_mm=%s",
                self.target_table,
                previous,
                pulse_total,
                pulse_total - previous,
                (pulse_total - previous) * self.mm_per_tip,
                level="WARNING",
            )

    def _store_tip_transaction(
        self,
        payload: Dict[str, Any],
        previous_total: Optional[int],
        pulse_total: int,
        missing_before: int,
    ) -> None:
        received_at = datetime.now(timezone.utc)
        event_time, time_source = self._tip_time(
            payload,
            received_at,
        )

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                if missing_before > 0 and previous_total is not None:
                    self._insert_recovery(
                        cur,
                        payload,
                        received_at,
                        previous_total,
                        pulse_total - 1,
                        missing_before,
                        "tip_counter_gap",
                    )

                metadata = self._base_metadata(payload)
                metadata.update(
                    {
                        "mqtt_topic": self.tip_topic,
                        "time_source": time_source,
                        "time_precision": (
                            "second"
                            if time_source == "logger_epoch_s"
                            else "received_at"
                        ),
                        "gap_before_tips": missing_before,
                        "gap_before_mm": (
                            missing_before * self.mm_per_tip
                        ),
                        "raw_pulse_total": payload.get(
                            "raw_pulse_total"
                        ),
                        "ignored_pulse_total": payload.get(
                            "ignored_pulse_total"
                        ),
                        "interval_ms": payload.get("interval_ms"),
                        "uptime_ms": payload.get("uptime_ms"),
                    }
                )

                self._insert_event(
                    cur=cur,
                    source_event_key=self._tip_source_event_key(
                        pulse_total
                    ),
                    event_time=event_time,
                    received_at=received_at,
                    event_type="rain_tip",
                    value=self.mm_per_tip,
                    unit="mm",
                    counter=pulse_total,
                    quality_flag=(
                        "counter_gap_before"
                        if missing_before > 0
                        else (
                            "unchecked"
                            if time_source == "logger_epoch_s"
                            else "time_invalid"
                        )
                    ),
                    raw_payload=payload,
                    metadata=metadata,
                )

                self._upsert_ingest_state(
                    cur,
                    pulse_total,
                    self._last_boot_count,
                    "tip",
                    payload,
                )

            conn.commit()

    def _store_recovery_transaction(
        self,
        payload: Dict[str, Any],
        previous_total: int,
        pulse_total: int,
        recovered_tips: int,
        boot_count: Optional[int],
    ) -> None:
        received_at = datetime.now(timezone.utc)

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                self._insert_recovery(
                    cur,
                    payload,
                    received_at,
                    previous_total,
                    pulse_total,
                    recovered_tips,
                    "retained_state",
                )
                self._upsert_ingest_state(
                    cur,
                    pulse_total,
                    boot_count,
                    "state_recovery",
                    payload,
                )
            conn.commit()

    def _store_state_only(
        self,
        pulse_total: int,
        boot_count: Optional[int],
        reason: str,
        payload: Dict[str, Any],
    ) -> None:
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                self._upsert_ingest_state(
                    cur,
                    pulse_total,
                    boot_count,
                    reason,
                    payload,
                )
            conn.commit()

    def _insert_recovery(
        self,
        cur,
        payload: Dict[str, Any],
        received_at: datetime,
        previous_total: int,
        counter_to: int,
        recovered_tips: int,
        recovery_origin: str,
    ) -> None:
        if recovered_tips <= 0:
            return

        counter_from = previous_total + 1
        metadata = self._base_metadata(payload)
        metadata.update(
            {
                "mqtt_topic": (
                    self.state_topic
                    if recovery_origin == "retained_state"
                    else self.tip_topic
                ),
                "recovery_origin": recovery_origin,
                "counter_from": counter_from,
                "counter_to": counter_to,
                "recovered_tips": recovered_tips,
                "recovered_mm": recovered_tips * self.mm_per_tip,
                "temporal_distribution_uncertain": True,
                "time_source": "received_at",
                "boot_count": payload.get(
                    "boot_count",
                    self._last_boot_count,
                ),
            }
        )

        self._insert_event(
            cur=cur,
            source_event_key=self._recovery_source_event_key(
                counter_from,
                counter_to,
            ),
            event_time=received_at,
            received_at=received_at,
            event_type="rain_recovery",
            value=recovered_tips * self.mm_per_tip,
            unit="mm",
            counter=counter_to,
            quality_flag="time_distribution_uncertain",
            raw_payload=payload,
            metadata=metadata,
        )

    def _insert_event(
        self,
        cur,
        source_event_key: str,
        event_time: datetime,
        received_at: datetime,
        event_type: str,
        value: Optional[float],
        unit: Optional[str],
        counter: Optional[int],
        quality_flag: str,
        raw_payload: Dict[str, Any],
        metadata: Dict[str, Any],
    ) -> bool:
        event_id = uuid4()

        cur.execute(
            """
            INSERT INTO hydromet.event_ingest_keys (
                target_table,
                source_event_key,
                event_time,
                event_id,
                series_id,
                event_type,
                counter
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (target_table, source_event_key) DO NOTHING
            RETURNING source_event_key;
            """,
            (
                self.target_table,
                source_event_key,
                event_time,
                event_id,
                self._series_id,
                event_type,
                counter,
            ),
        )

        if cur.fetchone() is None:
            return False

        insert_query = sql.SQL(
            """
            INSERT INTO {} (
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
                metadata,
                event_id
            )
            VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s::jsonb, %s::jsonb, %s
            );
            """
        ).format(self._target_identifier)

        cur.execute(
            insert_query,
            (
                event_time,
                received_at,
                self._series_id,
                self._setup_id,
                event_type,
                value,
                unit,
                counter,
                quality_flag,
                json.dumps(
                    raw_payload,
                    sort_keys=True,
                    default=str,
                ),
                json.dumps(
                    metadata,
                    sort_keys=True,
                    default=str,
                ),
                event_id,
            ),
        )
        return True

    def _upsert_ingest_state(
        self,
        cur,
        pulse_total: int,
        boot_count: Optional[int],
        reason: str,
        payload: Dict[str, Any],
    ) -> None:
        metadata = {
            "app_version": self.APP_VERSION,
            "topic_root": self.topic_root,
            "site_id": self.site_id,
            "logger_id": self.logger_id,
            "channel_id": self.channel_id,
            "sensor_id": self.sensor_id,
            "reason": reason,
            "payload_schema": payload.get("schema"),
        }

        cur.execute(
            """
            INSERT INTO hydromet.event_ingest_state (
                target_table,
                series_id,
                last_seen_pulse_total,
                last_boot_count,
                updated_at,
                metadata
            )
            VALUES (%s, %s, %s, %s, now(), %s::jsonb)
            ON CONFLICT (target_table, series_id) DO UPDATE
            SET
                last_seen_pulse_total = EXCLUDED.last_seen_pulse_total,
                last_boot_count = EXCLUDED.last_boot_count,
                updated_at = now(),
                metadata = EXCLUDED.metadata
            WHERE
                hydromet.event_ingest_state.last_seen_pulse_total
                <= EXCLUDED.last_seen_pulse_total
            RETURNING last_seen_pulse_total;
            """,
            (
                self.target_table,
                self._series_id,
                pulse_total,
                boot_count,
                json.dumps(metadata, sort_keys=True),
            ),
        )

        row = cur.fetchone()
        if row is None or int(row[0]) != pulse_total:
            raise RuntimeError(
                "Databasens ingest-state vägrade en icke-monoton uppdatering"
            )

    def _tip_time(
        self,
        payload: Dict[str, Any],
        received_at: datetime,
    ) -> Tuple[datetime, str]:
        time_valid = self._required_bool(payload, "time_valid")
        epoch_s = self._optional_nonnegative_int(payload.get("epoch_s"))

        if time_valid and epoch_s is not None and epoch_s > 0:
            try:
                return (
                    datetime.fromtimestamp(epoch_s, tz=timezone.utc),
                    "logger_epoch_s",
                )
            except (OverflowError, OSError, ValueError):
                pass

        return received_at, "received_at_invalid_logger_time"

    def _base_metadata(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "site_id": self.site_id,
            "logger_id": self.logger_id,
            "channel_id": self.channel_id,
            "sensor_id": self.sensor_id,
            "sensor_type": payload.get("sensor_type"),
            "schema": payload.get("schema"),
            "target_table": self.target_table,
            "topic_root": self.topic_root,
            "mm_per_tip": self.mm_per_tip,
            "firmware": payload.get("firmware"),
            "app_version": self.APP_VERSION,
        }

    def _tip_source_event_key(self, pulse_total: int) -> str:
        return (
            f"{self.site_id}|{self.logger_id}|{self.channel_id}|"
            f"{self.sensor_id}|tip|counter={pulse_total}"
        )

    def _recovery_source_event_key(
        self,
        counter_from: int,
        counter_to: int,
    ) -> str:
        return (
            f"{self.site_id}|{self.logger_id}|{self.channel_id}|"
            f"{self.sensor_id}|recovery|"
            f"from={counter_from}|to={counter_to}"
        )

    @staticmethod
    def _numbers_equal(
        left: float,
        right: float,
        *,
        abs_tol: float = 1e-9,
    ) -> bool:
        return abs(float(left) - float(right)) <= abs_tol

    def _required_nonnegative_number(
        self,
        payload: Dict[str, Any],
        field: str,
    ) -> float:
        value = payload.get(field)
        if value is None or isinstance(value, bool):
            raise ValueError(
                f"Payload saknar giltigt icke-negativt tal: {field}"
            )
        try:
            numeric = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Payload saknar giltigt icke-negativt tal: {field}"
            ) from exc
        if numeric < 0:
            raise ValueError(
                f"Payload saknar giltigt icke-negativt tal: {field}"
            )
        return numeric

    @staticmethod
    def _required_bool(
        payload: Dict[str, Any],
        field: str,
    ) -> bool:
        value = payload.get(field)
        if not isinstance(value, bool):
            raise ValueError(
                f"Payload saknar giltigt booleskt värde: {field}"
            )
        return value

    def _required_nonnegative_int(
        self,
        payload: Dict[str, Any],
        field: str,
    ) -> int:
        value = self._optional_nonnegative_int(payload.get(field))
        if value is None:
            raise ValueError(
                f"Payload saknar giltigt icke-negativt heltal: {field}"
            )
        return value

    @staticmethod
    def _optional_nonnegative_int(value: Any) -> Optional[int]:
        if value is None or isinstance(value, bool):
            return None

        try:
            numeric = int(value)
        except (TypeError, ValueError):
            return None

        return numeric if numeric >= 0 else None
