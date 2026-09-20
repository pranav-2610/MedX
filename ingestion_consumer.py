"""Trusted Kafka/Redpanda ingestion boundary for vital-event messages."""

import base64
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from threading import Event
from typing import Any, Protocol
from uuid import UUID

from pydantic import ValidationError

from dead_letter import (
    DeadLetterConfig,
    DeadLetterPublisher,
    KafkaDeadLetterPublisher,
    LoggingDeadLetterPublisher,
)
from models import DeadLetterRecord, IngestedVitalEvent, VitalEvent
from patient_profiles import PatientProfileRepository


LOGGER = logging.getLogger(__name__)


class KafkaMessage(Protocol):
    def value(self) -> bytes | None: ...

    def key(self) -> bytes | None: ...

    def error(self) -> Any: ...

    def topic(self) -> str: ...

    def partition(self) -> int: ...

    def offset(self) -> int: ...


class KafkaConsumerClient(Protocol):
    def subscribe(self, topics: list[str]) -> None: ...

    def poll(self, timeout: float) -> KafkaMessage | None: ...

    def commit(self, message: KafkaMessage, asynchronous: bool) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class ConsumerConfig:
    bootstrap_servers: str = "localhost:19092"
    topic: str = "patient_vitals"
    group_id: str = "medx-vital-ingestion"
    auto_offset_reset: str = "earliest"
    poll_timeout_seconds: float = 1.0
    dead_letter_topic: str = "patient_vitals_dlq"
    deduplication_capacity: int = 10_000

    def as_kafka_config(self) -> dict[str, str | bool]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": False,
        }


@dataclass(frozen=True)
class RejectedMessage:
    """Safe observability metadata for a message blocked at ingestion."""

    category: str
    reason: str
    topic: str
    partition: int
    offset: int


@dataclass
class ConsumerMetrics:
    accepted: int = 0
    rejected: int = 0
    dead_lettered: int = 0
    duplicates: int = 0
    unknown_patients: int = 0
    processing_errors: int = 0


class VitalIngestionConsumer:
    """Deserialize, validate, normalize, and release trusted vital observations."""

    def __init__(
        self,
        profile_repository: PatientProfileRepository,
        config: ConsumerConfig | None = None,
        *,
        client: KafkaConsumerClient | None = None,
        clock: Callable[[], datetime] | None = None,
        on_rejected: Callable[[RejectedMessage], None] | None = None,
        dead_letter_publisher: DeadLetterPublisher | None = None,
    ) -> None:
        self.config = config or ConsumerConfig()
        self._validate_config(self.config)
        injected_client = client is not None
        self._client = client or self._create_confluent_client(self.config)
        self._profiles = profile_repository
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._on_rejected = on_rejected
        self._dead_letter = dead_letter_publisher or self._default_dead_letter_publisher(
            injected_client
        )
        self.metrics = ConsumerMetrics()
        self._closed = False
        self._recent_event_ids: set[UUID] = set()
        self._recent_event_order: deque[UUID] = deque()
        self._client.subscribe([self.config.topic])

    def __enter__(self) -> "VitalIngestionConsumer":
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def consume_once(self, on_event: Callable[[IngestedVitalEvent], None]) -> bool:
        """Poll and process one record; return ``True`` only for accepted events."""
        if self._closed:
            raise RuntimeError("consumer is closed")
        message = self._client.poll(self.config.poll_timeout_seconds)
        if message is None:
            return False
        if message.error() is not None:
            self.metrics.processing_errors += 1
            LOGGER.error("Kafka consumer error: %s", message.error())
            return False

        ingested_event = self._deserialize_and_validate(message)
        if ingested_event is None:
            return False

        try:
            on_event(ingested_event)
        except Exception:
            self.metrics.processing_errors += 1
            LOGGER.exception(
                "Downstream boundary callback failed for topic=%s partition=%s offset=%s; "
                "offset was not committed",
                message.topic(),
                message.partition(),
                message.offset(),
            )
            return False

        self._remember_event_id(ingested_event.event.event_id)
        self._client.commit(message, asynchronous=False)
        self.metrics.accepted += 1
        return True

    def run(
        self,
        on_event: Callable[[IngestedVitalEvent], None],
        *,
        stop_event: Event | None = None,
    ) -> None:
        """Continuously poll until stopped, keeping malformed payloads isolated."""
        try:
            while stop_event is None or not stop_event.is_set():
                self.consume_once(on_event)
        finally:
            self.close()

    def close(self) -> None:
        if not self._closed:
            try:
                self._dead_letter.close()
            finally:
                self._client.close()
                self._closed = True

    def _deserialize_and_validate(self, message: KafkaMessage) -> IngestedVitalEvent | None:
        try:
            raw_value = message.value()
            if not isinstance(raw_value, bytes):
                self._reject(message, "incompatible_payload", "message value must be UTF-8 JSON bytes")
                return None
            try:
                payload = json.loads(raw_value.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self._reject(message, "malformed_message", "payload is not valid UTF-8 JSON")
                return None
            try:
                event = VitalEvent.model_validate(payload)
            except ValidationError as error:
                self._reject(message, "schema_validation", self._validation_reason(error))
                return None

            raw_key = message.key()
            if raw_key is not None:
                if not isinstance(raw_key, bytes):
                    self._reject(message, "key_mismatch", "message key must be UTF-8 bytes")
                    return None
                try:
                    key_patient_id = raw_key.decode("utf-8")
                except UnicodeDecodeError:
                    self._reject(message, "key_mismatch", "message key is not valid UTF-8")
                    return None
                if key_patient_id != event.patient_id:
                    self._reject(message, "key_mismatch", "message key must match payload patient_id")
                    return None

            if self._profiles.get(event.patient_id) is None:
                self.metrics.unknown_patients += 1
                self._reject(message, "unknown_patient", "patient_id has no loaded static profile")
                return None
            if event.event_id in self._recent_event_ids:
                self.metrics.duplicates += 1
                self._reject(message, "duplicate_event", "event_id was already accepted by this consumer")
                return None

            return IngestedVitalEvent(
                event=event,
                ingested_at=self._clock(),
                kafka_topic=message.topic(),
                kafka_partition=message.partition(),
                kafka_offset=message.offset(),
            )
        except Exception as error:
            self.metrics.processing_errors += 1
            LOGGER.exception("Unexpected ingestion failure: %s", error)
            self._reject(message, "unexpected_processing_error", "unexpected ingestion processing failure")
            return None

    def _reject(self, message: KafkaMessage, category: str, reason: str) -> None:
        rejected = RejectedMessage(
            category=category,
            reason=reason,
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
        )
        self.metrics.rejected += 1
        LOGGER.warning(
            "Rejected vital message category=%s topic=%s partition=%s offset=%s",
            rejected.category,
            rejected.topic,
            rejected.partition,
            rejected.offset,
        )
        record = DeadLetterRecord(
            category=category,
            reason=reason,
            ingested_at=self._clock(),
            source_topic=rejected.topic,
            source_partition=rejected.partition,
            source_offset=rejected.offset,
            original_payload_b64=self._encoded_bytes(message.value()),
            original_key_b64=self._encoded_bytes(message.key()),
        )
        try:
            self._dead_letter.publish(record)
        except Exception:
            self.metrics.processing_errors += 1
            LOGGER.exception("Dead-letter publishing failed; source offset was not committed")
            return
        self.metrics.dead_lettered += 1
        if self._on_rejected is not None:
            try:
                self._on_rejected(rejected)
            except Exception:
                self.metrics.processing_errors += 1
                LOGGER.exception("Rejected-message hook failed; continuing with rejection commit")
        # Commit only after the rejection has reached its explicit observable
        # sink. This prevents poison-message loops without silently discarding.
        self._client.commit(message, asynchronous=False)

    def _remember_event_id(self, event_id: UUID) -> None:
        self._recent_event_ids.add(event_id)
        self._recent_event_order.append(event_id)
        if len(self._recent_event_order) > self.config.deduplication_capacity:
            expired_event_id = self._recent_event_order.popleft()
            self._recent_event_ids.remove(expired_event_id)

    def _default_dead_letter_publisher(self, injected_client: bool) -> DeadLetterPublisher:
        if injected_client:
            return LoggingDeadLetterPublisher()
        return KafkaDeadLetterPublisher(
            DeadLetterConfig(
                bootstrap_servers=self.config.bootstrap_servers,
                topic=self.config.dead_letter_topic,
            )
        )

    @staticmethod
    def _validation_reason(error: ValidationError) -> str:
        first_error = error.errors()[0]
        location = ".".join(str(part) for part in first_error["loc"])
        return f"schema validation failed at {location}: {first_error['type']}"

    @staticmethod
    def _encoded_bytes(value: object) -> str | None:
        return base64.b64encode(value).decode("ascii") if isinstance(value, bytes) else None

    @staticmethod
    def _create_confluent_client(config: ConsumerConfig) -> KafkaConsumerClient:
        try:
            from confluent_kafka import Consumer
        except ImportError as error:
            raise RuntimeError(
                "confluent-kafka is required for real broker consumption; "
                "install dependencies from requirements.txt"
            ) from error
        return Consumer(config.as_kafka_config())

    @staticmethod
    def _validate_config(config: ConsumerConfig) -> None:
        if not config.bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must be non-empty")
        if not config.topic.strip():
            raise ValueError("topic must be non-empty")
        if not config.group_id.strip():
            raise ValueError("group_id must be non-empty")
        if config.auto_offset_reset not in {"earliest", "latest"}:
            raise ValueError("auto_offset_reset must be 'earliest' or 'latest'")
        if config.poll_timeout_seconds <= 0:
            raise ValueError("poll_timeout_seconds must be positive")
        if not config.dead_letter_topic.strip():
            raise ValueError("dead_letter_topic must be non-empty")
        if config.deduplication_capacity <= 0:
            raise ValueError("deduplication_capacity must be positive")
