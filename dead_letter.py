"""Kafka-backed dead-letter publishing for rejected ingestion messages."""

from collections.abc import Callable
from dataclasses import dataclass
import logging
from typing import Any, Protocol

from models import DeadLetterRecord


LOGGER = logging.getLogger(__name__)


class DeadLetterPublisher(Protocol):
    def publish(self, record: DeadLetterRecord) -> None: ...

    def close(self) -> None: ...


class KafkaProducerClient(Protocol):
    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        on_delivery: Callable[[Any, Any], None],
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


@dataclass(frozen=True)
class DeadLetterConfig:
    bootstrap_servers: str = "localhost:19092"
    topic: str = "patient_vitals_dlq"
    client_id: str = "medx-ingestion-dlq"
    delivery_timeout_seconds: float = 10.0

    def as_kafka_config(self) -> dict[str, str | bool]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "acks": "all",
            "enable.idempotence": True,
        }


class DeadLetterDeliveryError(RuntimeError):
    """A rejected source message could not be retained by the DLQ."""


class KafkaDeadLetterPublisher:
    """Publish opaque rejected-message envelopes without logging their payloads."""

    def __init__(
        self,
        config: DeadLetterConfig,
        *,
        client: KafkaProducerClient | None = None,
    ) -> None:
        self.config = config
        self._validate_config(config)
        self._client = client or self._create_confluent_client(config)
        self._delivery_errors: list[str] = []
        self._closed = False

    def publish(self, record: DeadLetterRecord) -> None:
        if self._closed:
            raise RuntimeError("dead-letter publisher is closed")
        self._raise_delivery_errors()
        key = f"{record.source_topic}:{record.source_partition}:{record.source_offset}".encode()
        self._client.produce(
            self.config.topic,
            value=record.model_dump_json().encode(),
            key=key,
            on_delivery=self._on_delivery,
        )
        # A source offset may be committed immediately after this method
        # returns.  Waiting for the delivery report here prevents an invalid
        # source record from being acknowledged before its DLQ record has
        # actually reached the broker.
        remaining = self._client.flush(self.config.delivery_timeout_seconds)
        self._raise_delivery_errors()
        if remaining:
            raise DeadLetterDeliveryError(
                f"{remaining} dead-letter message(s) were not delivered before timeout"
            )

    def close(self) -> None:
        if self._closed:
            return
        remaining = self._client.flush(self.config.delivery_timeout_seconds)
        self._closed = True
        self._raise_delivery_errors()
        if remaining:
            raise DeadLetterDeliveryError(
                f"{remaining} dead-letter message(s) were not delivered before shutdown"
            )

    def _on_delivery(self, error: Any, message: Any) -> None:
        if error is not None:
            self._delivery_errors.append(str(error))

    def _raise_delivery_errors(self) -> None:
        if self._delivery_errors:
            errors = "; ".join(self._delivery_errors)
            self._delivery_errors.clear()
            raise DeadLetterDeliveryError(f"dead-letter delivery failed: {errors}")

    @staticmethod
    def _create_confluent_client(config: DeadLetterConfig) -> KafkaProducerClient:
        try:
            from confluent_kafka import Producer
        except ImportError as error:
            raise RuntimeError(
                "confluent-kafka is required for Kafka dead-letter publishing; "
                "install dependencies from requirements.txt"
            ) from error
        return Producer(config.as_kafka_config())

    @staticmethod
    def _validate_config(config: DeadLetterConfig) -> None:
        if not config.bootstrap_servers.strip() or not config.topic.strip():
            raise ValueError("dead-letter broker and topic must be non-empty")
        if config.delivery_timeout_seconds <= 0:
            raise ValueError("dead-letter delivery_timeout_seconds must be positive")


class LoggingDeadLetterPublisher:
    """Development fallback that logs metadata only; it never logs raw payloads."""

    def publish(self, record: DeadLetterRecord) -> None:
        LOGGER.warning(
            "Dead-letter fallback category=%s topic=%s partition=%s offset=%s",
            record.category,
            record.source_topic,
            record.source_partition,
            record.source_offset,
        )

    def close(self) -> None:
        return None
