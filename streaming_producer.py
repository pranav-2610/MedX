"""Kafka-compatible producer for canonical vital-event messages."""

from collections.abc import Callable, Iterable
from dataclasses import dataclass
import logging
from threading import Event
from typing import Any, Protocol

from models import VitalEvent


LOGGER = logging.getLogger(__name__)


class KafkaProducerClient(Protocol):
    """The small subset of the confluent-kafka producer used by this boundary."""

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        on_delivery: Callable[[Any, Any], None],
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


class ProducerDeliveryError(RuntimeError):
    """A message could not be delivered to the configured Kafka broker."""


@dataclass(frozen=True)
class ProducerConfig:
    """Portable Kafka/Redpanda producer settings."""

    bootstrap_servers: str = "localhost:19092"
    topic: str = "patient_vitals"
    client_id: str = "medx-vital-producer"
    delivery_timeout_seconds: float = 10.0

    def as_kafka_config(self) -> dict[str, str | bool]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "acks": "all",
            "enable.idempotence": True,
        }


class VitalEventProducer:
    """Publish one complete ``VitalEvent`` per Kafka message.

    The event's ``patient_id`` is the UTF-8 Kafka key. Kafka's standard keyed
    partitioning therefore keeps a patient's logical event stream routed to the
    same partition while all patients share the ``patient_vitals`` topic.
    """

    def __init__(
        self,
        config: ProducerConfig | None = None,
        *,
        client: KafkaProducerClient | None = None,
    ) -> None:
        self.config = config or ProducerConfig()
        self._validate_config(self.config)
        self._client = client or self._create_confluent_client(self.config)
        self._closed = False
        self._delivery_errors: list[str] = []

    def __enter__(self) -> "VitalEventProducer":
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def publish(self, event: VitalEvent) -> None:
        """Queue one canonical event and process available delivery reports."""
        if self._closed:
            raise RuntimeError("producer is closed")
        self._raise_delivery_errors()

        self._client.produce(
            self.config.topic,
            key=event.patient_id.encode("utf-8"),
            value=event.model_dump_json().encode("utf-8"),
            on_delivery=self._on_delivery,
        )
        self._client.poll(0.0)
        self._raise_delivery_errors()

    def run(self, events: Iterable[VitalEvent], *, stop_event: Event | None = None) -> int:
        """Continuously publish an event source until it ends or is asked to stop."""
        published = 0
        try:
            for event in events:
                if stop_event is not None and stop_event.is_set():
                    break
                self.publish(event)
                published += 1
        finally:
            self.close()
        return published

    def close(self) -> None:
        """Flush queued records and surface delivery failures during shutdown."""
        if self._closed:
            return
        remaining = self._client.flush(self.config.delivery_timeout_seconds)
        self._closed = True
        self._raise_delivery_errors()
        if remaining:
            raise ProducerDeliveryError(
                f"{remaining} message(s) were not delivered before producer shutdown"
            )

    def _on_delivery(self, error: Any, message: Any) -> None:
        if error is not None:
            detail = str(error)
            self._delivery_errors.append(detail)
            LOGGER.error("VitalEvent delivery failed: %s", detail)

    def _raise_delivery_errors(self) -> None:
        if self._delivery_errors:
            errors = "; ".join(self._delivery_errors)
            self._delivery_errors.clear()
            raise ProducerDeliveryError(f"VitalEvent delivery failed: {errors}")

    @staticmethod
    def _create_confluent_client(config: ProducerConfig) -> KafkaProducerClient:
        try:
            from confluent_kafka import Producer
        except ImportError as error:
            raise RuntimeError(
                "confluent-kafka is required for real broker publishing; "
                "install dependencies from requirements.txt"
            ) from error
        return Producer(config.as_kafka_config())

    @staticmethod
    def _validate_config(config: ProducerConfig) -> None:
        if not config.bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must be non-empty")
        if not config.topic.strip():
            raise ValueError("topic must be non-empty")
        if not config.client_id.strip():
            raise ValueError("client_id must be non-empty")
        if config.delivery_timeout_seconds <= 0:
            raise ValueError("delivery_timeout_seconds must be positive")
