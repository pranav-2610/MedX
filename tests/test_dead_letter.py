import json
from datetime import datetime, timezone
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dead_letter import DeadLetterConfig, KafkaDeadLetterPublisher
from models import DeadLetterRecord


class FakeKafkaProducerClient:
    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []
        self.callbacks: list[object] = []
        self.flush_count = 0

    def produce(self, topic: str, value: bytes, key: bytes, on_delivery: object) -> None:
        self.records.append({"topic": topic, "value": value, "key": key})
        self.callbacks.append(on_delivery)

    def poll(self, timeout: float) -> int:
        callbacks, self.callbacks = self.callbacks, []
        for callback in callbacks:
            callback(None, object())
        return len(callbacks)

    def flush(self, timeout: float) -> int:
        self.flush_count += 1
        self.poll(timeout)
        return 0


class UndeliveredKafkaProducerClient(FakeKafkaProducerClient):
    def flush(self, timeout: float) -> int:
        self.flush_count += 1
        return 1


class KafkaDeadLetterPublisherTests(unittest.TestCase):
    def test_rejection_record_is_serialized_to_configured_dlq_topic(self) -> None:
        client = FakeKafkaProducerClient()
        publisher = KafkaDeadLetterPublisher(
            DeadLetterConfig(topic="patient_vitals_dlq"), client=client
        )
        record = DeadLetterRecord(
            category="schema_validation",
            reason="schema validation failed at spo2: less_than_equal",
            ingested_at=datetime(2026, 9, 19, 10, 5, tzinfo=timezone.utc),
            source_topic="patient_vitals",
            source_partition=2,
            source_offset=14,
            original_payload_b64="eyJiYWQifQ==",
        )

        publisher.publish(record)
        publisher.close()

        self.assertEqual(client.records[0]["topic"], "patient_vitals_dlq")
        self.assertEqual(client.records[0]["key"], b"patient_vitals:2:14")
        self.assertEqual(json.loads(client.records[0]["value"]), record.model_dump(mode="json"))
        self.assertEqual(client.flush_count, 2)

    def test_publish_fails_when_broker_does_not_confirm_dlq_delivery(self) -> None:
        client = UndeliveredKafkaProducerClient()
        publisher = KafkaDeadLetterPublisher(DeadLetterConfig(), client=client)
        record = DeadLetterRecord(
            category="malformed_message",
            reason="payload is not valid UTF-8 JSON",
            ingested_at=datetime(2026, 9, 19, 10, 5, tzinfo=timezone.utc),
            source_topic="patient_vitals",
            source_partition=0,
            source_offset=1,
        )

        with self.assertRaisesRegex(RuntimeError, "not delivered"):
            publisher.publish(record)


if __name__ == "__main__":
    unittest.main()
