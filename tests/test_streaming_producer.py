import json
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
import sys
import unittest
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from models import VitalEvent
from patient_profiles import PatientProfileRepository
from streaming_producer import ProducerConfig, ProducerDeliveryError, VitalEventProducer
from vital_stream_simulator import SimulatorConfig, VitalStreamSimulator


PROFILE_PATH = Path(__file__).resolve().parents[1] / "data" / "patients.json"


class FakeKafkaClient:
    def __init__(self, delivery_error: str | None = None) -> None:
        self.records: list[dict[str, object]] = []
        self.pending_callbacks: list[object] = []
        self.delivery_error = delivery_error
        self.flush_timeouts: list[float] = []

    def produce(self, topic: str, value: bytes, key: bytes, on_delivery: object) -> None:
        self.records.append({"topic": topic, "value": value, "key": key})
        self.pending_callbacks.append(on_delivery)

    def poll(self, timeout: float) -> int:
        callbacks, self.pending_callbacks = self.pending_callbacks, []
        for callback in callbacks:
            callback(self.delivery_error, object())
        return len(callbacks)

    def flush(self, timeout: float) -> int:
        self.flush_timeouts.append(timeout)
        self.poll(timeout)
        return 0


def vital_event() -> VitalEvent:
    return VitalEvent(
        event_id=uuid4(),
        patient_id="P003",
        event_time=datetime(2026, 9, 19, 10, 0, tzinfo=timezone.utc),
        heart_rate=82.0,
        spo2=97.0,
        respiratory_rate=16.0,
        systolic_bp=125.0,
        diastolic_bp=78.0,
        sequence_number=4,
        source="vital_stream_simulator",
    )


class VitalEventProducerTests(unittest.TestCase):
    def test_publish_serializes_complete_event_and_uses_patient_key(self) -> None:
        client = FakeKafkaClient()
        producer = VitalEventProducer(ProducerConfig(topic="patient_vitals"), client=client)
        event = vital_event()

        producer.publish(event)
        producer.close()

        self.assertEqual(len(client.records), 1)
        record = client.records[0]
        self.assertEqual(record["topic"], "patient_vitals")
        self.assertEqual(record["key"], b"P003")
        self.assertEqual(json.loads(record["value"]), event.model_dump(mode="json"))
        self.assertEqual(client.flush_timeouts, [10.0])

    def test_delivery_error_is_propagated(self) -> None:
        producer = VitalEventProducer(client=FakeKafkaClient(delivery_error="broker unavailable"))

        with self.assertRaisesRegex(ProducerDeliveryError, "broker unavailable"):
            producer.publish(vital_event())
        producer.close()

    def test_simulator_events_can_be_published_and_producer_flushes(self) -> None:
        profiles = PatientProfileRepository.from_json_file(PROFILE_PATH)
        simulator = VitalStreamSimulator(profiles, config=SimulatorConfig(random_seed=3))
        client = FakeKafkaClient()
        producer = VitalEventProducer(client=client)

        published = producer.run(islice(simulator, 6))

        self.assertEqual(published, 6)
        self.assertEqual(len(client.records), 6)
        self.assertEqual({record["key"] for record in client.records}, {b"P001", b"P002", b"P003", b"P004"})
        self.assertEqual(len(client.flush_timeouts), 1)

    def test_closed_producer_refuses_new_messages(self) -> None:
        producer = VitalEventProducer(client=FakeKafkaClient())
        producer.close()

        with self.assertRaisesRegex(RuntimeError, "closed"):
            producer.publish(vital_event())


if __name__ == "__main__":
    unittest.main()
