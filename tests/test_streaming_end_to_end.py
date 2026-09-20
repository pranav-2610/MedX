"""Offline contract test for the complete supported streaming path."""

from collections import deque
from datetime import timedelta
from itertools import islice
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion_consumer import ConsumerConfig, VitalIngestionConsumer
from patient_profiles import PatientProfileRepository
from patient_state_store import PatientStateStore
from raw_event_archive import ArchiveAndStateProcessor, RawEventArchive, RawEventArchiveConfig
from rolling_window_manager import RollingWindowManager
from streaming_producer import ProducerConfig, VitalEventProducer
from vital_stream_simulator import SimulatorConfig, VitalStreamSimulator


PROFILE_PATH = Path(__file__).resolve().parents[1] / "data" / "patients.json"


class RecordingProducerClient:
    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []
        self.callbacks: list[object] = []

    def produce(self, topic: str, value: bytes, key: bytes, on_delivery: object) -> None:
        self.records.append({"topic": topic, "value": value, "key": key})
        self.callbacks.append(on_delivery)

    def poll(self, timeout: float) -> int:
        callbacks, self.callbacks = self.callbacks, []
        for callback in callbacks:
            callback(None, object())
        return len(callbacks)

    def flush(self, timeout: float) -> int:
        self.poll(timeout)
        return 0


class RecordedMessage:
    def __init__(self, record: dict[str, object], offset: int) -> None:
        self._record = record
        self._offset = offset

    def value(self) -> bytes:
        return self._record["value"]  # type: ignore[return-value]

    def key(self) -> bytes:
        return self._record["key"]  # type: ignore[return-value]

    def error(self) -> None:
        return None

    def topic(self) -> str:
        return self._record["topic"]  # type: ignore[return-value]

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset


class QueueConsumerClient:
    def __init__(self, messages: list[RecordedMessage]) -> None:
        self.messages = deque(messages)
        self.committed: list[RecordedMessage] = []

    def subscribe(self, topics: list[str]) -> None:
        return None

    def poll(self, timeout: float) -> RecordedMessage | None:
        return self.messages.popleft() if self.messages else None

    def commit(self, message: RecordedMessage, asynchronous: bool) -> None:
        self.committed.append(message)

    def close(self) -> None:
        return None


class NoopDeadLetterPublisher:
    def publish(self, record: object) -> None:
        self.fail("a simulator-produced message should not reach the DLQ")

    def close(self) -> None:
        return None

    @staticmethod
    def fail(message: str) -> None:
        raise AssertionError(message)


class StreamingEndToEndTests(unittest.TestCase):
    def test_simulator_serialization_ingestion_archive_state_and_window_agree(self) -> None:
        profiles = PatientProfileRepository.from_json_file(PROFILE_PATH)
        simulator = VitalStreamSimulator(profiles, config=SimulatorConfig(random_seed=17))
        generated = list(islice(simulator, 8))
        producer_client = RecordingProducerClient()
        producer = VitalEventProducer(ProducerConfig(topic="patient_vitals"), client=producer_client)

        self.assertEqual(producer.run(generated), 8)
        messages = [
            RecordedMessage(record, offset)
            for offset, record in enumerate(producer_client.records)
        ]
        consumer_client = QueueConsumerClient(messages)

        with tempfile.TemporaryDirectory() as directory:
            archive_path = Path(directory) / "raw_events"
            archive = RawEventArchive(RawEventArchiveConfig(archive_path, batch_size=3))
            state_store = PatientStateStore()
            windows = RollingWindowManager(timedelta(minutes=30))
            processor = ArchiveAndStateProcessor(archive, state_store, windows)
            consumer = VitalIngestionConsumer(
                profiles,
                ConsumerConfig(topic="patient_vitals"),
                client=consumer_client,
                dead_letter_publisher=NoopDeadLetterPublisher(),
            )

            accepted = 0
            while consumer.consume_once(processor):
                accepted += 1
            archive.close()
            restored = list(RawEventArchive(RawEventArchiveConfig(archive_path)).iter_events())

        self.assertEqual(accepted, len(generated))
        self.assertEqual(len(consumer_client.committed), len(generated))
        self.assertEqual(restored, generated)
        for patient_id in ("P001", "P002", "P003", "P004"):
            self.assertEqual(state_store.get(patient_id), tuple(event for event in generated if event.patient_id == patient_id))
            self.assertEqual(windows.get_window(patient_id), tuple(event for event in generated if event.patient_id == patient_id))


if __name__ == "__main__":
    unittest.main()
