import base64
import json
from datetime import datetime, timedelta, timezone
from itertools import islice
from pathlib import Path
import sys
import tempfile
import unittest
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion_consumer import ConsumerConfig, RejectedMessage, VitalIngestionConsumer
from models import DeadLetterRecord, IngestedVitalEvent, VitalEvent
from patient_profiles import PatientProfileRepository
from patient_state_store import PatientStateStore
from raw_event_archive import ArchiveAndStateProcessor, RawEventArchive, RawEventArchiveConfig
from rolling_window_manager import RollingWindowManager
from vital_stream_simulator import SimulatorConfig, VitalStreamSimulator


PROFILE_PATH = Path(__file__).resolve().parents[1] / "data" / "patients.json"
INGESTED_AT = datetime(2026, 9, 19, 10, 5, tzinfo=timezone.utc)


class FakeMessage:
    def __init__(
        self,
        value: bytes,
        *,
        key: bytes | None = None,
        topic: str = "patient_vitals",
        offset: int = 0,
    ) -> None:
        self._value = value
        self._key = key
        self._topic = topic
        self._offset = offset

    def value(self) -> bytes:
        return self._value

    def key(self) -> bytes | None:
        return self._key

    def error(self) -> None:
        return None

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset


class FakeKafkaConsumerClient:
    def __init__(self, messages: list[FakeMessage]) -> None:
        self.messages = messages
        self.subscriptions: list[list[str]] = []
        self.committed: list[FakeMessage] = []
        self.closed = False
        self.dead_letters = FakeDeadLetterPublisher()

    def subscribe(self, topics: list[str]) -> None:
        self.subscriptions.append(topics)

    def poll(self, timeout: float) -> FakeMessage | None:
        return self.messages.pop(0) if self.messages else None

    def commit(self, message: FakeMessage, asynchronous: bool) -> None:
        self.committed.append(message)

    def close(self) -> None:
        self.closed = True


class FakeDeadLetterPublisher:
    def __init__(self) -> None:
        self.records: list[DeadLetterRecord] = []
        self.closed = False

    def publish(self, record: DeadLetterRecord) -> None:
        self.records.append(record)

    def close(self) -> None:
        self.closed = True


def vital_event(patient_id: str = "P001") -> VitalEvent:
    return VitalEvent(
        event_id=uuid4(),
        patient_id=patient_id,
        event_time=datetime(2026, 9, 19, 10, 0, tzinfo=timezone.utc),
        heart_rate=82.0,
        spo2=97.0,
        respiratory_rate=16.0,
        systolic_bp=125.0,
        diastolic_bp=78.0,
        sequence_number=1,
        source="vital_stream_simulator",
    )


class VitalIngestionConsumerTests(unittest.TestCase):
    def new_consumer(
        self, messages: list[FakeMessage], rejected: list[RejectedMessage] | None = None
    ) -> tuple[VitalIngestionConsumer, FakeKafkaConsumerClient]:
        client = FakeKafkaConsumerClient(messages)
        consumer = VitalIngestionConsumer(
            PatientProfileRepository.from_json_file(PROFILE_PATH),
            ConsumerConfig(),
            client=client,
            clock=lambda: INGESTED_AT,
            on_rejected=rejected.append if rejected is not None else None,
            dead_letter_publisher=client.dead_letters,
        )
        return consumer, client

    def test_valid_event_is_normalized_and_accepted(self) -> None:
        event = vital_event()
        consumer, client = self.new_consumer([FakeMessage(event.model_dump_json().encode())])
        accepted: list[IngestedVitalEvent] = []

        result = consumer.consume_once(accepted.append)

        self.assertTrue(result)
        self.assertEqual(client.subscriptions, [["patient_vitals"]])
        self.assertEqual(len(accepted), 1)
        self.assertEqual(accepted[0].event, event)
        self.assertEqual(accepted[0].event.event_time, event.event_time)
        self.assertEqual(accepted[0].ingested_at, INGESTED_AT)
        self.assertEqual(len(client.committed), 1)

    def test_malformed_json_is_rejected_and_next_valid_message_is_accepted(self) -> None:
        valid = FakeMessage(vital_event("P002").model_dump_json().encode(), offset=1)
        rejected: list[RejectedMessage] = []
        consumer, client = self.new_consumer([FakeMessage(b"{bad json"), valid], rejected)
        accepted: list[IngestedVitalEvent] = []

        self.assertFalse(consumer.consume_once(accepted.append))
        self.assertTrue(consumer.consume_once(accepted.append))

        self.assertEqual(consumer.metrics.rejected, 1)
        self.assertEqual(consumer.metrics.dead_lettered, 1)
        self.assertEqual(consumer.metrics.accepted, 1)
        self.assertEqual(rejected[0].offset, 0)
        self.assertEqual(client.dead_letters.records[0].category, "malformed_message")
        self.assertEqual([message.offset() for message in client.committed], [0, 1])
        self.assertEqual(accepted[0].event.patient_id, "P002")

    def test_schema_invalid_event_is_rejected_not_forwarded(self) -> None:
        payload = vital_event().model_dump(mode="json")
        payload["spo2"] = 101.0
        rejected: list[RejectedMessage] = []
        consumer, client = self.new_consumer([FakeMessage(json.dumps(payload).encode())], rejected)
        accepted: list[IngestedVitalEvent] = []

        self.assertFalse(consumer.consume_once(accepted.append))
        self.assertEqual(accepted, [])
        self.assertEqual(consumer.metrics.rejected, 1)
        self.assertEqual(len(client.committed), 1)
        self.assertEqual(rejected[0].category, "schema_validation")
        self.assertIn("spo2", rejected[0].reason)

    def test_schema_quality_failures_are_dead_lettered(self) -> None:
        cases: list[tuple[str, dict[str, object]]] = []

        missing_field = vital_event().model_dump(mode="json")
        del missing_field["heart_rate"]
        cases.append(("missing field", missing_field))

        invalid_type = vital_event().model_dump(mode="json")
        invalid_type["heart_rate"] = "eighty"
        cases.append(("invalid datatype", invalid_type))

        invalid_timestamp = vital_event().model_dump(mode="json")
        invalid_timestamp["event_time"] = "2026-09-19T10:00:00"
        cases.append(("invalid timestamp", invalid_timestamp))

        for label, payload in cases:
            with self.subTest(label=label):
                consumer, client = self.new_consumer([FakeMessage(json.dumps(payload).encode())])
                accepted: list[IngestedVitalEvent] = []

                self.assertFalse(consumer.consume_once(accepted.append))
                self.assertEqual(accepted, [])
                self.assertEqual(client.dead_letters.records[0].category, "schema_validation")

    def test_dead_letter_retains_original_payload_without_logging_it(self) -> None:
        raw_payload = b"{bad json"
        consumer, client = self.new_consumer([FakeMessage(raw_payload)])

        consumer.consume_once(lambda _: self.fail("invalid message must not be forwarded"))

        record = client.dead_letters.records[0]
        self.assertEqual(base64.b64decode(record.original_payload_b64 or ""), raw_payload)
        self.assertEqual(record.source_topic, "patient_vitals")

    def test_mismatched_kafka_key_is_rejected_not_forwarded(self) -> None:
        event = vital_event("P001")
        rejected: list[RejectedMessage] = []
        consumer, client = self.new_consumer(
            [FakeMessage(event.model_dump_json().encode(), key=b"P999")], rejected
        )
        accepted: list[IngestedVitalEvent] = []

        self.assertFalse(consumer.consume_once(accepted.append))
        self.assertEqual(accepted, [])
        self.assertEqual(len(client.committed), 1)
        self.assertEqual(rejected[0].category, "key_mismatch")

    def test_multiple_patients_from_simulator_are_accepted_from_one_topic(self) -> None:
        profiles = PatientProfileRepository.from_json_file(PROFILE_PATH)
        simulator = VitalStreamSimulator(profiles, config=SimulatorConfig(random_seed=4))
        messages = [
            FakeMessage(event.model_dump_json().encode(), offset=index)
            for index, event in enumerate(islice(simulator, 4))
        ]
        consumer, _ = self.new_consumer(messages)
        accepted: list[IngestedVitalEvent] = []

        while consumer.consume_once(accepted.append):
            pass

        self.assertEqual([item.event.patient_id for item in accepted], ["P001", "P002", "P003", "P004"])
        self.assertTrue(all(item.event.source == "vital_stream_simulator" for item in accepted))

    def test_downstream_failure_does_not_commit_valid_event(self) -> None:
        consumer, client = self.new_consumer([FakeMessage(vital_event().model_dump_json().encode())])

        result = consumer.consume_once(lambda _: (_ for _ in ()).throw(RuntimeError("not ready")))

        self.assertFalse(result)
        self.assertEqual(client.committed, [])
        self.assertEqual(consumer.metrics.processing_errors, 1)

    def test_duplicate_event_id_is_dead_lettered_and_not_forwarded(self) -> None:
        event = vital_event()
        messages = [
            FakeMessage(event.model_dump_json().encode(), offset=0),
            FakeMessage(event.model_dump_json().encode(), offset=1),
        ]
        rejected: list[RejectedMessage] = []
        consumer, client = self.new_consumer(messages, rejected)
        accepted: list[IngestedVitalEvent] = []

        self.assertTrue(consumer.consume_once(accepted.append))
        self.assertFalse(consumer.consume_once(accepted.append))

        self.assertEqual(len(accepted), 1)
        self.assertEqual(consumer.metrics.duplicates, 1)
        self.assertEqual(rejected[0].category, "duplicate_event")
        self.assertEqual(client.dead_letters.records[0].category, "duplicate_event")

    def test_unknown_patient_is_dead_lettered_not_created(self) -> None:
        consumer, client = self.new_consumer(
            [FakeMessage(vital_event("P999").model_dump_json().encode())]
        )
        accepted: list[IngestedVitalEvent] = []

        self.assertFalse(consumer.consume_once(accepted.append))

        self.assertEqual(accepted, [])
        self.assertEqual(consumer.metrics.unknown_patients, 1)
        self.assertEqual(client.dead_letters.records[0].category, "unknown_patient")

    def test_only_trusted_events_reach_patient_state_store(self) -> None:
        invalid = vital_event("P001").model_dump(mode="json")
        invalid["spo2"] = 101.0
        valid = vital_event("P002")
        consumer, _ = self.new_consumer(
            [
                FakeMessage(json.dumps(invalid).encode(), offset=0),
                FakeMessage(valid.model_dump_json().encode(), offset=1),
            ]
        )
        state_store = PatientStateStore()

        self.assertFalse(
            consumer.consume_once(lambda ingested_event: state_store.update(ingested_event.event))
        )
        self.assertTrue(
            consumer.consume_once(lambda ingested_event: state_store.update(ingested_event.event))
        )

        self.assertIsNone(state_store.get("P001"))
        self.assertEqual(state_store.get("P002"), (valid,))

    def test_invalid_event_is_not_archived_and_valid_event_reaches_archive_and_state(self) -> None:
        invalid = vital_event("P001").model_dump(mode="json")
        invalid["spo2"] = 101.0
        valid = vital_event("P002")
        consumer, _ = self.new_consumer(
            [
                FakeMessage(json.dumps(invalid).encode(), offset=0),
                FakeMessage(valid.model_dump_json().encode(), offset=1),
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            archive = RawEventArchive(RawEventArchiveConfig(Path(directory), batch_size=10))
            state_store = PatientStateStore()
            processor = ArchiveAndStateProcessor(
                archive, state_store, RollingWindowManager(timedelta(minutes=30))
            )

            self.assertFalse(consumer.consume_once(processor))
            self.assertTrue(consumer.consume_once(processor))
            archive.close()
            archived = list(RawEventArchive(RawEventArchiveConfig(Path(directory))).iter_events())

        self.assertEqual(archived, [valid])
        self.assertIsNone(state_store.get("P001"))
        self.assertEqual(state_store.get("P002"), (valid,))

    def test_archive_failure_leaves_offset_uncommitted_and_skips_state(self) -> None:
        class FailingArchive:
            def append(self, ingested_event: IngestedVitalEvent) -> None:
                raise OSError("archive storage unavailable")

        consumer, client = self.new_consumer([FakeMessage(vital_event().model_dump_json().encode())])
        state_store = PatientStateStore()
        processor = ArchiveAndStateProcessor(
            FailingArchive(), state_store, RollingWindowManager(timedelta(minutes=30))
        )

        self.assertFalse(consumer.consume_once(processor))
        self.assertEqual(client.committed, [])
        self.assertIsNone(state_store.get("P001"))
        self.assertEqual(consumer.metrics.processing_errors, 1)


if __name__ == "__main__":
    unittest.main()
