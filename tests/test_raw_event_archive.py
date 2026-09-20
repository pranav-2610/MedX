from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import tempfile
import unittest
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from models import IngestedVitalEvent, VitalEvent
from patient_state_store import PatientStateStore
from raw_event_archive import (
    ArchiveAndStateProcessor,
    RawEventArchive,
    RawEventArchiveConfig,
    ReplayProducer,
)
from rolling_window_manager import RollingWindowManager


UTC = timezone.utc


def ingested_event(
    patient_id: str = "P001",
    sequence_number: int = 1,
    *,
    event_time: datetime | None = None,
    offset: int = 0,
) -> IngestedVitalEvent:
    event = VitalEvent(
        event_id=uuid4(),
        patient_id=patient_id,
        event_time=event_time or datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
        heart_rate=81.125,
        spo2=96.75,
        respiratory_rate=16.5,
        systolic_bp=123.25,
        diastolic_bp=77.5,
        sequence_number=sequence_number,
        source="vital_stream_simulator",
    )
    return IngestedVitalEvent(
        event=event,
        ingested_at=datetime(2026, 9, 20, 10, 1, tzinfo=UTC),
        kafka_topic="patient_vitals",
        kafka_partition=sequence_number % 2,
        kafka_offset=offset,
    )


class FakePublisher:
    def __init__(self) -> None:
        self.events: list[VitalEvent] = []

    def publish(self, event: VitalEvent) -> None:
        self.events.append(event)


class RawEventArchiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.archive_directory = Path(self.temporary_directory.name) / "raw_events"

    def archive(self, batch_size: int = 10) -> RawEventArchive:
        return RawEventArchive(RawEventArchiveConfig(self.archive_directory, batch_size=batch_size))

    def test_accepted_event_round_trips_without_changing_canonical_values(self) -> None:
        original = ingested_event()
        archive = self.archive()

        archive.append(original)
        archive.close()
        restored = list(self.archive().iter_ingested_events())

        self.assertEqual(len(restored), 1)
        self.assertEqual(restored[0].event, original.event)
        self.assertEqual(restored[0].event.model_dump(mode="json"), original.event.model_dump(mode="json"))
        self.assertEqual(restored[0].ingested_at, original.ingested_at)
        self.assertEqual(restored[0].kafka_offset, original.kafka_offset)

    def test_partition_uses_utc_event_date_not_local_date(self) -> None:
        event = ingested_event(
            event_time=datetime(2026, 9, 21, 0, 15, tzinfo=timezone(timedelta(hours=5, minutes=30)))
        )
        archive = self.archive()

        archive.append(event)
        archive.close()

        self.assertTrue((self.archive_directory / "2026-09-20" / "part-000001.parquet").exists())

    def test_batched_writes_create_parts_and_pending_data_flushes_on_shutdown(self) -> None:
        archive = self.archive(batch_size=2)
        for sequence in (1, 2, 3):
            archive.append(ingested_event(sequence_number=sequence, offset=sequence))
        archive.close()
        part_files = sorted((self.archive_directory / "2026-09-20").glob("part-*.parquet"))

        self.assertEqual(len(part_files), 2)
        self.assertEqual([event.sequence_number for event in self.archive().iter_events()], [1, 2, 3])

    def test_archive_is_readable_after_writer_restart(self) -> None:
        first = self.archive(batch_size=10)
        first.append(ingested_event(sequence_number=1, offset=1))
        first.close()
        second = self.archive(batch_size=10)
        second.append(ingested_event(sequence_number=2, offset=2))
        second.close()

        self.assertEqual([event.sequence_number for event in self.archive().iter_events()], [1, 2])

    def test_retry_after_restart_does_not_duplicate_an_archived_event(self) -> None:
        record = ingested_event(sequence_number=1, offset=1)
        first = self.archive()
        self.assertTrue(first.append(record))
        first.close()

        retry = self.archive()
        self.assertFalse(retry.append(record))
        retry.close()

        self.assertEqual(list(self.archive().iter_events()), [record.event])

    def test_replay_republishes_original_events_in_archive_order(self) -> None:
        archive = self.archive(batch_size=10)
        p001_first = ingested_event("P001", 1, offset=3)
        p002 = ingested_event("P002", 1, offset=4)
        p001_second = ingested_event("P001", 2, offset=5)
        for record in (p001_first, p002, p001_second):
            archive.append(record)
        archive.close()
        publisher = FakePublisher()

        count = ReplayProducer(self.archive(), publisher).replay(patient_ids={"P001", "P002"})

        self.assertEqual(count, 3)
        self.assertEqual(publisher.events, [p001_first.event, p002.event, p001_second.event])
        self.assertEqual([event.sequence_number for event in publisher.events if event.patient_id == "P001"], [1, 2])
        self.assertEqual([event.event_id for event in publisher.events], [
            p001_first.event.event_id,
            p002.event.event_id,
            p001_second.event.event_id,
        ])

    def test_replay_supports_configurable_pacing_without_regenerating_events(self) -> None:
        archive = self.archive()
        archive.append(ingested_event("P001", 1, offset=1))
        archive.append(ingested_event("P001", 2, offset=2))
        archive.close()
        publisher = FakePublisher()
        pauses: list[float] = []

        count = ReplayProducer(self.archive(), publisher).replay(
            interval_seconds=0.25, sleeper=pauses.append
        )

        self.assertEqual(count, 2)
        self.assertEqual(pauses, [0.25])
        self.assertEqual([event.sequence_number for event in publisher.events], [1, 2])

    def test_archive_then_state_processor_updates_both_destinations(self) -> None:
        archive = self.archive()
        state_store = PatientStateStore()
        rolling_windows = RollingWindowManager(timedelta(minutes=30))
        processor = ArchiveAndStateProcessor(archive, state_store, rolling_windows)
        record = ingested_event("P003")

        processor(record)
        archive.close()

        self.assertEqual(state_store.get("P003"), (record.event,))
        self.assertEqual(rolling_windows.get_window("P003"), (record.event,))
        self.assertEqual(list(self.archive().iter_events()), [record.event])

    def test_retry_after_window_failure_keeps_all_destinations_singleton(self) -> None:
        class FailingOnceWindowManager(RollingWindowManager):
            def __init__(self) -> None:
                super().__init__(timedelta(minutes=30))
                self.fail_once = True

            def update(self, event: VitalEvent) -> tuple[VitalEvent, ...]:
                result = super().update(event)
                if self.fail_once:
                    self.fail_once = False
                    raise OSError("window update interruption")
                return result

        archive = self.archive()
        state_store = PatientStateStore()
        rolling_windows = FailingOnceWindowManager()
        processor = ArchiveAndStateProcessor(archive, state_store, rolling_windows)
        record = ingested_event("P003")

        with self.assertRaisesRegex(OSError, "interruption"):
            processor(record)
        processor(record)
        archive.close()

        self.assertEqual(state_store.get("P003"), (record.event,))
        self.assertEqual(rolling_windows.get_window("P003"), (record.event,))
        self.assertEqual(list(self.archive().iter_events()), [record.event])


if __name__ == "__main__":
    unittest.main()
