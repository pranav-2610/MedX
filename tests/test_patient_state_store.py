from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from models import VitalEvent
from patient_state_store import PatientStateStore


def vital_event(patient_id: str, sequence_number: int) -> VitalEvent:
    return VitalEvent(
        event_id=uuid4(),
        patient_id=patient_id,
        event_time=datetime(2026, 9, 20, 10, 0, tzinfo=timezone.utc)
        + timedelta(minutes=sequence_number),
        heart_rate=80.0 + sequence_number,
        spo2=97.0,
        respiratory_rate=16.0,
        systolic_bp=120.0,
        diastolic_bp=75.0,
        sequence_number=sequence_number,
        source="test",
    )


class PatientStateStoreTests(unittest.TestCase):
    def test_first_event_creates_patient_state(self) -> None:
        store = PatientStateStore()
        event = vital_event("P001", 1)

        updated = store.update(event)

        self.assertEqual(len(store), 1)
        self.assertEqual(updated, (event,))
        self.assertEqual(store.get("P001"), (event,))

    def test_events_accumulate_in_arrival_order_per_patient(self) -> None:
        store = PatientStateStore()
        first, second, third = (vital_event("P001", number) for number in (1, 2, 3))

        store.update(first)
        store.update(second)
        updated = store.update(third)

        self.assertEqual([event.sequence_number for event in updated], [1, 2, 3])

    def test_patient_states_are_isolated(self) -> None:
        store = PatientStateStore()
        first_patient_event = vital_event("P001", 1)
        second_patient_event = vital_event("P002", 1)

        store.update(first_patient_event)
        store.update(second_patient_event)
        store.update(vital_event("P001", 2))

        self.assertEqual([event.sequence_number for event in store.get("P001") or ()], [1, 2])
        self.assertEqual(store.get("P002"), (second_patient_event,))

    def test_maximum_window_evicts_oldest_event(self) -> None:
        store = PatientStateStore(max_events_per_patient=2)
        events = [vital_event("P001", sequence_number) for sequence_number in (1, 2, 3)]

        for event in events:
            store.update(event)

        retained = store.get("P001")
        self.assertEqual([event.sequence_number for event in retained or ()], [2, 3])

    def test_retry_of_retained_event_is_idempotent(self) -> None:
        store = PatientStateStore()
        event = vital_event("P001", 1)

        store.update(event)
        store.update(event)

        self.assertEqual(store.get("P001"), (event,))

    def test_updating_one_patient_does_not_modify_another(self) -> None:
        store = PatientStateStore()
        store.update(vital_event("P001", 1))
        p002_state = store.update(vital_event("P002", 1))

        store.update(vital_event("P001", 2))

        self.assertEqual(store.get("P002"), p002_state)

    def test_new_patient_id_creates_state_and_missing_patient_returns_none(self) -> None:
        store = PatientStateStore()

        self.assertIsNone(store.get("P404"))
        store.update(vital_event("P404", 1))

        self.assertEqual(len(store), 1)
        self.assertEqual(store.get("P404")[0].patient_id, "P404")

    def test_public_snapshots_cannot_corrupt_internal_state(self) -> None:
        store = PatientStateStore()
        input_event = vital_event("P001", 1)
        store.update(input_event)

        input_event.heart_rate = 200.0
        snapshot = store.get("P001")
        self.assertIsNotNone(snapshot)
        snapshot[0].heart_rate = 150.0

        fresh_snapshot = store.get("P001")
        self.assertEqual(fresh_snapshot[0].heart_rate, 81.0)
        with self.assertRaises(AttributeError):
            snapshot.append(vital_event("P001", 2))

    def test_invalid_window_capacity_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be positive"):
            PatientStateStore(max_events_per_patient=0)


if __name__ == "__main__":
    unittest.main()
