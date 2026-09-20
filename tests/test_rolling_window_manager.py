from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from models import VitalEvent
from rolling_window_manager import RollingWindowManager


BASE_TIME = datetime(2026, 9, 20, 10, 0, tzinfo=timezone.utc)


def vital_event(patient_id: str, minute: int, sequence_number: int) -> VitalEvent:
    return VitalEvent(
        event_id=uuid4(),
        patient_id=patient_id,
        event_time=BASE_TIME + timedelta(minutes=minute),
        heart_rate=80.0,
        spo2=97.0,
        respiratory_rate=16.0,
        systolic_bp=120.0,
        diastolic_bp=75.0,
        sequence_number=sequence_number,
        source="test",
    )


class RollingWindowManagerTests(unittest.TestCase):
    def test_first_event_enters_window(self) -> None:
        windows = RollingWindowManager(timedelta(minutes=10))
        event = vital_event("P001", 0, 1)

        self.assertEqual(windows.update(event), (event,))
        self.assertEqual(windows.anchor_time("P001"), BASE_TIME)

    def test_events_within_window_are_retained_in_arrival_order(self) -> None:
        windows = RollingWindowManager(timedelta(minutes=10))
        first = vital_event("P001", 0, 1)
        second = vital_event("P001", 5, 2)
        delayed = vital_event("P001", 3, 3)

        windows.update(first)
        windows.update(second)
        retained = windows.update(delayed)

        self.assertEqual([event.sequence_number for event in retained], [1, 2, 3])

    def test_retry_of_retained_event_is_idempotent(self) -> None:
        windows = RollingWindowManager(timedelta(minutes=10))
        event = vital_event("P001", 0, 1)

        windows.update(event)
        windows.update(event)

        self.assertEqual(windows.get_window("P001"), (event,))

    def test_events_older_than_duration_are_removed_and_boundary_is_exclusive(self) -> None:
        windows = RollingWindowManager(timedelta(minutes=10))
        at_zero = vital_event("P001", 0, 1)
        at_one = vital_event("P001", 1, 2)
        at_ten = vital_event("P001", 10, 3)

        windows.update(at_zero)
        windows.update(at_one)
        retained = windows.update(at_ten)

        self.assertEqual([event.sequence_number for event in retained], [2, 3])

    def test_new_event_advances_horizon_and_removes_expired_observations(self) -> None:
        windows = RollingWindowManager(timedelta(minutes=5))
        windows.update(vital_event("P001", 0, 1))
        windows.update(vital_event("P001", 3, 2))
        retained = windows.update(vital_event("P001", 7, 3))

        self.assertEqual([event.sequence_number for event in retained], [2, 3])

    def test_patients_are_isolated(self) -> None:
        windows = RollingWindowManager(timedelta(minutes=5))
        windows.update(vital_event("P001", 0, 1))
        p002_event = vital_event("P002", 0, 1)
        windows.update(p002_event)
        windows.update(vital_event("P001", 10, 2))

        self.assertEqual(windows.get_window("P002"), (p002_event,))

    def test_event_time_not_wall_clock_controls_expiration(self) -> None:
        windows = RollingWindowManager(timedelta(minutes=10))
        older_wall_clock_event = vital_event("P001", -10_000, 1)
        current_event = vital_event("P001", 0, 2)

        windows.update(older_wall_clock_event)
        retained = windows.update(current_event)

        self.assertEqual(retained, (current_event,))

    def test_unseen_patient_returns_none(self) -> None:
        self.assertIsNone(RollingWindowManager(timedelta(minutes=10)).get_window("P404"))

    def test_snapshots_cannot_mutate_internal_window(self) -> None:
        windows = RollingWindowManager(timedelta(minutes=10))
        input_event = vital_event("P001", 0, 1)
        windows.update(input_event)
        input_event.heart_rate = 200.0
        snapshot = windows.get_window("P001")
        self.assertIsNotNone(snapshot)
        snapshot[0].heart_rate = 150.0

        self.assertEqual(windows.get_window("P001")[0].heart_rate, 80.0)
        with self.assertRaises(AttributeError):
            snapshot.append(vital_event("P001", 1, 2))

    def test_different_durations_produce_different_windows(self) -> None:
        short = RollingWindowManager(timedelta(minutes=5))
        long = RollingWindowManager(timedelta(minutes=15))
        first = vital_event("P001", 0, 1)
        second = vital_event("P001", 10, 2)

        for manager in (short, long):
            manager.update(first)
            manager.update(second)

        self.assertEqual([event.sequence_number for event in short.get_window("P001") or ()], [2])
        self.assertEqual([event.sequence_number for event in long.get_window("P001") or ()], [1, 2])

    def test_non_positive_duration_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be positive"):
            RollingWindowManager(timedelta(0))


if __name__ == "__main__":
    unittest.main()
