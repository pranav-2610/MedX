from datetime import datetime, timedelta, timezone
from itertools import islice
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from models import VitalEvent
from patient_profiles import PatientProfileRepository
from vital_stream_simulator import SimulatorConfig, VitalStreamSimulator


PROFILE_PATH = Path(__file__).resolve().parents[1] / "data" / "patients.json"
START_TIME = datetime(2026, 9, 19, 10, 0, tzinfo=timezone.utc)


def configured_simulator(seed: int = 17) -> VitalStreamSimulator:
    profiles = PatientProfileRepository.from_json_file(PROFILE_PATH)
    return VitalStreamSimulator(
        profiles,
        config=SimulatorConfig(
            random_seed=seed,
            sampling_interval=timedelta(minutes=1),
            start_time=START_TIME,
        ),
    )


class VitalStreamSimulatorTests(unittest.TestCase):
    def test_stream_yields_valid_events_for_loaded_patients(self) -> None:
        events = list(islice(configured_simulator(), 4))
        repository = PatientProfileRepository.from_json_file(PROFILE_PATH)

        self.assertEqual([event.patient_id for event in events], ["P001", "P002", "P003", "P004"])
        self.assertTrue(all(isinstance(event, VitalEvent) for event in events))
        self.assertTrue(all(repository.get(event.patient_id) is not None for event in events))
        self.assertTrue(all(event.source == "vital_stream_simulator" for event in events))

    def test_sequence_numbers_and_timestamps_advance_per_patient(self) -> None:
        events = list(islice(configured_simulator(), 8))
        first_cycle, second_cycle = events[:4], events[4:]

        for first, second in zip(first_cycle, second_cycle):
            self.assertEqual(first.patient_id, second.patient_id)
            self.assertEqual(first.sequence_number, 1)
            self.assertEqual(second.sequence_number, 2)
            self.assertEqual(second.event_time - first.event_time, timedelta(minutes=1))

    def test_generated_values_always_satisfy_vital_event_contract(self) -> None:
        events = list(islice(configured_simulator(), 20))

        for event in events:
            self.assertGreaterEqual(event.heart_rate, 0)
            self.assertLessEqual(event.heart_rate, 300)
            self.assertGreaterEqual(event.spo2, 0)
            self.assertLessEqual(event.spo2, 100)
            self.assertGreaterEqual(event.respiratory_rate, 0)
            self.assertLessEqual(event.respiratory_rate, 120)
            self.assertGreaterEqual(event.systolic_bp, event.diastolic_bp)

    def test_fixed_seed_reproduces_vital_values_and_order(self) -> None:
        first = list(islice(configured_simulator(seed=42), 8))
        second = list(islice(configured_simulator(seed=42), 8))

        first_projection = [
            (event.patient_id, event.event_time, event.sequence_number, event.heart_rate,
             event.spo2, event.respiratory_rate, event.systolic_bp, event.diastolic_bp)
            for event in first
        ]
        second_projection = [
            (event.patient_id, event.event_time, event.sequence_number, event.heart_rate,
             event.spo2, event.respiratory_rate, event.systolic_bp, event.diastolic_bp)
            for event in second
        ]
        self.assertEqual(first_projection, second_projection)
        self.assertEqual(len({event.event_id for event in first}), len(first))

    def test_iterator_is_lazy_and_continuous(self) -> None:
        simulator = configured_simulator()
        stream = iter(simulator)
        first = next(stream)
        following = list(islice(stream, 4))

        self.assertEqual(first.patient_id, "P001")
        self.assertEqual([event.patient_id for event in following], ["P002", "P003", "P004", "P001"])
        self.assertEqual(following[-1].sequence_number, 2)

    def test_duplicate_patient_selection_is_rejected(self) -> None:
        profiles = PatientProfileRepository.from_json_file(PROFILE_PATH)

        with self.assertRaisesRegex(ValueError, "must not contain duplicates"):
            VitalStreamSimulator(profiles, patient_ids=["P001", "P001"])


if __name__ == "__main__":
    unittest.main()
