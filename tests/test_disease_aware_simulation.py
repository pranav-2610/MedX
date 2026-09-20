from datetime import datetime, timedelta, timezone
from itertools import islice
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from models import PatientProfile
from patient_profiles import PatientProfileRepository
from vital_stream_simulator import (
    ClinicalSimulationState,
    ScenarioPlan,
    SimulatorConfig,
    VitalStreamSimulator,
)


PROFILE_PATH = Path(__file__).resolve().parents[1] / "data" / "patients.json"
START_TIME = datetime(2026, 9, 20, 10, 0, tzinfo=timezone.utc)
PLAN = ScenarioPlan(
    stable_events=2,
    deteriorating_events=3,
    critical_events=2,
    recovering_events=3,
)


def scenario_simulator(
    patient_id: str = "P002", *, phase_offset_events: int = 0, seed: int = 19
) -> VitalStreamSimulator:
    profiles = PatientProfileRepository.from_json_file(PROFILE_PATH)
    plan = ScenarioPlan(
        stable_events=PLAN.stable_events,
        deteriorating_events=PLAN.deteriorating_events,
        critical_events=PLAN.critical_events,
        recovering_events=PLAN.recovering_events,
        phase_offset_events=phase_offset_events,
    )
    return VitalStreamSimulator(
        profiles,
        patient_ids=[patient_id],
        config=SimulatorConfig(
            random_seed=seed,
            start_time=START_TIME,
            variation_scale=0.01,
            scenario_plans={patient_id: plan},
        ),
    )


class DiseaseAwareSimulationTests(unittest.TestCase):
    def test_profile_history_changes_disease_aware_baseline(self) -> None:
        common = dict(
            patient_id="PX",
            age=45,
            current_medications=(),
            labs={},
        )
        asthma = PatientProfile(relevant_history=("asthma",), **common)
        no_condition = PatientProfile(relevant_history=(), **common)
        asthma_simulator = VitalStreamSimulator(
            PatientProfileRepository([asthma]),
            config=SimulatorConfig(random_seed=11, start_time=START_TIME),
        )
        generic_simulator = VitalStreamSimulator(
            PatientProfileRepository([no_condition]),
            config=SimulatorConfig(random_seed=11, start_time=START_TIME),
        )

        asthma_baseline = asthma_simulator.baseline_for_patient("PX")
        generic_baseline = generic_simulator.baseline_for_patient("PX")
        self.assertGreater(asthma_baseline.respiratory_rate, generic_baseline.respiratory_rate)
        self.assertLess(asthma_baseline.spo2, generic_baseline.spo2)

    def test_default_simulation_remains_stable_with_noise(self) -> None:
        profiles = PatientProfileRepository.from_json_file(PROFILE_PATH)
        simulator = VitalStreamSimulator(
            profiles,
            patient_ids=["P001"],
            config=SimulatorConfig(random_seed=3, start_time=START_TIME),
        )
        baseline = simulator.baseline_for_patient("P001")
        events = list(islice(simulator, 6))

        self.assertTrue(all(simulator.simulation_state("P001") == ClinicalSimulationState.STABLE for _ in events))
        self.assertTrue(all(abs(event.heart_rate - baseline.heart_rate) < 10 for event in events))
        self.assertGreater(len({event.heart_rate for event in events}), 1)

    def test_deterioration_is_gradual_and_multi_parameter(self) -> None:
        simulator = scenario_simulator()
        events = list(islice(simulator, 5))

        first_deteriorating, last_deteriorating = events[2], events[4]
        self.assertEqual(simulator.simulation_state("P002"), ClinicalSimulationState.DETERIORATING)
        self.assertGreater(last_deteriorating.heart_rate, first_deteriorating.heart_rate)
        self.assertGreater(last_deteriorating.respiratory_rate, first_deteriorating.respiratory_rate)
        self.assertLess(last_deteriorating.spo2, first_deteriorating.spo2)

    def test_critical_is_more_deviated_and_recovery_moves_toward_baseline(self) -> None:
        simulator = scenario_simulator()
        events = list(islice(simulator, 10))
        baseline = simulator.baseline_for_patient("P002")
        critical = events[6]
        last_recovering = events[9]

        self.assertEqual(simulator.simulation_state("P002"), ClinicalSimulationState.RECOVERING)
        self.assertGreater(abs(critical.heart_rate - baseline.heart_rate), abs(last_recovering.heart_rate - baseline.heart_rate))
        self.assertGreater(abs(critical.respiratory_rate - baseline.respiratory_rate), abs(last_recovering.respiratory_rate - baseline.respiratory_rate))
        self.assertGreater(abs(critical.spo2 - baseline.spo2), abs(last_recovering.spo2 - baseline.spo2))

    def test_full_lifecycle_returns_to_stable(self) -> None:
        simulator = scenario_simulator()
        states = []
        for _ in range(11):
            simulator.next_event("P002")
            states.append(simulator.simulation_state("P002"))

        self.assertEqual(
            states,
            [
                ClinicalSimulationState.STABLE,
                ClinicalSimulationState.STABLE,
                ClinicalSimulationState.DETERIORATING,
                ClinicalSimulationState.DETERIORATING,
                ClinicalSimulationState.DETERIORATING,
                ClinicalSimulationState.CRITICAL,
                ClinicalSimulationState.CRITICAL,
                ClinicalSimulationState.RECOVERING,
                ClinicalSimulationState.RECOVERING,
                ClinicalSimulationState.RECOVERING,
                ClinicalSimulationState.STABLE,
            ],
        )

    def test_patients_can_have_independent_states(self) -> None:
        profiles = PatientProfileRepository.from_json_file(PROFILE_PATH)
        simulator = VitalStreamSimulator(
            profiles,
            patient_ids=["P001", "P002"],
            config=SimulatorConfig(
                random_seed=5,
                start_time=START_TIME,
                scenario_plans={
                    "P001": PLAN,
                    "P002": ScenarioPlan(
                        stable_events=2,
                        deteriorating_events=3,
                        critical_events=2,
                        recovering_events=3,
                        phase_offset_events=5,
                    ),
                },
            ),
        )
        simulator.next_event("P001")
        simulator.next_event("P002")

        self.assertEqual(simulator.simulation_state("P001"), ClinicalSimulationState.STABLE)
        self.assertEqual(simulator.simulation_state("P002"), ClinicalSimulationState.CRITICAL)

    def test_seed_and_scenario_plan_reproduce_vital_trajectory(self) -> None:
        first = list(islice(scenario_simulator(seed=23), 10))
        second = list(islice(scenario_simulator(seed=23), 10))

        projection = lambda events: [
            (
                event.patient_id,
                event.event_time,
                event.sequence_number,
                event.heart_rate,
                event.spo2,
                event.respiratory_rate,
                event.systolic_bp,
                event.diastolic_bp,
            )
            for event in events
        ]
        self.assertEqual(projection(first), projection(second))
        self.assertEqual(len({event.event_id for event in first}), len(first))
        self.assertEqual(set(first[0].model_dump()), set(first[0].__class__.model_fields))
        self.assertNotIn("clinical_state", first[0].model_dump())
        self.assertNotIn("disease", first[0].model_dump())


if __name__ == "__main__":
    unittest.main()
