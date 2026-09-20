"""Run the stable vital simulator and publish its events to Kafka/Redpanda."""

import os
from itertools import islice

from patient_profiles import PatientProfileRepository
from streaming_producer import ProducerConfig, VitalEventProducer
from vital_stream_simulator import ScenarioPlan, SimulatorConfig, VitalStreamSimulator


def main() -> None:
    profiles = PatientProfileRepository.from_json_file("data/patients.json")
    simulator = VitalStreamSimulator(
        profiles,
        config=SimulatorConfig(
            random_seed=_optional_int("MEDX_SIMULATOR_SEED"),
            scenario_plans=_demo_scenario_plans() if os.getenv("MEDX_SCENARIO_DEMO") == "1" else {},
        ),
    )
    events = iter(simulator)
    max_events = _optional_int("MEDX_MAX_EVENTS")
    if max_events is not None:
        events = islice(events, max_events)

    config = ProducerConfig(
        bootstrap_servers=os.getenv("MEDX_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        topic=os.getenv("MEDX_KAFKA_TOPIC", "patient_vitals"),
        client_id=os.getenv("MEDX_KAFKA_CLIENT_ID", "medx-vital-producer"),
    )
    with VitalEventProducer(config) as producer:
        published = producer.run(events)
    print(f"Published {published} VitalEvent message(s) to {config.topic!r}.")


def _optional_int(name: str) -> int | None:
    value = os.getenv(name)
    return int(value) if value else None


def _demo_scenario_plans() -> dict[str, ScenarioPlan]:
    """A deterministic opt-in scenario for the repository's synthetic profiles."""
    return {
        "P002": ScenarioPlan(stable_events=5, deteriorating_events=8, critical_events=5, recovering_events=8),
        "P003": ScenarioPlan(
            stable_events=5,
            deteriorating_events=8,
            critical_events=5,
            recovering_events=8,
            phase_offset_events=13,
        ),
        "P004": ScenarioPlan(
            stable_events=5,
            deteriorating_events=8,
            critical_events=5,
            recovering_events=8,
            phase_offset_events=18,
        ),
    }


if __name__ == "__main__":
    main()
