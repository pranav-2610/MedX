# Disease-aware vital stream simulator

`VitalStreamSimulator` remains the lazy, round-robin source of canonical
`VitalEvent` observations. It uses loaded static profiles to shape synthetic
baselines and trajectories, then publishes ordinary vitals through the existing
producer path.

```text
Static Patient Profile
        │
        ├── disease/history
        ├── age
        ├── medications
        └── labs
        │
        ↓
Disease-Aware Simulation Model
        │
        ↓
Clinical State
(STABLE / DETERIORATING / CRITICAL / RECOVERING)
        │
        ↓
Physiological trajectory + noise
        │
        ↓
VitalEvent
        │
        ↓
Kafka
```

## Disease-aware baseline and trajectory

The small, extensible `DISEASE_MODELS` registry currently supports conditions
present in the synthetic profiles: hypertension, asthma, heart failure, chronic
kidney disease, and obstructive sleep apnea. Matching history items combine
their baseline and deterioration offsets. Age makes a small baseline adjustment;
the existing NumPy-generated patient variation remains in place. Medications and
labs remain available static context but do not receive speculative physiological
effects in this simulator version.

The resulting observation is approximately:

```text
patient random baseline + profile/age effect + state-progress effect + noise
```

This is synthetic scenario data, not a clinical model or diagnostic claim.

## Scenario plans

Without scenario plans, all patients remain stable exactly as in Block 2.
`ScenarioPlan` adds a deterministic lifecycle to selected patients:

```python
from datetime import datetime, timedelta, timezone
from vital_stream_simulator import ScenarioPlan, SimulatorConfig, VitalStreamSimulator

config = SimulatorConfig(
    random_seed=7,
    start_time=datetime(2026, 9, 20, tzinfo=timezone.utc),
    sampling_interval=timedelta(minutes=1),
    scenario_plans={
        "P002": ScenarioPlan(stable_events=5, deteriorating_events=8,
                               critical_events=5, recovering_events=8),
        "P003": ScenarioPlan(stable_events=5, deteriorating_events=8,
                               critical_events=5, recovering_events=8,
                               phase_offset_events=13),
    },
)
simulator = VitalStreamSimulator(profiles, config=config)
```

For a broker-backed demo using the repository's synthetic profiles, start the
Block 3 Redpanda setup and run this in PowerShell:

```powershell
$env:MEDX_SCENARIO_DEMO = "1"
$env:MEDX_SIMULATOR_SEED = "7"
$env:MEDX_MAX_EVENTS = "100"
python run_producer.py
```

`MEDX_SCENARIO_DEMO` is opt-in. Without it, `run_producer.py` preserves the
all-stable default behavior.

The lifecycle is `STABLE → DETERIORATING → CRITICAL → RECOVERING → STABLE`.
Phase lengths, severity, optional repetition, and a per-patient phase offset
are configurable. Offset enables an independent deterministic demo state for
each patient. State changes are gradual: deterioration ramps upward, critical
is stronger, and recovery decays back toward the disease-aware baseline.

`simulation_state(patient_id)` and `baseline_for_patient(patient_id)` expose
simulator-only ground truth for tests or demos. They never change `VitalEvent`.
Events contain only the Block 0 observation fields—no disease, scenario state,
risk, or alert label—so downstream analysis must independently infer patterns.

## Streaming and reproducibility

The iterator yields one event at a time forever in profile order, avoiding a
prebuilt dataset. Per-patient timestamps and sequence numbers advance as before.
The same profiles, seed, start time, and scenario plans reproduce vitals and
state progression; event IDs intentionally remain newly generated UUIDs.

The simulator is only the source layer. It does not run Kafka, ingestion,
data-quality handling, state windows, deterioration detection, scoring, or
alerts. Pass it unchanged to `run_producer.py` or `VitalEventProducer`.
