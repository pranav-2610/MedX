"""Disease-aware synthetic vital-event source for development and demos."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Iterator, Mapping, Sequence
from uuid import uuid4

import numpy as np

from models import PatientProfile, VitalEvent
from patient_profiles import PatientProfileRepository


class ClinicalSimulationState(StrEnum):
    """Simulator-only ground truth; never serialized into ``VitalEvent``."""

    STABLE = "STABLE"
    DETERIORATING = "DETERIORATING"
    CRITICAL = "CRITICAL"
    RECOVERING = "RECOVERING"


@dataclass(frozen=True)
class VitalOffsets:
    """Synthetic change vectors in the units defined by ``VitalEvent``."""

    heart_rate: float = 0.0
    spo2: float = 0.0
    respiratory_rate: float = 0.0
    systolic_bp: float = 0.0
    diastolic_bp: float = 0.0

    def __add__(self, other: "VitalOffsets") -> "VitalOffsets":
        return VitalOffsets(
            heart_rate=self.heart_rate + other.heart_rate,
            spo2=self.spo2 + other.spo2,
            respiratory_rate=self.respiratory_rate + other.respiratory_rate,
            systolic_bp=self.systolic_bp + other.systolic_bp,
            diastolic_bp=self.diastolic_bp + other.diastolic_bp,
        )

    def scaled(self, factor: float) -> "VitalOffsets":
        return VitalOffsets(
            heart_rate=self.heart_rate * factor,
            spo2=self.spo2 * factor,
            respiratory_rate=self.respiratory_rate * factor,
            systolic_bp=self.systolic_bp * factor,
            diastolic_bp=self.diastolic_bp * factor,
        )


@dataclass(frozen=True)
class DiseaseModel:
    """A small condition-specific influence on baseline and deterioration shape."""

    condition: str
    baseline_effect: VitalOffsets
    deterioration_effect: VitalOffsets


DISEASE_MODELS: dict[str, DiseaseModel] = {
    "hypertension": DiseaseModel(
        "hypertension",
        baseline_effect=VitalOffsets(systolic_bp=10, diastolic_bp=5),
        deterioration_effect=VitalOffsets(heart_rate=12, respiratory_rate=3, spo2=-2, systolic_bp=14, diastolic_bp=7),
    ),
    "asthma": DiseaseModel(
        "asthma",
        baseline_effect=VitalOffsets(respiratory_rate=1, spo2=-0.5),
        deterioration_effect=VitalOffsets(heart_rate=17, spo2=-11, respiratory_rate=12, systolic_bp=-3, diastolic_bp=-2),
    ),
    "heart failure": DiseaseModel(
        "heart failure",
        baseline_effect=VitalOffsets(heart_rate=3, spo2=-1, respiratory_rate=1),
        deterioration_effect=VitalOffsets(heart_rate=25, spo2=-8, respiratory_rate=9, systolic_bp=-20, diastolic_bp=-10),
    ),
    "chronic kidney disease": DiseaseModel(
        "chronic kidney disease",
        baseline_effect=VitalOffsets(systolic_bp=5, diastolic_bp=2),
        deterioration_effect=VitalOffsets(heart_rate=9, spo2=-2, respiratory_rate=4, systolic_bp=6, diastolic_bp=3),
    ),
    "obstructive sleep apnea": DiseaseModel(
        "obstructive sleep apnea",
        baseline_effect=VitalOffsets(spo2=-1, respiratory_rate=0.5),
        deterioration_effect=VitalOffsets(heart_rate=12, spo2=-9, respiratory_rate=8, systolic_bp=4, diastolic_bp=2),
    ),
}

GENERIC_DETERIORATION = VitalOffsets(
    heart_rate=12, spo2=-4, respiratory_rate=5, systolic_bp=-4, diastolic_bp=-2
)


@dataclass(frozen=True)
class ScenarioPlan:
    """A deterministic, per-patient STABLE→…→RECOVERING simulation schedule.

    ``phase_offset_events`` supports demo patients beginning at different points
    of the same lifecycle. When ``repeat`` is false, a completed lifecycle stays
    stable. ``severity`` scales only the synthetic state deviation, not baseline.
    """

    stable_events: int = 20
    deteriorating_events: int = 15
    critical_events: int = 10
    recovering_events: int = 15
    phase_offset_events: int = 0
    severity: float = 1.0
    repeat: bool = False


@dataclass(frozen=True)
class SimulatorConfig:
    """Configuration for virtual-time, disease-aware vital event generation."""

    sampling_interval: timedelta = timedelta(seconds=60)
    random_seed: int | None = None
    source: str = "vital_stream_simulator"
    variation_scale: float = 1.0
    start_time: datetime | None = None
    scenario_plans: Mapping[str, ScenarioPlan] = field(default_factory=dict)


@dataclass
class _PatientSimulationState:
    profile: PatientProfile
    baseline: VitalOffsets
    deterioration_effect: VitalOffsets
    scenario_plan: ScenarioPlan | None
    sequence_number: int = 0


class VitalStreamSimulator:
    """Generate lazy, round-robin ``VitalEvent`` observations for profile patients.

    With no ``scenario_plans``, every patient remains stable as in Block 2. A
    configured plan alters only the simulator's internal physiological trajectory;
    disease and clinical-state ground truth never appear in emitted events.
    """

    def __init__(
        self,
        profile_repository: PatientProfileRepository,
        *,
        patient_ids: Sequence[str] | None = None,
        config: SimulatorConfig | None = None,
    ) -> None:
        self._config = config or SimulatorConfig()
        self._validate_config(self._config)
        self._rng = np.random.default_rng(self._config.random_seed)
        self._start_time = self._normalise_start_time(self._config.start_time)

        profiles = self._select_profiles(profile_repository, patient_ids)
        if not profiles:
            raise ValueError("at least one patient profile is required for simulation")
        if len({profile.patient_id for profile in profiles}) != len(profiles):
            raise ValueError("patient_ids must not contain duplicates")
        unknown_plans = set(self._config.scenario_plans) - {profile.patient_id for profile in profiles}
        if unknown_plans:
            raise ValueError(f"scenario plans reference unconfigured patient IDs: {sorted(unknown_plans)}")

        self._states = {profile.patient_id: self._new_state(profile) for profile in profiles}
        self._patient_ids = tuple(profile.patient_id for profile in profiles)

    def __iter__(self) -> Iterator[VitalEvent]:
        return self.events()

    def events(self) -> Iterator[VitalEvent]:
        """Yield events forever in stable round-robin patient order."""
        while True:
            for patient_id in self._patient_ids:
                yield self.next_event(patient_id)

    def next_event(self, patient_id: str) -> VitalEvent:
        """Generate the next canonical observation for one configured patient."""
        try:
            state = self._states[patient_id]
        except KeyError as error:
            raise KeyError(f"patient_id {patient_id!r} is not configured for simulation") from error

        state.sequence_number += 1
        event_time = self._start_time + self._config.sampling_interval * (state.sequence_number - 1)
        state_kind, intensity = self._state_at(state)
        values = self._values(state, intensity)

        return VitalEvent(
            event_id=uuid4(),
            patient_id=patient_id,
            event_time=event_time,
            heart_rate=values.heart_rate,
            spo2=values.spo2,
            respiratory_rate=values.respiratory_rate,
            systolic_bp=values.systolic_bp,
            diastolic_bp=values.diastolic_bp,
            sequence_number=state.sequence_number,
            source=self._config.source,
        )

    def simulation_state(self, patient_id: str) -> ClinicalSimulationState:
        """Return simulator-only ground truth for test/demo inspection."""
        try:
            state = self._states[patient_id]
        except KeyError as error:
            raise KeyError(f"patient_id {patient_id!r} is not configured for simulation") from error
        return self._state_at(state)[0]

    def baseline_for_patient(self, patient_id: str) -> VitalOffsets:
        """Return a copy-safe synthetic baseline for test/demo inspection."""
        try:
            return self._states[patient_id].baseline
        except KeyError as error:
            raise KeyError(f"patient_id {patient_id!r} is not configured for simulation") from error

    def _new_state(self, profile: PatientProfile) -> _PatientSimulationState:
        baseline_effect, deterioration_effect = self._disease_effects(profile)
        age_effect = VitalOffsets(
            heart_rate=max(profile.age - 50, 0) * 0.04,
            systolic_bp=max(profile.age - 45, 0) * 0.12,
            diastolic_bp=max(profile.age - 45, 0) * 0.04,
        )
        baseline = VitalOffsets(
            heart_rate=float(self._rng.uniform(68, 82)),
            spo2=float(self._rng.uniform(96, 99)),
            respiratory_rate=float(self._rng.uniform(14, 18)),
            systolic_bp=float(self._rng.uniform(110, 130)),
            diastolic_bp=float(self._rng.uniform(68, 82)),
        ) + baseline_effect + age_effect
        return _PatientSimulationState(
            profile=profile,
            baseline=baseline,
            deterioration_effect=deterioration_effect,
            scenario_plan=self._config.scenario_plans.get(profile.patient_id),
        )

    def _disease_effects(self, profile: PatientProfile) -> tuple[VitalOffsets, VitalOffsets]:
        baseline = VitalOffsets()
        deterioration = VitalOffsets()
        matched = False
        for history_item in profile.relevant_history:
            model = DISEASE_MODELS.get(history_item.casefold())
            if model is not None:
                matched = True
                baseline = baseline + model.baseline_effect
                deterioration = deterioration + model.deterioration_effect
        return baseline, deterioration if matched else GENERIC_DETERIORATION

    def _state_at(self, state: _PatientSimulationState) -> tuple[ClinicalSimulationState, float]:
        plan = state.scenario_plan
        if plan is None:
            return ClinicalSimulationState.STABLE, 0.0
        lifecycle_length = (
            plan.stable_events
            + plan.deteriorating_events
            + plan.critical_events
            + plan.recovering_events
        )
        position = state.sequence_number - 1 + plan.phase_offset_events
        if plan.repeat:
            position %= lifecycle_length
        elif position >= lifecycle_length:
            return ClinicalSimulationState.STABLE, 0.0

        if position < plan.stable_events:
            return ClinicalSimulationState.STABLE, 0.0
        position -= plan.stable_events
        if position < plan.deteriorating_events:
            return (
                ClinicalSimulationState.DETERIORATING,
                plan.severity * 0.75 * (position + 1) / plan.deteriorating_events,
            )
        position -= plan.deteriorating_events
        if position < plan.critical_events:
            return (
                ClinicalSimulationState.CRITICAL,
                plan.severity * (0.75 + 0.25 * (position + 1) / plan.critical_events),
            )
        position -= plan.critical_events
        return (
            ClinicalSimulationState.RECOVERING,
            plan.severity * 0.75 * (1 - position / plan.recovering_events),
        )

    def _values(self, state: _PatientSimulationState, intensity: float) -> VitalOffsets:
        scale = self._config.variation_scale
        centre = state.baseline + state.deterioration_effect.scaled(intensity)
        values = VitalOffsets(
            heart_rate=self._bounded_value(centre.heart_rate, 2.5 * scale, 45, 180),
            spo2=self._bounded_value(centre.spo2, 0.5 * scale, 70, 100),
            respiratory_rate=self._bounded_value(centre.respiratory_rate, 1.0 * scale, 8, 45),
            systolic_bp=self._bounded_value(centre.systolic_bp, 3.0 * scale, 70, 200),
            diastolic_bp=self._bounded_value(centre.diastolic_bp, 2.0 * scale, 40, 120),
        )
        return VitalOffsets(
            heart_rate=values.heart_rate,
            spo2=values.spo2,
            respiratory_rate=values.respiratory_rate,
            systolic_bp=values.systolic_bp,
            diastolic_bp=min(values.diastolic_bp, values.systolic_bp),
        )

    def _bounded_value(self, baseline: float, deviation: float, lower: float, upper: float) -> float:
        value = self._rng.normal(baseline, deviation)
        return float(round(np.clip(value, lower, upper), 1))

    @staticmethod
    def _select_profiles(
        repository: PatientProfileRepository, patient_ids: Sequence[str] | None
    ) -> list[PatientProfile]:
        if patient_ids is None:
            return list(repository)
        return [repository.require(patient_id) for patient_id in patient_ids]

    @staticmethod
    def _normalise_start_time(start_time: datetime | None) -> datetime:
        if start_time is None:
            return datetime.now(timezone.utc)
        if start_time.tzinfo is None or start_time.utcoffset() is None:
            raise ValueError("simulator start_time must include a timezone offset")
        return start_time.astimezone(timezone.utc)

    @staticmethod
    def _validate_config(config: SimulatorConfig) -> None:
        if config.sampling_interval <= timedelta(0):
            raise ValueError("sampling_interval must be positive")
        if not config.source.strip():
            raise ValueError("simulator source must be non-empty")
        if config.variation_scale <= 0:
            raise ValueError("variation_scale must be positive")
        for patient_id, plan in config.scenario_plans.items():
            if not isinstance(patient_id, str) or not patient_id:
                raise ValueError("scenario plan patient IDs must be non-empty strings")
            if not isinstance(plan, ScenarioPlan):
                raise ValueError("scenario_plans values must be ScenarioPlan instances")
            if min(
                plan.stable_events,
                plan.deteriorating_events,
                plan.critical_events,
                plan.recovering_events,
            ) <= 0:
                raise ValueError("all scenario phase durations must be positive")
            if plan.phase_offset_events < 0:
                raise ValueError("phase_offset_events must not be negative")
            if plan.severity <= 0:
                raise ValueError("scenario severity must be positive")
