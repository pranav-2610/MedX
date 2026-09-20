"""Canonical domain contracts for the vitals ingestion pipeline."""

from datetime import datetime, timezone
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class VitalEvent(BaseModel):
    """One complete set of vital observations for one patient at one event time.

    This is the sole canonical event representation at the ingestion boundary.
    ``event_time`` is normalized to UTC and records observation time, not arrival
    time. All physiological values are required, finite numeric measurements.
    """

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    patient_id: Annotated[str, Field(min_length=1, max_length=128, strict=True)]
    event_time: datetime
    heart_rate: Annotated[float, Field(ge=0, le=300, allow_inf_nan=False, strict=True)]
    spo2: Annotated[float, Field(ge=0, le=100, allow_inf_nan=False, strict=True)]
    respiratory_rate: Annotated[
        float, Field(ge=0, le=120, allow_inf_nan=False, strict=True)
    ]
    systolic_bp: Annotated[float, Field(ge=0, le=300, allow_inf_nan=False, strict=True)]
    diastolic_bp: Annotated[float, Field(ge=0, le=200, allow_inf_nan=False, strict=True)]
    sequence_number: Annotated[int, Field(ge=1, strict=True)]
    source: Annotated[str, Field(min_length=1, max_length=128, strict=True)]

    @field_validator(
        "heart_rate",
        "spo2",
        "respiratory_rate",
        "systolic_bp",
        "diastolic_bp",
        mode="before",
    )
    @classmethod
    def reject_explicitly_missing_measurements(cls, value: object) -> object:
        if value is None:
            raise ValueError("explicitly missing physiological measurements are not valid")
        return value

    @field_validator("event_time")
    @classmethod
    def require_timezone_aware_observation_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("event_time must include a timezone offset")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_blood_pressure_pair(self) -> "VitalEvent":
        if self.diastolic_bp > self.systolic_bp:
            raise ValueError("diastolic_bp must not exceed systolic_bp")
        return self


class PatientProfile(BaseModel):
    """Static clinical context loaded once for a patient, outside the vital stream."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    patient_id: Annotated[str, Field(min_length=1, max_length=128, strict=True)]
    age: Annotated[int, Field(ge=0, le=130, strict=True)]
    relevant_history: tuple[
        Annotated[str, Field(min_length=1, max_length=256, strict=True)], ...
    ]
    current_medications: tuple[
        Annotated[str, Field(min_length=1, max_length=256, strict=True)], ...
    ]
    labs: dict[
        Annotated[str, Field(min_length=1, max_length=128, strict=True)],
        Annotated[float, Field(allow_inf_nan=False, strict=True)],
    ]


class IngestedVitalEvent(BaseModel):
    """A validated observation plus the time this service accepted it."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event: VitalEvent
    ingested_at: datetime
    kafka_topic: Annotated[str, Field(min_length=1)]
    kafka_partition: Annotated[int, Field(ge=0)]
    kafka_offset: Annotated[int, Field(ge=0)]

    @field_validator("ingested_at")
    @classmethod
    def require_timezone_aware_ingestion_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("ingested_at must include a timezone offset")
        return value.astimezone(timezone.utc)


class DeadLetterRecord(BaseModel):
    """Auditable metadata for a source record blocked at the ingestion boundary."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    category: Annotated[str, Field(min_length=1)]
    reason: Annotated[str, Field(min_length=1)]
    ingested_at: datetime
    source_topic: str
    source_partition: int
    source_offset: int
    original_payload_b64: str | None = None
    original_key_b64: str | None = None

    @field_validator("ingested_at")
    @classmethod
    def require_timezone_aware_dead_letter_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("ingested_at must include a timezone offset")
        return value.astimezone(timezone.utc)
