"""Loading and lookup of static patient context."""

import json
from collections.abc import Iterator
from pathlib import Path

from pydantic import ValidationError

from models import PatientProfile


class ProfileLoadError(ValueError):
    """Raised when the complete static-profile dataset cannot be trusted."""


class PatientProfileRepository:
    """A validated, in-memory lookup of static profiles keyed by patient ID."""

    def __init__(self, profiles: list[PatientProfile]) -> None:
        profiles_by_id: dict[str, PatientProfile] = {}
        for profile in profiles:
            if profile.patient_id in profiles_by_id:
                raise ProfileLoadError(
                    f"duplicate profile for patient_id {profile.patient_id!r}"
                )
            profiles_by_id[profile.patient_id] = profile
        self._profiles_by_id = profiles_by_id

    @classmethod
    def from_json_file(cls, path: str | Path) -> "PatientProfileRepository":
        source = Path(path)
        try:
            with source.open(encoding="utf-8") as file:
                document = json.load(file)
        except OSError as error:
            raise ProfileLoadError(f"unable to read profile file {source}: {error}") from error
        except json.JSONDecodeError as error:
            raise ProfileLoadError(f"malformed JSON in profile file {source}: {error}") from error

        if not isinstance(document, dict) or set(document) != {"profiles"}:
            raise ProfileLoadError("profile JSON must be an object with exactly one 'profiles' field")
        raw_profiles = document["profiles"]
        if not isinstance(raw_profiles, list):
            raise ProfileLoadError("profile JSON field 'profiles' must be an array")

        profiles: list[PatientProfile] = []
        for index, raw_profile in enumerate(raw_profiles):
            try:
                profiles.append(PatientProfile.model_validate(raw_profile))
            except ValidationError as error:
                raise ProfileLoadError(f"invalid profile at index {index}: {error}") from error
        return cls(profiles)

    def get(self, patient_id: str) -> PatientProfile | None:
        """Return static context, or ``None`` when the patient is unknown."""
        return self._profiles_by_id.get(patient_id)

    def require(self, patient_id: str) -> PatientProfile:
        """Return static context or raise a clear error for an unknown patient."""
        profile = self.get(patient_id)
        if profile is None:
            raise KeyError(f"no static profile loaded for patient_id {patient_id!r}")
        return profile

    def __len__(self) -> int:
        return len(self._profiles_by_id)

    def __iter__(self) -> Iterator[PatientProfile]:
        return iter(self._profiles_by_id.values())
