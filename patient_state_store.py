"""Local, bounded recent-observation state keyed by patient identifier."""

from collections import deque
from collections.abc import Iterator

from models import VitalEvent


class PatientStateStore:
    """Maintain independent, arrival-ordered windows of validated vital events.

    This local store deliberately has no Kafka, profile, data-quality, clinical,
    or persistence responsibilities. Its caller must pass only observations that
    have already crossed the trusted ingestion boundary.
    """

    def __init__(self, max_events_per_patient: int = 60) -> None:
        if max_events_per_patient <= 0:
            raise ValueError("max_events_per_patient must be positive")
        self.max_events_per_patient = max_events_per_patient
        self._events_by_patient: dict[str, deque[VitalEvent]] = {}

    def update(self, event: VitalEvent) -> tuple[VitalEvent, ...]:
        """Append one validated event and return a defensive state snapshot.

        Events retain their arrival order. When a window is full, ``deque``
        removes its oldest event as this new event is appended.
        """
        window = self._events_by_patient.setdefault(
            event.patient_id, deque(maxlen=self.max_events_per_patient)
        )
        # The surrounding ingestion path is at-least-once.  A retry can occur
        # after this store was updated but before a later callback step or the
        # Kafka offset commit completed.  Retaining a repeated event would
        # corrupt this local arrival history, so make retained entries
        # idempotent by canonical event identity.
        if any(existing.event_id == event.event_id for existing in window):
            return self.get(event.patient_id) or ()
        # Store and return copies: VitalEvent is intentionally mutable at the
        # schema boundary, but callers must not be able to mutate store state.
        window.append(event.model_copy(deep=True))
        return self.get(event.patient_id) or ()

    def get(self, patient_id: str) -> tuple[VitalEvent, ...] | None:
        """Return an immutable snapshot, or ``None`` when no state exists."""
        window = self._events_by_patient.get(patient_id)
        if window is None:
            return None
        return tuple(event.model_copy(deep=True) for event in window)

    def patient_ids(self) -> tuple[str, ...]:
        """Return the identifiers with currently retained state."""
        return tuple(self._events_by_patient)

    def __len__(self) -> int:
        """Return the number of patients currently represented in memory."""
        return len(self._events_by_patient)

    def __iter__(self) -> Iterator[tuple[str, tuple[VitalEvent, ...]]]:
        """Iterate over defensive patient-state snapshots."""
        for patient_id in self._events_by_patient:
            snapshot = self.get(patient_id)
            if snapshot is not None:
                yield patient_id, snapshot
