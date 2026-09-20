"""Event-time rolling windows over independently arriving patient observations."""

from collections import deque
from collections.abc import Iterator
from datetime import datetime, timedelta

from models import VitalEvent


class RollingWindowManager:
    """Maintain per-patient, event-time windows without clinical interpretation.

    For each patient, the window anchor is the greatest observed ``event_time``.
    A retained event satisfies ``anchor_time - window_duration < event_time <=
    anchor_time``. Retained events remain in their source arrival order.
    """

    def __init__(self, window_duration: timedelta) -> None:
        if window_duration <= timedelta(0):
            raise ValueError("window_duration must be positive")
        self.window_duration = window_duration
        self._windows_by_patient: dict[str, deque[VitalEvent]] = {}
        self._anchors_by_patient: dict[str, datetime] = {}

    def update(self, event: VitalEvent) -> tuple[VitalEvent, ...]:
        """Incorporate an event and return its patient's defensive window snapshot."""
        patient_id = event.patient_id
        anchor = self._anchors_by_patient.get(patient_id)
        if anchor is None or event.event_time > anchor:
            anchor = event.event_time
            self._anchors_by_patient[patient_id] = anchor

        window = self._windows_by_patient.setdefault(patient_id, deque())
        # Match the state store's retry behavior.  This protects a retry of a
        # partially completed archive → state → window callback without
        # claiming durable cross-process deduplication.
        if any(existing.event_id == event.event_id for existing in window):
            return self.get_window(patient_id) or ()
        # Keep a private copy; callers can mutate Pydantic instances received
        # from an ingestion callback without changing the window.
        window.append(event.model_copy(deep=True))
        self._prune(window, anchor)
        return self.get_window(patient_id) or ()

    def get_window(self, patient_id: str) -> tuple[VitalEvent, ...] | None:
        """Return an immutable defensive snapshot, or ``None`` if unseen."""
        window = self._windows_by_patient.get(patient_id)
        if window is None:
            return None
        return tuple(event.model_copy(deep=True) for event in window)

    def anchor_time(self, patient_id: str) -> datetime | None:
        """Return the current event-time horizon anchor for observability."""
        return self._anchors_by_patient.get(patient_id)

    def patient_ids(self) -> tuple[str, ...]:
        return tuple(self._windows_by_patient)

    def __len__(self) -> int:
        return len(self._windows_by_patient)

    def __iter__(self) -> Iterator[tuple[str, tuple[VitalEvent, ...]]]:
        for patient_id in self._windows_by_patient:
            snapshot = self.get_window(patient_id)
            if snapshot is not None:
                yield patient_id, snapshot

    def _prune(self, window: deque[VitalEvent], anchor: datetime) -> None:
        earliest_included = anchor - self.window_duration
        retained = [
            event for event in window if earliest_included < event.event_time <= anchor
        ]
        window.clear()
        window.extend(retained)
