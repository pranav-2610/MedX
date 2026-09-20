"""Durable local Parquet archive and replay source for trusted vital events."""

from collections import defaultdict
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
import os
from pathlib import Path
import time
from typing import Any, Protocol
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from models import IngestedVitalEvent, VitalEvent
from patient_state_store import PatientStateStore
from rolling_window_manager import RollingWindowManager


ARCHIVE_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("patient_id", pa.string(), nullable=False),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("heart_rate", pa.float64(), nullable=False),
        pa.field("spo2", pa.float64(), nullable=False),
        pa.field("respiratory_rate", pa.float64(), nullable=False),
        pa.field("systolic_bp", pa.float64(), nullable=False),
        pa.field("diastolic_bp", pa.float64(), nullable=False),
        pa.field("sequence_number", pa.int64(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("kafka_topic", pa.string(), nullable=False),
        pa.field("kafka_partition", pa.int64(), nullable=False),
        pa.field("kafka_offset", pa.int64(), nullable=False),
        pa.field("archive_sequence", pa.int64(), nullable=False),
        pa.field("archived_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


@dataclass(frozen=True)
class RawEventArchiveConfig:
    directory: Path | str = Path("raw_events")
    batch_size: int = 100


class VitalEventPublisher(Protocol):
    def publish(self, event: VitalEvent) -> None: ...


class RawEventArchive:
    """Batch trusted events into UTC-date-partitioned, atomically written Parquet.

    Every appended event is first persisted into a valid per-date staging Parquet
    file. This makes the archive callback durable before an ingestion consumer
    commits its source offset. Once a batch reaches capacity, it is atomically
    finalized as a ``part-*.parquet`` file.
    """

    def __init__(
        self,
        config: RawEventArchiveConfig | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config or RawEventArchiveConfig()
        if self.config.batch_size <= 0:
            raise ValueError("archive batch_size must be positive")
        self.directory = Path(self.config.directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._writer_id = uuid4().hex
        self._batches: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._next_archive_sequence, self._archived_event_ids = self._discover_archive_state()
        self._next_part_by_date: dict[str, int] = {}
        self._closed = False

    def __enter__(self) -> "RawEventArchive":
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def append(self, ingested_event: IngestedVitalEvent) -> bool:
        """Durably stage one accepted event and finalize a full date batch.

        Returns ``False`` when the canonical ``event_id`` already exists in
        this archive.  This makes a retry after an archive write but before a
        Kafka offset commit idempotent across process restarts.
        """
        if self._closed:
            raise RuntimeError("archive is closed")
        event_id = str(ingested_event.event.event_id)
        if event_id in self._archived_event_ids:
            return False
        partition = ingested_event.event.event_time.astimezone(timezone.utc).date().isoformat()
        self._next_archive_sequence += 1
        record = self._record_from_ingested(ingested_event)
        batch = self._batches[partition]
        batch.append(record)
        try:
            self._write_staging(partition)
        except Exception:
            batch.pop()
            raise
        self._archived_event_ids.add(event_id)
        if len(batch) >= self.config.batch_size:
            # If finalization fails, the complete batch remains both in memory
            # and in its durable staging file for retry; do not drop its event.
            self._flush_partition(partition)
        return True

    def flush(self) -> None:
        """Finalize all pending batches. Called during clean application shutdown."""
        if self._closed:
            return
        for partition in tuple(self._batches):
            if self._batches[partition]:
                self._flush_partition(partition)

    def close(self) -> None:
        if self._closed:
            return
        self.flush()
        self._closed = True

    def iter_ingested_events(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        patient_ids: set[str] | None = None,
        max_events: int | None = None,
    ) -> Iterator[IngestedVitalEvent]:
        """Read archived events in recorded archive-sequence order.

        ``archive_sequence`` represents this archive writer's persisted consumer
        processing order. It is not claimed to be a broker-global order across
        independently consumed Kafka partitions.
        """
        if max_events is not None and max_events <= 0:
            return
        records: list[dict[str, Any]] = []
        for path in self._parquet_paths():
            table = pq.read_table(path)
            records.extend(table.to_pylist())
        records.sort(
            key=lambda item: (
                item["archive_sequence"],
                item["kafka_topic"],
                item["kafka_partition"],
                item["kafka_offset"],
            )
        )
        yielded = 0
        for record in records:
            event_time = record["event_time"].astimezone(timezone.utc)
            if start_date is not None and event_time.date() < start_date:
                continue
            if end_date is not None and event_time.date() > end_date:
                continue
            if patient_ids is not None and record["patient_id"] not in patient_ids:
                continue
            yield self._ingested_from_record(record)
            yielded += 1
            if max_events is not None and yielded >= max_events:
                return

    def iter_events(self, **filters: Any) -> Iterator[VitalEvent]:
        """Read canonical events only, preserving archive replay order."""
        for ingested_event in self.iter_ingested_events(**filters):
            yield ingested_event.event

    def _record_from_ingested(self, ingested_event: IngestedVitalEvent) -> dict[str, Any]:
        event = ingested_event.event
        return {
            "event_id": str(event.event_id),
            "patient_id": event.patient_id,
            "event_time": event.event_time.astimezone(timezone.utc),
            "heart_rate": event.heart_rate,
            "spo2": event.spo2,
            "respiratory_rate": event.respiratory_rate,
            "systolic_bp": event.systolic_bp,
            "diastolic_bp": event.diastolic_bp,
            "sequence_number": event.sequence_number,
            "source": event.source,
            "ingested_at": ingested_event.ingested_at.astimezone(timezone.utc),
            "kafka_topic": ingested_event.kafka_topic,
            "kafka_partition": ingested_event.kafka_partition,
            "kafka_offset": ingested_event.kafka_offset,
            "archive_sequence": self._next_archive_sequence,
            "archived_at": self._clock().astimezone(timezone.utc),
        }

    def _write_staging(self, partition: str) -> None:
        destination = self._staging_path(partition)
        self._atomic_write(destination, self._batches[partition])

    def _flush_partition(self, partition: str) -> None:
        batch = self._batches[partition]
        if not batch:
            return
        part_number = self._next_part_number(partition)
        destination = self._partition_directory(partition) / f"part-{part_number:06d}.parquet"
        self._atomic_write(destination, batch)
        self._staging_path(partition).unlink(missing_ok=True)
        batch.clear()

    def _atomic_write(self, destination: Path, records: Sequence[dict[str, Any]]) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")
        try:
            pq.write_table(pa.Table.from_pylist(list(records), schema=ARCHIVE_SCHEMA), temporary, compression="zstd")
            # PyArrow has closed its writer at this point. Flush the file to
            # the operating system before its atomic promotion, so a process
            # crash cannot acknowledge data that only existed in Python-side
            # buffers.
            with temporary.open("r+b") as file:
                os.fsync(file.fileno())
            os.replace(temporary, destination)
            with destination.open("r+b") as file:
                os.fsync(file.fileno())
        finally:
            temporary.unlink(missing_ok=True)

    def _partition_directory(self, partition: str) -> Path:
        return self.directory / partition

    def _staging_path(self, partition: str) -> Path:
        return self._partition_directory(partition) / f"_pending-{self._writer_id}.parquet"

    def _next_part_number(self, partition: str) -> int:
        if partition not in self._next_part_by_date:
            highest = 0
            for path in self._partition_directory(partition).glob("part-*.parquet"):
                try:
                    highest = max(highest, int(path.stem.removeprefix("part-")))
                except ValueError:
                    continue
            self._next_part_by_date[partition] = highest + 1
        part = self._next_part_by_date[partition]
        self._next_part_by_date[partition] += 1
        return part

    def _discover_archive_state(self) -> tuple[int, set[str]]:
        highest = 0
        event_ids: set[str] = set()
        for path in self._parquet_paths():
            table = pq.read_table(path, columns=["archive_sequence", "event_id"])
            values = table.column("archive_sequence")
            if len(values):
                highest = max(highest, max(value.as_py() for value in values))
            event_ids.update(value.as_py() for value in table.column("event_id"))
        return highest, event_ids

    def _parquet_paths(self) -> list[Path]:
        return sorted(self.directory.rglob("*.parquet"))

    @staticmethod
    def _ingested_from_record(record: dict[str, Any]) -> IngestedVitalEvent:
        event = VitalEvent.model_validate(
            {
                "event_id": record["event_id"],
                "patient_id": record["patient_id"],
                "event_time": record["event_time"],
                "heart_rate": record["heart_rate"],
                "spo2": record["spo2"],
                "respiratory_rate": record["respiratory_rate"],
                "systolic_bp": record["systolic_bp"],
                "diastolic_bp": record["diastolic_bp"],
                "sequence_number": record["sequence_number"],
                "source": record["source"],
            }
        )
        return IngestedVitalEvent(
            event=event,
            ingested_at=record["ingested_at"],
            kafka_topic=record["kafka_topic"],
            kafka_partition=record["kafka_partition"],
            kafka_offset=record["kafka_offset"],
        )


class ArchiveAndStateProcessor:
    """Archive before updating local state so archive failure prevents commit."""

    def __init__(
        self,
        archive: RawEventArchive,
        state_store: PatientStateStore,
        rolling_windows: RollingWindowManager,
    ) -> None:
        self.archive = archive
        self.state_store = state_store
        self.rolling_windows = rolling_windows

    def __call__(self, ingested_event: IngestedVitalEvent) -> None:
        self.archive.append(ingested_event)
        self.state_store.update(ingested_event.event)
        self.rolling_windows.update(ingested_event.event)


class ReplayProducer:
    """Republish original archived events through an existing VitalEvent producer."""

    def __init__(self, archive: RawEventArchive, publisher: VitalEventPublisher) -> None:
        self.archive = archive
        self.publisher = publisher

    def replay(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        patient_ids: set[str] | None = None,
        max_events: int | None = None,
        interval_seconds: float = 0.0,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> int:
        if interval_seconds < 0:
            raise ValueError("replay interval_seconds must not be negative")
        events = iter(self.archive.iter_events(
            start_date=start_date,
            end_date=end_date,
            patient_ids=patient_ids,
            max_events=max_events,
        ))
        try:
            event = next(events)
        except StopIteration:
            return 0

        published = 0
        while True:
            self.publisher.publish(event)
            published += 1

            try:
                next_event = next(events)
            except StopIteration:
                break

            if interval_seconds:
                sleeper(interval_seconds)
            event = next_event
        return published
