"""Republish original VitalEvents from a local Parquet archive to Kafka/Redpanda."""

import os
from pathlib import Path
from datetime import date

from raw_event_archive import RawEventArchive, RawEventArchiveConfig, ReplayProducer
from streaming_producer import ProducerConfig, VitalEventProducer


def main() -> None:
    archive = RawEventArchive(
        RawEventArchiveConfig(directory=Path(os.getenv("MEDX_ARCHIVE_DIRECTORY", "raw_events")))
    )
    producer_config = ProducerConfig(
        bootstrap_servers=os.getenv("MEDX_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        topic=os.getenv("MEDX_REPLAY_KAFKA_TOPIC", "patient_vitals_replay"),
        client_id=os.getenv("MEDX_REPLAY_KAFKA_CLIENT_ID", "medx-vital-replay-producer"),
    )
    max_events = _optional_int("MEDX_REPLAY_MAX_EVENTS")
    patient_filter = _patient_filter(os.getenv("MEDX_REPLAY_PATIENT_IDS"))
    start_date = _optional_date("MEDX_REPLAY_START_DATE")
    end_date = _optional_date("MEDX_REPLAY_END_DATE")
    interval_seconds = float(os.getenv("MEDX_REPLAY_INTERVAL_SECONDS", "0"))
    try:
        with VitalEventProducer(producer_config) as producer:
            published = ReplayProducer(archive, producer).replay(
                start_date=start_date,
                end_date=end_date,
                patient_ids=patient_filter,
                max_events=max_events,
                interval_seconds=interval_seconds,
            )
    finally:
        archive.close()
    print(f"Replayed {published} original VitalEvent message(s) to {producer_config.topic!r}.")


def _optional_int(name: str) -> int | None:
    value = os.getenv(name)
    return int(value) if value else None


def _patient_filter(value: str | None) -> set[str] | None:
    return {patient_id.strip() for patient_id in value.split(",") if patient_id.strip()} if value else None


def _optional_date(name: str) -> date | None:
    value = os.getenv(name)
    return date.fromisoformat(value) if value else None


if __name__ == "__main__":
    main()
