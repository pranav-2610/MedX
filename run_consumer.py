"""Run the trusted vital-ingestion boundary against Kafka/Redpanda."""

import os
from datetime import timedelta
from pathlib import Path

from ingestion_consumer import ConsumerConfig, VitalIngestionConsumer
from models import IngestedVitalEvent
from patient_profiles import PatientProfileRepository
from patient_state_store import PatientStateStore
from raw_event_archive import ArchiveAndStateProcessor, RawEventArchive, RawEventArchiveConfig
from rolling_window_manager import RollingWindowManager


def archive_update_and_print_state(
    processor: ArchiveAndStateProcessor, ingested_event: IngestedVitalEvent
) -> None:
    """Archive trusted data durably before updating local state and window."""
    processor(ingested_event)
    updated_state = processor.state_store.get(ingested_event.event.patient_id) or ()
    updated_window = processor.rolling_windows.get_window(ingested_event.event.patient_id) or ()
    print(
        f"Updated {ingested_event.event.patient_id}: "
        f"{len(updated_state)} state event(s), {len(updated_window)} window event(s); "
        f"ingested_at={ingested_event.ingested_at.isoformat()}"
    )


def main() -> None:
    config = ConsumerConfig(
        bootstrap_servers=os.getenv("MEDX_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        topic=os.getenv("MEDX_KAFKA_TOPIC", "patient_vitals"),
        group_id=os.getenv("MEDX_KAFKA_CONSUMER_GROUP", "medx-vital-ingestion"),
        auto_offset_reset=os.getenv("MEDX_KAFKA_AUTO_OFFSET_RESET", "earliest"),
        dead_letter_topic=os.getenv("MEDX_KAFKA_DLQ_TOPIC", "patient_vitals_dlq"),
    )
    profiles = PatientProfileRepository.from_json_file("data/patients.json")
    state_store = PatientStateStore()
    rolling_windows = RollingWindowManager(timedelta(minutes=30))
    archive = RawEventArchive(
        RawEventArchiveConfig(
            directory=Path(os.getenv("MEDX_ARCHIVE_DIRECTORY", "raw_events")),
            batch_size=int(os.getenv("MEDX_ARCHIVE_BATCH_SIZE", "100")),
        )
    )
    processor = ArchiveAndStateProcessor(archive, state_store, rolling_windows)
    with archive, VitalIngestionConsumer(profiles, config) as consumer:
        try:
            consumer.run(
                lambda ingested_event: archive_update_and_print_state(processor, ingested_event)
            )
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
