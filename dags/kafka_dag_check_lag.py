import json
import uuid
from pathlib import Path

from airflow.sdk import dag, task, Asset, AssetWatcher
from airflow.providers.apache.kafka.triggers.msg_queue import KafkaMessageQueueTrigger


# Define a trigger that listens to an Apache Kafka message queue
trigger = KafkaMessageQueueTrigger(
    topics=["temperature"],
    apply_function="kafka_utils.process_msg",
    kafka_config_id="kafka_default",
    apply_function_args=None,
    apply_function_kwargs=None,
    poll_timeout=1,
    poll_interval=1,
)

# Define an asset that watches for messages on the queue
asset = Asset("kafka_topic_asset", watchers=[AssetWatcher(name="kafka_watcher_test", trigger=trigger)])

@dag(schedule=[asset])
def new_kafka_dag():
    @task
    def topic_test(**context):
        triggering_asset_events = context["triggering_asset_events"]
        process_time = context["dag_run"].run_after

        for event in triggering_asset_events.get(asset, []):
            payload = event.extra.get("payload")
            print("Raw payload dict:", payload)
            if not payload:
                continue

            event_time = payload.get("event_timestamp")

            enriched_payload = {
                **payload,
                "process_timestamp": process_time.isoformat(),
                "airflow_latency_seconds": (
                    process_time.timestamp() - event_time
                    if event_time else None
                ),
            }

            # ---- path partitioning ----
            date_part = process_time.strftime("%Y-%m-%d")
            hour_part = f"{process_time.hour:02d}"
            sensor_id = payload.get("sensor_id", "unknown")

            base_path = Path("data/temperature-data")
            target_dir = base_path / date_part / hour_part / sensor_id
            target_dir.mkdir(parents=True, exist_ok=True)

            # ---- filename ----
            ts_part = process_time.strftime("%Y-%m-%dT%H-%M-%S_%fZ")
            rand_part = uuid.uuid4().hex[:6]

            file_path = target_dir / f"payload_{ts_part}_{rand_part}.json"

            with open(file_path, "w") as f:
                json.dump(enriched_payload, f, indent=2)

            print(f"Latency = {enriched_payload['airflow_latency_seconds']} seconds")

    # This task will automatically run when trigger fires
    topic_test()

new_kafka_dag()