# import json
# import uuid
# from pathlib import Path
#
# from airflow.sdk import dag, task, Asset, AssetWatcher
# from airflow.providers.apache.kafka.triggers.msg_queue import KafkaMessageQueueTrigger
#
#
# # Define a trigger that listens to an Apache Kafka message queue
# trigger = KafkaMessageQueueTrigger(
#     topics=["test"],
#     apply_function="kafka_utils.process_msg",
#     kafka_config_id="kafka",
#     apply_function_args=None,
#     apply_function_kwargs=None,
#     poll_timeout=1,
#     poll_interval=5,
# )
#
# # Define an asset that watches for messages on the queue
# asset = Asset("kafka_topic_asset", watchers=[AssetWatcher(name="kafka_watcher_test", trigger=trigger)])
#
# @dag(schedule=[asset])
# def kafka_topic_test():
#     @task
#     def topic_test(**context):
#         triggering_asset_events = context["triggering_asset_events"]
#
#         for event in triggering_asset_events.get(asset, []):
#             event_time = event.timestamp  # true event time (or dag_run.run_after when dag started)
#             date_part = event_time.strftime("%Y-%m-%d")
#             hour_part = f"{event_time.hour:02d}"
#             payload = event.extra.get("payload")
#             print("Raw payload dict:", payload)
#             if not payload:
#                 continue
#
#             sensor_id = payload.get("sensor_id", "unknown")
#
#             base_path = Path("data/temperature-data")
#             target_dir = base_path / date_part / hour_part / sensor_id
#             target_dir.mkdir(parents=True, exist_ok=True)
#             target_dir.mkdir(parents=True, exist_ok=True)
#             # ---- filename ----
#             ts_part = event_time.strftime("%Y-%m-%dT%H-%M-%S_%fZ")
#             rand_part = uuid.uuid4().hex[:6]
#
#             file_name = f"payload_{ts_part}_{rand_part}.json"
#             file_path = target_dir / file_name
#
#             with open(file_path, "w") as f:
#                 json.dump(payload, f, indent=2)
#
#             print(f"Saved payload to {file_path}")
#
#     # This task will automatically run when trigger fires
#     topic_test()
#
# kafka_topic_test()