

mkdir airflow_home 
export AIRFLOW_HOME="$(pwd)/airflow_home"
export AIRFLOW__CORE__PLUGINS_FOLDER="$(pwd)/plugins"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW_CONN_KAFKA_DEFAULT='{
  "conn_type": "kafka",
  "extra": {
    "bootstrap.servers": "localhost:9092",
    "group.id": "group_1",
    "security.protocol": "PLAINTEXT",
    "enable.auto.commit": false,
    "auto.offset.reset": "beginning"
  }
}'
uv venv .venv 
source .venv/bin/activate 
uv pip install -r pyproject.toml
python -m airflow standalone