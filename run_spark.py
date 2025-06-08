import os
import subprocess
import tempfile

def run_spark():
    ivy_cache = tempfile.mkdtemp(prefix="ivy-cache-")
    env = os.environ.copy()
    env["SPARK_SUBMIT_OPTS"] = f"-Divy.cache.dir={ivy_cache} -Divy.home={ivy_cache}"
    env["KAFKA_BOOTSTRAP"] = env.get("KAFKA_BOOTSTRAP", "kafka:9092")

    cmd = [
        "/opt/spark/bin/spark-submit",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
        "/app/spark_processor.py"
    ]
    print(f"📦 Verwende Kafka-Bootstrap: {env['KAFKA_BOOTSTRAP']}")
    subprocess.run(cmd, env=env, check=True)

if __name__ == "__main__":
    run_spark()
