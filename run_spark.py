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
        "--packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1"
        ]),
        "/app/spark_processor.py"
    ]
    print(f"📦 Verwende Kafka-Bootstrap: {env['KAFKA_BOOTSTRAP']}")
    try:
        subprocess.run(cmd, env=env, check=True)
    except subprocess.CalledProcessError as e:
        print("❌ Spark-Job ist abgestürzt.")
        print(e)
        exit(1)


if __name__ == "__main__":
    run_spark()
