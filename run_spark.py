import os
import subprocess
import tempfile

def run_spark():
    ivy_cache = tempfile.mkdtemp(prefix="ivy-cache-")
    env = os.environ.copy()
    env["SPARK_SUBMIT_OPTS"] = f"-Divy.cache.dir={ivy_cache} -Divy.home={ivy_cache}"
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
        "/app/spark_processor.py"
    ]
    print("📦 Ivy-Cache:", ivy_cache)
    print("⚙️  Starte spark-submit ...")
    subprocess.run(cmd, env=env, check=True)

if __name__ == "__main__":
    run_spark()
