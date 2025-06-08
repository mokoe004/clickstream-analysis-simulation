import subprocess
import os
from pathlib import Path

def main():
    print("🚀 Starte Spark-Job im Docker-Container ...")

    project_dir = Path(__file__).resolve().parent
    image = "spark:3.5.6-scala2.12-java17-python3-ubuntu"
    run_command = [
        "docker", "run", "-it", "--rm",
        "--user", "root",
        "-v", f"{project_dir}:/app",
        image,
        "bash", "-c",
        (
            "apt update && "
            "apt install -y python3 >/dev/null && "
            "python3 /app/run_spark.py"
        )
    ]

    try:
        subprocess.run(run_command, check=True)
        print("✅ Spark-Job abgeschlossen.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Fehler beim Ausführen des Spark-Jobs: {e}")

if __name__ == "__main__":
    main()
