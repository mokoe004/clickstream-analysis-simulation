import docker
import time
import atexit

# Konfiguration
IMAGE_NAME     = "apache/kafka:latest"
CONTAINER_NAME = "kafka_click"
HOST_PORT      = 9092
CTRL_PORT      = 9093

TOPIC_NAME        = "clickstream"
NUM_PARTITIONS    = 3
REPLICATION_FACTOR = 1


def start_kafka():
    client = docker.from_env()

    # Cleanup, falls der Container noch existiert
    try:
        old = client.containers.get(CONTAINER_NAME)
        print(f"Entferne alten Container {CONTAINER_NAME}")
        old.remove(force=True)
    except docker.errors.NotFound:
        pass

    print("Starte Kafka im KRaft-Modus")

    container = client.containers.run(
        IMAGE_NAME,
        name=CONTAINER_NAME,
        detach=True,
        remove=True,
        ports={
            f"{HOST_PORT}/tcp": HOST_PORT,
            f"{CTRL_PORT}/tcp": CTRL_PORT
        },
        environment={
            # Broker + Controller in einem
            "KAFKA_PROCESS_ROLES": "broker,controller",
            # Controller-Listener
            "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            # Client-Listener
            "KAFKA_LISTENERS": f"PLAINTEXT://0.0.0.0:{HOST_PORT},CONTROLLER://0.0.0.0:{CTRL_PORT}",
            # Client-Ankündigung
            "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://localhost:{HOST_PORT}",
            # Quorum-Voter: 1 Instanz bei localhost:CTRL_PORT
            "KAFKA_CONTROLLER_QUORUM_VOTERS": f"1@localhost:{CTRL_PORT}",
            "KAFKA_BROKER_ID": "1",
            # Protocol-Mapping
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
        }
    )

    # Stelle sicher, dass beim Beenden gestoppt wird
    # atexit.register(stop_kafka)

    # Kafka Hochfahren
    print("Warte, bis Kafka bereit ist")
    time.sleep(15)
    print(f"Kafka bereit unter localhost:{HOST_PORT}")
    
    create_topic(container)

    return container

# TOPIC ERSTELLEN
def create_topic(container):
    print(f"Erstelle Topic '{TOPIC_NAME}' …")
    # Pfad zu kafka-topics.sh im Apache-Image
    kafka_topics = "/opt/kafka/bin/kafka-topics.sh"

    cmd = [
        kafka_topics,
        "--create",
        "--topic", TOPIC_NAME,
        "--bootstrap-server", f"localhost:{HOST_PORT}",
        "--partitions", str(NUM_PARTITIONS),
        "--replication-factor", str(REPLICATION_FACTOR)
    ]

    # exec_run gibt Tuple (exit_code, output_bytes)
    exit_code, output = container.exec_run(cmd, stderr=True, stdout=True)
    output = output.decode("utf-8").strip()

    if exit_code == 0:
        print(f"✅ Topic angelegt: {output}")
    else:
        # Wenn Topic schon existiert -> Fehler-Code
        if "already exists" in output:
            print(f"Topic existiert bereits: {output}")
        else:
            raise RuntimeError(f"Fehler beim Erstellen des Topics: {output}")

def stop_kafka():
    client = docker.from_env()
    try:
        print("Stoppe Kafka-Container")
        c = client.containers.get(CONTAINER_NAME)
        c.stop(timeout=5)
        print("Kafka gestoppt")
    except docker.errors.NotFound:
        print("Kein laufender Kafka-Container gefunden")

if __name__ == "__main__":
    container = start_kafka()

    # Hier könntest Du jetzt Deinen Producer- oder Consumer-Code ausführen.
    input(">>> Apache Kafka läuft. Mit ENTER beenden …\n")

    # Beim Skript-Ende wird stop_kafka() automatisch aufgerufen.
