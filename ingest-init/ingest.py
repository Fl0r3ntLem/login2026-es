import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
import requests
from requests.auth import HTTPBasicAuth

ELASTICSEARCH_URL = "http://elasticsearch:9200"
ELASTIC_USER = os.environ.get("ELASTIC_USER", "elastic")
ELASTIC_PASSWORD = os.environ.get("ELASTIC_PASSWORD", "111")

SOURCE_DIR = Path("/source")


def wait_for_elasticsearch() -> None:
    for attempt in range(1, 11):
        try:
            requests.get(f"{ELASTICSEARCH_URL}/", timeout=30, auth=(ELASTIC_USER, ELASTIC_PASSWORD)).raise_for_status()
            print("Elasticsearch is ready")
            return
        except (HTTPError, URLError):
            print(f"Waiting for Elasticsearch... ({attempt}/10)")
            time.sleep(10)
    raise RuntimeError("Elasticsearch did not become ready in time")


def install_pipeline_and_template() -> None:
    pipeline = (SOURCE_DIR / "ncedc-earthquakes-pipeline.json").read_bytes()
    template = (SOURCE_DIR / "ncedc-earthquakes-template.json").read_bytes()

    auth=HTTPBasicAuth(ELASTIC_USER, ELASTIC_PASSWORD)

    requests.put(
        f"{ELASTICSEARCH_URL}/_ingest/pipeline/ncedc-earthquakes",
        data=pipeline,
        headers={"Content-Type": "application/json"},
        auth=auth,
        timeout=30,
    ).raise_for_status()

    requests.put(
        f"{ELASTICSEARCH_URL}/_index_template/ncedc-earthquakes",
        data=template,
        headers={"Content-Type": "application/json"},
        auth=auth,
        timeout=30,
    ).raise_for_status()

    print("Installed ingest pipeline and index template")


def already_ingested() -> bool:
    auth = HTTPBasicAuth(ELASTIC_USER, ELASTIC_PASSWORD)
    indices = [
        "ncedc-earthquakes-earthquake",
        "ncedc-earthquakes-blast",
    ]

    for index_name in indices:
        response = requests.get(
            f"{ELASTICSEARCH_URL}/{index_name}/_count",
            auth=auth,
            timeout=30,
        )
        if response.status_code == 404:
            return False
        response.raise_for_status()
        if response.json().get("count", 0) == 0:
            return False

    return True


def bulk_ingest(file_path: Path, index_name: str, event_type: str) -> int:
    count = 0
    buffer: list[str] = []

    def flush(buffer) -> None:
        if not buffer:
            return
        payload = "\n".join(buffer) + "\n"
        response = requests.post(
            f"{ELASTICSEARCH_URL}/_bulk?pipeline=ncedc-earthquakes",
            data=payload.encode("utf-8"),
            headers={"Content-Type": "application/x-ndjson"},
            auth=HTTPBasicAuth(ELASTIC_USER, ELASTIC_PASSWORD),
            timeout=30,
        ).json()
        if response.get("errors"):
            raise RuntimeError(f"Bulk ingest failed for {index_name}")
        buffer = []

    with file_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            buffer.append(json.dumps({"index": {"_index": index_name}}))
            buffer.append(json.dumps({"message": line, "type": event_type}))
            count += 1
            if count % 1000 == 0:
                flush(buffer)

    flush(buffer)
    requests.post(
        f"{ELASTICSEARCH_URL}/{index_name}/_refresh",
        auth=HTTPBasicAuth(ELASTIC_USER, ELASTIC_PASSWORD),
        timeout=30,
    )

    return count


def ingest_dataset_once() -> None:
    if already_ingested():
        print("Dataset already ingested, skipping data upload")
        return

    eq_count = bulk_ingest(
        SOURCE_DIR / "earthquakes.txt",
        "ncedc-earthquakes-earthquake",
        "earthquake",
    )
    blast_count = bulk_ingest(
        SOURCE_DIR / "blasts.txt",
        "ncedc-earthquakes-blast",
        "blast",
    )
    print(f"Ingestion completed: earthquakes={eq_count}, blasts={blast_count}")


def main() -> None:
    wait_for_elasticsearch()
    install_pipeline_and_template()
    ingest_dataset_once()


if __name__ == "__main__":
    main()
