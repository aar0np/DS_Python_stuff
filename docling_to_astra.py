"""
docling_to_astra.py
-------------------
Submit a batch of web documents to Docling for IBM watsonx and write the
resulting chunks (with server-side embeddings) into an AstraDB collection.

Required environment variables:
  DOCLING_SERVICE_URL  – base URL for the Docling API, WITHOUT query params.
                         Examples:
                           https://dcls.saas.ibm.com
                           https://workbench.aws-c1.dcls.saas.ibm.com/instances/<id>
                         Do NOT include '?mcsp_metadata=...' or any other query
                         string – copy only the path portion shown in the browser
                         address bar up to (but not including) the '?'.
  DOCLING_API_KEY      – your Docling API key
  ASTRA_API_ENDPOINT   – AstraDB endpoint, e.g. https://<id>-<region>.apps.astra.datastax.com
  ASTRA_TOKEN          – AstraDB application token (AstraCS:...)
  ASTRA_COLLECTION     – target collection name

Optional environment variables:
  ASTRA_KEYSPACE           – defaults to "default_keyspace"
  ASTRA_VECTORIZE_PROVIDER – defaults to "openai"
  ASTRA_VECTORIZE_MODEL    – defaults to "text-embedding-3-small"
  ASTRA_VECTORIZE_KEY      – provider API key (passed as providerKey); required
                             if your AstraDB vectorize integration is not
                             pre-configured with a shared key

Usage:
  python docling_to_astra.py
"""

import os
import sys
import time
from urllib.parse import urlparse, urlunparse

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SERVICE_URL = os.environ["DOCLING_SERVICE_URL"]
API_KEY = os.environ["DOCLING_API_KEY"]

ASTRA_API_ENDPOINT = os.environ["ASTRA_API_ENDPOINT"]
ASTRA_TOKEN = os.environ["ASTRA_TOKEN"]
ASTRA_COLLECTION = os.environ["ASTRA_COLLECTION"]
ASTRA_KEYSPACE = os.getenv("ASTRA_KEYSPACE", "default_keyspace")
ASTRA_VECTORIZE_PROVIDER = os.getenv("ASTRA_VECTORIZE_PROVIDER", "openai")
ASTRA_VECTORIZE_MODEL = os.getenv("ASTRA_VECTORIZE_MODEL", "text-embedding-3-small")
ASTRA_VECTORIZE_KEY = os.getenv("ASTRA_VECTORIZE_KEY")  # optional

# Documents to convert – edit this list as needed.
WEB_SOURCES = [
    "https://arxiv.org/pdf/2408.09869",  # Docling paper
    #"https://arxiv.org/pdf/2501.17887",  # DoclingV2 paper
    #"https://arxiv.org/pdf/2501.08828",  # third example
]

# Poll interval (seconds) while waiting for the batch job to finish.
POLL_INTERVAL = 5

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

HEADERS = {
    "X-Api-Key": API_KEY,
    "Content-Type": "application/json",
}


def submit_batch(sources: list[str]) -> str:
    """Submit a batch job and return the task_id."""
    astradb_target: dict = {
        "kind": "astradb_chunks",
        "api_endpoint": ASTRA_API_ENDPOINT,
        "token": ASTRA_TOKEN,
        "keyspace": ASTRA_KEYSPACE,
        "collection_name": ASTRA_COLLECTION,
        "vectorize_provider": ASTRA_VECTORIZE_PROVIDER,
        "vectorize_model": ASTRA_VECTORIZE_MODEL,
    }

    if ASTRA_VECTORIZE_KEY:
        astradb_target["vectorize_authentication"] = {
            "providerKey": ASTRA_VECTORIZE_KEY
        }

    payload = {
        "sources": [{"kind": "http", "url": url} for url in sources],
        "target": astradb_target,
    }

    resp = requests.post(
        f"{SERVICE_URL}/v1/convert/source/batch",
        json=payload,
        headers=HEADERS,
        timeout=30,
    )
    resp.raise_for_status()
    task_id: str = resp.json()["task_id"]
    print(f"Submitted batch job  task_id={task_id}")
    return task_id


def poll_until_done(task_id: str) -> dict:
    """Poll /v1/status/poll/{task_id} until status == 'success' or 'failure'."""
    url = f"{SERVICE_URL}/v1/status/poll/{task_id}"
    while True:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("task_status", "")
        print(f"  status={status}")
        if status == "success":
            return data
        if status in ("failure", "failed"):
            raise RuntimeError(
                f"Batch job {task_id} failed: {data.get('error_message') or data}"
            )
        time.sleep(POLL_INTERVAL)


def get_result(task_id: str) -> dict:
    """Retrieve the final result from /v1/result/{task_id}."""
    resp = requests.get(
        f"{SERVICE_URL}/v1/result/{task_id}",
        headers=HEADERS,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not WEB_SOURCES:
        print("WEB_SOURCES is empty – nothing to do.")
        sys.exit(0)

    print(f"Submitting {len(WEB_SOURCES)} document(s) …")

    task_id = submit_batch(WEB_SOURCES)

    print("Polling for completion …")
    poll_until_done(task_id)

    result = get_result(task_id)
    num_ok = result.get("num_succeeded", "?")
    num_total = result.get("num_converted", "?")
    elapsed = result.get("processing_time", "?")
    print(
        f"\nDone. {num_ok}/{num_total} document(s) succeeded "
        f"in {elapsed}s – chunks written to AstraDB collection '{ASTRA_COLLECTION}'."
    )


if __name__ == "__main__":
    main()
