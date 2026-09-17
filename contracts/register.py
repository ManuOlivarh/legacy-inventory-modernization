#!/usr/bin/env python3
"""Registers the Avro contracts in Schema Registry and pins their compatibility.

Schemas are published deliberately, from files under version control. Services run
with auto.register.schemas=false, so a service can never invent a contract at runtime.
"""
import json
import pathlib
import sys
import urllib.error
import urllib.request

REGISTRY = sys.argv[1] if len(sys.argv) > 1 else "http://schema-registry:8081"
CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"

# One subject per topic value, using the default TopicNameStrategy.
SUBJECTS = {
    "inventory.stock-movements.v1-value": "StockMoved.avsc",
    "inventory.stock-levels.v1-value": "StockLevelChanged.avsc",
}


def request(method, path, payload):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{REGISTRY}{path}", data=body, method=method,
        headers={"Content-Type": CONTENT_TYPE},
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.load(response)


def main():
    here = pathlib.Path(__file__).parent
    for subject, filename in SUBJECTS.items():
        schema = (here / filename).read_text()
        # BACKWARD lets new consumers read old events, which is what a rolling
        # deployment needs. It is set before the first version exists.
        request("PUT", f"/config/{subject}", {"compatibility": "BACKWARD"})
        result = request("POST", f"/subjects/{subject}/versions", {"schema": schema})
        print(f"{subject}: id {result['id']}")


if __name__ == "__main__":
    try:
        main()
    except urllib.error.HTTPError as error:
        print(f"registration failed: {error.read().decode()}", file=sys.stderr)
        raise SystemExit(1)
