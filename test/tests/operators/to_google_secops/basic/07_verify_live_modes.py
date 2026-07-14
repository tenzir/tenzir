# runner: python

from __future__ import annotations

import json
import os
from pathlib import Path


def captures() -> list[dict]:
    path = Path(os.environ["GOOGLE_SECOPS_CAPTURE_FILE"])
    return [json.loads(line) for line in path.read_text().splitlines()]


def token_captures() -> list[dict]:
    path = Path(os.environ["GOOGLE_SECOPS_TOKEN_CAPTURE_FILE"])
    return [json.loads(line) for line in path.read_text().splitlines()]


def main() -> None:
    records = captures()
    udm = [record for record in records if record["path"].endswith("/events:import")]
    assert len(udm) == 1, udm
    events = udm[0]["payload"]["inlineSource"]["events"]
    assert events == [
        {
            "udm": {
                "metadata": {
                    "eventTimestamp": "2026-01-01T00:00:00Z",
                    "collectedTimestamp": "2026-01-01T00:00:01Z",
                    "eventType": "NETWORK_CONNECTION",
                    "vendorName": "Tenzir",
                    "productName": "Tenzir Pipeline",
                    "productVersion": "dev",
                    "productEventType": "live-connection",
                    "productLogId": "live-udm-001",
                    "description": "Network connection observed by Tenzir",
                },
                "principal": {
                    "hostname": "live-udm.example",
                    "ip": ["192.0.2.10"],
                    "user": {
                        "userid": "alice",
                        "emailAddresses": ["alice@example.com"],
                    },
                },
                "target": {
                    "hostname": "live-service.example",
                    "ip": ["198.51.100.20"],
                    "port": 443,
                },
                "observer": {
                    "hostname": "sensor-01.example",
                    "ip": ["192.0.2.5"],
                },
                "network": {
                    "applicationProtocol": "HTTPS",
                    "ipProtocol": "TCP",
                    "sentBytes": 1250,
                    "receivedBytes": 4096,
                },
                "securityResult": [
                    {
                        "action": ["ALLOW"],
                        "severity": "LOW",
                    },
                ],
                "additional": {
                    "fields": {
                        "sourcePipeline": {
                            "stringValue": "to_google_secops-basic-test",
                        },
                        "marker": {
                            "stringValue": "tenzir-live-udm-rich",
                        },
                    },
                },
            },
        },
    ]
    entities = [
        record for record in records if record["path"].endswith("/entities:import")
    ]
    assert len(entities) == 1, entities
    inline_source = entities[0]["payload"]["inlineSource"]
    assert inline_source == {
        "entities": [
            {
                "metadata": {
                    "collectedTimestamp": "2026-01-01T00:00:01Z",
                    "vendorName": "Tenzir",
                    "productName": "Tenzir Pipeline",
                    "entityType": "USER",
                },
                "entity": {
                    "user": {
                        "userid": "live-entity@example.com",
                        "productObjectId": "live-entity-0001",
                        "userDisplayName": "Live Entity Example",
                        "emailAddresses": [
                            "live-entity@example.com",
                            "live-entity@corp.example",
                        ],
                        "employeeId": "E0001",
                        "title": "Security Analyst",
                        "companyName": "Example Corp",
                        "department": "Security Operations",
                    },
                },
                "additional": {
                    "fields": {
                        "sourcePipeline": {
                            "stringValue": "to_google_secops-basic-test",
                        },
                        "marker": {
                            "stringValue": "tenzir-live-entity-rich",
                        },
                    },
                },
            },
        ],
        "logType": "AZURE_AD_CONTEXT",
    }
    ingestion = [
        record
        for record in records
        if record["path"] == "/v2/unstructuredlogentries:batchCreate"
    ]
    assert len(ingestion) == 2, ingestion
    ingestion_by_text = {
        record["payload"]["entries"][0]["log_text"]: record["payload"]
        for record in ingestion
    }
    raw_log = (
        "<134>1 2026-01-01T00:00:00Z host app - - - derive timestamp from this log"
    )
    assert ingestion_by_text[raw_log] == {
        "customer_id": "1234567890",
        "log_type": "CUSTOM_JSON",
        "namespace": "tenzir",
        "labels": [{"key": "env", "value": "test"}],
        "entries": [{"log_text": raw_log}],
    }
    assert ingestion_by_text["use explicit timestamp"] == {
        "customer_id": "1234567890",
        "log_type": "CUSTOM_JSON",
        "namespace": "explicit-ns",
        "entries": [
            {
                "log_text": "use explicit timestamp",
                "ts_epoch_microseconds": 1767225600123456,
            }
        ],
    }
    tokens = token_captures()
    assert tokens, tokens
    assert all(token["assertion_segments"] == 3 for token in tokens), tokens
    assert all(token["signature_verified"] for token in tokens), tokens
    assert all(token["claims_validated"] for token in tokens), tokens
    issuers = {token["issuer"] for token in tokens}
    assert issuers == {
        "test-only-email@test-only-project-id.iam.gserviceaccount.com"
    }, issuers
    scopes = {token["scope"] for token in tokens}
    assert "https://www.googleapis.com/auth/cloud-platform" in scopes, scopes
    assert "https://www.googleapis.com/auth/malachite-ingestion" in scopes, scopes
    audiences = {token["audience"] for token in tokens}
    assert audiences == {
        os.environ["GOOGLE_SECOPS_TOKEN_URL"],
        "https://oauth2.googleapis.com/token",
    }, audiences
    assert all(
        0 < token["expires_at"] - token["issued_at"] <= 3600 for token in tokens
    ), tokens
    print("udm_live: true")
    print("entity_live: true")
    print("raw_ingestion_live: true")
    print("optional_log_entry_time: true")
    print("oauth_token_exchange: true")


if __name__ == "__main__":
    main()
