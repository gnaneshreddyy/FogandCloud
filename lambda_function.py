"""AWS Lambda handler for the cloud layer (deployed by aws_setup.py).

Invoked through a Lambda Function URL (HTTPS). Stores everything in the
DynamoDB table named by the TABLE_NAME environment variable:
    pk = patient id, sk = "<kind>#<item id>"
"""
import base64
import json
import os
from decimal import Decimal

import boto3

from common.cloud_core import decode_body, handle

_table = boto3.resource("dynamodb").Table(os.environ.get("TABLE_NAME", "icu-monitoring"))


def _to_dynamo(obj):
    return json.loads(json.dumps(obj), parse_float=Decimal)


class DynamoStore:
    def put(self, kind, items, source, received):
        with _table.batch_writer(overwrite_by_pkeys=["pk", "sk"]) as batch:
            for it in items:
                item_id = it.get("alert_id") or it.get("reading_id") or it.get("first_reading")
                batch.put_item(Item=_to_dynamo({
                    "pk": it.get("patient", "ward"),
                    "sk": f"{kind}#{item_id}",
                    "kind": kind,
                    "source": source,
                    "received": received,
                    "body": it,
                }))


_store = DynamoStore()


def lambda_handler(event, context):
    if event.get("requestContext", {}).get("http", {}).get("method") == "GET":
        return {"statusCode": 200, "body": json.dumps({"ok": True, "service": "icu-cloud-aws"})}
    raw = event.get("body") or ""
    raw = base64.b64decode(raw) if event.get("isBase64Encoded") else raw.encode()
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    try:
        body = decode_body(raw, headers.get("content-encoding", ""))
        result = handle(body, _store)
        return {"statusCode": 200, "headers": {"Content-Type": "application/json"},
                "body": json.dumps(result)}
    except Exception as exc:
        return {"statusCode": 400, "body": json.dumps({"ok": False, "error": str(exc)})}
