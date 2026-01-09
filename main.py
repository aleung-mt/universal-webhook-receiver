from fastapi import FastAPI, Request, Response, HTTPException
from google.cloud import pubsub_v1
import os
import json
import base64
import uuid
import datetime

app = FastAPI()

# Required env vars
PROJECT_ID = os.environ["GCP_PROJECT"]          # e.g. "my-gcp-project"
TOPIC_ID = os.environ["PUBSUB_TOPIC"]          # e.g. "raw-webhooks"

# Optional env vars
SHARED_SECRET = os.environ.get("SHARED_SECRET")  # optional, recommended for PoC
MAX_INLINE_BYTES = int(os.environ.get("MAX_INLINE_BYTES", str(9 * 1024 * 1024)))  # ~9 MiB

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def safe_body_to_text(body_bytes: bytes) -> tuple[str, bool]:
    """
    Return (text, is_base64). If bytes aren't valid UTF-8, base64 encode them.
    """
    try:
        return body_bytes.decode("utf-8"), False
    except UnicodeDecodeError:
        return base64.b64encode(body_bytes).decode("ascii"), True

@app.post("/webhook/{source}")
async def receive_webhook(source: str, request: Request):
    # Optional shared secret check for generic sources
    if SHARED_SECRET:
        if request.headers.get("x-webhook-secret") != SHARED_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")

    body_bytes = await request.body()

    # Pub/Sub max message size is 10MB; keep envelope comfortably below it
    if len(body_bytes) > MAX_INLINE_BYTES:
        raise HTTPException(status_code=413, detail="Payload too large for this pipeline")

    body_text, body_is_base64 = safe_body_to_text(body_bytes)

    # Try parsing JSON if body is UTF-8 text
    body_json = None
    if not body_is_base64:
        try:
            body_json = json.loads(body_text)
        except Exception:
            body_json = None

    now = datetime.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
    request_id = str(uuid.uuid4())

    envelope = {
        "request_id": request_id,
        "received_at": now,
        "source": source,
        "method": request.method,
        "path": request.url.path,
        "query": dict(request.query_params),
        "headers": dict(request.headers),
        "content_type": request.headers.get("content-type"),
        "body_text": body_text,
        "body_is_base64": body_is_base64,
        "body_json": body_json,
        "remote_ip": request.client.host if request.client else None,
        "user_agent": request.headers.get("user-agent"),
    }

    data = json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    # Publish and wait briefly to ensure it succeeded before returning 204
    publisher.publish(
        topic_path,
        data,
        source=source,
        request_id=request_id
    ).result(timeout=10)

    # 204 is commonly used for webhooks (no body, success)
    return Response(status_code=204)