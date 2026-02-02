import base64
import hashlib
import hmac
import json
import time
from typing import Any, Dict, Optional


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * ((4 - (len(s) % 4)) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("utf-8"))


def sign_session(payload: Dict[str, Any], secret: str) -> str:
    """
    Creates a signed token: base64url(payload_json).base64url(hmac_sha256)
    """
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    msg = _b64url_encode(raw).encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).digest()
    return f"{msg.decode('utf-8')}.{_b64url_encode(sig)}"


def verify_session(token: str, secret: str) -> Optional[Dict[str, Any]]:
    try:
        if not token or "." not in token:
            return None
        msg_b64, sig_b64 = token.split(".", 1)

        msg_bytes = msg_b64.encode("utf-8")
        expected = hmac.new(secret.encode("utf-8"), msg_bytes, hashlib.sha256).digest()
        got = _b64url_decode(sig_b64)

        if not hmac.compare_digest(expected, got):
            return None

        payload = json.loads(_b64url_decode(msg_b64).decode("utf-8"))
        exp = int(payload.get("exp", 0))
        if exp and time.time() > exp:
            return None
        return payload
    except Exception:
        return None