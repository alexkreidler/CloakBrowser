"""Active response-body capture for cloakserve recording.

What it does
------------
Watches the CDP stream flowing through cloakserve.  After each
``Network.loadingFinished`` event, decides whether to fetch the body via
``Network.getResponseBody`` and, if so, generates a synthetic command for the
proxy to send to Chrome.  The reply is intercepted (not forwarded to the
client) and the body is fed back into the recorder so it lands in
``cdp.jsonl`` (as a synthetic frame, marked ``"injected": true``) and into
the HAR builder (so HAR entries get ``response.content.text``).

Why active capture exists
-------------------------
``Network.dataReceived`` events only carry byte counts, never bytes. The
on-the-wire body is only obtainable via ``Network.getResponseBody`` (or
``Fetch.*`` interception, which is heavier).  The original recorder was
"passive" — it only paired bodies when the *client* explicitly asked for
them. agent-browser, raw-CDP scripts, and most non-Playwright clients
never call ``getResponseBody``, so their transcripts contained zero
HTML/JSON bodies. This module adds a server-side opt-in.

Filters
-------
* MIME allow-list (default: text/*, application/json, etc. — no images,
  fonts, video, wasm, octet-stream, ...).
* Per-body cap (default 2 MiB encoded length).
* Per-session total cap (default 100 MiB).
* Skips ``loadingFailed`` and (optionally) cached responses.

Threading model
---------------
Single-threaded; safe to drive from one async task per direction. All
state is plain Python dicts; the proxy serializes calls.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Iterable

from .config import RecordingConfig

logger = logging.getLogger("cloakbrowser.recording.bodycap")


# Sentinel ID base.  CDP message IDs are typically small positive ints
# allocated by clients starting from 1; we pick a base far above any
# realistic client allocation so collisions are impossible in practice.
#
# IMPORTANT: Chromium's CDP dispatcher uses a *signed 32-bit int* for the
# message id field.  IDs >= 2**31 are silently dropped (no reply, no error
# event).  We pick 1e9 — comfortably above realistic client allocations
# (clients tend to use small monotonic integers in the low thousands) but
# well below the 2^31 ceiling so we have headroom for ~10**9 injected
# commands per session before we'd risk collision.
_INJECTED_ID_BASE = 1_000_000_000  # 10**9, well below 2**31 = 2_147_483_648
_INJECTED_ID_MAX = 2_000_000_000   # leave a safety margin under 2**31


@dataclass
class _PendingBody:
    """In-flight ``Network.getResponseBody`` we issued ourselves."""

    session_id: str
    request_id: str
    url: str
    mime: str
    encoded_size: int


@dataclass
class BodyCaptureMetrics:
    requested: int = 0    # bodies we asked Chrome for
    captured: int = 0     # bodies we successfully received
    skipped_mime: int = 0
    skipped_size: int = 0
    skipped_budget: int = 0
    skipped_failed: int = 0
    failed: int = 0       # CDP errors on getResponseBody
    bytes_captured: int = 0


@dataclass
class CapturedBody:
    """Result of a successfully-intercepted ``Network.getResponseBody`` reply."""

    session_id: str
    request_id: str
    body: str
    base64_encoded: bool
    mime: str


class BodyCapture:
    """Stateful active body-capture engine.

    Use ``feed_event()`` for every CDP frame coming from Chrome
    (``cdp_to_client`` direction), and ``intercept_reply()`` to detect
    replies to our injected commands.

    Parameters:
        config: ``RecordingConfig`` controlling MIME allow-list, size caps,
            and whether body capture is enabled at all.
    """

    def __init__(self, config: RecordingConfig) -> None:
        self._config = config

        # (sessionId, requestId) -> mime / encoded size, captured from
        # Network.responseReceived.  Cleared on loadingFinished/Failed
        # after the decision is made.
        self._response_meta: dict[tuple[str, str], tuple[str, int]] = {}

        # Our own outstanding command IDs -> what we asked for.
        self._pending: dict[int, _PendingBody] = {}

        self._next_id = _INJECTED_ID_BASE
        self._total_bytes = 0
        self.metrics = BodyCaptureMetrics()

    # ------------------------------------------------------------------
    # Predicates / config
    # ------------------------------------------------------------------

    @property
    def enabled(self) -> bool:
        return self._config.bodies_enabled

    def _budget_remaining(self) -> int | None:
        cap = self._config.max_total_body_bytes
        if not cap:
            return None  # unlimited
        return max(0, cap - self._total_bytes)

    # ------------------------------------------------------------------
    # Event ingestion
    # ------------------------------------------------------------------

    def feed_event(self, msg: dict) -> list[dict]:
        """Process a CDP frame from Chrome (cdp_to_client direction).

        Returns a list of CDP commands that the caller must send to
        Chrome on the same WebSocket. The caller is also responsible
        for mirroring those commands into the recorder's transcript
        (as ``client_to_cdp`` synthetic frames) so the JSONL stays a
        complete record of what flowed.

        Note: we only react to *events* here (frames with ``method`` and
        no ``id``).  Replies are handled by ``intercept_reply``.
        """
        if not self.enabled or not isinstance(msg, dict):
            return []
        method = msg.get("method")
        if not method or "id" in msg:
            return []

        params = msg.get("params") or {}
        session_id = msg.get("sessionId", "")

        if method == "Network.responseReceived":
            req_id = params.get("requestId")
            if not req_id:
                return []
            resp = params.get("response") or {}
            mime = resp.get("mimeType", "") or ""
            encoded = int(resp.get("encodedDataLength", 0) or 0)
            self._response_meta[(session_id, req_id)] = (mime, encoded)
            return []

        if method == "Network.loadingFinished":
            req_id = params.get("requestId")
            if not req_id:
                return []
            key = (session_id, req_id)
            meta = self._response_meta.pop(key, None)
            # Prefer the encodedDataLength from loadingFinished if larger
            # (it's the authoritative final number).
            final_size = int(params.get("encodedDataLength", 0) or 0)
            if meta is None and final_size <= 0:
                return []
            mime, _ = meta if meta else ("", 0)
            encoded = max(final_size, meta[1] if meta else 0)
            return self._maybe_request_body(session_id, req_id, mime, encoded)

        if method == "Network.loadingFailed":
            req_id = params.get("requestId")
            if req_id:
                self._response_meta.pop((session_id, req_id), None)
                self.metrics.skipped_failed += 1
            return []

        return []

    def _maybe_request_body(
        self,
        session_id: str,
        request_id: str,
        mime: str,
        encoded_size: int,
    ) -> list[dict]:
        if not self._config.mime_is_allowed(mime):
            self.metrics.skipped_mime += 1
            return []
        if encoded_size > self._config.max_body_bytes:
            self.metrics.skipped_size += 1
            return []
        remaining = self._budget_remaining()
        if remaining is not None and remaining <= 0:
            self.metrics.skipped_budget += 1
            return []
        if remaining is not None and encoded_size > remaining:
            self.metrics.skipped_budget += 1
            return []

        # Guard against the (extremely unlikely) case of running out of
        # sentinel ID space.  ~10^9 commands per session is plenty.
        if self._next_id >= _INJECTED_ID_MAX:
            self.metrics.skipped_budget += 1
            return []

        self._next_id += 1
        msg_id = self._next_id
        self._pending[msg_id] = _PendingBody(
            session_id=session_id,
            request_id=request_id,
            url="",  # url not needed here; HarBuilder already has it
            mime=mime,
            encoded_size=encoded_size,
        )
        cmd: dict[str, Any] = {
            "id": msg_id,
            "method": "Network.getResponseBody",
            "params": {"requestId": request_id},
        }
        if session_id:
            cmd["sessionId"] = session_id
        self.metrics.requested += 1
        return [cmd]

    # ------------------------------------------------------------------
    # Reply interception
    # ------------------------------------------------------------------

    def is_injected_id(self, msg_id: int) -> bool:
        return isinstance(msg_id, int) and msg_id >= _INJECTED_ID_BASE

    def intercept_reply(self, msg: dict) -> tuple[bool, CapturedBody | None]:
        """If ``msg`` is a reply to a command we issued, consume it.

        Returns ``(was_ours, body)``:

        * ``was_ours`` — True if the reply belonged to us. The caller
          must NOT forward it to the upstream client.
        * ``body`` — a ``CapturedBody`` if a body was successfully
          extracted; ``None`` if the reply was ours but carried an error
          or empty body.
        """
        if not isinstance(msg, dict):
            return False, None
        msg_id = msg.get("id")
        if not self.is_injected_id(msg_id):
            return False, None
        pend = self._pending.pop(msg_id, None)
        if pend is None:
            logger.debug("BodyCapture: untracked injected reply id=%s", msg_id)
            return True, None

        if "error" in msg:
            self.metrics.failed += 1
            err = msg.get("error") or {}
            logger.debug(
                "BodyCapture: getResponseBody failed for %s: %s",
                pend.request_id, err.get("message"),
            )
            return True, None

        result = msg.get("result") or {}
        body = result.get("body")
        b64 = bool(result.get("base64Encoded"))
        if body is None:
            return True, None

        try:
            byte_len = len(body.encode("utf-8")) if not b64 else (len(body) * 3) // 4
        except Exception:
            byte_len = len(body) if isinstance(body, str) else 0

        self._total_bytes += byte_len
        self.metrics.captured += 1
        self.metrics.bytes_captured += byte_len

        return True, CapturedBody(
            session_id=pend.session_id,
            request_id=pend.request_id,
            body=body,
            base64_encoded=b64,
            mime=pend.mime,
        )

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        return {
            "enabled": self.enabled,
            "requested": self.metrics.requested,
            "captured": self.metrics.captured,
            "failed": self.metrics.failed,
            "bytes_captured": self.metrics.bytes_captured,
            "skipped_mime": self.metrics.skipped_mime,
            "skipped_size": self.metrics.skipped_size,
            "skipped_budget": self.metrics.skipped_budget,
            "skipped_failed": self.metrics.skipped_failed,
            "pending": len(self._pending),
        }
