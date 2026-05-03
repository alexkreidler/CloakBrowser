"""CDP-to-HAR translator.

Consumes a stream of CDP frames (in both directions) and produces a HAR 1.2
document via :meth:`build`.

Why a stateful builder?
    HAR entries are aggregates over many CDP events (``Network.requestWillBeSent``
    + ``Network.responseReceived`` + ``Network.dataReceived`` +
    ``Network.loadingFinished`` + their ``ExtraInfo`` siblings, optionally
    ``Network.getResponseBody`` replies).  The builder collects per-request
    state keyed by ``(sessionId, requestId)`` — flattened CDP can route
    Network events through any session, so ``requestId`` alone is not unique.

Body capture
    HAR response bodies come from ``Network.getResponseBody`` *replies*.
    The builder doesn't issue these commands itself (passive recorder); it
    just notices when a CDP client asks for a body and pairs the reply with
    the original request via the message ``id``.

CDP events handled
    * ``Network.requestWillBeSent`` / ``Network.requestWillBeSentExtraInfo``
    * ``Network.responseReceived`` / ``Network.responseReceivedExtraInfo``
    * ``Network.dataReceived``
    * ``Network.loadingFinished`` / ``Network.loadingFailed``
    * ``Network.requestServedFromCache``
    * ``Page.frameNavigated`` / ``Page.frameStartedLoading`` (page entries)
    * ``Network.getResponseBody`` request/reply pairs for body capture
"""

from __future__ import annotations

import base64
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from .._version import __version__ as _CB_VERSION

logger = logging.getLogger("cloakbrowser.recording.har")


# ---------------------------------------------------------------------------
# Per-request state
# ---------------------------------------------------------------------------


@dataclass
class RequestState:
    """Aggregated state for a single network request."""

    request_id: str
    session_id: str
    frame_id: str | None = None
    loader_id: str | None = None

    # ``Network.requestWillBeSent.timestamp`` is monotonic seconds; ``wallTime``
    # is epoch seconds. We use wallTime for HAR ``startedDateTime`` and the
    # monotonic timestamp deltas for HAR ``timings``.
    wall_time: float | None = None
    monotonic_started: float | None = None

    # Request side
    method: str = "GET"
    url: str = ""
    http_version: str | None = None
    request_headers: dict[str, str] = field(default_factory=dict)
    post_data: str | None = None
    post_data_b64: str | None = None
    initiator: dict | None = None

    # Response side
    response_received: bool = False
    status: int = 0
    status_text: str = ""
    response_headers: dict[str, str] = field(default_factory=dict)
    mime_type: str = ""
    remote_ip: str | None = None
    remote_port: int | None = None
    from_cache: bool = False
    response_protocol: str | None = None
    security_state: str | None = None

    # Sizes (HAR semantics)
    request_header_size: int = -1
    response_header_size: int = -1
    response_body_size: int = -1   # decoded body size (HAR ``content.size``)
    encoded_body_size: int = -1    # bytes on wire (HAR ``response.bodySize``)
    transfer_size: int = -1        # total transfer size if known

    # Timings (CDP gives us ResourceTiming on Network.responseReceived)
    timing: dict | None = None
    monotonic_response: float | None = None
    monotonic_finished: float | None = None

    # Outcome
    finished: bool = False
    failed: bool = False
    error_text: str | None = None

    # Body
    body_text: str | None = None
    body_b64: str | None = None
    body_mime: str | None = None


# ---------------------------------------------------------------------------
# CdpHarBuilder
# ---------------------------------------------------------------------------


class CdpHarBuilder:
    """Stateful translator: CDP frames -> HAR 1.2 document."""

    def __init__(self, creator_comment: str | None = None):
        self._requests: dict[tuple[str, str], RequestState] = {}
        self._order: list[tuple[str, str]] = []
        # session_id -> page metadata. Top frames create page entries.
        self._pages: dict[str, dict] = {}
        self._page_order: list[str] = []
        # Outstanding Network.getResponseBody calls by message id, mapped to
        # the (session_id, request_id) they were asked about.  Populated when
        # we observe ``client_to_cdp`` frames.
        self._pending_body_calls: dict[tuple[str, int], tuple[str, str]] = {}
        self._creator_comment = creator_comment
        self._started_wall: float | None = None

    # ---- entry-point ------------------------------------------------------

    def feed(self, direction: str, msg: dict) -> None:
        """Consume a single decoded CDP frame.

        ``direction`` is ``"client_to_cdp"`` or ``"cdp_to_client"``.
        ``msg`` is the parsed JSON message as a dict.
        """
        session_id = msg.get("sessionId", "")  # "" = browser-level

        if direction == "cdp_to_client":
            method = msg.get("method")
            if method:
                params = msg.get("params") or {}
                self._handle_event(session_id, method, params)
            elif "id" in msg and "result" in msg:
                self._handle_response(session_id, msg["id"], msg["result"])
            return

        # client_to_cdp
        if "id" in msg and "method" in msg:
            self._handle_command(session_id, msg["id"], msg["method"], msg.get("params") or {})

    def set_response_body(
        self,
        session_id: str,
        request_id: str,
        body: str,
        base64_encoded: bool,
    ) -> None:
        """Attach a captured response body directly to a request state.

        Used by the active body-capture path in cloakserve, which has
        already paired a ``Network.getResponseBody`` reply with its
        original request and just needs to drop the body into the HAR.
        Falls back silently if we never saw the request (e.g. the body
        came from a redirect that was already finalized).
        """
        state = self._requests.get((session_id, request_id))
        if state is None:
            return
        if base64_encoded:
            state.body_b64 = body
        else:
            state.body_text = body

    # ---- command bookkeeping (for body capture) ---------------------------

    def _handle_command(self, session_id: str, msg_id: int, method: str, params: dict) -> None:
        if method == "Network.getResponseBody":
            req_id = params.get("requestId")
            if req_id:
                self._pending_body_calls[(session_id, msg_id)] = (session_id, req_id)

    def _handle_response(self, session_id: str, msg_id: int, result: dict) -> None:
        key = (session_id, msg_id)
        target = self._pending_body_calls.pop(key, None)
        if target is None:
            return
        sess, req_id = target
        state = self._requests.get((sess, req_id))
        if state is None:
            return
        body = result.get("body")
        b64 = bool(result.get("base64Encoded"))
        if body is None:
            return
        if b64:
            state.body_b64 = body
        else:
            state.body_text = body

    # ---- event dispatch ---------------------------------------------------

    def _handle_event(self, session_id: str, method: str, params: dict) -> None:
        # Page lifecycle — minimal page tracking
        if method == "Page.frameNavigated":
            self._on_frame_navigated(session_id, params)
            return
        if method == "Page.loadEventFired":
            self._on_load_event(session_id, params)
            return

        if not method.startswith("Network."):
            return

        if method == "Network.requestWillBeSent":
            self._on_request_will_be_sent(session_id, params)
        elif method == "Network.requestWillBeSentExtraInfo":
            self._on_request_extra_info(session_id, params)
        elif method == "Network.responseReceived":
            self._on_response_received(session_id, params)
        elif method == "Network.responseReceivedExtraInfo":
            self._on_response_extra_info(session_id, params)
        elif method == "Network.dataReceived":
            self._on_data_received(session_id, params)
        elif method == "Network.loadingFinished":
            self._on_loading_finished(session_id, params)
        elif method == "Network.loadingFailed":
            self._on_loading_failed(session_id, params)
        elif method == "Network.requestServedFromCache":
            self._on_served_from_cache(session_id, params)

    # ---- specific handlers ------------------------------------------------

    def _state(self, session_id: str, request_id: str) -> RequestState:
        key = (session_id, request_id)
        state = self._requests.get(key)
        if state is None:
            state = RequestState(request_id=request_id, session_id=session_id)
            self._requests[key] = state
            self._order.append(key)
        return state

    def _on_frame_navigated(self, session_id: str, params: dict) -> None:
        frame = params.get("frame") or {}
        if frame.get("parentId"):
            return  # only top frames create page entries
        page_id = f"page_{session_id or 'browser'}_{frame.get('id', '')}"
        if page_id not in self._pages:
            self._pages[page_id] = {
                "id": page_id,
                "title": frame.get("url", ""),
                "startedDateTime": _iso(time.time()),
                "pageTimings": {"onContentLoad": -1, "onLoad": -1},
            }
            self._page_order.append(page_id)
        else:
            self._pages[page_id]["title"] = frame.get("url", "")

    def _on_load_event(self, session_id: str, params: dict) -> None:
        # Best-effort: stamp the most recent page in this session.
        for pid in reversed(self._page_order):
            if f"_{session_id or 'browser'}_" in pid:
                self._pages[pid]["pageTimings"]["onLoad"] = int(
                    (params.get("timestamp", 0) * 1000)
                )
                return

    def _on_request_will_be_sent(self, session_id: str, params: dict) -> None:
        request_id = params.get("requestId")
        if not request_id:
            return

        # Redirect bookkeeping must happen BEFORE we overwrite the existing
        # state with the redirected request's data: the ``redirectResponse``
        # is the response to the *previous* request under this requestId.
        redirect = params.get("redirectResponse")
        key = (session_id, request_id)
        if redirect and key in self._requests:
            prior = self._requests.pop(key)
            try:
                self._order.remove(key)
            except ValueError:
                pass
            self._apply_response_payload(prior, redirect)
            prior.finished = True
            sentinel = (session_id, f"{request_id}#redirect{int(time.time()*1e6)}_{len(self._requests)}")
            self._requests[sentinel] = prior
            self._order.append(sentinel)

        state = self._state(session_id, request_id)
        req = params.get("request") or {}
        state.method = req.get("method", state.method)
        state.url = req.get("url", state.url)
        state.request_headers = dict(req.get("headers") or state.request_headers)
        post = req.get("postData")
        if post is not None:
            state.post_data = post
        state.frame_id = params.get("frameId") or state.frame_id
        state.loader_id = params.get("loaderId") or state.loader_id
        state.wall_time = params.get("wallTime") or state.wall_time
        state.monotonic_started = params.get("timestamp") or state.monotonic_started
        state.initiator = params.get("initiator") or state.initiator
        if self._started_wall is None and state.wall_time:
            self._started_wall = state.wall_time

    def _on_request_extra_info(self, session_id: str, params: dict) -> None:
        request_id = params.get("requestId")
        if not request_id:
            return
        state = self._state(session_id, request_id)
        # ExtraInfo carries the *real* on-the-wire headers — prefer them.
        headers = params.get("headers")
        if headers:
            state.request_headers = dict(headers)

    def _on_response_received(self, session_id: str, params: dict) -> None:
        request_id = params.get("requestId")
        if not request_id:
            return
        state = self._state(session_id, request_id)
        resp = params.get("response") or {}
        self._apply_response_payload(state, resp)
        state.monotonic_response = params.get("timestamp") or state.monotonic_response

    def _apply_response_payload(self, state: RequestState, resp: dict) -> None:
        state.response_received = True
        state.status = int(resp.get("status", 0))
        state.status_text = resp.get("statusText", "")
        state.mime_type = resp.get("mimeType", "")
        state.response_headers = dict(resp.get("headers") or state.response_headers)
        state.remote_ip = resp.get("remoteIPAddress") or state.remote_ip
        state.remote_port = resp.get("remotePort") or state.remote_port
        state.from_cache = bool(resp.get("fromDiskCache") or resp.get("fromPrefetchCache"))
        state.response_protocol = resp.get("protocol") or state.response_protocol
        state.http_version = resp.get("protocol") or state.http_version
        sec = resp.get("securityState")
        if sec:
            state.security_state = sec
        timing = resp.get("timing")
        if timing:
            state.timing = timing
        if "encodedDataLength" in resp:
            state.encoded_body_size = int(resp["encodedDataLength"])

    def _on_response_extra_info(self, session_id: str, params: dict) -> None:
        request_id = params.get("requestId")
        if not request_id:
            return
        state = self._state(session_id, request_id)
        headers = params.get("headers")
        if headers:
            state.response_headers = dict(headers)
        if "headersText" in params:
            state.response_header_size = len(params["headersText"].encode("utf-8"))

    def _on_data_received(self, session_id: str, params: dict) -> None:
        request_id = params.get("requestId")
        if not request_id:
            return
        state = self._state(session_id, request_id)
        encoded_inc = int(params.get("encodedDataLength", 0))
        decoded_inc = int(params.get("dataLength", 0))
        if state.encoded_body_size < 0:
            state.encoded_body_size = 0
        if state.response_body_size < 0:
            state.response_body_size = 0
        state.encoded_body_size += encoded_inc
        state.response_body_size += decoded_inc

    def _on_loading_finished(self, session_id: str, params: dict) -> None:
        request_id = params.get("requestId")
        if not request_id:
            return
        state = self._state(session_id, request_id)
        state.finished = True
        state.monotonic_finished = params.get("timestamp") or state.monotonic_finished
        if "encodedDataLength" in params:
            state.transfer_size = int(params["encodedDataLength"])

    def _on_loading_failed(self, session_id: str, params: dict) -> None:
        request_id = params.get("requestId")
        if not request_id:
            return
        state = self._state(session_id, request_id)
        state.failed = True
        state.finished = True
        state.error_text = params.get("errorText") or state.error_text
        state.monotonic_finished = params.get("timestamp") or state.monotonic_finished

    def _on_served_from_cache(self, session_id: str, params: dict) -> None:
        request_id = params.get("requestId")
        if not request_id:
            return
        state = self._state(session_id, request_id)
        state.from_cache = True

    # ---- HAR construction -------------------------------------------------

    def build(self) -> dict:
        """Return the HAR 1.2 document as a dict."""
        entries = []
        for key in self._order:
            state = self._requests.get(key)
            if state is None or not state.url:
                continue
            try:
                entries.append(self._entry(state))
            except Exception as exc:  # pragma: no cover — never break export
                logger.debug("Skipping HAR entry for %s: %s", state.request_id, exc)

        pages = [self._pages[pid] for pid in self._page_order]

        return {
            "log": {
                "version": "1.2",
                "creator": {
                    "name": "cloakbrowser",
                    "version": _CB_VERSION,
                    "comment": self._creator_comment or "CDP-derived HAR",
                },
                "pages": pages,
                "entries": entries,
            }
        }

    def _entry(self, state: RequestState) -> dict:
        started = _iso(state.wall_time) if state.wall_time else _iso(time.time())
        timings, total_time = _har_timings(state)

        request = {
            "method": state.method,
            "url": state.url,
            "httpVersion": state.http_version or "HTTP/1.1",
            "cookies": _cookies_from_headers(state.request_headers, "cookie"),
            "headers": _headers_list(state.request_headers),
            "queryString": _query_from_url(state.url),
            "headersSize": state.request_header_size,
            "bodySize": len(state.post_data.encode("utf-8")) if state.post_data else 0,
        }
        if state.post_data is not None:
            request["postData"] = {
                "mimeType": state.request_headers.get("content-type")
                or state.request_headers.get("Content-Type")
                or "",
                "text": state.post_data,
            }

        content: dict[str, Any] = {
            "size": state.response_body_size if state.response_body_size >= 0 else 0,
            "mimeType": state.mime_type,
        }
        if state.encoded_body_size >= 0 and state.response_body_size >= 0:
            compression = state.response_body_size - state.encoded_body_size
            if compression > 0:
                content["compression"] = compression
        if state.body_text is not None:
            content["text"] = state.body_text
        elif state.body_b64 is not None:
            content["text"] = state.body_b64
            content["encoding"] = "base64"

        response = {
            "status": state.status,
            "statusText": state.status_text,
            "httpVersion": state.http_version or "HTTP/1.1",
            "cookies": _cookies_from_headers(state.response_headers, "set-cookie"),
            "headers": _headers_list(state.response_headers),
            "content": content,
            "redirectURL": state.response_headers.get("location")
            or state.response_headers.get("Location", ""),
            "headersSize": state.response_header_size,
            "bodySize": state.encoded_body_size if state.encoded_body_size >= 0 else -1,
        }

        entry: dict[str, Any] = {
            "startedDateTime": started,
            "time": total_time,
            "request": request,
            "response": response,
            "cache": {},
            "timings": timings,
            "_cdp": {
                "requestId": state.request_id,
                "sessionId": state.session_id,
                "frameId": state.frame_id,
                "loaderId": state.loader_id,
                "fromCache": state.from_cache,
                "failed": state.failed,
                "errorText": state.error_text,
            },
        }
        if state.remote_ip:
            entry["serverIPAddress"] = state.remote_ip
        if state.remote_port is not None:
            entry["connection"] = str(state.remote_port)
        return entry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _iso(epoch_seconds: float) -> str:
    """ISO-8601 with millisecond precision (HAR ``startedDateTime`` format)."""
    secs = int(epoch_seconds)
    msec = int((epoch_seconds - secs) * 1000)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(secs)) + f".{msec:03d}Z"


def _headers_list(headers: dict[str, str]) -> list[dict[str, str]]:
    return [{"name": k, "value": str(v)} for k, v in headers.items()]


def _cookies_from_headers(headers: dict[str, str], header_name: str) -> list[dict[str, str]]:
    """Best-effort cookie parse from a single header line.

    HAR's cookie list is informational; we don't bother with full RFC parsing.
    """
    raw = None
    for k, v in headers.items():
        if k.lower() == header_name:
            raw = v
            break
    if not raw:
        return []
    out = []
    for piece in raw.split(";" if header_name == "cookie" else ","):
        piece = piece.strip()
        if not piece:
            continue
        if "=" in piece:
            name, _, value = piece.partition("=")
            out.append({"name": name.strip(), "value": value.strip()})
        else:
            out.append({"name": piece, "value": ""})
    return out


def _query_from_url(url: str) -> list[dict[str, str]]:
    from urllib.parse import urlparse, parse_qsl

    try:
        qs = urlparse(url).query
    except Exception:
        return []
    if not qs:
        return []
    return [{"name": k, "value": v} for k, v in parse_qsl(qs, keep_blank_values=True)]


def _har_timings(state: RequestState) -> tuple[dict, float]:
    """Map CDP ``ResourceTiming`` -> HAR ``timings`` dict.

    HAR timings are in milliseconds; -1 means "not applicable".
    Returns ``(timings_dict, total_time_ms)``.
    """
    timings = {
        "blocked": -1.0,
        "dns": -1.0,
        "connect": -1.0,
        "ssl": -1.0,
        "send": 0.0,
        "wait": 0.0,
        "receive": 0.0,
    }
    total = -1.0

    t = state.timing or {}
    # CDP ResourceTiming offsets are in milliseconds relative to ``requestTime`` (seconds).
    request_time = t.get("requestTime")

    def _delta(end_key: str, start_key: str | None) -> float:
        end = t.get(end_key, -1)
        if end is None or end < 0:
            return -1.0
        if start_key is None:
            return float(end)
        start = t.get(start_key, -1)
        if start is None or start < 0:
            return -1.0
        return float(end) - float(start)

    if t:
        # blocked = max(0, dnsStart) — proxy/queueing time is already absorbed
        # by the time CDP gives us. We use proxyStart..dnsStart as ``blocked``
        # when both present.
        blocked = _delta("dnsStart", "proxyStart")
        if blocked < 0:
            blocked = max(t.get("dnsStart", 0), 0)
        timings["blocked"] = blocked

        dns = _delta("dnsEnd", "dnsStart")
        if dns >= 0:
            timings["dns"] = dns

        connect = _delta("connectEnd", "connectStart")
        if connect >= 0:
            timings["connect"] = connect

        ssl = _delta("sslEnd", "sslStart")
        if ssl >= 0:
            timings["ssl"] = ssl

        send = _delta("sendEnd", "sendStart")
        if send >= 0:
            timings["send"] = send

        # wait = receiveHeadersStart..sendEnd, fall back to receiveHeadersEnd
        wait = _delta("receiveHeadersStart", "sendEnd")
        if wait < 0:
            wait = _delta("receiveHeadersEnd", "sendEnd")
        if wait >= 0:
            timings["wait"] = wait

        # receive = monotonic_finished - (requestTime + receiveHeadersEnd/1000)
        if (
            state.monotonic_finished is not None
            and request_time is not None
            and t.get("receiveHeadersEnd") is not None
        ):
            recv_ms = (state.monotonic_finished - request_time) * 1000.0 - float(
                t.get("receiveHeadersEnd", 0)
            )
            if recv_ms >= 0:
                timings["receive"] = recv_ms

    if state.monotonic_started is not None and state.monotonic_finished is not None:
        total = (state.monotonic_finished - state.monotonic_started) * 1000.0
    else:
        # Sum non-negative components.
        total = sum(v for v in timings.values() if v > 0) or -1.0

    return timings, total
