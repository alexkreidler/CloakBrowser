"""CDP traffic recording for CloakBrowser.

Captures every CDP frame flowing through ``cloakserve``'s WebSocket proxy
(both client->Chrome and Chrome->client) as a raw JSONL transcript, and
optionally derives a standard ``.har`` from the ``Network.*`` events.

Why both formats?
    HAR is lossy by design — it cannot represent ``Runtime.*``, ``DOM.*``,
    ``Input.*``, ``Page.*``, ``Target.*``, ``Browser.*``, etc.  The raw
    JSONL transcript is the source of truth; HAR is a derived artifact.

Layout under ``record_dir``::

    recordings/
      <session-id>/
        cdp.jsonl       # every raw command/event (one JSON per line)
        network.har     # derived network artifact (optional)
        metadata.json   # seed, port, pid, started_at, etc.

Recording happens at the proxy layer so it works for *any* CDP client
(Playwright, Puppeteer, ``agent-browser``, raw ``websockets``, ...) — not
just Playwright contexts.
"""

from .config import RecordingConfig, mime_allowed, DEFAULT_BODY_MIME_ALLOW
from .recorder import CdpJsonlRecorder, HarRecorder, NullRecorder, Recorder, SessionRecorder
from .har_builder import CdpHarBuilder
from .body_capture import BodyCapture, BodyCaptureMetrics

__all__ = [
    "RecordingConfig",
    "Recorder",
    "NullRecorder",
    "CdpJsonlRecorder",
    "HarRecorder",
    "SessionRecorder",
    "CdpHarBuilder",
    "BodyCapture",
    "BodyCaptureMetrics",
    "mime_allowed",
    "DEFAULT_BODY_MIME_ALLOW",
]
