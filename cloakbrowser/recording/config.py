"""Recording configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


# MIME types we WANT to capture bodies for.  Everything else (images, video,
# audio, fonts, octet-stream, wasm, pdf, ...) is skipped by default to avoid
# blowing up the recording with binary noise.
DEFAULT_BODY_MIME_ALLOW: tuple[str, ...] = (
    "text/",                          # text/html, text/css, text/plain, text/javascript, ...
    "application/json",
    "application/ld+json",
    "application/manifest+json",
    "application/xml",
    "application/xhtml+xml",
    "application/javascript",
    "application/x-javascript",
    "application/ecmascript",
    "application/graphql",
    "application/x-www-form-urlencoded",
    "image/svg+xml",                  # SVG is text
)


def mime_allowed(
    mime: str | None,
    allow: Iterable[str] = DEFAULT_BODY_MIME_ALLOW,
) -> bool:
    """Return True if ``mime`` matches the allow-list.

    Each entry in ``allow`` is matched as a *prefix* (case-insensitive),
    so ``text/`` covers ``text/html``, ``text/css``, etc., and exact
    types like ``application/json`` match only themselves (because they
    don't end with ``/``).
    """
    if not mime:
        return False
    m = mime.lower().split(";", 1)[0].strip()  # strip ``; charset=...``
    for prefix in allow:
        p = prefix.lower()
        if p.endswith("/"):
            if m.startswith(p):
                return True
        else:
            if m == p:
                return True
    return False


@dataclass(frozen=True)
class RecordingConfig:
    """Configuration for CDP recording in cloakserve.

    Attributes:
        record_dir: Directory where per-session recording folders are written.
            ``None`` disables all recording.
        record_cdp: Write the raw ``cdp.jsonl`` transcript (every CDP frame).
        record_har: Write a derived ``network.har`` from ``Network.*`` events.
        record_bodies: Actively capture response bodies by injecting
            ``Network.getResponseBody`` commands after each ``loadingFinished``
            event.  This works for *any* CDP client, including ones that don't
            cooperate (agent-browser, raw websockets).  Filtered by
            ``body_mime_allow`` and bounded by ``max_body_bytes`` /
            ``max_total_body_bytes``.
        body_mime_allow: Iterable of MIME prefixes that are eligible for body
            capture. Everything else is skipped. See ``DEFAULT_BODY_MIME_ALLOW``.
        max_body_bytes: Per-response cap on captured body size (encoded length
            from the response). Bodies larger than this are skipped.
            Default 2 MiB.
        max_total_body_bytes: Per-session cap on total bytes of captured
            bodies. Once reached, no further bodies are captured for the
            session. Default 100 MiB. ``0`` / ``None`` disables the cap.
        max_jsonl_bytes: Soft cap (per session) on the JSONL file. ``0`` /
            ``None`` means unlimited. When the cap is hit, further frames are
            dropped and a single ``{"truncated": true}`` line is appended.
        include_binary: Whether to include binary WebSocket frames (encoded
            as base64) in the JSONL transcript. CDP itself is JSON over text
            frames; binary frames are rare but possible (e.g. screencast).
        capture_response_bodies: Deprecated alias for ``record_bodies``,
            preserved for backward compatibility.
    """

    record_dir: str | os.PathLike | None = None
    record_cdp: bool = True
    record_har: bool = False
    record_bodies: bool = False
    body_mime_allow: tuple[str, ...] = DEFAULT_BODY_MIME_ALLOW
    max_body_bytes: int = 2 * 1024 * 1024          # 2 MiB per body
    max_total_body_bytes: int | None = 100 * 1024 * 1024   # 100 MiB per session
    max_jsonl_bytes: int | None = None
    include_binary: bool = True
    capture_response_bodies: bool = False  # deprecated alias

    @property
    def enabled(self) -> bool:
        return bool(self.record_dir) and (self.record_cdp or self.record_har)

    @property
    def bodies_enabled(self) -> bool:
        return self.enabled and (self.record_bodies or self.capture_response_bodies)

    def session_dir(self, session_id: str) -> Path:
        """Return the per-session recording directory (does not create it)."""
        if not self.record_dir:
            raise ValueError("record_dir is not configured")
        return Path(self.record_dir) / session_id

    def mime_is_allowed(self, mime: str | None) -> bool:
        return mime_allowed(mime, self.body_mime_allow)

    @classmethod
    def from_cli(
        cls,
        record_dir: str | None,
        record_cdp: bool = True,
        record_har: bool = False,
        record_bodies: bool = False,
        max_body_bytes: int | None = None,
        max_total_body_bytes: int | None = None,
        max_jsonl_bytes: int | None = None,
    ) -> "RecordingConfig":
        kwargs: dict = {
            "record_dir": record_dir,
            "record_cdp": record_cdp,
            "record_har": record_har,
            "record_bodies": record_bodies,
        }
        if max_body_bytes is not None:
            kwargs["max_body_bytes"] = max_body_bytes
        if max_total_body_bytes is not None:
            kwargs["max_total_body_bytes"] = max_total_body_bytes
        if max_jsonl_bytes is not None:
            kwargs["max_jsonl_bytes"] = max_jsonl_bytes
        return cls(**kwargs)

    def with_overrides(self, **changes) -> "RecordingConfig":
        from dataclasses import replace
        return replace(self, **changes)


def parse_bool_flag(value: str) -> bool:
    """Parse ``--flag=true|false|1|0|yes|no`` style values."""
    return value.strip().lower() in ("1", "true", "yes", "on")
