"""Recording configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class RecordingConfig:
    """Configuration for CDP recording in cloakserve.

    Attributes:
        record_dir: Directory where per-session recording folders are written.
            ``None`` disables all recording.
        record_cdp: Write the raw ``cdp.jsonl`` transcript (every CDP frame).
        record_har: Write a derived ``network.har`` from ``Network.*`` events.
        capture_response_bodies: Issue ``Network.getResponseBody`` requests via
            the proxy (passive mode: only buffer bodies the client itself asks
            for — no extra commands injected).  Currently always passive.
        max_jsonl_bytes: Soft cap (per session) on the JSONL file. ``0``/``None``
            means unlimited. When the cap is hit, further frames are dropped
            and a single ``{"truncated": true}`` line is appended.
        include_binary: Whether to include binary WebSocket frames (encoded as
            base64) in the JSONL transcript.  CDP itself is JSON over text
            frames; binary frames are rare but possible (e.g. screencast).
    """

    record_dir: str | os.PathLike | None = None
    record_cdp: bool = True
    record_har: bool = False
    capture_response_bodies: bool = False
    max_jsonl_bytes: int | None = None
    include_binary: bool = True

    @property
    def enabled(self) -> bool:
        return bool(self.record_dir) and (self.record_cdp or self.record_har)

    def session_dir(self, session_id: str) -> Path:
        """Return the per-session recording directory (does not create it)."""
        if not self.record_dir:
            raise ValueError("record_dir is not configured")
        return Path(self.record_dir) / session_id

    @classmethod
    def from_cli(
        cls,
        record_dir: str | None,
        record_cdp: bool = True,
        record_har: bool = False,
        max_jsonl_bytes: int | None = None,
    ) -> "RecordingConfig":
        return cls(
            record_dir=record_dir,
            record_cdp=record_cdp,
            record_har=record_har,
            max_jsonl_bytes=max_jsonl_bytes,
        )

    def with_overrides(self, **changes) -> "RecordingConfig":
        from dataclasses import replace
        return replace(self, **changes)


def parse_bool_flag(value: str) -> bool:
    """Parse ``--flag=true|false|1|0|yes|no`` style values."""
    return value.strip().lower() in ("1", "true", "yes", "on")
