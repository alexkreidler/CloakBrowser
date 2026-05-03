"""Recorder implementations for CDP traffic.

Hierarchy::

    Recorder            # interface — async record() / close()
    ├─ NullRecorder     # no-op (used when recording is disabled)
    └─ SessionRecorder  # per-WebSocket-connection recorder, fan-outs to:
       ├─ CdpJsonlRecorder
       └─ HarRecorder

A new ``SessionRecorder`` is created per client WebSocket connection in
``cloakserve.proxy_cdp_websocket``.  It writes to a session-scoped
directory under ``record_dir/<session-id>/``.

Direction labels:
    * ``client_to_cdp`` — frame received from CDP client, forwarded to Chrome
    * ``cdp_to_client`` — frame received from Chrome, forwarded to CDP client
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Iterable

from .config import RecordingConfig

logger = logging.getLogger("cloakbrowser.recording")


# ---------------------------------------------------------------------------
# Base interface
# ---------------------------------------------------------------------------


class Recorder:
    """Abstract recorder interface."""

    async def record_text(self, direction: str, raw: str) -> None:
        raise NotImplementedError

    async def record_binary(self, direction: str, data: bytes) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        return None


class NullRecorder(Recorder):
    """No-op recorder used when recording is disabled.

    Methods are async but cheap — callers don't need to special-case ``None``.
    """

    async def record_text(self, direction: str, raw: str) -> None:
        return None

    async def record_binary(self, direction: str, data: bytes) -> None:
        return None

    async def close(self) -> None:
        return None


# ---------------------------------------------------------------------------
# CdpJsonlRecorder — append-only JSONL transcript
# ---------------------------------------------------------------------------


class CdpJsonlRecorder(Recorder):
    """Append every CDP frame to a JSONL file.

    Each line is a JSON object with the schema::

        {
            "ts": <float>,                       # epoch seconds
            "label": <str>,                      # session label, e.g. "seed=42"
            "direction": "client_to_cdp" | "cdp_to_client",
            "type": "text" | "binary",
            "message": <obj>,                    # parsed JSON (text frames)
            "raw": <str>,                        # original string (text frames)
            "data_b64": <str>                    # base64 (binary frames)
        }

    Writes are serialized through an ``asyncio.Lock`` so multiple concurrent
    forwarder tasks don't interleave bytes within a single line.
    """

    def __init__(
        self,
        path: str | os.PathLike,
        label: str,
        max_bytes: int | None = None,
        include_binary: bool = True,
    ):
        self._path = Path(path)
        self._label = label
        self._max_bytes = max_bytes or 0
        self._include_binary = include_binary
        self._fh = None  # opened lazily on first write to avoid empty files
        self._lock = asyncio.Lock()
        self._bytes_written = 0
        self._truncated = False

    def _ensure_open(self) -> None:
        if self._fh is None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            # line-buffered text mode so a SIGKILL still leaves complete lines.
            self._fh = open(self._path, "a", buffering=1, encoding="utf-8")

    async def _write_line(self, obj: dict) -> None:
        if self._truncated:
            return
        line = json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
        encoded_len = len(line.encode("utf-8"))
        if self._max_bytes and self._bytes_written + encoded_len > self._max_bytes:
            self._truncated = True
            try:
                self._ensure_open()
                self._fh.write(json.dumps({"truncated": True, "ts": time.time()}) + "\n")
            except Exception as exc:  # pragma: no cover — best-effort
                logger.debug("Failed to write truncation marker: %s", exc)
            return
        try:
            self._ensure_open()
            self._fh.write(line)
            self._bytes_written += encoded_len
        except Exception as exc:  # pragma: no cover — best-effort
            logger.warning("CdpJsonlRecorder write failed: %s", exc)

    async def record_text(self, direction: str, raw: str) -> None:
        # Parse JSON best-effort. Most CDP frames are JSON; preserve raw on failure.
        message: Any = None
        try:
            message = json.loads(raw)
        except (ValueError, TypeError):
            message = None

        record = {
            "ts": time.time(),
            "label": self._label,
            "direction": direction,
            "type": "text",
            "message": message,
        }
        if message is None:
            # Only keep the raw string when JSON parsing failed; otherwise
            # ``message`` is the canonical form and ``raw`` is redundant.
            record["raw"] = raw

        async with self._lock:
            await self._write_line(record)

    async def record_binary(self, direction: str, data: bytes) -> None:
        if not self._include_binary:
            return
        record = {
            "ts": time.time(),
            "label": self._label,
            "direction": direction,
            "type": "binary",
            "data_b64": base64.b64encode(data).decode("ascii"),
        }
        async with self._lock:
            await self._write_line(record)

    async def close(self) -> None:
        async with self._lock:
            if self._fh is not None:
                try:
                    self._fh.flush()
                    self._fh.close()
                except Exception as exc:  # pragma: no cover
                    logger.debug("CdpJsonlRecorder close failed: %s", exc)
                self._fh = None


# ---------------------------------------------------------------------------
# HarRecorder — feeds CDP frames to a CdpHarBuilder, flushes HAR on close
# ---------------------------------------------------------------------------


class HarRecorder(Recorder):
    """Build a HAR file from CDP ``Network.*`` events.

    Only ``cdp_to_client`` text frames carry the events we need; client-bound
    requests are also inspected so we can pair ``Network.getResponseBody``
    requests with their replies (response body capture).

    The HAR is *not* written incrementally — it's flushed once on ``close()``
    when the connection ends.  This keeps the on-disk file consistent and
    well-formed at all times.
    """

    def __init__(self, path: str | os.PathLike, label: str):
        from .har_builder import CdpHarBuilder

        self._path = Path(path)
        self._label = label
        self._builder = CdpHarBuilder(creator_comment=f"cloakserve session {label}")
        self._lock = asyncio.Lock()
        self._closed = False

    async def record_text(self, direction: str, raw: str) -> None:
        if self._closed:
            return
        try:
            msg = json.loads(raw)
        except (ValueError, TypeError):
            return
        if not isinstance(msg, dict):
            return
        async with self._lock:
            try:
                self._builder.feed(direction, msg)
            except Exception as exc:  # pragma: no cover — never break recording
                logger.debug("HarRecorder.feed failed: %s", exc)

    async def record_binary(self, direction: str, data: bytes) -> None:
        # Binary frames carry no Network events.
        return None

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                har = self._builder.build()
                self._path.parent.mkdir(parents=True, exist_ok=True)
                with open(self._path, "w", encoding="utf-8") as fh:
                    json.dump(har, fh, ensure_ascii=False, indent=2)
            except Exception as exc:  # pragma: no cover
                logger.warning("HarRecorder write failed: %s", exc)


# ---------------------------------------------------------------------------
# SessionRecorder — fan-out to multiple recorders for a single connection
# ---------------------------------------------------------------------------


class SessionRecorder(Recorder):
    """Per-WebSocket-connection recorder.

    Owns:
        * a session id (uuid4 hex)
        * a session directory under ``config.record_dir``
        * a JSONL recorder (if enabled)
        * a HAR recorder    (if enabled)
        * a metadata.json file written on ``__init__``

    Use ``SessionRecorder.create()`` to build one based on a
    :class:`RecordingConfig` — it returns a :class:`NullRecorder` when
    recording is disabled, so callers don't need to branch.
    """

    def __init__(
        self,
        session_id: str,
        session_dir: Path,
        label: str,
        recorders: Iterable[Recorder],
    ):
        self.session_id = session_id
        self.session_dir = session_dir
        self.label = label
        self._recorders: list[Recorder] = list(recorders)

    @classmethod
    def create(
        cls,
        config: RecordingConfig,
        label: str,
        metadata: dict | None = None,
    ) -> Recorder:
        """Build a SessionRecorder, or a NullRecorder if recording is disabled."""
        if not config.enabled:
            return NullRecorder()

        session_id = _make_session_id()
        session_dir = config.session_dir(session_id)
        session_dir.mkdir(parents=True, exist_ok=True)

        recorders: list[Recorder] = []
        if config.record_cdp:
            recorders.append(
                CdpJsonlRecorder(
                    path=session_dir / "cdp.jsonl",
                    label=label,
                    max_bytes=config.max_jsonl_bytes,
                    include_binary=config.include_binary,
                )
            )
        if config.record_har:
            recorders.append(HarRecorder(path=session_dir / "network.har", label=label))

        # Write metadata.json synchronously — small file, written once.
        meta = {
            "session_id": session_id,
            "label": label,
            "started_at": time.time(),
            "record_cdp": config.record_cdp,
            "record_har": config.record_har,
        }
        if metadata:
            meta.update(metadata)
        try:
            with open(session_dir / "metadata.json", "w", encoding="utf-8") as fh:
                json.dump(meta, fh, indent=2, default=str)
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to write recording metadata: %s", exc)

        logger.info("Recording session %s started (label=%s, dir=%s)", session_id, label, session_dir)
        return cls(session_id, session_dir, label, recorders)

    async def record_text(self, direction: str, raw: str) -> None:
        for r in self._recorders:
            await r.record_text(direction, raw)

    async def record_binary(self, direction: str, data: bytes) -> None:
        for r in self._recorders:
            await r.record_binary(direction, data)

    async def close(self) -> None:
        for r in self._recorders:
            try:
                await r.close()
            except Exception as exc:  # pragma: no cover
                logger.debug("Recorder close failed: %s", exc)


def _make_session_id() -> str:
    """Sortable, unique session id: ``YYYYMMDD-HHMMSS-<rand6>``."""
    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    return f"{ts}-{uuid.uuid4().hex[:6]}"
