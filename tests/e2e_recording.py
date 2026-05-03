#!/usr/bin/env python3
"""End-to-end test: cloakserve recording via raw CDP (no Playwright).

This simulates a native CDP client like ``agent-browser``: we open a plain
WebSocket directly to cloakserve's rewritten ``webSocketDebuggerUrl`` and
drive Chrome through the Browser/Target/Page domains by hand.  Then we
verify that cloakserve produced both ``cdp.jsonl`` (raw transcript) and
``network.har`` (derived) on disk.

The test passes if:
    1. cloakserve starts and responds on /json/version.
    2. We can open a CDP WebSocket and run Target.createTarget -> Page.navigate.
    3. Network.requestWillBeSent + Network.responseReceived events for the
       target URL appear in cdp.jsonl.
    4. network.har contains an entry whose request.url matches the target URL.

Designed to run inside the cloakbrowser docker image where:
    - cloakserve is on $PATH
    - chromium binary is pre-downloaded
    - aiohttp + websockets are installed (cloakbrowser[serve])
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import aiohttp
import websockets


TARGET_URL = os.environ.get("E2E_URL", "https://example.com/")
CLOAKSERVE_PORT = int(os.environ.get("E2E_PORT", "9333"))
RECORD_DIR = Path(os.environ.get("E2E_RECORD_DIR", tempfile.mkdtemp(prefix="cloak-e2e-")))


# ---------------------------------------------------------------------------
# Tiny CDP client (id-based, single session)
# ---------------------------------------------------------------------------


class CdpClient:
    def __init__(self, ws):
        self._ws = ws
        self._next_id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._events: list[dict] = []
        self._reader_task = asyncio.create_task(self._reader())

    async def _reader(self) -> None:
        try:
            async for raw in self._ws:
                msg = json.loads(raw)
                if "id" in msg:
                    fut = self._pending.pop(msg["id"], None)
                    if fut and not fut.done():
                        fut.set_result(msg)
                else:
                    self._events.append(msg)
        except Exception:
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(RuntimeError("CDP connection closed"))

    async def call(
        self,
        method: str,
        params: dict | None = None,
        session_id: str | None = None,
        timeout: float = 15.0,
    ) -> dict:
        self._next_id += 1
        msg_id = self._next_id
        payload = {"id": msg_id, "method": method, "params": params or {}}
        if session_id:
            payload["sessionId"] = session_id
        fut = asyncio.get_event_loop().create_future()
        self._pending[msg_id] = fut
        await self._ws.send(json.dumps(payload))
        msg = await asyncio.wait_for(fut, timeout=timeout)
        if "error" in msg:
            raise RuntimeError(f"CDP {method} failed: {msg['error']}")
        return msg.get("result", {})

    def events(self) -> list[dict]:
        return list(self._events)

    async def wait_event(self, method: str, timeout: float = 30.0) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for ev in self._events:
                if ev.get("method") == method:
                    return ev
            await asyncio.sleep(0.05)
        raise asyncio.TimeoutError(f"timed out waiting for {method}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def fetch_json(url: str, timeout: float = 30.0) -> Any:
    deadline = time.monotonic() + timeout
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    return await resp.json()
        except Exception as exc:
            last_exc = exc
            await asyncio.sleep(0.5)
    raise RuntimeError(f"fetch {url} timed out: {last_exc}")


def find_har_entry(har: dict, url_substr: str) -> dict | None:
    for entry in har.get("log", {}).get("entries", []):
        if url_substr in entry.get("request", {}).get("url", ""):
            return entry
    return None


def find_cdp_event(jsonl_lines: list[str], method: str, url_substr: str | None = None) -> dict | None:
    for raw in jsonl_lines:
        rec = json.loads(raw)
        msg = rec.get("message")
        if not isinstance(msg, dict):
            continue
        if msg.get("method") != method:
            continue
        if url_substr is None:
            return rec
        params = msg.get("params") or {}
        url = ""
        if method == "Network.requestWillBeSent":
            url = (params.get("request") or {}).get("url", "")
        elif method == "Page.frameNavigated":
            url = (params.get("frame") or {}).get("url", "")
        else:
            url = json.dumps(params)
        if url_substr in url:
            return rec
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def run() -> int:
    RECORD_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[e2e] record_dir={RECORD_DIR}", flush=True)

    cmd = [
        "cloakserve",
        f"--port={CLOAKSERVE_PORT}",
        f"--record-dir={RECORD_DIR}",
        "--record-cdp=true",
        "--record-har=true",
        "--record-bodies=true",
        "--record-body-max=1048576",     # 1 MiB per body for the e2e
        "--record-body-total=10485760",  # 10 MiB session cap
        "--no-sandbox",  # passed through to Chromium; required when running as root
    ]
    print(f"[e2e] starting: {' '.join(cmd)}", flush=True)
    proc = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)

    try:
        # 1. Wait for /json/version.
        url = f"http://127.0.0.1:{CLOAKSERVE_PORT}/json/version"
        print(f"[e2e] waiting for {url}", flush=True)
        version = await fetch_json(url, timeout=60)
        ws_url = version["webSocketDebuggerUrl"]
        # cloakserve rewrites the URL host to whatever Host header we sent;
        # we sent localhost:<port>, so this should be ws://127.0.0.1:<port>/...
        ws_url = ws_url.replace("localhost", "127.0.0.1")
        print(f"[e2e] CDP ws_url={ws_url}", flush=True)

        # 2. Connect raw WebSocket and drive Chrome.
        async with websockets.connect(ws_url, max_size=None) as ws:
            client = CdpClient(ws)

            # Create a fresh page target.
            print("[e2e] Target.createTarget", flush=True)
            r = await client.call("Target.createTarget", {"url": "about:blank"})
            target_id = r["targetId"]

            # Attach (flattened sessions).
            r = await client.call("Target.attachToTarget", {"targetId": target_id, "flatten": True})
            session_id = r["sessionId"]
            print(f"[e2e] attached, sessionId={session_id}", flush=True)

            # Enable the domains we care about.
            await client.call("Page.enable", session_id=session_id)
            await client.call("Network.enable", session_id=session_id)

            # Navigate.
            print(f"[e2e] Page.navigate -> {TARGET_URL}", flush=True)
            await client.call("Page.navigate", {"url": TARGET_URL}, session_id=session_id)

            # Wait for the load event so we know responses have arrived.
            try:
                await client.wait_event("Page.loadEventFired", timeout=30.0)
                print("[e2e] Page.loadEventFired", flush=True)
            except asyncio.TimeoutError:
                print("[e2e] WARN: no loadEventFired within 30s — continuing anyway", flush=True)

            # Give Chrome a beat to flush trailing Network.* events.
            await asyncio.sleep(1.0)

            await client.call("Target.closeTarget", {"targetId": target_id})

        # 3. Verify recordings on disk.
        sessions = sorted(p for p in RECORD_DIR.iterdir() if p.is_dir())
        if not sessions:
            print(f"[e2e] FAIL: no session dir under {RECORD_DIR}", flush=True)
            return 1
        # The most recent session is ours.
        sess = sessions[-1]
        print(f"[e2e] inspecting session dir: {sess}", flush=True)
        for f in sess.iterdir():
            print(f"[e2e]   {f.name}  ({f.stat().st_size} bytes)", flush=True)

        cdp_path = sess / "cdp.jsonl"
        har_path = sess / "network.har"
        meta_path = sess / "metadata.json"

        failures: list[str] = []

        if not meta_path.exists():
            failures.append("metadata.json missing")
        else:
            meta = json.loads(meta_path.read_text())
            print(f"[e2e] metadata: {json.dumps(meta, indent=2)}", flush=True)

        if not cdp_path.exists():
            failures.append("cdp.jsonl missing")
        else:
            lines = cdp_path.read_text().splitlines()
            print(f"[e2e] cdp.jsonl line count: {len(lines)}", flush=True)
            host = TARGET_URL.split("//", 1)[1].split("/", 1)[0]
            req = find_cdp_event(lines, "Network.requestWillBeSent", host)
            resp = find_cdp_event(lines, "Network.responseReceived", host)
            if req is None:
                failures.append(f"cdp.jsonl: no Network.requestWillBeSent for {host}")
            else:
                print(f"[e2e] OK cdp.jsonl has Network.requestWillBeSent for {host}", flush=True)
            if resp is None:
                failures.append(f"cdp.jsonl: no Network.responseReceived for {host}")
            else:
                print(f"[e2e] OK cdp.jsonl has Network.responseReceived for {host}", flush=True)

        if not har_path.exists():
            failures.append("network.har missing")
        else:
            har = json.loads(har_path.read_text())
            entries = har.get("log", {}).get("entries", [])
            print(f"[e2e] HAR entries: {len(entries)}", flush=True)
            host = TARGET_URL.split("//", 1)[1].split("/", 1)[0]
            entry = find_har_entry(har, host)
            if entry is None:
                failures.append(f"network.har: no entry matching {host}")
            else:
                print(
                    f"[e2e] OK HAR entry found: {entry['request']['method']} "
                    f"{entry['request']['url']} -> {entry['response']['status']}",
                    flush=True,
                )
                # New: body capture assertion.
                content = (entry.get("response") or {}).get("content") or {}
                body = content.get("text")
                if body and "<" in body and ">" in body:
                    print(
                        f"[e2e] OK HAR entry has captured body "
                        f"({len(body)} chars, mime={content.get('mimeType')})",
                        flush=True,
                    )
                else:
                    failures.append(
                        f"network.har: expected captured HTML body for {host}, "
                        f"got {len(body or '')} chars"
                    )

        # New: scan cdp.jsonl for the injected_body marker.
        if cdp_path.exists():
            host = TARGET_URL.split("//", 1)[1].split("/", 1)[0]
            injected_lines = [
                line for line in cdp_path.read_text().splitlines()
                if '"injected":true' in line or '"injected": true' in line
            ]
            print(f"[e2e] injected_body frames in cdp.jsonl: {len(injected_lines)}", flush=True)
            if injected_lines:
                # Look for one whose body contains text we'd expect from example.com.
                for line in injected_lines:
                    rec = json.loads(line)
                    body = rec.get("body") or ""
                    if "Example Domain" in body or "<html" in body.lower():
                        print(
                            f"[e2e] OK cdp.jsonl has injected body for "
                            f"{rec.get('mime')} ({len(body)} chars)",
                            flush=True,
                        )
                        break
                else:
                    failures.append("cdp.jsonl: no injected_body looked like HTML")
            else:
                failures.append("cdp.jsonl: zero injected_body frames")

        if failures:
            print("[e2e] FAILED:", flush=True)
            for f in failures:
                print(f"[e2e]   - {f}", flush=True)
            return 1

        print("[e2e] PASS — cloakserve recorded raw CDP + HAR for native CDP client", flush=True)
        return 0

    finally:
        print("[e2e] terminating cloakserve", flush=True)
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
