"""Unit tests for active response-body capture (BodyCapture)."""

from __future__ import annotations

import json

import pytest

from cloakbrowser.recording import (
    BodyCapture,
    CdpHarBuilder,
    DEFAULT_BODY_MIME_ALLOW,
    RecordingConfig,
    SessionRecorder,
    mime_allowed,
)


# ---------------------------------------------------------------------------
# MIME allow-list
# ---------------------------------------------------------------------------


class TestMimeAllowed:
    def test_text_html(self):
        assert mime_allowed("text/html") is True
        assert mime_allowed("text/html; charset=utf-8") is True

    def test_text_css_js(self):
        assert mime_allowed("text/css") is True
        assert mime_allowed("text/javascript") is True
        assert mime_allowed("text/plain") is True

    def test_application_json_variants(self):
        assert mime_allowed("application/json") is True
        assert mime_allowed("application/json; charset=utf-8") is True
        assert mime_allowed("application/ld+json") is True
        assert mime_allowed("application/manifest+json") is True

    def test_xml_variants(self):
        assert mime_allowed("application/xml") is True
        assert mime_allowed("application/xhtml+xml") is True
        assert mime_allowed("text/xml") is True   # via text/ prefix

    def test_javascript_variants(self):
        assert mime_allowed("application/javascript") is True
        assert mime_allowed("application/x-javascript") is True
        assert mime_allowed("application/ecmascript") is True

    def test_svg_allowed(self):
        assert mime_allowed("image/svg+xml") is True

    def test_binary_disallowed(self):
        for m in [
            "image/png", "image/jpeg", "image/webp", "image/gif",
            "video/mp4", "video/webm", "audio/mpeg", "audio/ogg",
            "application/octet-stream", "application/wasm",
            "application/pdf", "application/zip", "application/x-protobuf",
            "font/woff", "font/woff2", "application/font-woff2",
        ]:
            assert mime_allowed(m) is False, f"{m} should be disallowed"

    def test_empty_or_none(self):
        assert mime_allowed("") is False
        assert mime_allowed(None) is False

    def test_case_insensitive(self):
        assert mime_allowed("TEXT/HTML") is True
        assert mime_allowed("Application/JSON") is True


# ---------------------------------------------------------------------------
# RecordingConfig fields
# ---------------------------------------------------------------------------


class TestBodyCaptureConfig:
    def test_disabled_by_default(self, tmp_path):
        cfg = RecordingConfig(record_dir=tmp_path)
        assert cfg.record_bodies is False
        assert cfg.bodies_enabled is False

    def test_bodies_require_recording_dir(self):
        cfg = RecordingConfig(record_bodies=True)  # no record_dir
        assert cfg.bodies_enabled is False

    def test_enabled_when_dir_and_bodies(self, tmp_path):
        cfg = RecordingConfig(record_dir=tmp_path, record_bodies=True)
        assert cfg.bodies_enabled is True

    def test_default_caps(self, tmp_path):
        cfg = RecordingConfig(record_dir=tmp_path, record_bodies=True)
        assert cfg.max_body_bytes == 2 * 1024 * 1024
        assert cfg.max_total_body_bytes == 100 * 1024 * 1024

    def test_from_cli_flags(self, tmp_path):
        cfg = RecordingConfig.from_cli(
            record_dir=str(tmp_path),
            record_bodies=True,
            max_body_bytes=1024,
            max_total_body_bytes=8192,
        )
        assert cfg.record_bodies is True
        assert cfg.max_body_bytes == 1024
        assert cfg.max_total_body_bytes == 8192


# ---------------------------------------------------------------------------
# BodyCapture core behavior
# ---------------------------------------------------------------------------


def _cfg(tmp_path, **overrides) -> RecordingConfig:
    base = dict(record_dir=tmp_path, record_bodies=True)
    base.update(overrides)
    return RecordingConfig(**base)


class TestBodyCaptureFeed:
    def test_disabled_returns_no_commands(self, tmp_path):
        cfg = RecordingConfig(record_dir=tmp_path, record_bodies=False)
        bc = BodyCapture(cfg)
        cmds = bc.feed_event({
            "method": "Network.loadingFinished",
            "params": {"requestId": "R1", "encodedDataLength": 100},
        })
        assert cmds == []

    def test_emits_command_after_html_response(self, tmp_path):
        bc = BodyCapture(_cfg(tmp_path))
        # 1. Chrome announces an HTML response.
        bc.feed_event({
            "method": "Network.responseReceived",
            "params": {
                "requestId": "R1",
                "response": {"status": 200, "mimeType": "text/html",
                             "encodedDataLength": 1024, "headers": {}},
            },
        })
        # 2. Loading finishes — we should request the body.
        cmds = bc.feed_event({
            "method": "Network.loadingFinished",
            "params": {"requestId": "R1", "encodedDataLength": 1024},
        })
        assert len(cmds) == 1
        cmd = cmds[0]
        assert cmd["method"] == "Network.getResponseBody"
        assert cmd["params"] == {"requestId": "R1"}
        assert cmd["id"] >= 9_000_000_000
        assert "sessionId" not in cmd  # browser-level session

    def test_session_id_round_trips(self, tmp_path):
        bc = BodyCapture(_cfg(tmp_path))
        bc.feed_event({
            "sessionId": "S1",
            "method": "Network.responseReceived",
            "params": {
                "requestId": "R1",
                "response": {"mimeType": "application/json",
                             "encodedDataLength": 50, "headers": {}},
            },
        })
        cmds = bc.feed_event({
            "sessionId": "S1",
            "method": "Network.loadingFinished",
            "params": {"requestId": "R1", "encodedDataLength": 50},
        })
        assert len(cmds) == 1
        assert cmds[0]["sessionId"] == "S1"

    def test_skipped_on_disallowed_mime(self, tmp_path):
        bc = BodyCapture(_cfg(tmp_path))
        bc.feed_event({
            "method": "Network.responseReceived",
            "params": {
                "requestId": "R1",
                "response": {"mimeType": "image/png",
                             "encodedDataLength": 5000, "headers": {}},
            },
        })
        cmds = bc.feed_event({
            "method": "Network.loadingFinished",
            "params": {"requestId": "R1", "encodedDataLength": 5000},
        })
        assert cmds == []
        assert bc.metrics.skipped_mime == 1
        assert bc.metrics.requested == 0

    def test_skipped_when_too_large(self, tmp_path):
        cfg = _cfg(tmp_path, max_body_bytes=1000)
        bc = BodyCapture(cfg)
        bc.feed_event({
            "method": "Network.responseReceived",
            "params": {
                "requestId": "R1",
                "response": {"mimeType": "text/html",
                             "encodedDataLength": 9999, "headers": {}},
            },
        })
        cmds = bc.feed_event({
            "method": "Network.loadingFinished",
            "params": {"requestId": "R1", "encodedDataLength": 9999},
        })
        assert cmds == []
        assert bc.metrics.skipped_size == 1

    def test_skipped_when_session_budget_exhausted(self, tmp_path):
        cfg = _cfg(tmp_path, max_total_body_bytes=500)
        bc = BodyCapture(cfg)
        # Pretend we've already captured 500 bytes.
        bc._total_bytes = 500
        bc.feed_event({
            "method": "Network.responseReceived",
            "params": {
                "requestId": "R1",
                "response": {"mimeType": "text/html",
                             "encodedDataLength": 100, "headers": {}},
            },
        })
        cmds = bc.feed_event({
            "method": "Network.loadingFinished",
            "params": {"requestId": "R1", "encodedDataLength": 100},
        })
        assert cmds == []
        assert bc.metrics.skipped_budget == 1

    def test_loading_failed_clears_meta(self, tmp_path):
        bc = BodyCapture(_cfg(tmp_path))
        bc.feed_event({
            "method": "Network.responseReceived",
            "params": {"requestId": "R1",
                       "response": {"mimeType": "text/html", "headers": {}}},
        })
        cmds = bc.feed_event({
            "method": "Network.loadingFailed",
            "params": {"requestId": "R1", "errorText": "net::ERR"},
        })
        assert cmds == []
        assert bc.metrics.skipped_failed == 1


class TestBodyCaptureReply:
    def test_intercept_non_injected_id_returns_false(self, tmp_path):
        bc = BodyCapture(_cfg(tmp_path))
        was_ours, body = bc.intercept_reply({"id": 5, "result": {"body": "hi"}})
        assert was_ours is False
        assert body is None

    def test_intercept_injected_id_returns_body(self, tmp_path):
        bc = BodyCapture(_cfg(tmp_path))
        # Trigger an injection so we have a pending entry.
        bc.feed_event({
            "method": "Network.responseReceived",
            "params": {"requestId": "R1",
                       "response": {"mimeType": "text/html",
                                    "encodedDataLength": 50, "headers": {}}},
        })
        cmds = bc.feed_event({
            "method": "Network.loadingFinished",
            "params": {"requestId": "R1", "encodedDataLength": 50},
        })
        msg_id = cmds[0]["id"]
        was_ours, body = bc.intercept_reply({
            "id": msg_id,
            "result": {"body": "<html>hi</html>", "base64Encoded": False},
        })
        assert was_ours is True
        assert body is not None
        assert body.request_id == "R1"
        assert body.body == "<html>hi</html>"
        assert body.base64_encoded is False
        assert body.mime == "text/html"
        assert bc.metrics.captured == 1
        assert bc.metrics.bytes_captured == len("<html>hi</html>".encode())

    def test_intercept_handles_cdp_error(self, tmp_path):
        bc = BodyCapture(_cfg(tmp_path))
        bc.feed_event({
            "method": "Network.responseReceived",
            "params": {"requestId": "R1",
                       "response": {"mimeType": "text/html", "headers": {}}},
        })
        cmds = bc.feed_event({
            "method": "Network.loadingFinished",
            "params": {"requestId": "R1", "encodedDataLength": 10},
        })
        msg_id = cmds[0]["id"]
        was_ours, body = bc.intercept_reply({
            "id": msg_id, "error": {"code": -32000, "message": "No data found"},
        })
        assert was_ours is True
        assert body is None
        assert bc.metrics.failed == 1

    def test_budget_decrements_after_capture(self, tmp_path):
        cfg = _cfg(tmp_path, max_total_body_bytes=200)
        bc = BodyCapture(cfg)
        # First request — 100 bytes.
        bc.feed_event({"method": "Network.responseReceived",
                       "params": {"requestId": "R1",
                                  "response": {"mimeType": "text/html",
                                               "encodedDataLength": 50, "headers": {}}}})
        cmds = bc.feed_event({"method": "Network.loadingFinished",
                              "params": {"requestId": "R1", "encodedDataLength": 50}})
        bc.intercept_reply({"id": cmds[0]["id"],
                            "result": {"body": "x" * 100, "base64Encoded": False}})
        # Second request — 150 bytes. Should still emit (we're at 100/200).
        bc.feed_event({"method": "Network.responseReceived",
                       "params": {"requestId": "R2",
                                  "response": {"mimeType": "text/html",
                                               "encodedDataLength": 80, "headers": {}}}})
        cmds2 = bc.feed_event({"method": "Network.loadingFinished",
                               "params": {"requestId": "R2", "encodedDataLength": 80}})
        assert len(cmds2) == 1
        bc.intercept_reply({"id": cmds2[0]["id"],
                            "result": {"body": "y" * 150, "base64Encoded": False}})
        # Third request should be blocked (we're at 250 > 200).
        bc.feed_event({"method": "Network.responseReceived",
                       "params": {"requestId": "R3",
                                  "response": {"mimeType": "text/html",
                                               "encodedDataLength": 10, "headers": {}}}})
        cmds3 = bc.feed_event({"method": "Network.loadingFinished",
                               "params": {"requestId": "R3", "encodedDataLength": 10}})
        assert cmds3 == []
        assert bc.metrics.skipped_budget >= 1


class TestHarBuilderInjectedBody:
    def test_set_response_body_text(self):
        b = CdpHarBuilder()
        b.feed("cdp_to_client", {
            "method": "Network.requestWillBeSent",
            "params": {
                "requestId": "R1",
                "request": {"url": "https://x/", "method": "GET", "headers": {}},
                "wallTime": 1.0, "timestamp": 1.0,
            },
        })
        b.feed("cdp_to_client", {
            "method": "Network.responseReceived",
            "params": {
                "requestId": "R1", "timestamp": 1.1,
                "response": {"status": 200, "statusText": "OK",
                             "mimeType": "text/html", "headers": {}},
            },
        })
        b.set_response_body("", "R1", "<html>", base64_encoded=False)
        e = b.build()["log"]["entries"][0]
        assert e["response"]["content"]["text"] == "<html>"
        assert "encoding" not in e["response"]["content"]

    def test_set_response_body_base64(self):
        b = CdpHarBuilder()
        b.feed("cdp_to_client", {
            "method": "Network.requestWillBeSent",
            "params": {
                "requestId": "R1",
                "request": {"url": "https://x/", "method": "GET", "headers": {}},
                "wallTime": 1.0, "timestamp": 1.0,
            },
        })
        b.feed("cdp_to_client", {
            "method": "Network.responseReceived",
            "params": {
                "requestId": "R1", "timestamp": 1.1,
                "response": {"status": 200, "statusText": "OK",
                             "mimeType": "image/svg+xml", "headers": {}},
            },
        })
        b.set_response_body("", "R1", "AAEC", base64_encoded=True)
        e = b.build()["log"]["entries"][0]
        assert e["response"]["content"]["text"] == "AAEC"
        assert e["response"]["content"]["encoding"] == "base64"

    def test_set_response_body_unknown_request_silent(self):
        b = CdpHarBuilder()
        # Should not raise — there's no R1 yet.
        b.set_response_body("", "R1", "x", base64_encoded=False)
        assert b.build()["log"]["entries"] == []


# ---------------------------------------------------------------------------
# SessionRecorder.record_injected_body fan-out
# ---------------------------------------------------------------------------


class TestSessionRecorderInjectedBody:
    @pytest.mark.asyncio
    async def test_writes_synthetic_jsonl_frame(self, tmp_path):
        cfg = RecordingConfig(record_dir=tmp_path, record_cdp=True, record_har=False)
        rec = SessionRecorder.create(cfg, label="t")
        await rec.record_injected_body(
            session_id="", request_id="R1",
            body="<html>hi</html>", base64_encoded=False, mime="text/html",
        )
        await rec.close()
        line = (rec.session_dir / "cdp.jsonl").read_text().strip()
        record = json.loads(line)
        assert record["injected"] is True
        assert record["type"] == "injected_body"
        assert record["body"] == "<html>hi</html>"
        assert record["mime"] == "text/html"
        assert record["request_id"] == "R1"

    @pytest.mark.asyncio
    async def test_propagates_to_har(self, tmp_path):
        cfg = RecordingConfig(record_dir=tmp_path, record_cdp=False, record_har=True)
        rec = SessionRecorder.create(cfg, label="t")
        # Seed a request in the HAR builder via a normal frame.
        await rec.record_text("cdp_to_client", json.dumps({
            "method": "Network.requestWillBeSent",
            "params": {
                "requestId": "R1",
                "request": {"url": "https://x/", "method": "GET", "headers": {}},
                "wallTime": 1.0, "timestamp": 1.0,
            },
        }))
        await rec.record_text("cdp_to_client", json.dumps({
            "method": "Network.responseReceived",
            "params": {
                "requestId": "R1", "timestamp": 1.1,
                "response": {"status": 200, "statusText": "OK",
                             "mimeType": "text/html", "headers": {}},
            },
        }))
        await rec.record_injected_body("", "R1", "<html>FT</html>", False, "text/html")
        await rec.close()

        har = json.loads((rec.session_dir / "network.har").read_text())
        e = har["log"]["entries"][0]
        assert e["response"]["content"]["text"] == "<html>FT</html>"
