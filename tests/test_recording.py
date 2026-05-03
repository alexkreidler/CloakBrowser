"""Unit tests for cloakbrowser.recording — recorder, har_builder, config."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from cloakbrowser.recording import (
    CdpHarBuilder,
    CdpJsonlRecorder,
    HarRecorder,
    NullRecorder,
    RecordingConfig,
    SessionRecorder,
)
from cloakbrowser.recording.config import parse_bool_flag


# ---------------------------------------------------------------------------
# RecordingConfig
# ---------------------------------------------------------------------------


class TestRecordingConfig:
    def test_disabled_by_default(self):
        cfg = RecordingConfig()
        assert cfg.enabled is False

    def test_enabled_requires_dir_and_at_least_one_format(self):
        assert RecordingConfig(record_dir="/tmp").enabled is True
        assert RecordingConfig(record_dir="/tmp", record_cdp=False).enabled is False
        assert RecordingConfig(record_dir="/tmp", record_cdp=False, record_har=True).enabled is True
        assert RecordingConfig(record_dir=None, record_cdp=True, record_har=True).enabled is False

    def test_session_dir(self, tmp_path):
        cfg = RecordingConfig(record_dir=tmp_path)
        assert cfg.session_dir("abc") == tmp_path / "abc"

    def test_session_dir_no_record_dir_raises(self):
        cfg = RecordingConfig()
        with pytest.raises(ValueError):
            cfg.session_dir("x")

    def test_from_cli(self):
        cfg = RecordingConfig.from_cli(record_dir="/tmp/r", record_cdp=False, record_har=True)
        assert cfg.record_dir == "/tmp/r"
        assert cfg.record_cdp is False
        assert cfg.record_har is True

    def test_with_overrides_returns_new(self):
        cfg = RecordingConfig(record_dir="/a")
        cfg2 = cfg.with_overrides(record_har=True)
        assert cfg.record_har is False
        assert cfg2.record_har is True

    def test_parse_bool_flag(self):
        for v in ("true", "True", "1", "yes", "ON", " on "):
            assert parse_bool_flag(v) is True, v
        for v in ("false", "0", "no", "anything"):
            assert parse_bool_flag(v) is False, v


# ---------------------------------------------------------------------------
# CdpJsonlRecorder
# ---------------------------------------------------------------------------


class TestCdpJsonlRecorder:
    @pytest.mark.asyncio
    async def test_writes_json_per_line(self, tmp_path):
        path = tmp_path / "cdp.jsonl"
        rec = CdpJsonlRecorder(path, label="test")
        await rec.record_text("client_to_cdp", json.dumps({"id": 1, "method": "Page.enable"}))
        await rec.record_text("cdp_to_client", json.dumps({"id": 1, "result": {}}))
        await rec.close()

        lines = path.read_text().splitlines()
        assert len(lines) == 2
        a = json.loads(lines[0])
        b = json.loads(lines[1])
        assert a["direction"] == "client_to_cdp"
        assert a["message"] == {"id": 1, "method": "Page.enable"}
        assert "raw" not in a  # raw is omitted when JSON parses cleanly
        assert b["direction"] == "cdp_to_client"

    @pytest.mark.asyncio
    async def test_keeps_raw_when_not_json(self, tmp_path):
        path = tmp_path / "cdp.jsonl"
        rec = CdpJsonlRecorder(path, label="test")
        await rec.record_text("client_to_cdp", "this is not json")
        await rec.close()
        line = json.loads(path.read_text().splitlines()[0])
        assert line["message"] is None
        assert line["raw"] == "this is not json"

    @pytest.mark.asyncio
    async def test_binary_record_b64(self, tmp_path):
        path = tmp_path / "cdp.jsonl"
        rec = CdpJsonlRecorder(path, label="test")
        await rec.record_binary("cdp_to_client", b"\x00\x01\x02hello")
        await rec.close()
        line = json.loads(path.read_text().splitlines()[0])
        assert line["type"] == "binary"
        assert "data_b64" in line

    @pytest.mark.asyncio
    async def test_binary_disabled(self, tmp_path):
        path = tmp_path / "cdp.jsonl"
        rec = CdpJsonlRecorder(path, label="test", include_binary=False)
        await rec.record_binary("cdp_to_client", b"hello")
        await rec.close()
        # File should not exist (lazy open) or be empty
        assert (not path.exists()) or path.read_text() == ""

    @pytest.mark.asyncio
    async def test_max_bytes_truncation(self, tmp_path):
        path = tmp_path / "cdp.jsonl"
        rec = CdpJsonlRecorder(path, label="t", max_bytes=200)
        big_payload = json.dumps({"id": 1, "method": "x", "params": {"data": "x" * 500}})
        await rec.record_text("client_to_cdp", big_payload)
        # Second write should be dropped + truncation marker added
        await rec.record_text("client_to_cdp", '{"id":2}')
        await rec.close()

        lines = path.read_text().splitlines()
        # Could be just the truncation marker (first frame too big), or
        # first frame + marker.
        assert any(json.loads(l).get("truncated") for l in lines)

    @pytest.mark.asyncio
    async def test_concurrent_writes_serialize(self, tmp_path):
        path = tmp_path / "cdp.jsonl"
        rec = CdpJsonlRecorder(path, label="t")

        async def writer(idx):
            for i in range(20):
                await rec.record_text("client_to_cdp", json.dumps({"id": idx * 100 + i}))

        await asyncio.gather(*(writer(k) for k in range(5)))
        await rec.close()
        # Every line must parse cleanly — proves we didn't interleave bytes.
        lines = path.read_text().splitlines()
        assert len(lines) == 5 * 20
        for line in lines:
            json.loads(line)


# ---------------------------------------------------------------------------
# CdpHarBuilder
# ---------------------------------------------------------------------------


class TestCdpHarBuilder:
    def test_empty(self):
        har = CdpHarBuilder().build()
        assert har["log"]["version"] == "1.2"
        assert har["log"]["entries"] == []

    def test_basic_request_response_finished(self):
        b = CdpHarBuilder()
        b.feed("cdp_to_client", {
            "method": "Network.requestWillBeSent",
            "params": {
                "requestId": "R1",
                "request": {"url": "https://example.com/", "method": "GET", "headers": {"User-Agent": "x"}},
                "wallTime": 1700000000.0,
                "timestamp": 100.0,
                "frameId": "F1",
                "loaderId": "L1",
            },
        })
        b.feed("cdp_to_client", {
            "method": "Network.responseReceived",
            "params": {
                "requestId": "R1",
                "timestamp": 100.5,
                "response": {
                    "status": 200,
                    "statusText": "OK",
                    "mimeType": "text/html",
                    "headers": {"Content-Type": "text/html"},
                    "remoteIPAddress": "93.184.216.34",
                    "remotePort": 443,
                    "protocol": "h2",
                    "timing": {
                        "requestTime": 100.0,
                        "dnsStart": 1.0, "dnsEnd": 5.0,
                        "connectStart": 5.0, "connectEnd": 20.0,
                        "sslStart": 10.0, "sslEnd": 20.0,
                        "sendStart": 20.0, "sendEnd": 21.0,
                        "receiveHeadersStart": 100.0,
                        "receiveHeadersEnd": 110.0,
                    },
                },
            },
        })
        b.feed("cdp_to_client", {
            "method": "Network.dataReceived",
            "params": {"requestId": "R1", "encodedDataLength": 1500, "dataLength": 4000, "timestamp": 100.6},
        })
        b.feed("cdp_to_client", {
            "method": "Network.loadingFinished",
            "params": {"requestId": "R1", "encodedDataLength": 1500, "timestamp": 100.7},
        })

        har = b.build()
        assert len(har["log"]["entries"]) == 1
        e = har["log"]["entries"][0]
        assert e["request"]["url"] == "https://example.com/"
        assert e["request"]["method"] == "GET"
        assert e["response"]["status"] == 200
        assert e["response"]["content"]["mimeType"] == "text/html"
        assert e["response"]["content"]["size"] == 4000
        assert e["serverIPAddress"] == "93.184.216.34"
        assert e["_cdp"]["requestId"] == "R1"
        assert e["_cdp"]["sessionId"] == ""
        # Total time should be derived from monotonic deltas (0.7s = 700ms).
        assert abs(e["time"] - 700.0) < 1.0

    def test_session_id_isolation(self):
        b = CdpHarBuilder()
        for sess in ("S1", "S2"):
            b.feed("cdp_to_client", {
                "sessionId": sess,
                "method": "Network.requestWillBeSent",
                "params": {
                    "requestId": "SHARED",  # Same requestId across sessions!
                    "request": {"url": f"https://example.com/{sess}", "method": "GET", "headers": {}},
                    "wallTime": 1700000000.0,
                    "timestamp": 1.0,
                },
            })
            b.feed("cdp_to_client", {
                "sessionId": sess,
                "method": "Network.responseReceived",
                "params": {
                    "requestId": "SHARED",
                    "timestamp": 1.5,
                    "response": {"status": 200, "statusText": "OK", "mimeType": "text/html", "headers": {}},
                },
            })
        har = b.build()
        urls = sorted(e["request"]["url"] for e in har["log"]["entries"])
        assert urls == ["https://example.com/S1", "https://example.com/S2"]

    def test_response_body_capture_via_command_pair(self):
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
                "response": {"status": 200, "statusText": "OK", "mimeType": "text/plain", "headers": {}},
            },
        })
        # Client asks for body
        b.feed("client_to_cdp", {"id": 99, "method": "Network.getResponseBody", "params": {"requestId": "R1"}})
        # Reply
        b.feed("cdp_to_client", {"id": 99, "result": {"body": "hello world", "base64Encoded": False}})

        har = b.build()
        e = har["log"]["entries"][0]
        assert e["response"]["content"]["text"] == "hello world"
        assert "encoding" not in e["response"]["content"]

    def test_response_body_base64(self):
        b = CdpHarBuilder()
        b.feed("cdp_to_client", {
            "method": "Network.requestWillBeSent",
            "params": {
                "requestId": "R1",
                "request": {"url": "https://x/img.png", "method": "GET", "headers": {}},
                "wallTime": 1.0, "timestamp": 1.0,
            },
        })
        b.feed("cdp_to_client", {
            "method": "Network.responseReceived",
            "params": {
                "requestId": "R1", "timestamp": 1.1,
                "response": {"status": 200, "statusText": "OK", "mimeType": "image/png", "headers": {}},
            },
        })
        b.feed("client_to_cdp", {"id": 7, "method": "Network.getResponseBody", "params": {"requestId": "R1"}})
        b.feed("cdp_to_client", {"id": 7, "result": {"body": "AAEC", "base64Encoded": True}})

        e = b.build()["log"]["entries"][0]
        assert e["response"]["content"]["text"] == "AAEC"
        assert e["response"]["content"]["encoding"] == "base64"

    def test_loading_failed_marks_entry(self):
        b = CdpHarBuilder()
        b.feed("cdp_to_client", {
            "method": "Network.requestWillBeSent",
            "params": {
                "requestId": "R1",
                "request": {"url": "https://broken/", "method": "GET", "headers": {}},
                "wallTime": 1.0, "timestamp": 1.0,
            },
        })
        b.feed("cdp_to_client", {
            "method": "Network.loadingFailed",
            "params": {"requestId": "R1", "errorText": "net::ERR_NAME_NOT_RESOLVED", "timestamp": 1.1},
        })
        e = b.build()["log"]["entries"][0]
        assert e["_cdp"]["failed"] is True
        assert "ERR_NAME_NOT_RESOLVED" in e["_cdp"]["errorText"]

    def test_redirect_creates_separate_entry(self):
        b = CdpHarBuilder()
        b.feed("cdp_to_client", {
            "method": "Network.requestWillBeSent",
            "params": {
                "requestId": "R1",
                "request": {"url": "http://example.com/", "method": "GET", "headers": {}},
                "wallTime": 1.0, "timestamp": 1.0,
            },
        })
        # Redirect response delivered as part of the second requestWillBeSent
        b.feed("cdp_to_client", {
            "method": "Network.requestWillBeSent",
            "params": {
                "requestId": "R1",
                "request": {"url": "https://example.com/", "method": "GET", "headers": {}},
                "wallTime": 1.05, "timestamp": 1.05,
                "redirectResponse": {
                    "status": 301, "statusText": "Moved",
                    "mimeType": "", "headers": {"Location": "https://example.com/"},
                },
            },
        })
        b.feed("cdp_to_client", {
            "method": "Network.responseReceived",
            "params": {
                "requestId": "R1", "timestamp": 1.1,
                "response": {"status": 200, "statusText": "OK", "mimeType": "text/html", "headers": {}},
            },
        })
        entries = b.build()["log"]["entries"]
        assert len(entries) == 2
        assert entries[0]["response"]["status"] == 301
        assert entries[1]["response"]["status"] == 200

    def test_extra_info_overrides_headers(self):
        b = CdpHarBuilder()
        b.feed("cdp_to_client", {
            "method": "Network.requestWillBeSent",
            "params": {
                "requestId": "R1",
                "request": {"url": "https://x/", "method": "GET", "headers": {"User-Agent": "fake"}},
                "wallTime": 1.0, "timestamp": 1.0,
            },
        })
        b.feed("cdp_to_client", {
            "method": "Network.requestWillBeSentExtraInfo",
            "params": {"requestId": "R1", "headers": {"user-agent": "real", "accept": "*/*"}},
        })
        b.feed("cdp_to_client", {
            "method": "Network.responseReceived",
            "params": {
                "requestId": "R1", "timestamp": 1.1,
                "response": {"status": 200, "statusText": "OK", "mimeType": "text/plain", "headers": {}},
            },
        })
        e = b.build()["log"]["entries"][0]
        names = {h["name"] for h in e["request"]["headers"]}
        assert "user-agent" in names and "accept" in names

    def test_query_string_extracted(self):
        b = CdpHarBuilder()
        b.feed("cdp_to_client", {
            "method": "Network.requestWillBeSent",
            "params": {
                "requestId": "R1",
                "request": {"url": "https://x/?a=1&b=two", "method": "GET", "headers": {}},
                "wallTime": 1.0, "timestamp": 1.0,
            },
        })
        e = b.build()["log"]["entries"][0]
        qs = {q["name"]: q["value"] for q in e["request"]["queryString"]}
        assert qs == {"a": "1", "b": "two"}


# ---------------------------------------------------------------------------
# HarRecorder + SessionRecorder integration
# ---------------------------------------------------------------------------


class TestHarRecorder:
    @pytest.mark.asyncio
    async def test_writes_har_on_close(self, tmp_path):
        path = tmp_path / "network.har"
        rec = HarRecorder(path, label="t")
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
                "response": {"status": 200, "statusText": "OK", "mimeType": "text/plain", "headers": {}},
            },
        }))
        await rec.close()

        har = json.loads(path.read_text())
        assert har["log"]["version"] == "1.2"
        assert len(har["log"]["entries"]) == 1
        assert har["log"]["entries"][0]["request"]["url"] == "https://x/"

    @pytest.mark.asyncio
    async def test_ignores_non_json(self, tmp_path):
        rec = HarRecorder(tmp_path / "n.har", label="t")
        await rec.record_text("cdp_to_client", "garbage")
        await rec.record_binary("cdp_to_client", b"binary")
        await rec.close()
        har = json.loads((tmp_path / "n.har").read_text())
        assert har["log"]["entries"] == []

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self, tmp_path):
        rec = HarRecorder(tmp_path / "n.har", label="t")
        await rec.close()
        await rec.close()  # Must not raise


class TestSessionRecorder:
    @pytest.mark.asyncio
    async def test_disabled_returns_null_recorder(self):
        cfg = RecordingConfig()
        rec = SessionRecorder.create(cfg, label="x")
        assert isinstance(rec, NullRecorder)
        # NullRecorder methods are safe and no-op
        await rec.record_text("client_to_cdp", '{"x":1}')
        await rec.record_binary("client_to_cdp", b"ab")
        await rec.close()

    @pytest.mark.asyncio
    async def test_creates_session_dir_with_metadata(self, tmp_path):
        cfg = RecordingConfig(record_dir=tmp_path, record_cdp=True, record_har=True)
        rec = SessionRecorder.create(cfg, label="seed=42", metadata={"seed": "42", "pid": 12345})
        assert isinstance(rec, SessionRecorder)
        assert rec.session_dir.exists()

        meta = json.loads((rec.session_dir / "metadata.json").read_text())
        assert meta["label"] == "seed=42"
        assert meta["seed"] == "42"
        assert meta["pid"] == 12345
        assert meta["record_cdp"] is True
        assert meta["record_har"] is True

        await rec.close()

    @pytest.mark.asyncio
    async def test_fanout_writes_both_files(self, tmp_path):
        cfg = RecordingConfig(record_dir=tmp_path, record_cdp=True, record_har=True)
        rec = SessionRecorder.create(cfg, label="t")

        await rec.record_text("cdp_to_client", json.dumps({
            "method": "Network.requestWillBeSent",
            "params": {
                "requestId": "R1",
                "request": {"url": "https://y/", "method": "GET", "headers": {}},
                "wallTime": 1.0, "timestamp": 1.0,
            },
        }))
        await rec.record_text("cdp_to_client", json.dumps({
            "method": "Network.responseReceived",
            "params": {
                "requestId": "R1", "timestamp": 1.1,
                "response": {"status": 200, "statusText": "OK", "mimeType": "text/plain", "headers": {}},
            },
        }))
        await rec.close()

        cdp_path = rec.session_dir / "cdp.jsonl"
        har_path = rec.session_dir / "network.har"
        assert cdp_path.exists()
        assert har_path.exists()
        assert len(cdp_path.read_text().splitlines()) == 2
        har = json.loads(har_path.read_text())
        assert har["log"]["entries"][0]["request"]["url"] == "https://y/"

    @pytest.mark.asyncio
    async def test_only_cdp(self, tmp_path):
        cfg = RecordingConfig(record_dir=tmp_path, record_cdp=True, record_har=False)
        rec = SessionRecorder.create(cfg, label="t")
        await rec.record_text("cdp_to_client", '{"id":1,"result":{}}')
        await rec.close()
        assert (rec.session_dir / "cdp.jsonl").exists()
        assert not (rec.session_dir / "network.har").exists()

    @pytest.mark.asyncio
    async def test_only_har(self, tmp_path):
        cfg = RecordingConfig(record_dir=tmp_path, record_cdp=False, record_har=True)
        rec = SessionRecorder.create(cfg, label="t")
        await rec.close()
        assert not (rec.session_dir / "cdp.jsonl").exists()
        assert (rec.session_dir / "network.har").exists()


# ---------------------------------------------------------------------------
# Browser HAR helper
# ---------------------------------------------------------------------------


class TestResolveHarKwargs:
    def test_no_path_means_no_kwargs(self):
        from cloakbrowser.browser import _resolve_har_kwargs
        assert _resolve_har_kwargs(None, None, None, None, None) == {}
        assert _resolve_har_kwargs(None, "full", "embed", None, True) == {}

    def test_path_only(self):
        from cloakbrowser.browser import _resolve_har_kwargs
        assert _resolve_har_kwargs("/tmp/x.har", None, None, None, None) == {
            "record_har_path": "/tmp/x.har"
        }

    def test_all_options(self):
        from cloakbrowser.browser import _resolve_har_kwargs
        out = _resolve_har_kwargs("/tmp/x.zip", "minimal", "attach", "**/api/*", None)
        assert out == {
            "record_har_path": "/tmp/x.zip",
            "record_har_mode": "minimal",
            "record_har_content": "attach",
            "record_har_url_filter": "**/api/*",
        }

    def test_pathlib_path_converted(self):
        from cloakbrowser.browser import _resolve_har_kwargs
        p = Path("/tmp/y.har")
        out = _resolve_har_kwargs(p, None, None, None, None)
        assert isinstance(out["record_har_path"], str)
        assert out["record_har_path"] == "/tmp/y.har"
