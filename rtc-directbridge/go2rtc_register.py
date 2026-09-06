"""Register a stream into go2rtc via its REST API.

Adapted from Mammotion bridge's Go2RTCStreamRegistrar (aiohttp-based),
simplified to use only Python stdlib (urllib) — no extra dependencies.

go2rtc API contract:
  GET    /api/streams              — list all streams
  POST   /api/streams?dst=&src=    — create stream (push source to dst)
  PUT    /api/streams?name=&src=   — create stream (alternative)
  PATCH  /api/streams?name=&src=   — update stream source
  DELETE /api/streams?src=          — delete stream
"""
from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

_API_PATH = "api/streams"
_TIMEOUT = 10


class Go2RTCStreamRegistrar:
    """Register one go2rtc stream pointing at our RTSP source."""

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/") + "/"

    def ensure_stream(self, stream_name: str, source: str) -> bool:
        """Ensure go2rtc has stream_name wired to source.

        Idempotent: returns True immediately if the stream already
        exists with the expected producer URL.
        """
        if self._stream_matches(stream_name, source):
            logger.debug("go2rtc stream %s already wired to %s", stream_name, source)
            return True

        methods: tuple[tuple[str, dict[str, str]], ...] = (
            ("POST", {"dst": stream_name, "src": source}),
            ("PUT", {"name": stream_name, "src": source}),
            ("PATCH", {"name": stream_name, "src": source}),
            ("PATCH", {"dst": stream_name, "src": source}),
        )

        statuses: list[str] = []
        for method, params in methods:
            status = self._call_api(method, params)
            statuses.append(f"{method}={status}")
            if status in (200, 201, 204):
                return True
            if self._stream_matches(stream_name, source):
                return True

        logger.warning("Failed to register go2rtc stream %s (%s)", stream_name, ", ".join(statuses))
        return False

    def remove_stream(self, stream_name: str) -> bool:
        for params in ({"dst": stream_name}, {"name": stream_name}):
            status = self._call_api("DELETE", params)
            if status in (200, 204):
                return True
        return False

    def _get_streams(self) -> dict | None:
        try:
            req = urllib.request.Request(self._base_url + _API_PATH, method="GET")
            with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
                if resp.status != 200:
                    return None
                return json.loads(resp.read())
        except Exception as e:
            logger.debug("Failed to query go2rtc streams: %s", e)
            return None

    def _stream_matches(self, stream_name: str, source: str) -> bool:
        streams = self._get_streams()
        if streams is None:
            return False
        stream = streams.get(stream_name)
        if not isinstance(stream, dict):
            return False
        producers = stream.get("producers") or []
        norm = source.rstrip("/")
        return any(
            isinstance(p, dict) and str(p.get("url", "")).rstrip("/") == norm
            for p in producers
        )

    def _call_api(self, method: str, params: dict[str, str]) -> int:
        url = self._base_url + _API_PATH + "?" + urlencode(params)
        try:
            req = urllib.request.Request(url, method=method)
            with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
                resp.read()
                return resp.status
        except urllib.error.HTTPError as e:
            return e.code
        except Exception as e:
            logger.debug("go2rtc %s failed: %s", method.upper(), e)
            return 0
