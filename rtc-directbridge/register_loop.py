#!/usr/bin/env python3
"""Periodic go2rtc stream registration loop.

Runs as a background process alongside bridge.py. Every RECONCILE_SECONDS,
checks if go2rtc has our stream registered and re-registers if needed.
Self-heals after go2rtc restarts.
"""
import logging
import os
import socket
import sys
import time

from go2rtc_register import Go2RTCStreamRegistrar

logging.basicConfig(
    stream=sys.stderr,
    level=os.environ.get("LOXONE_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("loxone-register")


def main():
    api_url = os.environ.get("GO2RTC_API_URL", "http://go2rtc:1984")
    stream_name = os.environ.get("LOXONE_STREAM_NAME", "loxone_intercom")
    rtsp_host = os.environ.get("LOXONE_RTSP_HOST", socket.gethostname())
    rtsp_port = os.environ.get("LOXONE_RTSP_PORT", "8554")
    interval = int(os.environ.get("LOXONE_GO2RTC_RECONCILE_SECONDS", "20"))

    source = f"rtsp://{rtsp_host}:{rtsp_port}/{stream_name}"
    logger.info("go2rtc registration loop: stream=%s source=%s interval=%ds", stream_name, source, interval)

    registrar = Go2RTCStreamRegistrar(api_url)

    while True:
        try:
            ok = registrar.ensure_stream(stream_name, source)
            if ok:
                logger.debug("go2rtc stream %s registered", stream_name)
            else:
                logger.warning("go2rtc stream %s not confirmed; retry in %ds", stream_name, interval)
        except Exception as e:
            logger.warning("go2rtc registration error: %s; retry in %ds", e, interval)
        time.sleep(interval)


if __name__ == "__main__":
    main()
