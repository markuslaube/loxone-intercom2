#!/usr/bin/env python3
"""Loxone Intercom Gen.2 WebRTC Bridge

Receives video from a Loxone Intercom Gen.2 via WebRTC and outputs
raw H.264 Annex-B to stdout for consumption by go2rtc (exec source).

Architecture:
  Intercom → WSS Proxy (Miniserver) → WebRTC → RTP → H.264 depacketize → stdout

Requirements:
  - aiortc >= 1.15
  - websockets >= 12.0
  - Python 3.11+

Usage:
  python3 bridge.py

go2rtc config:
  streams:
    loxone_intercom:
      - exec:python3 /opt/loxone-bridge/bridge.py

Environment variables (see .env.example):
  LOXONE_MINISERVER_IP    Miniserver IP address (e.g. 192.168.1.100)
  LOXONE_MINISERVER_USER  Miniserver username (needs rights on the Intercom)
  LOXONE_MINISERVER_PASS  Miniserver password
  LOXONE_DEVICE_UUID   Optional: specific Intercom device UUID (multi-intercom setups)
  LOXONE_PROXY_URL     Optional override: full WSS proxy URL (skips auto-discovery)
  LOXONE_TURN_SERVER   TURN server address (host:port)
  LOXONE_STUN_SERVER   STUN server address (host:port)
  LOXONE_LOG_LEVEL     Log level (INFO, DEBUG, WARNING)

Auto-Discovery:
  If LOXONE_PROXY_URL is not set, the bridge auto-discovers the WSS URL:
  1. GET http://<ip>/jdev/cfg/apiKey → Miniserver MAC + IP → DDNS hostname
  2. GET http://<ip>/data/LoxAPP3.json (with Intercom auth) → IntercomV2 controls
  3. Pick first IntercomV2 deviceUuid (or the one matching LOXONE_DEVICE_UUID)
  4. Construct: wss://<ddns-hostname>/proxy/<deviceUuid>/

TURN credentials (username + password) are fetched dynamically from the
Miniserver via the JSON-RPC `info` method on each WSS connect.
"""
import asyncio
import base64
import json
import logging
import os
import re
import ssl
import struct
import sys
import time
import urllib.parse
import urllib.request

import websockets
from aiortc import (
    RTCConfiguration,
    RTCIceCandidate,
    RTCIceServer,
    RTCPeerConnection,
    RTCSessionDescription,
)
from aiortc.rtcdtlstransport import RTCDtlsTransport

MINISERVER_IP = os.environ.get("LOXONE_MINISERVER_IP", "")
DEVICE_UUID_OVERRIDE = os.environ.get("LOXONE_DEVICE_UUID", "")
PROXY_URL_OVERRIDE = os.environ.get("LOXONE_PROXY_URL", "")
AUTH_USER = os.environ.get("LOXONE_MINISERVER_USER", "")
AUTH_PASS = os.environ.get("LOXONE_MINISERVER_PASS", "")
TURN_SERVER = os.environ.get("LOXONE_TURN_SERVER", "stun.loxonecloud.com:3478")
STUN_SERVER = os.environ.get("LOXONE_STUN_SERVER", "stun.l.google.com:19302")
LOG_LEVEL = os.environ.get("LOXONE_LOG_LEVEL", "INFO")

logging.basicConfig(
    stream=sys.stderr,
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("loxone-bridge")

START_CODE = b"\x00\x00\x00\x01"

NAL_FU_A = 28
NAL_STAPA = 24
SPS_NAL = 7
PPS_NAL = 8
IDR_NAL = 5

_stdout_fd = os.dup(1)


def write_stdout(data):
    os.write(_stdout_fd, data)


def depacketize_h264(payload, state):
    nals = []
    if len(payload) == 0:
        return nals

    nal_header = payload[0]
    nal_type = nal_header & 0x1F

    if 1 <= nal_type <= 23:
        nal = bytes([nal_header]) + payload[1:]
        nals.append(nal)
        if nal_type == SPS_NAL:
            state["sps"] = nal
        elif nal_type == PPS_NAL:
            state["pps"] = nal

    elif nal_type == NAL_STAPA:
        offset = 1
        while offset + 2 <= len(payload):
            size = struct.unpack("!H", payload[offset : offset + 2])[0]
            offset += 2
            if offset + size <= len(payload):
                nal = payload[offset : offset + size]
                nals.append(nal)
                if len(nal) > 0:
                    nt = nal[0] & 0x1F
                    if nt == SPS_NAL:
                        state["sps"] = nal
                    elif nt == PPS_NAL:
                        state["pps"] = nal
                offset += size
            else:
                break

    elif nal_type == NAL_FU_A:
        if len(payload) < 2:
            return nals
        fu_indicator = payload[0]
        fu_header = payload[1]
        fu_type = fu_header & 0x1F
        fu_start = (fu_header >> 7) & 1
        fu_end = (fu_header >> 6) & 1

        if fu_start:
            nri = (fu_indicator >> 5) & 0x03
            hdr = (nri << 5) | fu_type
            state["fu_buf"] = bytes([hdr])

        if "fu_buf" in state:
            state["fu_buf"] += payload[2:]

        if fu_end and "fu_buf" in state:
            nal = state["fu_buf"]
            nals.append(nal)
            if len(nal) > 0:
                nt = nal[0] & 0x1F
                if nt == SPS_NAL:
                    state["sps"] = nal
                elif nt == PPS_NAL:
                    state["pps"] = nal
            del state["fu_buf"]

    return nals


_depacket_state = {}


async def _patched_rtp_data(self, data, arrival_time_ms):
    if len(data) >= 12:
        b0 = data[0]
        b1 = data[1]
        cc = b0 & 0x0F
        extension = (b0 >> 4) & 1
        payload_offset = 12 + cc * 4
        if extension and payload_offset + 4 <= len(data):
            ext_len = struct.unpack("!H", data[payload_offset + 2 : payload_offset + 4])[0]
            payload_offset += 4 + ext_len * 4
        payload = data[payload_offset:]

        nals = depacketize_h264(payload, _depacket_state)
        for nal in nals:
            if len(nal) > 0:
                nt = nal[0] & 0x1F
                if nt == IDR_NAL:
                    if "sps" in _depacket_state:
                        write_stdout(START_CODE + _depacket_state["sps"])
                    if "pps" in _depacket_state:
                        write_stdout(START_CODE + _depacket_state["pps"])
                write_stdout(START_CODE + nal)

    return await _orig_rtp_data(self, data, arrival_time_ms)


_orig_rtp_data = RTCDtlsTransport._handle_rtp_data
RTCDtlsTransport._handle_rtp_data = _patched_rtp_data


def parse_candidate_line(line):
    parts = line.replace("candidate:", "").split()
    typ_idx = parts.index("typ")
    cand = RTCIceCandidate(
        foundation=parts[0],
        component=int(parts[1]),
        protocol=parts[2].lower(),
        ip=parts[4],
        port=int(parts[5]),
        priority=int(parts[3]),
        type=parts[typ_idx + 1],
    )
    i = typ_idx + 2
    while i + 1 < len(parts):
        if parts[i] == "raddr":
            cand.relatedAddress = parts[i + 1]
        elif parts[i] == "rport":
            cand.relatedPort = int(parts[i + 1])
        i += 2
    return cand


def strip_candidates(sdp):
    return "\n".join(l for l in sdp.split("\n") if "a=candidate:" not in l)


def _extract_turn_creds(msg):
    data = msg
    if isinstance(msg, dict) and "result" in msg:
        data = msg["result"]
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
    if not isinstance(data, dict):
        return None, None
    return data.get("turnuser"), data.get("turnpass")


def _http_get_json(url, auth=None, timeout=10):
    req = urllib.request.Request(url)
    if auth:
        cred = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
        req.add_header("Authorization", f"Basic {cred}")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def discover_proxy_url():
    """Auto-discover the WSS proxy URL from the Miniserver.

    Steps:
      1. GET /jdev/cfg/apiKey → Miniserver MAC + certTLD → DDNS hostname
      2. GET /data/LoxAPP3.json → find IntercomV2 controls → deviceUuid
      3. Construct wss://<ddns-hostname>/proxy/<deviceUuid>/
    """
    if not MINISERVER_IP:
        raise RuntimeError("LOXONE_MINISERVER_IP not set and LOXONE_PROXY_URL not provided")

    base = f"http://{MINISERVER_IP}"
    logger.info("Discovering Miniserver info from %s", base)

    info = _http_get_json(f"{base}/jdev/cfg/apiKey")
    api_value = info.get("LL", {}).get("value", "")

    snr_match = re.search(r"'snr':\s*'([^']+)'", api_value)
    ip_match = re.search(r"'address':\s*'([^']+)'", api_value)
    tld_match = re.search(r"'certTLD':\s*'([^']+)'", api_value)

    mac_raw = snr_match.group(1) if snr_match else ""
    ms_ip = ip_match.group(1) if ip_match else MINISERVER_IP
    cert_tld = tld_match.group(1) if tld_match else "com"

    if not mac_raw:
        raise RuntimeError("Could not extract Miniserver serial number from apiKey response")

    mac_clean = mac_raw.replace(":", "").lower()
    ip_dash = ms_ip.replace(".", "-")
    ddns_host = f"{ip_dash}.{mac_clean}.dyndns.loxonecloud.{cert_tld}"
    logger.info("Miniserver: MAC=%s, IP=%s, DDNS=%s", mac_clean, ms_ip, ddns_host)

    app = _http_get_json(f"{base}/data/LoxAPP3.json", auth=(AUTH_USER, AUTH_PASS))
    controls = app.get("controls", {})

    intercom_uuid = None
    for uuid, ctrl in controls.items():
        if ctrl.get("type") != "IntercomV2":
            continue
        dev_uuid = ctrl.get("details", {}).get("deviceUuid", "")
        if DEVICE_UUID_OVERRIDE:
            if dev_uuid == DEVICE_UUID_OVERRIDE:
                intercom_uuid = dev_uuid
                logger.info("Found matching IntercomV2: %s (deviceName=%s)", dev_uuid, ctrl.get("details", {}).get("deviceName", ""))
                break
        else:
            intercom_uuid = dev_uuid
            logger.info("Found IntercomV2: %s (deviceName=%s)", dev_uuid, ctrl.get("details", {}).get("deviceName", ""))
            break

    if not intercom_uuid:
        if DEVICE_UUID_OVERRIDE:
            raise RuntimeError(f"No IntercomV2 with deviceUuid={DEVICE_UUID_OVERRIDE} found")
        raise RuntimeError("No IntercomV2 control found in Miniserver config")

    proxy_url = f"wss://{ddns_host}/proxy/{intercom_uuid}/"
    logger.info("Discovered WSS proxy URL: %s", proxy_url)
    return proxy_url


def get_proxy_url():
    """Return the WSS proxy URL — from override or via auto-discovery."""
    if PROXY_URL_OVERRIDE:
        logger.info("Using LOXONE_PROXY_URL override: %s", PROXY_URL_OVERRIDE)
        return PROXY_URL_OVERRIDE
    return discover_proxy_url()


async def fetch_device_info(ws):
    turn_user = None
    turn_pass = None

    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        msg = json.loads(raw)
        turn_user, turn_pass = _extract_turn_creds(msg)
        if turn_user:
            logger.info("Received device info (pushed): turnuser=%s", turn_user)
    except asyncio.TimeoutError:
        pass
    except Exception:
        pass

    if not turn_user:
        logger.info("Sending info request for TURN credentials")
        await ws.send(
            json.dumps(
                {"jsonrpc": "2.0", "method": "info", "id": 1, "params": []}
            )
        )
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=10)
                msg = json.loads(raw)
                turn_user, turn_pass = _extract_turn_creds(msg)
                if turn_user:
                    logger.info("Received device info (requested): turnuser=%s", turn_user)
                    break
        except Exception as e:
            logger.warning("Failed to fetch device info: %s", e)

    return turn_user, turn_pass


async def run_session():
    proxy_url = get_proxy_url()

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    auth = base64.b64encode(f"{AUTH_USER}:{AUTH_PASS}".encode()).decode()

    sent = set()
    rpc_id = [3]
    got_answer = False

    async with websockets.connect(
        proxy_url,
        subprotocols=["webrtc-signaling"],
        ssl=ctx,
        additional_headers={"Authorization": f"Basic {auth}"},
        open_timeout=15,
    ) as ws:
        logger.info("WSS connected")

        turn_user, turn_pass = await fetch_device_info(ws)
        if not turn_user or not turn_pass:
            raise RuntimeError("Failed to fetch TURN credentials from Miniserver")
        logger.info("TURN credentials: user=%s pass=%s***", turn_user, turn_pass[:4])

        stun_url = f"stun:{STUN_SERVER}"
        turn_url = f"turn:{TURN_SERVER}"
        logger.info("ICE servers: stun=%s turn=%s (user=%s)", stun_url, turn_url, turn_user)

        config = RTCConfiguration(
            iceServers=[
                RTCIceServer(urls=[stun_url]),
                RTCIceServer(
                    urls=[turn_url],
                    username=turn_user,
                    credential=turn_pass,
                ),
            ]
        )
        pc = RTCPeerConnection(config)

        @pc.on("track")
        def on_track(track):
            logger.info("Track: %s", track.kind)

            async def consume():
                try:
                    while True:
                        await track.recv()
                except Exception:
                    pass

            asyncio.ensure_future(consume())

        @pc.on("connectionstatechange")
        def on_state():
            logger.info("PC: %s", pc.connectionState)

        pc.addTransceiver("video", direction="sendrecv")
        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)

        sdp = pc.localDescription.sdp
        trickle = strip_candidates(sdp)
        if "a=ice-options:trickle" not in trickle:
            trickle = trickle.replace(
                "a=msid-semantic:WMS *",
                "a=ice-options:trickle\r\na=msid-semantic:WMS *",
            )

        await ws.send(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "call",
                    "id": 2,
                    "params": [{"type": "offer", "sdp": trickle}, "new", False],
                }
            )
        )
        logger.info("Offer sent (trickle)")

        while True:
            try:
                current = set(l.strip() for l in pc.localDescription.sdp.split("\n") if "a=candidate:" in l)
                for cl in current - sent:
                    sent.add(cl)
                    cd = cl.replace("a=candidate:", "candidate:")
                    if "typ host" in cd:
                        continue
                    await asyncio.sleep(0.1)
                    await ws.send(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "method": "addIceCandidate",
                                "id": rpc_id[0],
                                "params": [cd, 0, "0"],
                            }
                        )
                    )
                    rpc_id[0] += 1
            except Exception:
                pass

            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
                msg = json.loads(raw)

                if "result" in msg and not got_answer:
                    result = msg["result"]
                    if isinstance(result, dict) and "data" in result:
                        answer_sdp = result["data"]["sdp"]
                        answer_sdp = answer_sdp.replace("SAVPF 101", "SAVPF 99")
                        answer_sdp = answer_sdp.replace("a=rtpmap:101 ", "a=rtpmap:99 ")
                        answer_sdp = answer_sdp.replace("a=fmtp:101 ", "a=fmtp:99 ")
                        await pc.setRemoteDescription(
                            RTCSessionDescription(sdp=answer_sdp, type="answer")
                        )
                        got_answer = True
                        logger.info("Answer received, PT rewritten")

                elif msg.get("method") == "addIceCandidate":
                    params = msg.get("params", [])
                    cs = params[0] if params else ""
                    sm = params[2] if len(params) > 2 else "0"
                    sl = params[1] if len(params) > 1 else 0
                    cand = parse_candidate_line(cs)
                    cand.sdpMid = str(sm)
                    cand.sdpMLineIndex = int(sl)
                    await pc.addIceCandidate(cand)

            except asyncio.TimeoutError:
                pass
            except websockets.exceptions.ConnectionClosed:
                logger.info("WSS closed")
                break

            if pc.connectionState in ("failed", "closed"):
                break

    await pc.close()


async def main():
    logger.info("Loxone Intercom WebRTC Bridge starting")
    while True:
        try:
            await run_session()
            logger.info("Session ended, reconnecting in 5s...")
            await asyncio.sleep(5)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.exception("Session error: %s", e)
            logger.info("Reconnecting in 5s...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
