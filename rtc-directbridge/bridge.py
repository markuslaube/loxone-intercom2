#!/usr/bin/env python3
"""Loxone Intercom Gen.2 WebRTC Bridge

Receives video from a Loxone Intercom Gen.2 via WebRTC and outputs
raw H.264 Annex-B to stdout for consumption by go2rtc (exec source).

Architecture:
  Intercom → WS (direct, no auth) → WebRTC → RTP → H.264 depacketize → stdout

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
  LOXONE_INTERCOM_IP     Intercom IP address (e.g. 192.168.1.190)
  LOXONE_TURN_SERVER     TURN server address (host:port)
  LOXONE_STUN_SERVER     STUN server address (host:port)
  LOXONE_LOG_LEVEL       Log level (INFO, DEBUG, WARNING)

The bridge connects directly to the Intercom via plain WS — no Miniserver,
no auth, no TLS required. TURN credentials are fetched dynamically from
the Intercom via the JSON-RPC `info` method on each WS connect.
"""
import asyncio
import json
import logging
import os
import struct
import sys
import time

import websockets
from aiortc import (
    RTCConfiguration,
    RTCIceCandidate,
    RTCIceServer,
    RTCPeerConnection,
    RTCSessionDescription,
)
from aiortc.rtcdtlstransport import RTCDtlsTransport

INTERCOM_IP = os.environ.get("LOXONE_INTERCOM_IP", "")
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

_last_rtp_time = [0.0]


def write_stdout(data):
    try:
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()
    except BrokenPipeError:
        logger.error("stdout pipe broken (ffmpeg dead?), exiting")
        os._exit(1)
    except OSError:
        logger.error("stdout write failed, exiting")
        os._exit(1)


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
        _last_rtp_time[0] = time.monotonic()
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


def get_ws_url():
    """Construct the WS URL for direct Intercom connection."""
    if not INTERCOM_IP:
        raise RuntimeError("LOXONE_INTERCOM_IP not set")
    return f"ws://{INTERCOM_IP}/"


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
    _last_rtp_time[0] = 0.0
    ws_url = get_ws_url()

    sent = set()
    rpc_id = [3]
    got_answer = False

    async with websockets.connect(
        ws_url,
        subprotocols=["webrtc-signaling"],
        open_timeout=15,
    ) as ws:
        logger.info("WS connected")

        turn_user, turn_pass = await fetch_device_info(ws)
        if not turn_user or not turn_pass:
            raise RuntimeError("Failed to fetch TURN credentials from Intercom")
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
                logger.info("WS closed")
                break

            if pc.connectionState in ("failed", "closed"):
                break

            if got_answer and _last_rtp_time[0] > 0:
                silence = time.monotonic() - _last_rtp_time[0]
                if silence > 10:
                    logger.warning("No RTP for %.0fs, ending session", silence)
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
