#!/usr/bin/env python3
"""sip-directbridge — SIP conference bridge for Loxone Intercom Gen.2.

Triggers a SIP conference call between the Intercom and a SIP phone
(FritzBox) when a doorbell event is detected.

Usage:
  python sip_directbridge.py --websocket
  python sip_directbridge.py --webhook
  python sip_directbridge.py --mqtt  (placeholder)
"""

import argparse
import asyncio
import base64
import gzip
import hashlib
import hmac
import json
import logging
import os
import socket
import struct
import threading
import time
import uuid as uuid_mod
from urllib.parse import quote

import aiohttp
import websockets

logger = logging.getLogger("sip-directbridge")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def env(key, default=None, required=False):
    val = os.environ.get(key, default)
    if required and not val:
        raise RuntimeError(f"Environment variable {key} is required")
    return val


class Config:
    def __init__(self):
        self.trigger_mode = None
        # Loxone Miniserver
        self.loxone_ms_ip = env("LOXONE_MINISERVER_IP", "192.168.1.196")
        self.loxone_ms_port = int(env("LOXONE_MINISERVER_PORT", "80"))
        self.loxone_ms_user = env("LOXONE_MINISERVER_USER", "myuser")
        self.loxone_ms_pass = env("LOXONE_MINISERVER_PASS", "mypassword")
        self.loxone_doorbell_uuid = env("LOXONE_DOORBELL_UUID", "")
        # Intercom
        self.intercom_ip = env("LOXONE_INTERCOM_IP", "192.168.1.190")
        self.intercom_sip_uri = env("LOXONE_INTERCOM_SIP_URI", f"sip:smarthome@{self.intercom_ip}")
        # SIP registrar (FritzBox)
        self.sip_registrar = env("SIP_REGISTRAR", "192.168.1.253")
        self.sip_user = env("SIP_USER", "sipuser")
        self.sip_password = env("SIP_PASSWORD", "")
        self.sip_destination = env("SIP_DESTINATION", "**610")
        # Local
        self.local_ip = env("LOCAL_IP", self._detect_local_ip())
        self.local_rtp_port_intercom = int(env("LOCAL_RTP_PORT_INTERCOM", "5004"))
        self.local_rtp_port_fritzbox = int(env("LOCAL_RTP_PORT_FRITZBOX", "5006"))
        # Webhook
        self.webhook_port = int(env("WEBHOOK_PORT", "8080"))
        self.sip_listen_port = int(env("SIP_LISTEN_PORT", "5060"))
        # Misc
        self.keepalive_seconds = int(env("KEEPALIVE_SECONDS", "240"))
        self.call_timeout = int(env("CALL_TIMEOUT", "0"))  # 0 = no timeout

    @staticmethod
    def _detect_local_ip():
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"


# ---------------------------------------------------------------------------
# Loxone WebSocket Protocol
# ---------------------------------------------------------------------------

WS_PATH = "/ws/rfc6455"
KEEPALIVE_RESPONSE = 6
OUT_OF_SERVICE = 5
TEXT_MESSAGE = 0
VALUE_STATE_TABLE = 2
TEXT_STATE_TABLE = 3
COMPRESSED_EVENT = 1  # gzip-compressed event data

DOORBELL_STATE_NAMES = {"bell", "doorbell", "ring", "ringing", "isringing", "call"}
INTERCOM_CONTROL_TYPES = {"Intercom", "IntercomV2", "DoorController", "DoorControllerV2",
                          "DoorStation", "DoorStationV2"}


def _hash_password(password, salt, hash_algorithm):
    payload = f"{password}:{salt}".encode("utf-8")
    algo = hashlib.sha256 if "256" in hash_algorithm.upper() else hashlib.sha1
    return algo(payload).hexdigest().upper()


def _hmac_user_hash(user, password_hash, key_hex, hash_algorithm):
    digest_mod = hashlib.sha256 if "256" in hash_algorithm.upper() else hashlib.sha1
    message = f"{user}:{password_hash}".encode("utf-8")
    return hmac.new(bytes.fromhex(key_hex), message, digest_mod).hexdigest()


def _parse_loxone_header(data):
    if len(data) < 8 or data[0] != 0x03:
        return None
    return data[1], bool(data[2] & 0x01)


def _parse_value_state_table(data):
    values = {}
    offset = 0
    while offset + 24 <= len(data):
        raw_uuid = data[offset:offset + 16]
        state_uuid = str(uuid_mod.UUID(bytes_le=raw_uuid))
        offset += 16
        value = struct.unpack("<d", data[offset:offset + 8])[0]
        offset += 8
        values[state_uuid] = value
    return values


def _parse_text_state_table(data):
    values = {}
    offset = 0
    while offset + 36 <= len(data):
        state_uuid = str(uuid_mod.UUID(bytes_le=data[offset:offset + 16]))
        offset += 16
        offset += 16  # icon UUID
        text_length = struct.unpack("<I", data[offset:offset + 4])[0]
        offset += 4
        padded_length = (text_length + 3) & ~0x03
        raw_text = data[offset:offset + text_length]
        offset += padded_length
        values[state_uuid] = raw_text.decode("utf-8", errors="ignore")
    return values


class LoxoneWSClient:
    def __init__(self, config, on_trigger):
        self.config = config
        self.on_trigger = on_trigger
        self.ws = None
        self._pending_header = None
        self._pending_response = None
        self._command_lock = asyncio.Lock()
        self._keepalive_task = None
        self._reader_task = None
        self._running = False
        self._doorbell_uuid = None
        self._previous_values = {}

    async def start(self):
        self._running = True
        while self._running:
            try:
                await self._connect_and_run()
            except Exception as e:
                logger.error(f"Loxone WS error: {e}")
            if self._running:
                logger.info("Reconnecting in 5s...")
                await asyncio.sleep(5)

    async def stop(self):
        self._running = False
        if self._keepalive_task:
            self._keepalive_task.cancel()
        if self._reader_task:
            self._reader_task.cancel()
        if self.ws and not getattr(self.ws, 'closed', False):
            await self.ws.close()

    async def _connect_and_run(self):
        import ssl
        scheme = "wss"
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        url = f"{scheme}://{self.config.loxone_ms_ip}{WS_PATH}"
        logger.info(f"Connecting to Loxone Miniserver: {url}")

        async with websockets.connect(url, subprotocols=["remotecontrol"],
                                       ssl=ssl_ctx,
                                       open_timeout=10, close_timeout=2) as ws:
            self.ws = ws
            logger.info("WebSocket connected")

            self._reader_task = asyncio.create_task(self._reader_loop(ws))
            self._keepalive_task = asyncio.create_task(self._keepalive_loop(ws))

            try:
                await self._authenticate()
                logger.info("Authenticated with Miniserver")

                await self._send_command("jdev/sps/enablebinstatusupdate")
                logger.info("Binary status updates enabled")

                if not self.config.loxone_doorbell_uuid:
                    await self._discover_doorbell_uuid()
                else:
                    self._doorbell_uuid = self.config.loxone_doorbell_uuid

                logger.info(f"Monitoring doorbell UUID: {self._doorbell_uuid}")

                await self._reader_task
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in WS session: {e}")
                raise
            finally:
                if self._keepalive_task and not self._keepalive_task.done():
                    self._keepalive_task.cancel()

    async def _reader_loop(self, ws):
        try:
            async for message in ws:
                if isinstance(message, bytes):
                    logger.debug(f"WS binary: {len(message)}b hex={message[:20].hex()}")
                    await self._handle_binary(message)
                elif isinstance(message, str):
                    logger.debug(f"WS text: {message[:100]}")
                    self._handle_text(message)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"WS reader stopped: {e}")
            if self._running:
                raise

    async def _keepalive_loop(self, ws):
        try:
            while True:
                await asyncio.sleep(self.config.keepalive_seconds)
                if getattr(ws, 'closed', False):
                    return
                await ws.send("keepalive")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Keepalive failed: {e}")

    async def _handle_binary(self, data):
        if len(data) == 8 and data[0] == 0x03:
            identifier = data[1]
            if identifier == KEEPALIVE_RESPONSE:
                return
            if identifier == OUT_OF_SERVICE:
                logger.error("Miniserver reported OUT OF SERVICE")
                return
            self._pending_header = identifier
            return

        header = self._pending_header
        self._pending_header = None
        if header is None:
            return

        if header == COMPRESSED_EVENT:
            try:
                data = gzip.decompress(data)
            except Exception as e:
                logger.debug(f"gzip decompress failed: {e}")
                return
            # After decompression, data contains nested event(s)
            # Could be value_table, text_table, etc.
            # Try parsing as value state table
            if len(data) >= 24 and (len(data) % 24) == 0:
                changed = _parse_value_state_table(data)
                await self._process_state_changes(changed)
            else:
                logger.debug(f"Decompressed data: {len(data)}b hex={data[:40].hex()}")
            return

        if header == VALUE_STATE_TABLE:
            changed = _parse_value_state_table(data)
            await self._process_state_changes(changed)
        elif header == TEXT_STATE_TABLE:
            _parse_text_state_table(data)
        elif header == TEXT_MESSAGE:
            logger.debug(f"Text message: {data.decode('utf-8', errors='ignore')[:200]}")

    def _handle_text(self, payload):
        if self._pending_response and not self._pending_response.done():
            try:
                root = json.loads(payload).get("LL", {})
                if "code" in root or "Code" in root or "control" in root:
                    self._pending_response.set_result(payload)
                    return
            except (json.JSONDecodeError, AttributeError):
                pass
            self._pending_response.set_result(payload)

    async def _process_state_changes(self, changed):
        for state_uuid, new_value in changed.items():
            old_value = self._previous_values.get(state_uuid)
            self._previous_values[state_uuid] = new_value
            logger.debug(f"State change: {state_uuid} = {new_value} (was {old_value})")

            doorbell_match = False
            if self._doorbell_uuid:
                try:
                    doorbell_match = str(uuid_mod.UUID(self._doorbell_uuid)) == state_uuid
                except (ValueError, AttributeError):
                    doorbell_match = self._doorbell_uuid == state_uuid
            
            if doorbell_match:
                if old_value is not None and old_value <= 0 and new_value > 0:
                    logger.info(f"DOORBELL EVENT! UUID={state_uuid} value={new_value}")
                    await self.on_trigger()
                elif old_value is not None:
                    logger.debug(f"Bell state changed: {old_value} -> {new_value} (no trigger)")
    async def _send_command(self, command, timeout=20):
        async with self._command_lock:
            loop = asyncio.get_running_loop()
            self._pending_response = loop.create_future()
            try:
                await self.ws.send(command)
                return await asyncio.wait_for(self._pending_response, timeout=timeout)
            except asyncio.TimeoutError:
                raise RuntimeError(f"No response for command: {command}")
            finally:
                self._pending_response = None

    async def _authenticate(self):
        user = self.config.loxone_ms_user
        encoded_user = quote(user, safe="")

        resp = await self._send_command(f"jdev/sys/getkey2/{encoded_user}")
        data = json.loads(resp).get("LL", {}).get("value", {})
        if isinstance(data, str):
            data = json.loads(data)
        key = str(data["key"])
        salt = str(data["salt"])
        hash_algo = str(data.get("hashAlg", "SHA256"))

        pw_hash = _hash_password(self.config.loxone_ms_pass, salt, hash_algo)
        final_hash = _hmac_user_hash(user, pw_hash, key, hash_algo)

        client_uuid = str(uuid_mod.uuid4())
        permissions = "4"
        client_info = "sip-directbridge"

        resp = await self._send_command(
            f"jdev/sys/getjwt/{final_hash}/{encoded_user}/{permissions}/"
            f"{quote(client_uuid, safe='-')}/{quote(client_info, safe='')}"
        )
        token_data = json.loads(resp).get("LL", {}).get("value", {})
        if isinstance(token_data, str):
            token_data = json.loads(token_data)
        token = token_data.get("token", "")
        if not token:
            raise RuntimeError("No JWT token received")
        logger.debug(f"Got JWT token, valid until: {token_data.get('validUntil')}")

    async def _discover_doorbell_uuid(self):
        logger.info("Fetching LoxAPP3.json from Miniserver...")
        raw = await self._send_command("data/LoxAPP3.json")
        structure = json.loads(raw)
        if "controls" not in structure:
            raise RuntimeError("Invalid LoxAPP3.json: no 'controls' key")

        for ctrl_uuid, ctrl in structure["controls"].items():
            ctrl_type = ctrl.get("type", "")
            if ctrl_type not in INTERCOM_CONTROL_TYPES:
                continue
            states = ctrl.get("states", {})
            for state_name, state_uuid in states.items():
                if state_name.lower() in DOORBELL_STATE_NAMES:
                    self._doorbell_uuid = state_uuid
                    logger.info(f"Discovered doorbell: control={ctrl_uuid} "
                                f"type={ctrl_type} state={state_name} uuid={state_uuid}")
                    return

        raise RuntimeError("No doorbell state found in LoxAPP3.json. "
                           "Set LOXONE_DOORBELL_UUID manually.")


# ---------------------------------------------------------------------------
# Webhook Trigger
# ---------------------------------------------------------------------------

class WebhookTrigger:
    def __init__(self, config, on_trigger):
        self.config = config
        self.on_trigger = on_trigger
        self.runner = None

    async def start(self):
        from aiohttp import web
        app = web.Application()

        async def handle_trigger(request):
            logger.info("Webhook trigger received!")
            asyncio.create_task(self.on_trigger())
            return web.json_response({"status": "triggered"})

        async def handle_health(request):
            return web.json_response({"status": "ok"})

        app.router.add_post("/trigger", handle_trigger)
        app.router.add_get("/health", handle_health)

        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "0.0.0.0", self.config.webhook_port)
        await site.start()
        logger.info(f"Webhook listener on port {self.config.webhook_port}")

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()


# ---------------------------------------------------------------------------
# SIP Call + RTP Bridge
# ---------------------------------------------------------------------------

def pcm_to_mulaw(sample):
    bias = 0x84
    clip = 32635
    sign = 0x80 if sample < 0 else 0x00
    sample = abs(sample)
    if sample > clip:
        sample = clip
    sample += bias
    exp = 0
    while sample > (0x3F << (exp + 3)):
        exp += 1
    mantissa = (sample >> (exp + 3)) & 0x0F
    return (~(sign | (exp << 4) | mantissa)) & 0xFF


def build_rtp(seq, ts, ssrc, payload, pt=0):
    return struct.pack("!BBHII", 0x80, 0x80 | pt, seq & 0xFFFF,
                       ts & 0xFFFFFFFF, ssrc) + payload


def parse_rtp(data):
    if len(data) < 12:
        return None
    b0, b1, seq, ts, ssrc = struct.unpack("!BBHII", data[:12])
    pt = b1 & 0x7F
    cc = b0 & 0x0F
    offset = 12 + cc * 4
    return pt, seq, ts, ssrc, data[offset:]


class SIPLeg:
    def __init__(self, name, dst_ip, dst_port, local_ip, rtp_port,
                 sip_uri, auth_user=None, auth_pass=None, realm_hint=None):
        self.name = name
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.local_ip = local_ip
        self.rtp_port = rtp_port
        self.sip_uri = sip_uri
        self.auth_user = auth_user
        self.auth_pass = auth_pass
        self.realm_hint = realm_hint or ""
        self.sip_sock = None
        self.rtp_sock = None
        self.remote_rtp_port = None
        self.contact_uri = None
        self.to_tag = ""
        self.from_tag = hashlib.md5(name.encode()).hexdigest()[:8]
        self.call_id = hashlib.md5(f"{name}-{time.time()}".encode()).hexdigest()[:16]
        self.cseq = 1
        self.ssrc = zlib_random_ssrc()
        self.rtp_seq = 0
        self.rtp_ts = 0
        self._buf = b""
        self._bye_received = threading.Event()
        self._sip_reader_thread = None

    async def connect(self):
        self.sip_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sip_sock.settimeout(15)
        self.sip_sock.connect((self.dst_ip, self.dst_port))

        self.rtp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.rtp_sock.bind(("0.0.0.0", self.rtp_port))
        self.rtp_sock.setblocking(False)

    def _send_sip(self, msg):
        self.sip_sock.sendall(msg)

    def _recv_sip(self, timeout=10):
        deadline = time.time() + timeout
        while True:
            if b"\r\n\r\n" in self._buf:
                header, sep, rest = self._buf.partition(b"\r\n\r\n")
                cl = 0
                for line in header.split(b"\r\n"):
                    if line.lower().startswith(b"content-length:"):
                        cl = int(line.split(b":")[1].strip())
                if len(rest) >= cl:
                    msg = header + sep + rest[:cl]
                    self._buf = rest[cl:]
                    return msg
            remaining = deadline - time.time()
            if remaining <= 0:
                return self._buf if self._buf else b""
            self.sip_sock.settimeout(min(remaining, 5))
            try:
                chunk = self.sip_sock.recv(4096)
                if not chunk:
                    return self._buf
                self._buf += chunk
            except socket.timeout:
                continue

    def _parse_status_headers(self, data):
        text = data.decode(errors="replace")
        lines = text.split("\r\n")
        status = int(lines[0].split()[1]) if len(lines) > 1 and len(lines[0].split()) > 1 else 0
        headers = {}
        body = ""
        in_body = False
        for line in lines[1:]:
            if in_body:
                body += line + "\r\n"
            elif line == "":
                in_body = True
            elif ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip()] = v.strip()
        return status, headers, body

    def _digest_auth(self, realm, nonce, method, uri):
        ha1 = hashlib.md5(f"{self.auth_user}:{realm}:{self.auth_pass}".encode()).hexdigest()
        ha2 = hashlib.md5(f"{method}:{uri}".encode()).hexdigest()
        return hashlib.md5(f"{ha1}:{nonce}:{ha2}".encode()).hexdigest()

    def _wait_for_final_response(self, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            resp = self._recv_sip(timeout=min(deadline - time.time(), 15))
            if not resp:
                continue
            status, headers, body = self._parse_status_headers(resp)
            if status == 0:
                continue
            if status < 200:
                logger.debug(f"{self.name}: Provisional response {status}")
                continue
            return status, headers, body
        raise RuntimeError(f"{self.name}: Timeout waiting for final response")

    def invite(self):
        sdp = (
            "v=0\r\n"
            f"o=bridge {self.cseq} {self.cseq} IN IP4 {self.local_ip}\r\n"
            "s=bridge\r\n"
            f"c=IN IP4 {self.local_ip}\r\n"
            "t=0 0\r\n"
            f"m=audio {self.rtp_port} RTP/AVP 0 8\r\n"
            "a=rtpmap:0 PCMU/8000\r\n"
            "a=rtpmap:8 PCMA/8000\r\n"
            "a=sendrecv\r\n"
            "a=ptime:20\r\n"
        ).encode()

        def build_invite(auth_header=""):
            msg = (
                f"INVITE {self.sip_uri} SIP/2.0\r\n"
                f"Via: SIP/2.0/TCP {self.local_ip}:5060;branch=z9hG4bK{self.name}{self.cseq};rport\r\n"
                f"From: <sip:{self.auth_user or 'bridge'}@{self.local_ip}>;tag={self.from_tag}\r\n"
                f"To: <{self.sip_uri}>\r\n"
                f"Call-ID: {self.call_id}@{self.local_ip}\r\n"
                f"CSeq: {self.cseq} INVITE\r\n"
                "Max-Forwards: 70\r\n"
                "User-Agent: sip-directbridge\r\n"
                f"Contact: <sip:{self.auth_user or 'bridge'}@{self.local_ip}:5060>\r\n"
                f"{auth_header}"
                f"Content-Type: application/sdp\r\n"
                f"Content-Length: {len(sdp)}\r\n"
                "\r\n"
            ).encode() + sdp
            return msg

        self._send_sip(build_invite())
        status, headers, body = self._wait_for_final_response(timeout=60)

        if status == 401 and self.auth_pass:
            realm = ""
            nonce = ""
            auth_line = headers.get("WWW-Authenticate", "")
            for kv in auth_line[len("Digest "):].split(","):
                k, v = kv.strip().split("=", 1)
                if k == "realm":
                    realm = v.strip('"')
                elif k == "nonce":
                    nonce = v.strip('"')

            self.cseq += 1
            response = self._digest_auth(realm, nonce, "INVITE", self.sip_uri)
            auth_hdr = (
                f'Authorization: Digest username="{self.auth_user}",'
                f'realm="{realm}",nonce="{nonce}",'
                f'uri="{self.sip_uri}",response="{response}",algorithm=MD5\r\n'
            )
            self._send_sip(build_invite(auth_hdr))
            status, headers, body = self._wait_for_final_response(timeout=60)

        if status != 200:
            raise RuntimeError(f"{self.name}: INVITE failed with status {status}")

        for line in body.split("\r\n"):
            if line.startswith("m=audio"):
                self.remote_rtp_port = int(line.split()[1])
        if ">;" in headers.get("Contact", ""):
            contact = headers["Contact"]
            self.contact_uri = contact[contact.index("<") + 1:contact.index(">")]
        else:
            self.contact_uri = self.sip_uri

        to_hdr = headers.get("To", "")
        if ";tag=" in to_hdr:
            self.to_tag = to_hdr.split(";tag=")[1].strip()

        self.ack()
        self.start_sip_reader()
        logger.info(f"{self.name}: Call established, RTP port={self.remote_rtp_port}")
        return True

    def start_sip_reader(self):
        def reader():
            try:
                self.sip_sock.settimeout(None)
                buf = b""
                while not self._bye_received.is_set():
                    try:
                        chunk = self.sip_sock.recv(4096)
                        if not chunk:
                            logger.info(f"{self.name}: SIP connection closed by peer")
                            self._bye_received.set()
                            break
                        buf += chunk
                        while b"\r\n\r\n" in buf:
                            msg, _, buf = buf.partition(b"\r\n\r\n")
                            text = msg.decode(errors="replace")
                            if "BYE" in text.split("\r\n")[0]:
                                logger.info(f"{self.name}: received BYE")
                                self._bye_received.set()
                                resp = (
                                    f"SIP/2.0 200 OK\r\n"
                                    f"Via: {text.split('Via: ')[1].split(chr(13))[0]}\r\n"
                                    f"From: {text.split('From: ')[1].split(chr(13))[0]}\r\n"
                                    f"To: {text.split('To: ')[1].split(chr(13))[0]}\r\n"
                                    f"Call-ID: {text.split('Call-ID: ')[1].split(chr(13))[0]}\r\n"
                                    f"CSeq: {text.split('CSeq: ')[1].split(chr(13))[0]}\r\n"
                                    "Content-Length: 0\r\n"
                                    "\r\n"
                                ).encode()
                                try:
                                    self.sip_sock.sendall(resp)
                                    logger.info(f"{self.name}: sent 200 OK for BYE")
                                except Exception:
                                    pass
                                break
                    except (OSError, socket.error):
                        if not self._bye_received.is_set():
                            logger.info(f"{self.name}: SIP reader disconnected")
                            self._bye_received.set()
                        break
            except Exception as e:
                logger.debug(f"{self.name}: SIP reader error: {e}")
                self._bye_received.set()

        self._sip_reader_thread = threading.Thread(target=reader, daemon=True)
        self._sip_reader_thread.start()

    def ack(self):
        msg = (
            f"ACK {self.contact_uri} SIP/2.0\r\n"
            f"Via: SIP/2.0/TCP {self.local_ip}:5060;branch=z9hG4bKack{self.name}{self.cseq};rport\r\n"
            f"From: <sip:{self.auth_user or 'bridge'}@{self.local_ip}>;tag={self.from_tag}\r\n"
            f"To: <{self.sip_uri}>;tag={self.to_tag}\r\n"
            f"Call-ID: {self.call_id}@{self.local_ip}\r\n"
            f"CSeq: {self.cseq} ACK\r\n"
            "Max-Forwards: 70\r\n"
            "Content-Length: 0\r\n"
            "\r\n"
        ).encode()
        self._send_sip(msg)

    def bye(self):
        self.cseq += 1
        msg = (
            f"BYE {self.contact_uri} SIP/2.0\r\n"
            f"Via: SIP/2.0/TCP {self.local_ip}:5060;branch=z9hG4bKbye{self.name}{self.cseq};rport\r\n"
            f"From: <sip:{self.auth_user or 'bridge'}@{self.local_ip}>;tag={self.from_tag}\r\n"
            f"To: <{self.sip_uri}>;tag={self.to_tag}\r\n"
            f"Call-ID: {self.call_id}@{self.local_ip}\r\n"
            f"CSeq: {self.cseq} BYE\r\n"
            "Max-Forwards: 70\r\n"
            "Content-Length: 0\r\n"
            "\r\n"
        ).encode()
        try:
            self._send_sip(msg)
            self._recv_sip(timeout=3)
        except Exception:
            pass

    def send_rtp(self, payload, pt=0):
        pkt = build_rtp(self.rtp_seq, self.rtp_ts, self.ssrc, payload, pt)
        self.rtp_sock.sendto(pkt, (self.dst_ip, self.remote_rtp_port))
        self.rtp_seq += 1
        self.rtp_ts += 160

    def send_silence(self):
        self.send_rtp(b'\xff' * 160)

    def close(self):
        try:
            self.bye()
        except Exception:
            pass
        if self.sip_sock:
            self.sip_sock.close()
        if self.rtp_sock:
            self.rtp_sock.close()


def zlib_random_ssrc():
    import random
    return random.randint(1, 0xFFFFFFFF)


class ConferenceBridge:
    def __init__(self, config):
        self.config = config
        self.active = False

    async def run(self):
        self.active = True
        logger.info("Starting SIP conference...")

        leg_intercom = SIPLeg(
            "intercom",
            self.config.intercom_ip, 5060,
            self.config.local_ip, self.config.local_rtp_port_intercom,
            f"sip:smarthome@{self.config.intercom_ip}",
        )

        leg_fritzbox = SIPLeg(
            "fritzbox",
            self.config.sip_registrar, 5060,
            self.config.local_ip, self.config.local_rtp_port_fritzbox,
            f"sip:{self.config.sip_destination}@{self.config.sip_registrar}",
            auth_user=self.config.sip_user,
            auth_pass=self.config.sip_password,
            realm_hint="fritz.box",
        )

        try:
            await leg_intercom.connect()
            leg_intercom.invite()
        except Exception as e:
            logger.error(f"Intercom call failed: {e}")
            leg_intercom.close()
            return

        try:
            await leg_fritzbox.connect()
            leg_fritzbox.invite()
        except Exception as e:
            logger.error(f"FritzBox call failed: {e}")
            leg_intercom.close()
            leg_fritzbox.close()
            return

        logger.info("Both legs established, bridging audio...")

        await self._bridge_rtp(leg_intercom, leg_fritzbox)

        logger.info("Conference ended")
        leg_intercom.close()
        leg_fritzbox.close()
        self.active = False

    async def _bridge_rtp(self, leg_a, leg_b):
        loop = asyncio.get_running_loop()
        silence_interval = 0.02
        last_silence = time.time()
        deadline = time.time() + self.config.call_timeout if self.config.call_timeout > 0 else None

        while self.active and (deadline is None or time.time() < deadline):
            readable = [leg_a.rtp_sock, leg_b.rtp_sock]
            writable = []
            exceptional = [(leg_a.rtp_sock, leg_a.rtp_sock),
                          (leg_b.rtp_sock, leg_b.rtp_sock)]

            try:
                r, _, _ = await loop.run_in_executor(
                    None,
                    lambda: select_fn(readable, [], [], 0.05)
                )
            except Exception:
                break

            for sock in r:
                try:
                    data, addr = sock.recvfrom(4096)
                except BlockingIOError:
                    continue

                parsed = parse_rtp(data)
                if not parsed:
                    continue
                pt, seq, ts, ssrc, payload = parsed

                if sock == leg_a.rtp_sock:
                    leg_b.send_rtp(payload, pt)
                else:
                    leg_a.send_rtp(payload, pt)

            now = time.time()
            if now - last_silence >= silence_interval:
                leg_a.send_silence()
                leg_b.send_silence()
                last_silence = now

            if leg_a._bye_received.is_set() or leg_b._bye_received.is_set():
                break
            if hasattr(self, 'bye_listener') and self.bye_listener.bye_received.is_set():
                logger.info("BYE received via listener")
                break


def select_fn(r, w, x, timeout):
    import select
    return select.select(r, w, x, timeout)


def parse_sip_message(data):
    text = data.decode(errors="replace")
    lines = text.split("\r\n")
    first_line = lines[0] if lines else ""
    headers = {}
    body = ""
    in_body = False
    for line in lines[1:]:
        if in_body:
            body += line + "\r\n"
        elif line == "":
            in_body = True
        elif ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip()] = v.strip()
    return first_line, headers, body


def sip_response(status_code, reason, headers_dict, body=b"", via="", extra=""):
    hdrs = f"SIP/2.0 {status_code} {reason}\r\n"
    if via:
        hdrs += f"Via: {via}\r\n"
    for k, v in headers_dict.items():
        hdrs += f"{k}: {v}\r\n"
    if extra:
        hdrs += extra
    hdrs += f"Content-Length: {len(body)}\r\n"
    hdrs += "\r\n"
    return hdrs.encode() + body


# ---------------------------------------------------------------------------
# BYE Listener (listens on 5060 TCP+UDP for incoming BYE from FritzBox)
# ---------------------------------------------------------------------------

class ByeListener:
    def __init__(self, config):
        self.config = config
        self._running = False
        self.tcp_sock = None
        self.udp_sock = None
        self._threads = []
        self.bye_received = threading.Event()

    def start(self):
        self._running = True
        port = self.config.sip_listen_port if hasattr(self.config, 'sip_listen_port') else 5060

        self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_sock.bind(("0.0.0.0", port))
        self.tcp_sock.listen(5)
        self.tcp_sock.settimeout(1)
        t = threading.Thread(target=self._tcp_loop, daemon=True)
        t.start()
        self._threads.append(t)

        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_sock.bind(("0.0.0.0", port))
        self.udp_sock.settimeout(1)
        t2 = threading.Thread(target=self._udp_loop, daemon=True)
        t2.start()
        self._threads.append(t2)

        logger.info(f"BYE listener on port {port} (TCP+UDP)")

    def stop(self):
        self._running = False
        if self.tcp_sock:
            self.tcp_sock.close()
        if self.udp_sock:
            self.udp_sock.close()

    def _tcp_loop(self):
        while self._running:
            try:
                conn, addr = self.tcp_sock.accept()
                t = threading.Thread(target=self._handle_tcp, args=(conn, addr), daemon=True)
                t.start()
            except socket.timeout:
                continue
            except OSError:
                break

    def _handle_tcp(self, conn, addr):
        try:
            conn.settimeout(10)
            data = conn.recv(4096)
            if data:
                self._process(data, conn, addr, is_udp=False)
        except Exception:
            pass
        finally:
            conn.close()

    def _udp_loop(self):
        while self._running:
            try:
                data, addr = self.udp_sock.recvfrom(4096)
                if data:
                    self._process(data, None, addr, is_udp=True)
            except socket.timeout:
                continue
            except OSError:
                break

    def _process(self, data, conn, addr, is_udp=False):
        first_line, headers, body = parse_sip_message(data)
        method = first_line.split()[0] if first_line else ""
        via = headers.get("Via", "")
        from_hdr = headers.get("From", "")
        to_hdr = headers.get("To", "")
        call_id = headers.get("Call-ID", "")
        cseq = headers.get("CSeq", "")

        def send_resp(resp):
            if is_udp and self.udp_sock:
                self.udp_sock.sendto(resp, addr)
            elif conn:
                conn.sendall(resp)

        if method == "BYE":
            logger.info(f"BYE received from {addr} ({'UDP' if is_udp else 'TCP'})")
            self.bye_received.set()
            resp = sip_response(200, "OK", {
                "From": from_hdr, "To": to_hdr,
                "Call-ID": call_id, "CSeq": cseq,
            }, via=via)
            send_resp(resp)
        elif method == "OPTIONS":
            resp = sip_response(200, "OK", {
                "From": from_hdr, "To": to_hdr,
                "Call-ID": call_id, "CSeq": cseq,
            }, via=via, extra="Allow: INVITE,ACK,BYE,CANCEL,OPTIONS\r\n")
            send_resp(resp)
        else:
            logger.debug(f"BYE listener: {method} from {addr}")
            resp = sip_response(200, "OK", {
                "From": from_hdr, "To": to_hdr,
                "Call-ID": call_id, "CSeq": cseq,
            }, via=via)
            send_resp(resp)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main_async(config):
    bye_listener = ByeListener(config)
    bye_listener.start()

    bridge = ConferenceBridge(config)
    bridge.bye_listener = bye_listener
    trigger_busy = asyncio.Event()
    trigger_busy.clear()

    async def on_trigger():
        if trigger_busy.is_set():
            logger.warning("Trigger received but conference already active, ignoring")
            return
        trigger_busy.set()
        bye_listener.bye_received.clear()
        try:
            await bridge.run()
        except Exception as e:
            logger.error(f"Conference error: {e}")
        finally:
            trigger_busy.clear()

    if config.trigger_mode == "websocket":
        trigger = LoxoneWSClient(config, on_trigger)
    elif config.trigger_mode == "webhook":
        trigger = WebhookTrigger(config, on_trigger)
    else:
        logger.error(f"Unknown trigger mode: {config.trigger_mode}")
        return

    logger.info(f"Starting in {config.trigger_mode} mode...")
    await trigger.start()

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        await trigger.stop()


def main():
    parser = argparse.ArgumentParser(description="sip-directbridge")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--websocket", action="store_true",
                       help="Listen for doorbell events via Loxone Miniserver WebSocket")
    group.add_argument("--webhook", action="store_true",
                       help="Listen for HTTP webhook triggers on WEBHOOK_PORT")
    group.add_argument("--mqtt", action="store_true",
                       help="Listen for MQTT triggers (placeholder)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config = Config()
    if args.websocket:
        config.trigger_mode = "websocket"
    elif args.webhook:
        config.trigger_mode = "webhook"
    elif args.mqtt:
        config.trigger_mode = "mqtt"

    asyncio.run(main_async(config))


if __name__ == "__main__":
    main()
