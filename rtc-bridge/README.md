# Loxone Intercom2 Bridge

Docker container that bridges a Loxone Intercom Gen.2 doorbell camera into
go2rtc / Home Assistant via WebRTC. No hardcoded credentials — everything
is auto-discovered from the Miniserver.

```
Loxone Intercom Gen.2
    │
    │ WSS (via Miniserver reverse proxy)
    ▼
bridge.py (aiortc WebRTC peer)
    │
    │ raw H.264 (pipe)
    ▼
ffmpeg (-c copy, no transcode)
    │
    │ RTSP publish
    ▼
mediamtx (:8554, inside container)
    │
    │ RTSP subscribe (overlay DNS)
    ▼
go2rtc (auto-registered via REST API)
    │
    ▼
Home Assistant / Frigate / Browser
```

## What it does

- Connects to the Loxone Intercom Gen.2 via WebRTC (through the Miniserver WSS proxy)
- Receives H.264 video and republishes it as RTSP
- Auto-registers the stream with go2rtc via REST API — no go2rtc config edits needed
- Auto-discovers the WSS URL and TURN credentials from the Miniserver

## Prerequisites

- **Loxone Miniserver** (any generation that supports IntercomV2)
- **Loxone Intercom Gen.2** configured in Loxone Config
- **go2rtc** running and reachable from the container (e.g. in the same Docker network)
- **Router DNS**: Disable DNS rebinding protection for `*.dyndns.loxonecloud.com`
  (otherwise the Miniserver's DDNS hostname won't resolve to its local IP)

## Quick start

1. Pull the image:

```bash
docker pull laubi/loxone-intercom-bridge:latest
```

2. Create a `compose.yml` (see `compose.yml` template in this repo).

3. Set your Miniserver credentials and deploy:

```bash
docker compose up -d
```

4. Open go2rtc's web interface to view the stream:

```
http://<your-go2rtc-host>:1984/stream.html?src=loxone_intercom
```

## Configuration

Only three environment variables are required:

| Variable | Example | Description |
|----------|---------|-------------|
| `LOXONE_MINISERVER_IP` | `192.168.1.100` | Miniserver IP address |
| `LOXONE_MINISERVER_USER` | _(none)_ | Miniserver username (needs rights on the Intercom) |
| `LOXONE_MINISERVER_PASS` | _(none)_ | Miniserver password |

Everything else is auto-discovered or has sensible defaults:

| Variable | Default | Description |
|----------|---------|-------------|
| `LOXONE_DEVICE_UUID` | _(auto)_ | Specific Intercom device UUID (multi-intercom setups) |
| `LOXONE_PROXY_URL` | _(auto)_ | Full WSS URL override (skips auto-discovery) |
| `LOXONE_TURN_SERVER` | `stun.loxonecloud.com:3478` | TURN server |
| `LOXONE_STUN_SERVER` | `stun.l.google.com:19302` | STUN server |
| `LOXONE_RTSP_HOST` | `$(hostname)` | Hostname go2rtc uses to reach this container |
| `LOXONE_RTSP_PORT` | `8554` | Internal RTSP port |
| `LOXONE_STREAM_NAME` | `loxone_intercom` | Stream name registered in go2rtc |
| `GO2RTC_API_URL` | `http://go2rtc:1984` | go2rtc REST API base URL |
| `LOXONE_GO2RTC_RECONCILE_SECONDS` | `20` | Re-registration interval |
| `LOXONE_LOG_LEVEL` | `INFO` | Log level (`INFO`, `DEBUG`, `WARNING`) |

## Auto-Discovery

The bridge auto-discovers the WSS proxy URL from the Miniserver:

1. `GET http://<miniserver-ip>/jdev/cfg/apiKey` → Miniserver MAC + IP → DDNS hostname
2. `GET http://<miniserver-ip>/data/LoxAPP3.json` (authenticated) → IntercomV2 device UUID
3. Constructs: `wss://<ddns-hostname>/proxy/<device-uuid>/`

TURN credentials (username + password) are fetched dynamically from the Miniserver
via the JSON-RPC `info` method on each WSS connect. No static credentials needed.

## Building from source

```bash
docker build -t laubi/loxone-intercom-bridge:latest .
```

## Status

**Beta.** This is an unofficial community project. It is not affiliated with
or endorsed by Loxone Electronics GmbH. The WebRTC signaling protocol was
reverse-engineered from the Intercom's web interface — it may break with
future firmware updates.

Currently **video only**. Bidirectional audio (SIP/PJSUA2) is not implemented.

## Credits

- **[go2rtc](https://github.com/AlexxIT/go2rtc)** by [@AlexxIT](https://github.com/AlexxIT) —
  the ultimate camera streaming application this bridge feeds into.
- **[mediamtx](https://github.com/bluenviron/mediamtx)** by [@bluenviron](https://github.com/bluenviron) —
  the RTSP server running inside the container.
- **[aiortc](https://github.com/aiortc/aiortc)** — WebRTC implementation for Python.
- The go2rtc auto-registration pattern is inspired by the
  [Mammotion RTSP Bridge](https://github.com/Bleialf/mammotion-rtsp-bridge).

## License

Apache-2.0

## Author

Markus Laube — [GitHub](https://github.com/markuslaube)

Co-authored by AI [GLM-5.2](https://huggingface.co/zai-org/GLM-5.2/)
