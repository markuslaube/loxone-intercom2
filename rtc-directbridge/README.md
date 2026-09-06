# Loxone Intercom2 Direct Bridge

Docker container that bridges a Loxone Intercom Gen.2 doorbell camera into
go2rtc / Home Assistant via WebRTC. Connects directly to the Intercom —
no Miniserver required.

```
Loxone Intercom Gen.2
    |
    | WS (direct, plain WebSocket)
    v
bridge.py (aiortc WebRTC peer)
    |
    | raw H.264 (pipe)
    v
ffmpeg (-c copy, no transcode)
    |
    | RTSP publish
    v
mediamtx (:8554, inside container)
    |
    | RTSP subscribe (overlay DNS)
    v
go2rtc (auto-registered via REST API)
    |
    v
Home Assistant / Frigate / Browser
```

## What it does

- Connects directly to the Loxone Intercom Gen.2 via plain WebSocket (`ws://`)
- No Miniserver, no TLS, no reverse proxy involved
- Receives H.264 video and repackages it as RTSP
- Auto-registers the stream with go2rtc via REST API — no go2rtc config edits needed
- TURN credentials are fetched dynamically from the Intercom on each connect

## Prerequisites

- **Loxone Intercom Gen.2** reachable on your network
- **go2rtc** running and reachable from the container (e.g. in the same Docker network)

## Quick start

1. Build the image:

```bash
docker build -t loxone-intercom-directbridge:latest .
```

2. Create a `compose.yml` (see `compose.yml` template in this repo).

3. Set the Intercom IP and deploy:

```bash
docker compose up -d
```

4. Open go2rtc's web interface to view the stream:

```
http://<your-go2rtc-host>:1984/stream.html?src=loxone_intercom
```

## Configuration

Only one environment variable is required:

| Variable | Example | Description |
|----------|---------|-------------|
| `LOXONE_INTERCOM_IP` | `192.168.1.190` | Intercom IP address |

Everything else has sensible defaults:

| Variable | Default | Description |
|----------|---------|-------------|
| `LOXONE_TURN_SERVER` | `stun.loxonecloud.com:3478` | TURN server |
| `LOXONE_STUN_SERVER` | `stun.l.google.com:19302` | STUN server |
| `LOXONE_RTSP_HOST` | `$(hostname)` | Hostname go2rtc uses to reach this container |
| `LOXONE_RTSP_PORT` | `8554` | Internal RTSP port |
| `LOXONE_STREAM_NAME` | `loxone_intercom` | Stream name registered in go2rtc |
| `GO2RTC_API_URL` | `http://go2rtc:1984` | go2rtc REST API base URL |
| `LOXONE_GO2RTC_RECONCILE_SECONDS` | `20` | Re-registration interval |
| `LOXONE_LOG_LEVEL` | `INFO` | Log level (`INFO`, `DEBUG`, `WARNING`) |

## How it works

The bridge opens a plain WebSocket connection to the Intercom and negotiates
a WebRTC session. Video arrives as RTP/H.264, gets depacketized, and is
piped through ffmpeg into mediamtx for RTSP publishing. go2rtc picks up the
RTSP feed via automatic REST API registration.

Since the bridge connects via `ws://` directly to the Intercom IP, no
Miniserver, DDNS hostname, or DNS rebinding configuration is needed.

## Building from source

```bash
docker build -t loxone-intercom-directbridge:latest .
```

## Status

**Beta.** This is an unofficial community project. It is not affiliated with
or endorsed by Loxone Electronics GmbH. The WebRTC signaling protocol was
reverse-engineered from the Intercom's web interface — it may break with
future firmware updates.

Currently **video only**. Bidirectional audio is not yet implemented.

## Difference to rtc-bridge

The sibling `rtc-bridge/` connects through the Miniserver's WSS reverse proxy
and auto-discovers the WSS URL from the Miniserver. This `rtc-directbridge/`
variant connects directly to the Intercom via plain `ws://`, eliminating the
Miniserver dependency entirely.

## Credits

- **[go2rtc](https://github.com/AlexxIT/go2rtc)** by [@AlexxIT](https://github.com/AlexxIT) --
  the ultimate camera streaming application this bridge feeds into.
- **[mediamtx](https://github.com/bluenviron/mediamtx)** by [@bluenviron](https://github.com/bluenviron) --
  the RTSP server running inside the container.
- **[aiortc](https://github.com/aiortc/aiortc)** -- WebRTC implementation for Python.
- The go2rtc auto-registration pattern is inspired by the
  [Mammotion RTSP Bridge](https://github.com/Bleialf/mammotion-rtsp-bridge).

## License

Apache-2.0

## Author

Markus Laube -- [GitHub](https://github.com/markuslaube)

Co-authored by AI [GLM-5.2](https://huggingface.co/zai-org/GLM-5.2/)
