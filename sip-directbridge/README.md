![AI Contribution](https://raw.githubusercontent.com/Essk/ai-contribution-level/main/badges/level-3.svg)

# sip-directbridge

Bridges the Intercom's SIP audio to any SIP account — makes your phone
ring when someone's at the door and lets you talk to the visitor.
Supports both directions: doorbell-triggered outbound calls and
inbound calls to reach the Intercom directly.

## How it works

```
Direction 1: Doorbell → Phone (outbound)
─────────────────────────────────────────────
Doorbell Event (WebSocket / Webhook)
    │
    ▼
sip-directbridge
    ├── SIP INVITE → Intercom (TCP 5060, no auth)      [visitor audio]
    ├── SIP INVITE → FritzBox / SIP phone              [your phone rings]
    ├── RTP Bridge: Intercom ⟷ FritzBox               [audio forwarding]
    ├── SIP Listener: 5060 TCP+UDP                     [detects hangup]
    └── Self = muted (PCMU silence to both)

Direction 2: Phone → Intercom (inbound, ACCEPT_CALL=true)
─────────────────────────────────────────────
Someone dials the bridge's SIP extension (**623)
    │
    ▼
sip-directbridge
    ├── SIP REGISTER → FritzBox (periodic, keeps extension reachable)
    ├── SIP INVITE ← FritzBox (incoming call)
    ├── 200 OK → FritzBox (auto-answer)
    ├── SIP INVITE → Intercom (TCP 5060, no auth)
    ├── RTP Bridge: Caller ⟷ Intercom
    └── BYE from either side → clean teardown
```

Pick up the phone → two-way audio runs between you and the visitor
through the bridge. Hang up → BYE is received → bridge tears down both
legs and waits for the next event. Both directions work simultaneously.

## Trigger Modes

The bridge supports three trigger modes (mutually exclusive):

| Flag | Description |
|------|-------------|
| `--websocket` | Connects to the Loxone Miniserver via WebSocket (`wss://`), authenticates, auto-discovers the doorbell UUID from `LoxAPP3.json`, and monitors binary state events. **Recommended for production.** |
| `--webhook` | Starts an HTTP server (`POST /trigger`) that can be called from Home Assistant automations, Node-RED, or any HTTP client. |
| `--mqtt` | Placeholder for future MQTT trigger support. |

## Requirements

- Loxone Intercom Gen.2 (reachable via TCP 5060, no auth needed)
- Loxone Miniserver (for WebSocket trigger mode and auto-discovery)
- A SIP registrar (FritzBox, Asterisk, or any SIP server)
- A phone connected to the SIP registrar
- Docker (recommended) or Python 3.11+

## Configuration

### Loxone Miniserver (WebSocket mode)

| Variable | Example | Description |
|---|---|---|
| `LOXONE_MINISERVER_IP` | `192.168.1.196` | Miniserver IP address |
| `LOXONE_MINISERVER_USER` | `myuser` | Miniserver username |
| `LOXONE_MINISERVER_PASS` | `mypassword` | Miniserver password |
| `LOXONE_DOORBELL_UUID` | *(empty)* | Doorbell state UUID. Leave empty to auto-discover from `LoxAPP3.json`. |

### Intercom

| Variable | Example | Description |
|---|---|---|
| `LOXONE_INTERCOM_IP` | `192.168.1.190` | Intercom IP address |
| `LOXONE_INTERCOM_SIP_URI` | `sip:smarthome@192.168.1.190` | Intercom SIP URI |

### SIP Registrar (FritzBox)

| Variable | Example | Description |
|---|---|---|
| `SIP_REGISTRAR` | `192.168.1.253` | SIP registrar IP address |
| `SIP_USER` | `sipuser` | SIP account username |
| `SIP_PASSWORD` | `sippassword` | SIP account password |
| `SIP_DESTINATION` | `**610` | Number to call (FritzBox internal extension) |
| `ACCEPT_CALL` | `true` | Register as SIP extension and accept incoming calls. Dial the bridge's extension to speak to the Intercom visitor. Set to `false` to disable. When enabled, the bridge registers at the SIP registrar using `SIP_USER`/ `SIP_PASSWORD` and maintains presence. Incoming calls are answered automatically and bridged to the Intercom. This runs alongside the normal doorbell trigger — both directions work simultaneously.|

### Networking

| Variable | Default | Description |
|---|---|---|
| `LOCAL_IP` | *(auto)* | Local IP for SIP/RTP. Auto-detected if empty. |
| `SIP_LISTEN_PORT` | `5060` | SIP listener for incoming BYE (TCP+UDP) |
| `WEBHOOK_PORT` | `42713` | HTTP server port (webhook mode only) |
| `CALL_TIMEOUT` | `0` | Max call duration in seconds. 0 = no timeout. |

## Intercom SIP details

The Intercom Gen.2 runs **baresip v1.0.0** internally and listens on
TCP 5060. It accepts incoming SIP INVITEs without authentication or
registration. The default SIP URI is `sip:smarthome@<intercom-ip>`.

Supported codecs: G.711 PCMU (PT 0), G.711 PCMA (PT 8).
Audio is bidirectional (`a=sendrecv`), 20ms ptime, plain RTP (no DTLS/SRTP).

## FritzFon live image

To display a live camera image on FritzFon when the doorbell rings,
configure the MJPEG stream URL in the FritzBox telephony settings for
the corresponding SIP device:

```
http://${LOXONE_MINISERVER_USER}:${LOXONE_MINISERVER_PASS}@${LOXONE_INTERCOM_IP}/mjpg/video.mjpg
```
for example:
```
http://myuser:mypassword@192.168.1.190/mjpg/video.mjpg
```

Set the refresh interval to 1 second. This streams the MJPEG feed
directly from the Intercom hardware and works reliably on FritzFon.

## WebSocket trigger details

The bridge connects to the Miniserver via `wss://<ip>/ws/rfc6455` using
the `remotecontrol` subprotocol. Authentication uses `getkey2` → HMAC-SHA256
→ `getjwt` (permission level 4). No `authwithtoken` required.

After connecting, the bridge enables binary status updates and optionally
fetches `LoxAPP3.json` to auto-discover the doorbell UUID (looks for
`IntercomV2` controls with a `bell` state).

The Miniserver sends binary state events either uncompressed (id=2) or
gzip-compressed (id=1, after `LoxAPP3.json` queries). Both formats are
handled automatically.

## BYE handling

The FritzBox sends its BYE (hangup) signal via **UDP** on port 5060, not
on the existing TCP SIP connection. The bridge runs a dedicated SIP
listener on port 5060 (TCP+UDP) that handles:

- **BYE** — tears down both call legs cleanly (sends BYE to Intercom)
- **OPTIONS** — responds with supported methods
- **INVITE** — accepts incoming calls (when `ACCEPT_CALL=true`), answers
  with 200 OK + SDP, and bridges audio to the Intercom

## Status

Working. End-to-end tested with:
- WebSocket trigger (Miniserver doorbell event → automatic call)
- Webhook trigger (manual `POST /trigger`)
- FritzBox SIP (Digest auth, no prior registration needed for outbound)
- FritzBox SIP registration (inbound calls, periodic re-registration)
- Inbound calls (dial bridge extension → forwarded to Intercom)
- BYE detection via UDP listener (both directions)
- Auto-discovery of doorbell UUID from `LoxAPP3.json`

## License

Apache-2.0

## Author

Markus Laube — [GitHub](https://github.com/markuslaube)

Co-authored by AI [GLM-5.2](https://huggingface.co/zai-org/GLM-5.2/)
