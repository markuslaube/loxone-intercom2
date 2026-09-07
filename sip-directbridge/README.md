![AI Contribution](https://raw.githubusercontent.com/Essk/ai-contribution-level/main/badges/level-3.svg)

# sip-directbridge

Bridges the Intercom's SIP audio to any SIP account — makes your phone
ring when someone's at the door and lets you talk to the visitor.

## How it works

```
Doorbell Event
    │
    ▼
sip-directbridge (.23)
    ├── SIP INVITE → Intercom (172.16.2.190:5060)   [visitor audio]
    ├── SIP INVITE → FritzBox / SIP phone           [your phone rings]
    └── Self = muted (PCMU silence to both)
         Audio routing: Intercom ⟷ FritzBox
```

You pick up the phone → two-way audio runs between you and the visitor
through the bridge. Hang up → bridge tears down both legs.

## Requirements

- Loxone Intercom Gen.2 (reachable via TCP 5060, no auth needed)
- A SIP registrar (FritzBox, Asterisk, or any SIP server)
- A SIP client (FritzFon, smartphone SIP app, softphone)
- Docker (recommended) or Python 3.11+

## Configuration

| Variable | Example | Description |
|---|---|---|
| `LOXONE_INTERCOM_IP` | `172.16.2.190` | Intercom IP address |
| `LOXONE_INTERCOM_SIP_URI` | `sip:smarthome@172.16.2.190` | Intercom SIP URI (auto-detected if Miniserver configured) |
| `SIP_REGISTRAR` | `fritz.box` | SIP registrar hostname |
| `SIP_USER` | `doorbell` | SIP account username |
| `SIP_PASSWORD` | `secret` | SIP account password |
| `SIP_DESTINATION` | `**610` | Number to call (FritzBox internal extension) |

## Intercom SIP details

The Intercom Gen.2 runs **baresip v1.0.0** internally and listens on
TCP 5060. It accepts incoming SIP INVITEs without authentication or
registration. The default SIP URI is `sip:smarthome@<intercom-ip>`.

Supported codecs: G.711 PCMU (PT 0), G.711 PCMA (PT 8).
Audio is bidirectional (`a=sendrecv`), 20ms ptime, plain RTP (no DTLS/SRTP).

## Status

Development. Core SIP call lifecycle proven (INVITE → 200 → ACK → RTP → BYE).
Conference bridging (two simultaneous calls + audio forwarding) not yet
implemented.

## License

Apache-2.0

## Author

Markus Laube — [GitHub](https://github.com/markuslaube)

Co-authored by AI [GLM-5.2](https://huggingface.co/zai-org/GLM-5.2/)
