# loxone-intercom2

![AI Contribution](https://raw.githubusercontent.com/Essk/ai-contribution-level/main/badges/level-3.svg)

Unofficial community integrations for the Loxone Intercom Gen.2.

## Components

### [rtc-bridge](rtc-bridge/)

Docker container that bridges the Intercom's WebRTC video stream into
go2rtc / Home Assistant via RTSP. Auto-discovers everything from the
Miniserver — no hardcoded URLs or credentials.

**Status:** Beta · Video only · [Details](rtc-bridge/)

### [rtc-directbridge](rtc-directbridge/)

Same as rtc-bridge, but connects directly to the Intercom via plain `ws://`.
No Miniserver, no TLS, no reverse proxy — just the Intercom IP.

**Status:** Beta · Video only · [Details](rtc-directbridge/)

## Wishlist

- [ ] Two-way audio (SIP/PJSUA2)
- [ ] Motion/event snapshots
- [ ] Doorbell ring notifications
- [ ] Direct Home Assistant integration (custom component)
- [ ] Multi-intercom support (tested with single device so far)

---

_Not affiliated with or endorsed by Loxone Electronics GmbH._
