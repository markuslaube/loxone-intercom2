![AI Contribution](https://raw.githubusercontent.com/Essk/ai-contribution-level/main/badges/level-3.svg)

# loxone-intercom2

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

### [sip-directbridge](sip-directbridge/)

Bridges the Intercom's SIP audio to any SIP account for integration in Home Assistant. 
Makes your phone ring when someone's at the door and establishes a two-way audio intercom call.

**Status:** Beta · Audio only · [Details](sip-directbridge/)

## Wishlist

- [ ] One-way audio (webrtc) für rtc-directbridge (wird erstmal sip2rtc)
- [ ] One-way audio (webrtc) für rtc-bridge (wird erstmal sip2rtc)
- [ ] Two-way audio (webrtc)
- [ ] Motion/event snapshots
- [ ] Doorbell ring notifications
- [ ] Direct Home Assistant integration (custom component)
- [ ] Multi-intercom support (tested with single device so far)

---

_Not affiliated with or endorsed by Loxone Electronics GmbH._
