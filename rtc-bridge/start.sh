#!/bin/bash
set -e

STREAM_NAME="${LOXONE_STREAM_NAME:-loxone_intercom}"
RTSP_PORT="${LOXONE_RTSP_PORT:-8554}"
RTSP_HOST="${LOXONE_RTSP_HOST:-$(hostname)}"
LOG_LEVEL="${LOXONE_LOG_LEVEL:-INFO}"

export LOXONE_LOG_LEVEL="$LOG_LEVEL"

echo "[start] Loxone Intercom2 Bridge starting"
echo "[start] stream=$STREAM_NAME  rtsp=$RTSP_HOST:$RTSP_PORT/$STREAM_NAME"

# --- mediamtx ---
mediamtx /opt/loxone-bridge/mediamtx.yml &
MTX_PID=$!

# --- go2rtc registration loop (respawn on crash) ---
(
  while true; do
    python3 /opt/loxone-bridge/register_loop.py
    echo "[start] register_loop exited, restarting in 5s..."
    sleep 5
  done
) &
REG_PID=$!

cleanup() {
    kill $MTX_PID $REG_PID 2>/dev/null || true
    wait $MTX_PID $REG_PID 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Wait for mediamtx RTSP port
for i in $(seq 1 20); do
    if python3 -c "import socket; socket.socket().connect(('127.0.0.1', $RTSP_PORT))" 2>/dev/null; then
        echo "[start] mediamtx RTSP ready on :$RTSP_PORT"
        break
    fi
    sleep 0.25
done

# --- Main pipeline: bridge.py | ffmpeg -> mediamtx RTSP ---
RTSP_PUBLISH="rtsp://127.0.0.1:${RTSP_PORT}/${STREAM_NAME}"
echo "[start] pipeline: bridge.py | ffmpeg -> $RTSP_PUBLISH"

while true; do
    python3 /opt/loxone-bridge/bridge.py 2>>/tmp/bridge.log | \
        ffmpeg -hide_banner -loglevel error \
               -f h264 -i - \
               -c copy \
               -rtsp_transport tcp \
               -f rtsp "$RTSP_PUBLISH"
    echo "[start] pipeline exited, restarting in 5s..."
    sleep 5
done
