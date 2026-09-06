FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg curl && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH=arm64
RUN case "$TARGETARCH" in \
      amd64) MTX_ARCH=amd64 ;; \
      arm64) MTX_ARCH=arm64 ;; \
      *) echo "unsupported: $TARGETARCH"; exit 1 ;; \
    esac && \
    curl -L "https://github.com/bluenviron/mediamtx/releases/download/v1.21.0/mediamtx_v1.21.0_linux_${MTX_ARCH}.tar.gz" \
    -o /tmp/mediamtx.tar.gz && \
    tar xzf /tmp/mediamtx.tar.gz -C /usr/local/bin mediamtx && \
    rm /tmp/mediamtx.tar.gz

WORKDIR /opt/loxone-bridge

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bridge.py go2rtc_register.py register_loop.py start.sh mediamtx.yml ./
RUN chmod +x start.sh

ENV PYTHONUNBUFFERED=1
ENV LOXONE_LOG_LEVEL=INFO

CMD ["./start.sh"]
