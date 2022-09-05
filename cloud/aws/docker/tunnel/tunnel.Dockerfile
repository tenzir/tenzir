FROM python:3.10.3-slim

RUN apt-get update && \
    apt-get install -y wget && \
    rm -rf /var/lib/apt/lists/*


RUN wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb && dpkg -i cloudflared-linux-amd64.deb

RUN useradd -m cloudflared

USER cloudflared

WORKDIR /home/cloudflared
RUN pip install boto3
COPY scripts/cloudflared.py .

ENTRYPOINT [ "python", "cloudflared.py" ]
