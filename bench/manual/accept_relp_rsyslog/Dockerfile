FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends rsyslog rsyslog-relp \
 && mkdir -p /tmp/rsyslog \
 && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["rsyslogd", "-n", "-f", "/etc/rsyslog.conf"]
