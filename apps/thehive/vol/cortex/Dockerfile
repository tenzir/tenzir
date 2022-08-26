ARG VAST_VERSION=latest
ARG CORTEX_VERSION=latest

FROM tenzir/vast:$VAST_VERSION as vast

FROM thehiveproject/cortex:$CORTEX_VERSION

ENV PREFIX="/opt/tenzir/vast" \
    PATH="/opt/tenzir/vast/bin:${PATH}"

RUN apt-get update && \
apt-get install -y python3-pip

COPY --from=vast --chown=cortex:cortex $PREFIX/ $PREFIX/
COPY --from=vast --chown=cortex:cortex /var/lib/vast/ /var/lib/vast
COPY --from=vast --chown=cortex:cortex /var/log/vast/ /var/log/vast
COPY --from=vast --chown=cortex:cortex /usr/lib/ /usr/lib/





