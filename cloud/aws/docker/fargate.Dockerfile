ARG VAST_VERSION

FROM tenzir/vast:$VAST_VERSION AS production

COPY schema/ /opt/tenzir/vast/etc/vast/schema/
COPY vast-fargate.yaml /opt/tenzir/vast/etc/vast/vast.yaml
