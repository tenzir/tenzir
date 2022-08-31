ARG VAST_VERSION
ARG VAST_IMAGE

FROM $VAST_IMAGE:$VAST_VERSION AS production

COPY schema/ /opt/tenzir/vast/etc/vast/schema/
COPY configs/vast/fargate.yaml /opt/tenzir/vast/etc/vast/vast.yaml
