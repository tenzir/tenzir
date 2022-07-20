ARG VAST_VERSION
ARG BASE_IMAGE

FROM $BASE_IMAGE:$VAST_VERSION AS production

COPY schema/ /opt/tenzir/vast/etc/vast/schema/
COPY vast-fargate.yaml /opt/tenzir/vast/etc/vast/vast.yaml
