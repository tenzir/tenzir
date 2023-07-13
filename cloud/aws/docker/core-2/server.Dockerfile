ARG TENZIR_VERSION
ARG TENZIR_IMAGE

FROM $TENZIR_IMAGE:$TENZIR_VERSION AS production

COPY schema/ /opt/tenzir/vast/etc/vast/schema/
COPY configs/vast/fargate.yaml /opt/tenzir/vast/etc/vast/vast.yaml
