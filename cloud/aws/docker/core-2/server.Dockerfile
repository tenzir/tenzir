ARG TENZIR_VERSION
ARG TENZIR_IMAGE

FROM $TENZIR_IMAGE:$TENZIR_VERSION AS production

COPY schema/ /opt/tenzir/tenzir/etc/tenzir/schema/
COPY configs/tenzir/fargate.yaml /opt/tenzir/tenzir/etc/tenzir/tenzir.yaml
