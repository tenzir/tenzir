ARG TENZIR_VERSION
ARG TENZIR_IMAGE
ARG BOTO3_VERSION=1.24.50

FROM $TENZIR_IMAGE:$TENZIR_VERSION AS build

ARG BOTO3_VERSION

USER root

RUN apt-get update && \
    apt-get -y --no-install-recommends install python3-pip && \
    pip install --target /dependencies boto3==${BOTO3_VERSION}



FROM $TENZIR_IMAGE:$TENZIR_VERSION AS production

USER tenzir:tenzir

COPY --from=build /dependencies/ ./

COPY scripts/matcher-client.py .
COPY schema/ /opt/tenzir/tenzir/etc/tenzir/schema/
COPY configs/tenzir/fargate.yaml /opt/tenzir/tenzir/etc/tenzir/tenzir.yaml

ENTRYPOINT [ "/usr/bin/python3.9", "matcher-client.py" ]
