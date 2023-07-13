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

USER vast:vast

COPY --from=build /dependencies/ ./

COPY scripts/matcher-client.py .
COPY schema/ /opt/tenzir/vast/etc/vast/schema/
COPY configs/vast/fargate.yaml /opt/tenzir/vast/etc/vast/vast.yaml

ENTRYPOINT [ "/usr/bin/python3.9", "matcher-client.py" ]
