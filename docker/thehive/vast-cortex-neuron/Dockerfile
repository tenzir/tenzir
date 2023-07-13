ARG TENZIR_VERSION
ARG TENZIR_CONTAINER_REGISTRY

FROM $TENZIR_CONTAINER_REGISTRY/tenzir/tenzir:$TENZIR_VERSION

USER root

WORKDIR /worker
COPY . vast
RUN apt-get update && \
    apt install -y \
        python3 && \
        rm -rf /var/lib/apt/lists/* && \
        pip3 install --no-cache-dir -r vast/requirements.txt

ENTRYPOINT ["vast/search.py"]
