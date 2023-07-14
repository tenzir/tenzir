ARG TENZIR_VERSION
ARG TENZIR_CONTAINER_REGISTRY

FROM $TENZIR_CONTAINER_REGISTRY/tenzir/tenzir:$TENZIR_VERSION

USER root

WORKDIR /worker
COPY . tenzir
RUN apt-get update && \
    apt install -y \
        python3 && \
        rm -rf /var/lib/apt/lists/* && \
        pip3 install --no-cache-dir -r tenzir/requirements.txt

ENTRYPOINT ["tenzir/search.py"]
