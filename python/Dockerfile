# Provides an environment where the python binary and the python bindings
# are installed and can be run using uv.

ARG TENZIR_VERSION
ARG TENZIR_CONTAINER_REGISTRY

FROM $TENZIR_CONTAINER_REGISTRY/tenzir/tenzir:$TENZIR_VERSION
USER root
RUN apt-get update && \
    apt install -y \
        build-essential \
        curl \
        python3-dev && \
        rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/tenzir && chown tenzir:tenzir /home/tenzir
USER tenzir:tenzir
ENV PATH="/home/tenzir/.local/bin:${PATH}"
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

WORKDIR /tenzir/python/
# Layer the uv install to optimize the dev experience.
COPY --chown=tenzir:tenzir \
    ./python/pyproject.toml \
    ./python/uv.lock \
    ./
RUN uv sync --all-packages --all-extras --group dev --no-install-project
COPY --chown=tenzir:tenzir \
    ./python/ \
    ./
RUN uv sync --all-packages --all-extras --group dev
ENTRYPOINT [ "uv", "run" ]
