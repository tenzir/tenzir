# Provides an environment where the python binary and the python bindings
# are installed and can be run using poetry.

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
ENV POETRY_HOME=$PREFIX
RUN curl -sSL https://install.python-poetry.org | python3 - --version 2.1.2

WORKDIR /tenzir/python/
# Layer the Poetry install to optimize the dev experience.
COPY --chown=tenzir:tenzir \
    ./python/pyproject.toml \
    ./python/poetry.lock \
    ./
RUN poetry install --no-root --all-extras
COPY --chown=tenzir:tenzir \
    ./python/ \
    ./
RUN poetry install --all-extras
ENTRYPOINT [ "poetry", "run" ]
