ARG VAST_VERSION
ARG VAST_CONTAINER_REGISTRY

FROM $VAST_CONTAINER_REGISTRY/tenzir/tenzir:$VAST_VERSION
USER root
RUN apt-get update && \
    apt install -y \
        curl \
        python3 && \
        rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/tenzir && chown tenzir:tenzir /home/tenzir
USER tenzir:tenzir
ENV POETRY_HOME=$PREFIX
RUN curl -sSL https://install.python-poetry.org | python3 - --version 1.4.0

WORKDIR /tenzir/python/
# Layer the Poetry install to optimize the dev experience.
COPY --chown=tenzir:tenzir \ 
    ./vast/integration/data/ \
    /tenzir/vast/integration/data/
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
