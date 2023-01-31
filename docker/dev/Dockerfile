ARG VAST_VERSION
ARG VAST_CONTAINER_REGISTRY

FROM $VAST_CONTAINER_REGISTRY/tenzir/vast:$VAST_VERSION
USER root
RUN apt-get update && \
    apt install -y \
        curl && \
        rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/vast && chown vast:vast /home/vast
USER vast:vast
ENV POETRY_HOME=$PREFIX
RUN curl -sSL https://install.python-poetry.org | python3 -

WORKDIR /vast/python/
# Layer the Poetry install to optimize the dev experience.
COPY --chown=vast:vast \ 
    ./vast/integration/data/ \
    /vast/vast/integration/data/
COPY --chown=vast:vast \
    ./python/pyproject.toml \
    ./python/poetry.lock \
    ./
RUN poetry install --no-root --all-extras
COPY --chown=vast:vast \
    ./python/ \
    ./
RUN poetry install --all-extras
ENTRYPOINT [ "poetry", "run" ]
