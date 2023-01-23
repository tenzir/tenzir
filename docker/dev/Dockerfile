ARG VAST_VERSION
ARG VAST_CONTAINER_REGISTRY

FROM $VAST_CONTAINER_REGISTRY/tenzir/vast:$VAST_VERSION as poetry-base
USER root
RUN apt-get update && \
    apt install -y \
        curl && \
        rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/vast && chown vast:vast /home/vast
USER vast:vast
ENV POETRY_HOME=$PREFIX
RUN curl -sSL https://install.python-poetry.org | python3 -


FROM poetry-base as vast-python-script
WORKDIR /vast/python/
# Layer the Poetry install to optimize the dev experience.
COPY --chown=vast:vast \ 
    ./vast/integration/data/ \
    /vast/integration/data/
COPY --chown=vast:vast \
    ./python/pyproject.toml \
    ./python/poetry.lock \
    ./
RUN poetry install --no-root
COPY --chown=vast:vast \
    ./python/ \
    ./
RUN poetry install
ENV VAST_PYTHON_INTEGRATION=1
ENTRYPOINT [ "poetry", "run" ]


FROM poetry-base as vast-python-app
ARG RELATIVE_APP_DIR
ENV CONTAINER_APP_DIR=/vast/$RELATIVE_APP_DIR
COPY --chown=vast:vast ./python /vast/python
# Layer the Poetry install to optimize the dev experience.
WORKDIR $CONTAINER_APP_DIR
COPY --chown=vast:vast \
    $RELATIVE_APP_DIR/pyproject.toml \
    $RELATIVE_APP_DIR/poetry.lock \
    $CONTAINER_APP_DIR
RUN poetry install --no-root
COPY --chown=vast:vast \
    $RELATIVE_APP_DIR/ \
    $CONTAINER_APP_DIR
RUN poetry install
ENTRYPOINT [ "poetry", "run" ]
