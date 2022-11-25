ARG VAST_VERSION
ARG VAST_CONTAINER_REGISTRY
ARG RELATIVE_APP_DIR


FROM $VAST_CONTAINER_REGISTRY/tenzir/vast:$VAST_VERSION
ARG RELATIVE_APP_DIR
ENV CONTAINER_APP_DIR=/vast/$RELATIVE_APP_DIR

USER root
RUN apt-get update && \
    apt install -y \
        curl && \
        rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/vast && chown vast:vast /home/vast

USER vast:vast
ENV POETRY_HOME=$PREFIX
RUN curl -sSL https://install.python-poetry.org | python3 -

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
