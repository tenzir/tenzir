ARG VAST_VERSION
ARG VAST_CONTAINER_REGISTRY

# This base image is the same accross all images using VAST+Poetry
# We could make this DRYer by pushing this image to a public registry
FROM $VAST_CONTAINER_REGISTRY/tenzir/vast:$VAST_VERSION as vast-poetry

USER root
RUN apt-get update && \
    apt install -y \
        curl && \
        rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/vast && chown vast:vast /home/vast && \
    mkdir -p /python && chown vast:vast /python && \
    mkdir -p /opt/thehive/app && chown vast:vast /opt/thehive/app && \
    chown -R vast:vast /usr/local/lib/python3.10/

USER vast:vast
ENV POETRY_HOME=$PREFIX
RUN curl -sSL https://install.python-poetry.org | python3 -

COPY --chown=vast:vast ./python /python

FROM vast-poetry

# Layer the Poetry install to optimize the dev experience
WORKDIR /opt/thehive/app
ARG APPDIR=docker/thehive/app
COPY --chown=vast:vast $APPDIR/pyproject.toml $APPDIR/poetry.lock /opt/thehive/app/
RUN poetry install --no-root
COPY --chown=vast:vast $APPDIR/app.py /opt/thehive/app/
RUN poetry install

ENTRYPOINT [ "poetry", "run" ]
