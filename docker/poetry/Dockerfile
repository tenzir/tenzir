ARG VAST_VERSION
ARG VAST_CONTAINER_REGISTRY

FROM $VAST_CONTAINER_REGISTRY/tenzir/vast:$VAST_VERSION

USER root

RUN apt-get update && \
    apt install -y \
        curl && \
        rm -rf /var/lib/apt/lists/*

RUN echo "#!/bin/bash" > /entrypoint.sh && \
    echo 'poetry install && poetry run $1' >> /entrypoint.sh && \
    chmod +x /entrypoint.sh

# Poetry uses $HOME as cache
RUN mkdir /home/vast && chown vast:vast /home/vast
USER vast:vast

# We install Poetry alongside VAST, they shouldn't disturb each other
ENV POETRY_HOME=$PREFIX
RUN curl -sSL https://install.python-poetry.org | python3 -

ENTRYPOINT [ "/entrypoint.sh" ]
