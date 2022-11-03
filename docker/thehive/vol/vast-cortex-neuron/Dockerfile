ARG VAST_VERSION
ARG VAST_CONTAINER_REGISTRY

FROM $VAST_CONTAINER_REGISTRY/tenzir/vast:$VAST_VERSION

USER root

WORKDIR /worker
COPY . vast
RUN pip3 install --no-cache-dir -r vast/requirements.txt

# We install Poetry alongside VAST, they shouldn't disturb each other
ENV POETRY_HOME=$PREFIX

RUN apt-get update && \
    apt install -y curl && \
        rm -rf /var/lib/apt/lists/*
RUN curl -sSL https://install.python-poetry.org | python3 -

ENTRYPOINT ["vast/search.py"]
