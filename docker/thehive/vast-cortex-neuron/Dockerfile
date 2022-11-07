ARG VAST_VERSION
ARG VAST_CONTAINER_REGISTRY

FROM $VAST_CONTAINER_REGISTRY/tenzir/vast:$VAST_VERSION

USER root

WORKDIR /worker
COPY . vast
RUN pip3 install --no-cache-dir -r vast/requirements.txt

ENTRYPOINT ["vast/search.py"]
