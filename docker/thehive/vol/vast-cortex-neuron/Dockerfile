ARG VAST_VERSION=latest
ARG PYTHON_VERSION=latest

FROM tenzir/vast:$VAST_VERSION as vast

FROM python:$PYTHON_VERSION

ENV PREFIX="/opt/tenzir/vast" \
    PATH="/opt/tenzir/vast/bin:${PATH}"

COPY --from=vast $PREFIX/ $PREFIX/
COPY --from=vast /var/lib/vast/ /var/lib/vast
COPY --from=vast /var/log/vast/ /var/log/vast

RUN apt-get update && \
    apt-get -y --no-install-recommends install \
      ca-certificates \
      gnupg2 \
      libasan5 \
      libcaf-core0.17 \
      libcaf-io0.17 \
      libcaf-openssl0.17 \
      libbroker2 \
      libc++1 \
      libc++abi1 \
      libflatbuffers1 \
      libfmt7 \
      libpcap0.8 \
      libsimdjson5 \
      libspdlog1 \
      libunwind8 \
      libyaml-cpp0.6 \
      libxxhash-dev \
      lsb-release \
      openssl \
      robin-map-dev \
      wget && \
    wget "https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb" && \
    apt-get -y --no-install-recommends install \
      ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt-get update && \
    apt-get -y --no-install-recommends install libarrow900 libparquet900 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /worker
COPY . VAST
RUN chmod +x VAST/search.py && \
pip3 install --no-cache-dir -r VAST/requirements.txt

ENTRYPOINT VAST/search.py
