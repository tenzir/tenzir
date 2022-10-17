ARG VAST_VERSION
ARG QUARTO_VERSION

FROM tenzir/vast:$VAST_VERSION
ARG QUARTO_VERSION
ARG TARGETPLATFORM
ARG TARGETARCH

USER root

RUN apt-get update && \
    apt-get install -y \
        r-base \
        python3-pip \
        git \
        curl && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

RUN git clone -b v${QUARTO_VERSION} https://github.com/quarto-dev/quarto-cli.git /opt/quarto && \
    cd /opt/quarto && \
    ./configure.sh

USER vast:vast

# We install Poetry alongside VAST, they shouldn't disturb each other :)
ENV POETRY_HOME=$PREFIX

RUN curl -sSL https://install.python-poetry.org | python3 -

COPY --chown=vast:vast examples/notebooks /notebooks

WORKDIR /notebooks
