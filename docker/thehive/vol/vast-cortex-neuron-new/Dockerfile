ARG VAST_VERSION=latest
ARG VAST_CONTAINER_REGISTRY=docker.io

FROM $VAST_CONTAINER_REGISTRY/tenzir/vast:$VAST_VERSION

USER root

# libcurl4-openssl-dev is required to install pkgdepends (R)
# libfontconfig1-dev is required to install systemfonts (R)
RUN apt-get update && \
    apt install -y \
        apt-transport-https \
        ca-certificates \
        software-properties-common \
        python3-pip \
        git \
        curl \
        libcurl4-openssl-dev \
        libfontconfig1-dev && \
        rm -rf /var/lib/apt/lists/*

RUN mkdir /home/vast && chown vast:vast /home/vast

USER vast:vast

# We install Poetry alongside VAST, they shouldn't disturb each other
ENV POETRY_HOME=$PREFIX

RUN curl -sSL https://install.python-poetry.org | python3 -

# RUN echo 'deb http://deb.debian.org/debian bullseye-backports main' \
#       > /etc/apt/sources.list.d/backports.list

# RUN apt-get update
# RUN apt-get install -y\
#       cmake \
#       wget \
#       cmake/bullseye-backports \
#       cmake-data/bullseye-backports \
#       lsb-release

# ENV POETRY_VERSION=1.2.2
# ENV POETRY_VENV=/opt/poetry-venv

# # Install poetry separated from system interpreter
# RUN python3 -m venv $POETRY_VENV \
#     && $POETRY_VENV/bin/pip install -U pip setuptools \
#     && $POETRY_VENV/bin/pip install poetry==${POETRY_VERSION}

# Add `poetry` to PATH
# ENV PATH="${PATH}:${POETRY_VENV}/bin" 

# COPY python /app
WORKDIR /app

# RUN poetry install --all-extras

