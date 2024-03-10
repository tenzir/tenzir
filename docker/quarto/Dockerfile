ARG TENZIR_VERSION
ARG QUARTO_VERSION
ARG TENZIR_CONTAINER_REGISTRY

FROM $TENZIR_CONTAINER_REGISTRY/tenzir/tenzir:$TENZIR_VERSION AS quarto-base
ARG QUARTO_VERSION

USER root

# libcurl4-openssl-dev is required to install pkgdepends (R)
# libfontconfig1-dev is required to install systemfonts (R)
RUN apt-get update && \
    apt install -y \
        apt-transport-https \
        bindfs \
        ca-certificates \
        curl \
        dirmngr \
        git \
        gnupg \
        libcurl4-openssl-dev \
        libfontconfig1-dev \
        python3-pip \
        software-properties-common \
        sudo && \
    gpg --keyserver keyserver.ubuntu.com \
        --recv-key '95C0FAF38DB3CCAD0C080A7BDC78B2DDEABC47B7' && \
    gpg --armor --export '95C0FAF38DB3CCAD0C080A7BDC78B2DDEABC47B7' > /etc/apt/keyrings/cran.asc && \
    echo "deb [signed-by=/etc/apt/keyrings/cran.asc] http://cloud.r-project.org/bin/linux/debian $(lsb_release --codename --short)-cran40/" \
      | tee /etc/apt/sources.list.d/cran.list && \
    apt-get update && \
    apt-get install -y r-base && \
    rm -rf /var/lib/apt/lists/*

RUN git clone -b v${QUARTO_VERSION} https://github.com/quarto-dev/quarto-cli.git /opt/quarto && \
    cd /opt/quarto && \
    ./configure.sh

RUN mkdir /home/tenzir && chown tenzir:tenzir /home/tenzir

USER tenzir:tenzir

# We install Poetry alongside Tenzir, they shouldn't disturb each other
ENV POETRY_HOME=$PREFIX

RUN curl -sSL https://install.python-poetry.org | python3 - --version 1.8.2

# Extra layers with configurations avoiding permission conflicts on host FS
FROM quarto-base as quarto-bind

USER root

COPY docker/quarto/entrypoint.sh /entrypoint.sh

# the user whith which the entrypoint script will run the provided commands
ENV CONTAINER_USER=tenzir
ENV CONTAINER_GROUP=tenzir

ENTRYPOINT [ "/entrypoint.sh" ]

# Default to a simple image with a bash entrypoint
FROM quarto-base

ENTRYPOINT [ "/bin/bash" ]
