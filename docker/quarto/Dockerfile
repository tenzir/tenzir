ARG VAST_VERSION
ARG QUARTO_VERSION

FROM tenzir/vast:$VAST_VERSION AS quarto-base
ARG QUARTO_VERSION

USER root

# libcurl4-openssl-dev is required to install pkgdepends (R)
# libfontconfig1-dev is required to install systemfonts (R)
RUN apt-get update && \
    apt install -y \
        dirmngr \ 
        gnupg \
        apt-transport-https \
        ca-certificates \
        software-properties-common \
        python3-pip \
        git \
        curl \
        libcurl4-openssl-dev \
        libfontconfig1-dev && \
        rm -rf /var/lib/apt/lists/*

# install a recent version of R
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-key '95C0FAF38DB3CCAD0C080A7BDC78B2DDEABC47B7' && \
    add-apt-repository 'deb http://cloud.r-project.org/bin/linux/debian bullseye-cran40/' && \
    apt-get update && \
    apt-get install -y  r-base && \
    rm -rf /var/lib/apt/lists/*

RUN git clone -b v${QUARTO_VERSION} https://github.com/quarto-dev/quarto-cli.git /opt/quarto && \
    cd /opt/quarto && \
    ./configure.sh

RUN mkdir /home/vast && chown vast:vast /home/vast

USER vast:vast

# We install Poetry alongside VAST, they shouldn't disturb each other
ENV POETRY_HOME=$PREFIX

RUN curl -sSL https://install.python-poetry.org | python3 -


# extra layers to have clean write on host file system
FROM quarto-base as quarto-bind-mount

USER root

RUN apt update && \
    apt install -y bindfs sudo && \
    rm -rf /var/lib/apt/lists/*

COPY docker/quarto/entrypoint.sh /entrypoint.sh

# the user whith which the entrypoint script will run the provided commands
ENV CONTAINER_USER=vast
ENV CONTAINER_GROUP=vast

ENTRYPOINT [ "/entrypoint.sh" ]


# The default build doesn't need bindfs and related configs
FROM quarto-base as quarto-default

USER vast:vast
