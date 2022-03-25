FROM python:3.10.3-slim

ARG UNAME=host
ARG DOCKER_ID
ARG UID
ARG GID

RUN apt-get update && \
    apt-get install -y gnupg software-properties-common curl unzip git && \
    curl -fsSL https://apt.releases.hashicorp.com/gpg | apt-key add - && \
    apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main" && \
    apt-get update && \
    apt-get install terraform=1.1.6 &&\
    curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg | apt-key add - && \
    add-apt-repository \
    "deb [arch=amd64] https://download.docker.com/linux/$(. /etc/os-release; echo "$ID") \
    $(lsb_release -cs) \
    stable" && \
    apt-get update && \
    apt-get -y install docker-ce

RUN curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/ubuntu_64bit/session-manager-plugin.deb" -o "session-manager-plugin.deb" && \
    dpkg -i session-manager-plugin.deb

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install

RUN groupadd -g $GID -o $UNAME && \
    useradd -m -u $UID -g $GID -o -s /bin/bash $UNAME && \
    groupmod -g $DOCKER_ID docker && \
    usermod --append --groups docker $UNAME

USER $UNAME

RUN pip install boto3 python-dotenv invoke

WORKDIR /vast/cloud/aws

ENTRYPOINT [ "/home/host/.local/bin/invoke" ]
