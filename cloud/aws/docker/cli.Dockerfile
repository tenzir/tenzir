FROM python:3.10.3-slim

ARG TERRAFORM_VERSION=1.1.6
ARG TERRAGRUNT_VERSION=0.36.0

RUN apt-get update

# Install Terraform
RUN apt-get install -y gnupg software-properties-common curl unzip git wget expect sudo && \
    curl -fsSL https://apt.releases.hashicorp.com/gpg | apt-key add - && \
    apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main" && \
    apt-get update && \
    apt-get install terraform=$TERRAFORM_VERSION

RUN curl -L https://github.com/gruntwork-io/terragrunt/releases/download/v${TERRAGRUNT_VERSION}/terragrunt_linux_amd64 -o /usr/local/bin/terragrunt && \
    chmod +x /usr/local/bin/terragrunt

# Install Docker
RUN curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg | apt-key add - && \
    add-apt-repository \
    "deb [arch=amd64] https://download.docker.com/linux/$(. /etc/os-release; echo "$ID") \
    $(lsb_release -cs) \
    stable" && \
    apt-get update && \
    apt-get -y install docker-ce docker-compose-plugin

# Install AWS Session Manager Plugin
RUN curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/ubuntu_64bit/session-manager-plugin.deb" -o "session-manager-plugin.deb" && \
    dpkg -i session-manager-plugin.deb

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install

# Install GCP CLI
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
    | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    wget -O - "https://packages.cloud.google.com/apt/doc/apt-key.gpg" \
    | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && \
    apt-get -y --no-install-recommends install python3-crcmod google-cloud-sdk

ARG UNAME=hostcaller
ARG DOCKER_GID
ARG CALLER_UID
ARG CALLER_GID

# Configure the host caller user/group and host docker group in the image
RUN groupadd -g $CALLER_GID -o $UNAME && \
    useradd -m -u $CALLER_UID -g $CALLER_GID -o -s /bin/bash $UNAME && \
    groupadd -g $DOCKER_GID -o hostdocker && \
    usermod --append --groups hostdocker $UNAME && \
    adduser $UNAME sudo && \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Setup persistent folders for configs
RUN owneddir() { mkdir -p $1 && chown $UNAME $1 ; } && \
    owneddir /etc/persistant-configs/docker && \
    owneddir /etc/persistant-configs/gcloud && \
    owneddir /home/$UNAME/.config && \
    ln -s /etc/persistant-configs/docker /home/$UNAME/.docker && \
    ln -s /etc/persistant-configs/gcloud /home/$UNAME/.config/gcloud

USER $UNAME

# Install Python dependencies
RUN pip install boto3==1.24.27 dynaconf==3.1.9 invoke==1.7.1 requests==2.28.1 
