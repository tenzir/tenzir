ARG VAST_VERSION
ARG BASE_IMAGE
ARG AWS_CLI_VERSION=2.4.20
ARG AWSLAMBDARIC_VERSION=2.0.1

FROM $BASE_IMAGE:$VAST_VERSION AS common_base

USER root

RUN apt-get update && \
    apt-get -y --no-install-recommends install curl



# The first objective here is to install the AWS Lambda Runtime Interface client that allows
# the image to be used from within lambda. Additionally, we can install any tooling that comes
# handy to interact with VAST (AWS CLI,...)
FROM common_base AS build

ARG AWS_CLI_VERSION
ARG AWSLAMBDARIC_VERSION

RUN apt-get -y --no-install-recommends install python3-pip unzip && \
    mkdir -p ${PREFIX}/lambda/aws-cli && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64-${AWS_CLI_VERSION}.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install --install-dir ${PREFIX}/lambda/aws-cli && \
    pip install --target ${PREFIX}/lambda awslambdaric==${AWSLAMBDARIC_VERSION}



FROM common_base AS production

COPY --from=build $PREFIX/ $PREFIX/

WORKDIR ${PREFIX}/lambda

RUN ln -s $PWD/aws-cli/v2/current/dist/aws /usr/local/bin/aws && \
    apt-get -y --no-install-recommends install jq && \
    rm -rf /var/lib/apt/lists/*

USER vast:vast

COPY scripts/lambda-handler.py .
COPY schema/ /opt/tenzir/vast/etc/vast/schema/
COPY configs/vast/lambda.yaml /opt/tenzir/vast/etc/vast/vast.yaml

ENTRYPOINT [ "/usr/bin/python3.9", "-m", "awslambdaric" ]
CMD [ "lambda-handler.handler" ]
