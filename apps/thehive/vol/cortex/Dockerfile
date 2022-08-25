FROM thehiveproject/cortex:latest

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
apt-get install -y python3-pip && \
mkdir /opt/vast && \
cd /opt/vast && \
wget https://github.com/tenzir/vast/releases/latest/download/vast-linux-static.tar.gz && \
mkdir -p /opt/vast && \
tar xzf vast-linux-static.tar.gz -C /opt/vast



ENV PATH="${PATH}:/opt/vast/bin"
