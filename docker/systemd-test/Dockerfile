FROM eniocarboni/docker-ubuntu-systemd:22.04

RUN apt-get update && \
    apt-get -y --no-install-recommends install \
    curl && \
    rm -rf /var/lib/apt/lists/* && \
    rm /usr/sbin/policy-rc.d
