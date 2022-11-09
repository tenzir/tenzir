ARG CORTEX_VERSION=3.1.7-1-withdeps


FROM thehiveproject/cortex:$CORTEX_VERSION

COPY application.conf.template /etc/cortex/application.conf
COPY local-analyzers.json /opt/cortex/analyzers/local-analyzers.json
COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]
