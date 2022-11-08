ARG MISP_VERSION

FROM coolacid/misp-docker:core-$MISP_VERSION

COPY misp-init.py /misp-init.py
COPY misp-init.conf /etc/supervisor/conf.d/misp-init.conf
COPY misp-config.yaml /misp-config.yaml

ENTRYPOINT [ "/usr/bin/supervisord" ]
