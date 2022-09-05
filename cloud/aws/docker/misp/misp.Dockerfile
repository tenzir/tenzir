ARG VAST_MISP_VERSION

FROM coolacid/misp-docker:core-$VAST_MISP_VERSION

COPY configs/misp/misp-init.py /misp-init.py
COPY configs/misp/misp-init.conf /etc/supervisor/conf.d/misp-init.conf
COPY configs/misp/misp-config.yaml /misp-config.yaml

ENTRYPOINT [ "/usr/bin/supervisord" ]
