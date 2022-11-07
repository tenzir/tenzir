FROM strangebee/thehive:5.0.17-1

USER root

RUN apt update && \
    apt install -y python3 pip && \
    pip3 install requests

COPY ./application.conf.template .

COPY ./entrypoint.py .

ENTRYPOINT [ "python3", "entrypoint.py" ]
