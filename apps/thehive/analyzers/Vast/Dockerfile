ARG VAST_VERSION=latest
ARG PYTHON_VERSION=latest

FROM tenzir/vast:$VAST_VERSION as vast

FROM python:$PYTHON_VERSION

ENV PREFIX="/opt/tenzir/vast" \
    PATH="/opt/tenzir/vast/bin:${PATH}"

RUN useradd --system --user-group vast
COPY --from=vast --chown=vast:vast $PREFIX/ $PREFIX/
COPY --from=vast --chown=vast:vast /var/lib/vast/ /var/lib/vast
COPY --from=vast --chown=vast:vast /var/log/vast/ /var/log/vast

WORKDIR /worker
COPY . Vast
RUN chmod +x Vast/search.py && \
pip3 install --no-cache-dir -r Vast/requirements.txt

USER vast:vast
ENTRYPOINT Vast/search.py
