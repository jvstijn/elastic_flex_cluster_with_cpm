# Build CPM Kibana plugin and install into reference-stack Kibana image.
#
# Build (slow first time — clones Kibana v8.19.16):
#   docker build -t kibana-cpm:8.19.16 .
#
# Requires network during build.

ARG KIBANA_VERSION=8.19.16

FROM node:22.22.2-bookworm-slim AS builder
ARG KIBANA_VERSION
ENV HOME=/opt/moon-home
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1 \
    CI=true
RUN apt-get update \
  && apt-get install -y --no-install-recommends git python3 build-essential ca-certificates \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /kibana
RUN git clone --depth 1 --branch "v${KIBANA_VERSION}" https://github.com/elastic/kibana.git .
RUN mkdir -p /opt/moon-home
RUN corepack enable \
  && corepack prepare yarn@1.22.21 --activate \
  && yarn kbn bootstrap --skip-os-packages --allow-root \
  && for attempt in 1 2 3 4 5; do \
       yarn kbn build-shared --quiet --dist --allow-root && break; \
       if [ "$attempt" -eq 5 ]; then exit 1; fi; \
       sleep 20; \
     done
COPY cpm /kibana/plugins/cpm
RUN chown -R node:node /kibana /opt/moon-home \
  && su node -s /bin/sh -c "corepack enable && corepack prepare yarn@1.22.21 --activate"
USER node
WORKDIR /kibana/plugins/cpm
RUN for attempt in 1 2 3 4 5; do yarn build && break; [ "$attempt" -eq 5 ] && exit 1; sleep 20; done

FROM docker.elastic.co/kibana/kibana:${KIBANA_VERSION}
USER root
COPY --from=builder /kibana/plugins/cpm/build/cpm-*.zip /tmp/cpm.zip
RUN bin/kibana-plugin install file:///tmp/cpm.zip && rm -f /tmp/cpm.zip
USER kibana
