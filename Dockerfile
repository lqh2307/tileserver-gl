ARG BUILDER_IMAGE=ubuntu:22.04
ARG TARGET_IMAGE=ubuntu:22.04

FROM ${BUILDER_IMAGE} AS builder

ARG NODEJS_VERSION=22.18.0

RUN \
  export DEBIAN_FRONTEND=noninteractive; \
  apt-get -y update; \
  apt-get -y install \
    wget; \
  apt-get -y --purge autoremove; \
  apt-get clean; \
  rm -rf /var/lib/apt/lists/*;

RUN \
  wget -q https://nodejs.org/download/release/v${NODEJS_VERSION}/node-v${NODEJS_VERSION}-linux-x64.tar.gz; \
  tar -xzf node-v${NODEJS_VERSION}-linux-x64.tar.gz; \
  mkdir -p /usr/local/opt/nodejs; \
  cp -r ./node-v${NODEJS_VERSION}-linux-x64/* /usr/local/opt/nodejs; \
  rm -rf node-v${NODEJS_VERSION}-linux-x64*;

ENV PATH=/usr/local/opt/nodejs/bin:${PATH}

WORKDIR /tile-server

ADD . .

RUN \
  npm install --omit=dev; \
  rm -rf node_modules/.package-lock.json package-lock.json;


FROM ${TARGET_IMAGE} AS final

RUN \
  export DEBIAN_FRONTEND=noninteractive; \
  apt-get -y update; \
  apt-get -y install \
    xvfb \
    fontconfig \
    libglfw3 \
    libuv1 \
    libjpeg-turbo8 \
    libicu70 \
    libgif7 \
    libopengl0 \
    libpng16-16 \
    libwebp7 \
    libcurl4; \
  apt-get -y --purge autoremove; \
  apt-get clean; \
  rm -rf /var/lib/apt/lists/*;

WORKDIR /tile-server

COPY --from=builder /tile-server .
COPY --from=builder /usr/local/opt /usr/local/opt

ENV PATH=/usr/local/opt/nodejs/bin:${PATH}

VOLUME /tile-server/data

EXPOSE 8080

ENTRYPOINT ["./entrypoint.sh"]
