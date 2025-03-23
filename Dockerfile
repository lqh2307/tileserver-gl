ARG BUILDER_IMAGE=ubuntu:22.04
ARG TARGET_IMAGE=ubuntu:22.04

FROM ${BUILDER_IMAGE} AS builder

ARG GDAL_VERSION=3.10.2
ARG NODEJS_VERSION=22.14.0

RUN \
  apt-get -y update; \
  apt-get -y upgrade; \
  apt-get -y install \
    ca-certificates \
    wget \
    cmake \
    build-essential \
    libproj-dev;

RUN \
  wget -q http://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz; \
  tar -xzf ./gdal-${GDAL_VERSION}.tar.gz; \
  cd ./gdal-${GDAL_VERSION}; \
  mkdir -p build; \
  cd build; \
  cmake .. -DCMAKE_BUILD_TYPE=Release; \
  cmake --build .; \
  cmake --build . --target install; \
  cd ../..; \
  rm -rf ./gdal-${GDAL_VERSION}*;

RUN \
  wget -q https://nodejs.org/download/release/v${NODEJS_VERSION}/node-v${NODEJS_VERSION}-linux-x64.tar.gz; \
  mkdir -p /usr/local/lib/nodejs; \
  tar -xzf node-v${NODEJS_VERSION}-linux-x64.tar.gz --strip-components=1 -C /usr/local/lib/nodejs; \
  rm -rf node-v${NODEJS_VERSION}-linux-x64.tar.gz;

ENV PATH=/usr/local/lib/nodejs/bin:$PATH

WORKDIR /tile-server

ADD . .

RUN \
  npm install --omit=dev; \
  rm -rf package-lock.json; \
  apt-get -y --purge autoremove; \
  apt-get clean; \
  rm -rf /var/lib/apt/lists/*; \
  ldconfig;


FROM ${TARGET_IMAGE} AS final

RUN \
  apt-get -y update; \
  apt-get -y upgrade; \
  apt-get -y install \
    xvfb \
    libglfw3 \
    libuv1 \
    libjpeg-turbo8 \
    libicu70 \
    libgif7 \
    libopengl0 \
    libpng16-16 \
    libwebp7 \
    libsqlite3-0 \
    libcurl4 \
    libproj22;

WORKDIR /tile-server

COPY --from=builder /tile-server .
COPY --from=builder /usr/local /usr/local

ENV PATH=/usr/local/lib/nodejs/bin:$PATH

RUN \
  apt-get -y --purge autoremove; \
  apt-get clean; \
  rm -rf /var/lib/apt/lists/*; \
  ldconfig;

VOLUME /tile-server/data

EXPOSE 8080

ENTRYPOINT ["./entrypoint.sh"]
