ARG BUILDER_IMAGE=ubuntu:22.04
ARG TARGET_IMAGE=ubuntu:22.04

FROM ${BUILDER_IMAGE} AS builder

ARG GDAL_VERSION=3.11.0
ARG NODEJS_VERSION=22.16.0

RUN \
  apt-get -y update; \
  apt-get -y install \
    ca-certificates \
    wget \
    cmake \
    build-essential \
    libproj-dev \
    libcurl4-openssl-dev \
    libexpat-dev \
    libsqlite3-dev \
    librasterlite2-dev \
    libspatialite-dev \
    libpng-dev \
    libjpeg-dev \
    libgif-dev \
    libwebp-dev \
    libtiff-dev; \
  apt-get -y --purge autoremove; \
  apt-get clean; \
  rm -rf /var/lib/apt/lists/*;

RUN \
  wget -q http://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz; \
  tar -xzf ./gdal-${GDAL_VERSION}.tar.gz; \
  cd ./gdal-${GDAL_VERSION}; \
  mkdir -p build; \
  cd build; \
  cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_RPATH='$ORIGIN/../lib' \
    -DCMAKE_INSTALL_PREFIX=/usr/local/opt/gdal; \
  cmake --build .; \
  cmake --build . --target install; \
  cd ../..; \
  rm -rf ./gdal-${GDAL_VERSION}*;

RUN \
  wget -q https://nodejs.org/download/release/v${NODEJS_VERSION}/node-v${NODEJS_VERSION}-linux-x64.tar.gz; \
  tar -xzf node-v${NODEJS_VERSION}-linux-x64.tar.gz; \
  mkdir -p /usr/local/opt/nodejs; \
  cp -r ./node-v${NODEJS_VERSION}-linux-x64/* /usr/local/opt/nodejs; \
  rm -rf node-v${NODEJS_VERSION}-linux-x64*;

ENV PATH=/usr/local/opt/gdal/bin:/usr/local/opt/nodejs/bin:${PATH}

WORKDIR /tile-server

ADD . .

RUN \
  npm install --omit=dev; \
  rm -rf node_modules/.package-lock.json package-lock.json;


FROM ${TARGET_IMAGE} AS final

RUN \
  apt-get -y update; \
  apt-get -y install \
    xvfb \
    libglfw3 \
    libuv1 \
    libproj22 \
    libexpat1 \
    libjpeg-turbo8 \
    libicu70 \
    libgif7 \
    libopengl0 \
    libpng16-16 \
    libwebp7 \
    libcurl4 \
    libsqlite3-0 \
    librasterlite2-1 \
    libspatialite7 \
    libtiff5; \
  apt-get -y --purge autoremove; \
  apt-get clean; \
  rm -rf /var/lib/apt/lists/*;

WORKDIR /tile-server

COPY --from=builder /tile-server .
COPY --from=builder /usr/local/opt /usr/local/opt

ENV PATH=/usr/local/opt/gdal/bin:/usr/local/opt/nodejs/bin:${PATH}

VOLUME /tile-server/data

EXPOSE 8080

ENTRYPOINT ["./entrypoint.sh"]
