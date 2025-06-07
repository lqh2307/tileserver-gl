ARG TARGET_IMAGE=registry.c4i.vn/map-server/ms-tile-server:0.0.27-base

FROM ${TARGET_IMAGE} AS final

WORKDIR /tile-server

COPY . .

VOLUME /tile-server/data

ENTRYPOINT ["./entrypoint.sh"]
