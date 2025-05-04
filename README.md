# Tile server

## Build & Run

### Prepare

Clone source:

```bash
git clone --single-branch -b 0.0.27 https://github.com/lqh2307/tileserver-gl.git
```

Jump to folder:

```bash
cd tile-server
```

Switch to 0.0.27 branch:

```bash
git checkout 0.0.27
```

### Run with nodejs - native (on ubuntu 22.04 x86_64 amd)

Install dependencies:

```bash
apt-get -y update; \
apt-get -y install \
  ca-certificates \
  wget \
  cmake \
  build-essential \
  libproj-dev \
  libproj22 \
  libexpat1 \
  xvfb \
  libglfw3 \
  libuv1 \
  libjpeg-turbo8 \
  libicu70 \
  libgif7 \
  libopengl0 \
  libpng16-16 \
  libwebp7 \
  libcurl4;
```

If use export (Install gdal):

```bash
export GDAL_VERSION=3.10.3

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
rm -rf ./gdal-${GDAL_VERSION}*; \
grep -q '/usr/local/opt/gdal/bin' ~/.bashrc || echo 'export PATH=/usr/local/opt/gdal/bin:${PATH}' >> ~/.bashrc; \
source ~/.bashrc;
```

Install nodejs:

```bash
export NODEJS_VERSION=22.15.0

wget -q https://nodejs.org/download/release/v${NODEJS_VERSION}/node-v${NODEJS_VERSION}-linux-x64.tar.gz; \
tar -xzf node-v${NODEJS_VERSION}-linux-x64.tar.gz; \
mkdir -p /usr/local/opt/nodejs; \
cp -r ./node-v${NODEJS_VERSION}-linux-x64/* /usr/local/opt/nodejs; \
rm -rf node-v${NODEJS_VERSION}-linux-x64*; \
grep -q '/usr/local/opt/nodejs/bin' ~/.bashrc || echo 'export PATH=/usr/local/opt/nodejs/bin:${PATH}' >> ~/.bashrc; \
source ~/.bashrc;
```

Clean:

```bash
apt-get -y remove \
  wget \
  cmake \
  build-essential \
  libproj-dev; \
apt-get -y --purge autoremove; \
apt-get clean; \
rm -rf /var/lib/apt/lists/*;
```

Install nodejs packages:

```bash
npm install --omit=dev
```

Run:

```bash
npm run server
```

ENVs:

```bash
DATA_DIR: path_to_data_folder (default: data)
SERVICE_NAME: service_name (default: tile-server)
RESTART_AFTER_CONFIG_CHANGE: true/false (default: true)
LOGGING_TO_FILE: true/false (default: true)
```

### Run with docker

Build image:

```bash
docker build -t tile-server:0.0.27 .
```

Run container:

```bash
docker run --rm -it -p 8080:8080 --name tile-server -v path_to_data_folder:/tile-server/data tile-server:0.0.27
```

### Prepare data

```bash
wget https://github.com/acalcutt/tileserver-gl/releases/download/test_data/zurich_switzerland.mbtiles
```

## Example config.json

```json
{
  "options": {
    "listenPort": 8080,
    "serveFrontPage": true,
    "serveSwagger": true,
    "taskSchedule": "0 00 18 * * *",
    "postgreSQLBaseURI": "postgresql://postgres:postgres@172.26.192.1:5432",
    "process": 2,
    "thread": 128
  },
  "styles": {
    "osm": {
      "style": "osm/style.json"
    },
    "3d": {
      "style": "3d/style.json"
    },
    "backdrop": {
      "style": "backdrop/style.json"
    },
    "basic": {
      "style": "basic/style.json"
    },
    "basic-v2": {
      "style": "basic-v2/style.json"
    },
    "bright": {
      "style": "bright/style.json"
    },
    "dark-matter": {
      "style": "dark-matter/style.json"
    },
    "dataviz": {
      "style": "dataviz/style.json"
    },
    "dataviz-dark": {
      "style": "dataviz-dark/style.json"
    },
    "dataviz-light": {
      "style": "dataviz-light/style.json"
    },
    "fiord": {
      "style": "fiord/style.json"
    },
    "hybrid": {
      "style": "hybrid/style.json"
    },
    "landscape": {
      "style": "landscape/style.json"
    },
    "liberty": {
      "style": "liberty/style.json"
    },
    "openstreetmap": {
      "style": "openstreetmap/style.json"
    },
    "outdoor-v2": {
      "style": "outdoor-v2/style.json"
    },
    "positron": {
      "style": "positron/style.json"
    },
    "protomap": {
      "style": "protomap/style.json"
    },
    "streets-v2": {
      "style": "streets-v2/style.json"
    },
    "terrain": {
      "style": "terrain/style.json"
    },
    "toner": {
      "style": "toner/style.json"
    },
    "topo": {
      "style": "topo/style.json"
    },
    "winter-v2": {
      "style": "winter-v2/style.json"
    },
    "demotiles": {
      "style": "demotiles_cache",
      "cache": {
        "forward": true,
        "store": true
      }
    }
  },
  "geojsons": {
    "test": {
      "test": {
        "geojson": "test/geojson.geojson"
      }
    },
    "U37AG001": {
      "bridge": {
        "geojson": "U37AG001/bridge.geojson"
      },
      "cblohd": {
        "geojson": "U37AG001/cblohd.geojson"
      },
      "lokbsn": {
        "geojson": "U37AG001/lokbsn.geojson"
      },
      "m_nsys": {
        "geojson": "U37AG001/m_nsys.geojson"
      },
      "notmrk": {
        "geojson": "U37AG001/notmrk.geojson"
      }
    }
  },
  "datas": {
    "asia_vietnam": {
      "mbtiles": "asia_vietnam/asia_vietnam.mbtiles"
    },
    "satellite": {
      "mbtiles": "satellite_cache",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "satellite_md5": {
      "mbtiles": "satellite_md5_cache",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "asia_cambodia": {
      "mbtiles": "asia_cambodia/asia_cambodia.mbtiles"
    },
    "openstreetmap": {
      "pmtiles": "https://data.source.coop/protomaps/openstreetmap/tiles/v3.pmtiles"
    },
    "building_footprints": {
      "pmtiles": "https://data.source.coop/vida/google-microsoft-open-buildings/pmtiles/go_ms_building_footprints.pmtiles"
    },
    "ODbL_firenze": {
      "pmtiles": "ODbL_firenze/ODbL_firenze.pmtiles"
    },
    "zurich_switzerland": {
      "mbtiles": "zurich_switzerland/zurich_switzerland.mbtiles"
    },
    "osm": {
      "mbtiles": "osm_cache",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "planet": {
      "mbtiles": "planet_cache",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "osm_pg": {
      "pg": "osm_pg_cache",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "osm_pg_2": {
      "pg": "osm_pg_2_cache",
      "cache": {
        "forward": true,
        "store": true
      }
    }
  },
  "sprites": {
    "basic-v2": {
      "sprite": "basic-v2"
    },
    "bright": {
      "sprite": "bright"
    },
    "dark-matter": {
      "sprite": "dark-matter"
    },
    "dataviz": {
      "sprite": "dataviz"
    },
    "dataviz-dark": {
      "sprite": "dataviz-dark"
    },
    "dataviz-light": {
      "sprite": "dataviz-light"
    },
    "fiord": {
      "sprite": "fiord"
    },
    "hybrid": {
      "sprite": "hybrid"
    },
    "landscape": {
      "sprite": "landscape"
    },
    "liberty": {
      "sprite": "liberty"
    },
    "openstreetmap": {
      "sprite": "openstreetmap"
    },
    "outdoor-v2": {
      "sprite": "outdoor-v2"
    },
    "positron": {
      "sprite": "positron"
    },
    "protomap": {
      "sprite": "protomap"
    },
    "streets-v2": {
      "sprite": "streets-v2"
    },
    "toner": {
      "sprite": "toner"
    },
    "topo": {
      "sprite": "topo"
    },
    "winter-v2": {
      "sprite": "winter-v2"
    }
  },
  "fonts": {
    "Open Sans Regular": {
      "font": "Open Sans Regular"
    },
    "Times New Roman": {
      "font": "Times New Roman"
    },
    "Roboto Medium": {
      "font": "Roboto Medium",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "Noto Sans Regular": {
      "font": "Noto Sans Regular",
      "cache": {
        "forward": true,
        "store": true
      }
    }
  }
}
```

## Example seed.json

```json
{
  "styles": {
    "demotiles_cache": {
      "metadata": {
        "name": "demotiles"
      },
      "url": "https://demotiles.maplibre.org/style.json",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      }
    }
  },
  "geojsons": {},
  "datas": {
    "osm_cache": {
      "metadata": {
        "name": "osm",
        "description": "osm",
        "format": "png",
        "bounds": [
          96,
          4,
          120,
          28
        ],
        "center": [
          108,
          16,
          10
        ],
        "minzoom": 0,
        "maxzoom": 18
      },
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
      "coverages": [
        {
          "bbox": [
            96,
            4,
            120,
            28
          ],
          "zoom": 0
        },
        {
          "bbox": [
            96,
            4,
            120,
            28
          ],
          "zoom": 5
        },
        {
          "bbox": [
            96,
            4,
            120,
            28
          ],
          "zoom": 9
        }
      ],
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5,
      "storeType": "mbtiles",
      "storeTransparent": false
    },
    "planet_cache": {
      "metadata": {
        "name": "planet",
        "description": "planet",
        "format": "pbf",
        "bounds": [
          96,
          4,
          120,
          28
        ],
        "center": [
          108,
          16,
          10
        ],
        "vector_layers": [
          {
            "id": "aerodrome_label"
          },
          {
            "id": "aeroway"
          },
          {
            "id": "boundary"
          },
          {
            "id": "building"
          },
          {
            "id": "housenumber"
          },
          {
            "id": "landcover"
          },
          {
            "id": "landuse"
          },
          {
            "id": "mountain_peak"
          },
          {
            "id": "park"
          },
          {
            "id": "place"
          },
          {
            "id": "poi"
          },
          {
            "id": "transportation"
          },
          {
            "id": "transportation_name"
          },
          {
            "id": "water"
          },
          {
            "id": "water_name"
          },
          {
            "id": "waterway"
          }
        ],
        "minzoom": 0,
        "maxzoom": 18
      },
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "url": "https://dwuxtsziek7cf.cloudfront.net/planet/{z}/{x}/{y}.pbf",
      "coverages": [
        {
          "bbox": [
            108,
            20,
            114,
            28
          ],
          "zoom": 0
        },
        {
          "bbox": [
            96,
            8,
            102,
            16
          ],
          "zoom": 5
        },
        {
          "bbox": [
            96,
            8,
            102,
            16
          ],
          "zoom": 10
        },
        {
          "bbox": [
            96,
            4,
            120,
            28
          ],
          "zoom": 10
        }
      ],
      "timeout": 60000,
      "concurrency": 100,
      "maxTry": 5,
      "storeType": "mbtiles",
      "storeTransparent": false,
      "skip": true
    },
    "satellite_cache": {
      "metadata": {
        "name": "satellite",
        "description": "satellite",
        "format": "jpeg",
        "bounds": [
          -180,
          -90,
          180,
          90
        ],
        "center": [
          108,
          16,
          10
        ],
        "minzoom": 0,
        "maxzoom": 18
      },
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "url": "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
      "coverages": [
        {
          "bbox": [
            104.4,
            8.55,
            106.8,
            10.42
          ],
          "zoom": 17
        },
        {
          "bbox": [
            102.5,
            20.5,
            108,
            21.5
          ],
          "zoom": 17
        },
        {
          "bbox": [
            103.8,
            20.15,
            106.65,
            20.5
          ],
          "zoom": 17
        },
        {
          "bbox": [
            103.8,
            19,
            106.39,
            20.15
          ],
          "zoom": 17
        },
        {
          "bbox": [
            104.4,
            8.55,
            106.8,
            10.42
          ],
          "zoom": 18
        },
        {
          "bbox": [
            102.5,
            20.5,
            108,
            21.5
          ],
          "zoom": 18
        },
        {
          "bbox": [
            103.8,
            20.15,
            106.65,
            20.5
          ],
          "zoom": 18
        },
        {
          "bbox": [
            103.8,
            19,
            106.39,
            20.15
          ],
          "zoom": 18
        }
      ],
      "timeout": 180000,
      "concurrency": 30,
      "maxTry": 5,
      "storeType": "mbtiles",
      "storeTransparent": true,
      "skip": true
    },
    "satellite_md5_cache": {
      "metadata": {
        "name": "satellite",
        "description": "satellite",
        "format": "jpeg",
        "bounds": [
          -180,
          -90,
          180,
          90
        ],
        "center": [
          108,
          16,
          10
        ],
        "minzoom": 0,
        "maxzoom": 18
      },
      "refreshBefore": {
        "md5": true
      },
      "url": "http://localhost:8080/datas/satellite/{z}/{x}/{y}.jpeg",
      "coverages": [
        {
          "bbox": [
            106.3654661178589,
            20.785024793097858,
            106.40363931655885,
            20.80269765224451
          ],
          "zoom": 16
        }
      ],
      "timeout": 180000,
      "concurrency": 30,
      "maxTry": 5,
      "storeType": "mbtiles",
      "storeTransparent": true
    }
  },
  "sprites": {},
  "fonts": {
    "Roboto Medium": {
      "url": "https://api.maptiler.com/fonts/Roboto Medium/{range}.pbf?key=aXcjPEauI4sBZOUkbLlP&mtsid=d7a93ef3-ffe6-4930-aa29-e9533fa57b83",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Noto Sans Regular": {
      "url": "https://api.maptiler.com/fonts/Noto Sans Regular/{range}.pbf?key=aXcjPEauI4sBZOUkbLlP&mtsid=d7a93ef3-ffe6-4930-aa29-e9533fa57b83",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    }
  }
}
```

## Example cleanup.json

```json
{
  "styles": {
    "demotiles_cache": {
      "cleanUpBefore": {
        "time": "2024-10-10T00:00:00"
      }
    }
  },
  "geojsons": {},
  "datas": {
    "osm_cache": {
      "coverages": [
        {
          "bbox": [
            96,
            8,
            102,
            16
          ],
          "zoom": 0
        },
        {
          "bbox": [
            96,
            8,
            102,
            16
          ],
          "zoom": 5
        },
        {
          "bbox": [
            108,
            20,
            114,
            28
          ],
          "zoom": 10
        }
      ],
      "cleanUpBefore": {
        "time": "2025-12-10T00:00:00"
      }
    }
  },
  "sprites": {},
  "fonts": {
    "Roboto Medium": {
      "cleanUpBefore": {
        "time": "2025-10-10T00:00:00"
      }
    }
  }
}
```


## Example export

```json
{
  "id": "osm_style_export",
  "metadata": {
    "name": "osm",
    "description": "osm",
    "format": "png",
    "bounds": [
      96,
      4,
      120,
      28
    ],
    "center": [
      108,
      16,
      10
    ],
    "minzoom": 9,
    "maxzoom": 9
  },
  "refreshBefore": {
    "time": "2024-10-10T00:00:00"
  },
  "tileScale": 1,
  "tileSize": 256,
  "coverages": [
    {
      "bbox": [
        96,
        4,
        120,
        28
      ],
      "zoom": 9
    }
  ],
  "createOverview": true,
  "maxRendererPoolSize": 40,
  "concurrency": 50,
  "storeType": "mbtiles",
  "storeTransparent": false
}

{
  "id": "osm_data_export",
  "metadata": {
    "name": "osm",
    "description": "osm",
    "format": "png",
    "bounds": [
      96,
      4,
      120,
      28
    ],
    "center": [
      108,
      16,
      10
    ],
    "minzoom": 9,
    "maxzoom": 9
  },
  "refreshBefore": {
    "time": "2024-10-10T00:00:00"
  },
  "coverages": [
    {
      "bbox": [
        96,
        4,
        120,
        28
      ],
      "zoom": 9
    }
  ],
  "concurrency": 50,
  "storeType": "mbtiles",
  "storeTransparent": true
}
```