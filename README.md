# Tile server

## Build & Run

### Prepare

Clone source:

```bash
git clone --single-branch -b 1.0.0 https://github.com/lqh2307/tileserver-gl.git
```

Jump to folder:

```bash
cd tile-server
```

Switch to 1.0.0 branch:

```bash
git checkout 1.0.0
```

### Run with nodejs - native

Install dependencies (ubuntu 22.04):

```bash
export DEBIAN_FRONTEND=noninteractive; \
apt-get -y update; \
apt-get -y install \
  wget \
  xvfb \
  x11-utils \
  fontconfig \
  libglfw3 \
  libuv1 \
  libsqlite3-0 \
  libjpeg-turbo8 \
  libicu70 \
  libopengl0 \
  libpng16-16 \
  libwebp7 \
  libcurl4;
```

Install dependencies (ubuntu 24.04):

```bash
export DEBIAN_FRONTEND=noninteractive; \
apt-get -y update; \
apt-get -y install \
  wget \
  xvfb \
  x11-utils \
  fontconfig \
  libglfw3 \
  libuv1t64 \
  libsqlite3-0 \
  libjpeg-turbo8 \
  libicu74 \
  libopengl0 \
  libpng16-16t64 \
  libwebp7 \
  libcurl4t64;
```

Install nodejs:

```bash
export NODEJS_VERSION=24.13.1

wget -q https://nodejs.org/download/release/v${NODEJS_VERSION}/node-v${NODEJS_VERSION}-linux-x64.tar.gz; \
tar -xzf node-v${NODEJS_VERSION}-linux-x64.tar.gz; \
mkdir -p /usr/local/opt/nodejs; \
cp -r ./node-v${NODEJS_VERSION}-linux-x64/* /usr/local/opt/nodejs; \
rm -rf node-v${NODEJS_VERSION}-linux-x64*; \
grep -q '/usr/local/opt/nodejs/bin' ~/.bashrc || echo 'export PATH=/usr/local/opt/nodejs/bin:${PATH}' >> ~/.bashrc; \
source ~/.bashrc;
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
LISTEN_PORT: service name, will overwrite config.json file (default: 8080)
DATA_DIR: path to data folder (default: data)
SERVICE_NAME: service name (default: tile-server)
RESTART_AFTER_CONFIG_CHANGE: restart server after config file changed, true/false (default: true)
NUM_OF_THREAD: number of threads, will overwrite config.json file (default: num of cpus)
NUM_OF_PROCESS: number of process, will overwrite config.json file (default: 1)
LOG_LEVEL: log level (default: info)
```

### Run with docker

Build image:

```bash
docker build -t quanghuy2307/tile-server:1.0.0 .
```

Run container:

```bash
docker run --rm -it -p 8080:8080 --name tile-server -v path_to_data_folder:/tile-server/data quanghuy2307/tile-server:1.0.0
```

### Prepare data

```bash
wget https://github.com/acalcutt/tileserver-gl/releases/download/test_data/zurich_switzerland.mbtiles
```

## Contribute

### Source

Push source:

```bash
git add . && git commit -m "1.0.0" && git push
```

### Image

Push image:

```bash
docker push quanghuy2307/tile-server:1.0.0
```

## Example config.json

```json
{
  "options": {
    "listenPort": 8080,
    "serveFrontPage": true,
    "serveSwagger": true,
    "restartSchedule": "00 00 00 * * *",
    "taskSchedule": "00 00 00 * * *",
    "postgreSQLBaseURI": "postgresql://postgres:postgres@172.26.192.1:5432",
    "process": 2,
    "thread": 128
  },
  "styles": {
    "dark": {
      "style": "dark/style.json"
    },
    "bright": {
      "style": "bright/style.json"
    },
    "fiord": {
      "style": "fiord/style.json"
    },
    "liberty": {
      "style": "liberty/style.json"
    },
    "openstreetmap": {
      "style": "openstreetmap/style.json"
    },
    "positron": {
      "style": "positron/style.json"
    },
    "toner": {
      "style": "toner/style.json"
    }
  },
  "geojsons": {
    "admin": {
      "quocgia": {
        "geojson": "admin/quocgia.geojson"
      },
      "tinh": {
        "geojson": "admin/tinh.geojson"
      },
      "xa": {
        "geojson": "admin/xa.geojson"
      }
    }
  },
  "datas": {
    "satellite_google": {
      "mbtiles": "satellite_google_cache",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "satellite_arcgis": {
      "mbtiles": "satellite_arcgis_cache",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "openstreetmap": {
      "pmtiles": "https://data.source.coop/protomaps/openstreetmap/tiles/v3.pmtiles"
    },
    "ODbL_firenze": {
      "pmtiles": "ODbL_firenze/ODbL_firenze.pmtiles"
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
    }
  },
  "sprites": {
    "bright": {
      "sprite": "bright"
    },
    "dark": {
      "sprite": "dark"
    },
    "fiord": {
      "sprite": "fiord"
    },
    "liberty": {
      "sprite": "liberty"
    },
    "openstreetmap": {
      "sprite": "openstreetmap"
    },
    "positron": {
      "sprite": "positron"
    },
    "toner": {
      "sprite": "toner"
    },
    "topo": {
      "sprite": "topo"
    }
  },
  "fonts": {
    "Open Sans Italic": {
      "font": "Open Sans Italic",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "Open Sans Bold": {
      "font": "Open Sans Bold",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "Open Sans Regular": {
      "font": "Open Sans Regular",
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
    },
    "Noto Sans Bold": {
      "font": "Noto Sans Bold",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "Noto Sans Italic": {
      "font": "Noto Sans Italic",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "Roboto Medium": {
      "font": "Roboto Medium",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "Roboto Regular": {
      "font": "Roboto Regular",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "Roboto Bold": {
      "font": "Roboto Bold",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "Roboto Italic": {
      "font": "Roboto Italic",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "Roboto Condensed Italic": {
      "font": "Roboto Condensed Italic",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "Roboto Condensed Regular": {
      "font": "Roboto Condensed Regular",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "Roboto Condensed Bold": {
      "font": "Roboto Condensed Bold",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "Frutiger Neue Condensed Italic": {
      "font": "Frutiger Neue Condensed Italic",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "Frutiger Neue Condensed Regular": {
      "font": "Frutiger Neue Condensed Regular",
      "cache": {
        "forward": true,
        "store": true
      }
    },
    "Frutiger Neue Condensed Bold": {
      "font": "Frutiger Neue Condensed Bold",
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
  "styles": {},
  "geojsons": {},
  "datas": {
    "osm_cache": {
      "metadata": {
        "name": "osm",
        "description": "osm",
        "format": "png",
        "bounds": [-180, -85.051129, 180, 85.051129],
        "center": [108, 16, 10],
        "minzoom": 11,
        "maxzoom": 11
      },
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
      "coverages": [
        {
          "bbox": [-180, -85.051129, 180, 85.051129],
          "zoom": 11
        }
      ],
      "timeout": 20000,
      "concurrency": 50,
      "maxTry": 5,
      "storeType": "mbtiles",
      "storeTransparent": false,
      "skip": true
    },
    "satellite_google_cache": {
      "metadata": {
        "name": "Satellite Google",
        "description": "Satellite Google",
        "format": "jpeg",
        "bounds": [-180, -85.051129, 180, 85.051129],
        "center": [108, 16, 9],
        "minzoom": 0,
        "maxzoom": 18
      },
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "url": "https://mt0.google.com/vt/lyrs=s&x={x}&y={y}&z={z}",
      "coverages": [
        {
          "bbox": [-180, -85.051129, 180, 85.051129],
          "zoom": 0
        },
        {
          "bbox": [-180, -85.051129, 180, 85.051129],
          "zoom": 1
        },
        {
          "bbox": [-180, -85.051129, 180, 85.051129],
          "zoom": 2
        },
        {
          "bbox": [-180, -85.051129, 180, 85.051129],
          "zoom": 3
        },
        {
          "bbox": [-180, -85.051129, 180, 85.051129],
          "zoom": 4
        },
        {
          "bbox": [-180, -85.051129, 180, 85.051129],
          "zoom": 5
        },
        {
          "bbox": [-180, -85.051129, 180, 85.051129],
          "zoom": 6
        },
        {
          "bbox": [-180, -85.051129, 180, 85.051129],
          "zoom": 7
        },
        {
          "bbox": [-180, -85.051129, 180, 85.051129],
          "zoom": 8
        },
        {
          "bbox": [-180, -85.051129, 180, 85.051129],
          "zoom": 9
        },
        {
          "bbox": [-180, -85.051129, 180, 85.051129],
          "zoom": 10
        },
        {
          "bbox": [96, 4, 120, 28],
          "zoom": 11
        },
        {
          "bbox": [96, 4, 120, 28],
          "zoom": 12
        },
        {
          "bbox": [100, 8, 110, 24],
          "zoom": 13
        },
        {
          "bbox": [100, 8, 110, 24],
          "zoom": 14
        },
        {
          "bbox": [102, 8, 110, 24],
          "zoom": 15
        },
        {
          "bbox": [102, 8, 110, 24],
          "zoom": 16
        },
        {
          "bbox": [104.78485107421875, 23, 105.82031250000001, 23.5],
          "zoom": 17
        },
        {
          "bbox": [102.11266173642161, 22.01436065310322, 106.842041015625, 23],
          "zoom": 17
        },
        {
          "bbox": [102.46948242187501, 21.54506606426624, 108.02307128906251, 22.01436065310322],
          "zoom": 17
        },
        {
          "bbox": [102.81005859375, 20.673905264672843, 108.02307128906251, 21.54506606426624],
          "zoom": 17
        },
        {
          "bbox": [104.36183562406808, 19.896331213634156, 106.62066771804632, 20.673905264672843],
          "zoom": 17
        },
        {
          "bbox": [103.85627556338335, 18.675174012686966, 105.949538812777, 19.896331213634156],
          "zoom": 17
        },
        {
          "bbox": [105.09033044608685, 18.247006521537713, 106.11631501265589, 18.675174012686966],
          "zoom": 17
        },
        {
          "bbox": [105.08569377073371, 17.682479646533977, 106.54541015625, 18.247006521537713],
          "zoom": 17
        },
        {
          "bbox": [105.72940942553402, 17.012506420381214, 107.1332187063714, 17.682479646533977],
          "zoom": 17
        },
        {
          "bbox": [106.52474296141287, 16.525632239869275, 107.73742675781251, 17.012506420381214],
          "zoom": 17
        },
        {
          "bbox": [106.52474296141287, 15.87336009641129, 108.4130859375, 16.525632239869275],
          "zoom": 17
        },
        {
          "bbox": [107.19248569402788, 12.367312668121698, 109.5, 15.87336009641129],
          "zoom": 17
        },
        {
          "bbox": [105.780413191197, 11.055370895327243, 109.37751394557404, 12.367312668121698],
          "zoom": 17
        },
        {
          "bbox": [104.40660624946082, 10.293301000109102, 108.36364746093751, 11.055370895327243],
          "zoom": 17
        },
        {
          "bbox": [104.40660624946082, 8.5, 106.80872190635347, 10.293301000109102],
          "zoom": 17
        },
        {
          "bbox": [103.82593390474472, 9.89372811153305, 104.08596902176743, 10.466205555063882],
          "zoom": 17
        },
        {
          "bbox": [104.78485107421875, 23, 105.82031250000001, 23.5],
          "zoom": 18
        },
        {
          "bbox": [102.11266173642161, 22.01436065310322, 106.842041015625, 23],
          "zoom": 18
        },
        {
          "bbox": [102.46948242187501, 21.54506606426624, 108.02307128906251, 22.01436065310322],
          "zoom": 18
        },
        {
          "bbox": [102.81005859375, 20.673905264672843, 108.02307128906251, 21.54506606426624],
          "zoom": 18
        },
        {
          "bbox": [104.36183562406808, 19.896331213634156, 106.62066771804632, 20.673905264672843],
          "zoom": 18
        },
        {
          "bbox": [103.85627556338335, 18.675174012686966, 105.949538812777, 19.896331213634156],
          "zoom": 18
        },
        {
          "bbox": [105.09033044608685, 18.247006521537713, 106.11631501265589, 18.675174012686966],
          "zoom": 18
        },
        {
          "bbox": [105.08569377073371, 17.682479646533977, 106.54541015625, 18.247006521537713],
          "zoom": 18
        },
        {
          "bbox": [105.72940942553402, 17.012506420381214, 107.1332187063714, 17.682479646533977],
          "zoom": 18
        },
        {
          "bbox": [106.52474296141287, 16.525632239869275, 107.73742675781251, 17.012506420381214],
          "zoom": 18
        },
        {
          "bbox": [106.52474296141287, 15.87336009641129, 108.4130859375, 16.525632239869275],
          "zoom": 18
        },
        {
          "bbox": [107.19248569402788, 12.367312668121698, 109.5, 15.87336009641129],
          "zoom": 18
        },
        {
          "bbox": [105.780413191197, 11.055370895327243, 109.37751394557404, 12.367312668121698],
          "zoom": 18
        },
        {
          "bbox": [104.40660624946082, 10.293301000109102, 108.36364746093751, 11.055370895327243],
          "zoom": 18
        },
        {
          "bbox": [104.40660624946082, 8.5, 106.80872190635347, 10.293301000109102],
          "zoom": 18
        },
        {
          "bbox": [103.82593390474472, 9.89372811153305, 104.08596902176743, 10.466205555063882],
          "zoom": 18
        },
        {
          "bbox": [105.5, 20.5, 106.5, 21.5],
          "zoom": 19
        }
      ],
      "timeout": 20000,
      "concurrency": 50,
      "maxTry": 5,
      "storeType": "mbtiles",
      "storeTransparent": true,
      "skip": false
    },
    "satellite_arcgis_cache": {
      "metadata": {
        "name": "Satellite ArcGIS",
        "description": "Satellite ArcGIS",
        "format": "jpeg",
        "bounds": [-180, -85.051129, 180, 85.051129],
        "center": [108, 16, 9],
        "minzoom": 11,
        "maxzoom": 11
      },
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "url": "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
      "coverages": [
        {
          "bbox": [-180, -85.051129, 180, 85.051129],
          "zoom": 11
        }
      ],
      "timeout": 20000,
      "concurrency": 50,
      "maxTry": 5,
      "storeType": "mbtiles",
      "storeTransparent": true,
      "skip": false
    },
    "planet_cache": {
      "metadata": {
        "name": "planet",
        "description": "planet",
        "format": "pbf",
        "bounds": [96, 4, 120, 28],
        "center": [108, 16, 9],
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
          "bbox": [108, 20, 114, 28],
          "zoom": 0
        },
        {
          "bbox": [96, 8, 102, 16],
          "zoom": 5
        },
        {
          "bbox": [96, 8, 102, 16],
          "zoom": 10
        },
        {
          "bbox": [96, 4, 120, 28],
          "zoom": 10
        }
      ],
      "timeout": 60000,
      "concurrency": 100,
      "maxTry": 5,
      "storeType": "mbtiles",
      "storeTransparent": false,
      "skip": true
    }
  },
  "sprites": {
    "common": {
      "url": "https://api.maptiler.com/sprites/common/{name}",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "maxTry": 5
    }
  },
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
    "Roboto Italic": {
      "url": "https://api.maptiler.com/fonts/Roboto Italic/{range}.pbf?key=aXcjPEauI4sBZOUkbLlP&mtsid=d7a93ef3-ffe6-4930-aa29-e9533fa57b83",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Roboto Bold": {
      "url": "https://api.maptiler.com/fonts/Roboto Bold/{range}.pbf?key=aXcjPEauI4sBZOUkbLlP&mtsid=d7a93ef3-ffe6-4930-aa29-e9533fa57b83",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Roboto Regular": {
      "url": "https://api.maptiler.com/fonts/Roboto Regular/{range}.pbf?key=aXcjPEauI4sBZOUkbLlP&mtsid=d7a93ef3-ffe6-4930-aa29-e9533fa57b83",
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
    },
    "Noto Sans Italic": {
      "url": "https://api.maptiler.com/fonts/Noto Sans Italic/{range}.pbf?key=aXcjPEauI4sBZOUkbLlP&mtsid=d7a93ef3-ffe6-4930-aa29-e9533fa57b83",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Noto Sans Bold": {
      "url": "https://api.maptiler.com/fonts/Noto Sans Bold/{range}.pbf?key=aXcjPEauI4sBZOUkbLlP&mtsid=d7a93ef3-ffe6-4930-aa29-e9533fa57b83",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Open Sans Bold": {
      "url": "https://api.maptiler.com/fonts/Open Sans Bold/{range}.pbf?key=aXcjPEauI4sBZOUkbLlP&mtsid=d7a93ef3-ffe6-4930-aa29-e9533fa57b83",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Open Sans Italic": {
      "url": "https://api.maptiler.com/fonts/Open Sans Italic/{range}.pbf?key=aXcjPEauI4sBZOUkbLlP&mtsid=d7a93ef3-ffe6-4930-aa29-e9533fa57b83",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Open Sans Regular": {
      "url": "https://api.maptiler.com/fonts/Open Sans Regular/{range}.pbf?key=aXcjPEauI4sBZOUkbLlP&mtsid=d7a93ef3-ffe6-4930-aa29-e9533fa57b83",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Roboto Condensed Italic": {
      "url": "https://api.maptiler.com/fonts/Roboto Condensed Italic/{range}.pbf?key=aXcjPEauI4sBZOUkbLlP&mtsid=d7a93ef3-ffe6-4930-aa29-e9533fa57b83",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Roboto Condensed Regular": {
      "url": "https://api.maptiler.com/fonts/Roboto Condensed Regular/{range}.pbf?key=aXcjPEauI4sBZOUkbLlP&mtsid=d7a93ef3-ffe6-4930-aa29-e9533fa57b83",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Roboto Condensed Bold": {
      "url": "https://api.maptiler.com/fonts/Roboto Condensed Bold/{range}.pbf?key=aXcjPEauI4sBZOUkbLlP&mtsid=d7a93ef3-ffe6-4930-aa29-e9533fa57b83",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Frutiger Neue Condensed Italic": {
      "url": "https://api.maptiler.com/fonts/Frutiger Neue Condensed Italic/{range}.pbf?key=aXcjPEauI4sBZOUkbLlP&mtsid=d7a93ef3-ffe6-4930-aa29-e9533fa57b83",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Frutiger Neue Condensed Regular": {
      "url": "https://api.maptiler.com/fonts/Frutiger Neue Condensed Regular/{range}.pbf?key=aXcjPEauI4sBZOUkbLlP&mtsid=d7a93ef3-ffe6-4930-aa29-e9533fa57b83",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Frutiger Neue Condensed Bold": {
      "url": "https://api.maptiler.com/fonts/Frutiger Neue Condensed Bold/{range}.pbf?key=aXcjPEauI4sBZOUkbLlP&mtsid=d7a93ef3-ffe6-4930-aa29-e9533fa57b83",
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
          "bbox": [96, 8, 102, 16],
          "zoom": 0
        },
        {
          "bbox": [96, 8, 102, 16],
          "zoom": 5
        },
        {
          "bbox": [108, 20, 114, 28],
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

## Example export style

```json
{
  "id": "osm_style_export",
  "metadata": {
    "name": "osm_style",
    "description": "osm_style",
    "format": "png",
    "bounds": [104.55758541249514, 20.40418806657307, 106.68188322743384, 22.266067290668104],
    "center": [108, 5, 10],
    "minzoom": 10,
    "maxzoom": 10
  },
  "refreshBefore": {
    "time": "2024-10-10T00:00:00"
  },
  "concurrency": 50,
  "storeType": "mbtiles",
  "storeTransparent": true
}
```

## Example render data

```json
{
  "id": "satellite",
  "metadata": {
    "name": "satellite",
    "description": "satellite",
    "format": "png",
    "bounds": [96, 4, 120, 28],
    "center": [105.8, 21.0, 10],
    "minzoom": 0,
    "maxzoom": 5
  },
  "refreshBefore": {
    "time": "2024-10-10T00:00:00"
  },
  "concurrency": 100,
  "storeType": "mbtiles",
  "storeTransparent": true,
  "coverages": [
    {
      "zoom": 0,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    },
    {
      "zoom": 1,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    },
    {
      "zoom": 2,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    },
    {
      "zoom": 3,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    },
    {
      "zoom": 4,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    },
    {
      "zoom": 5,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    }
  ]
}
```

## Example render styleJSON

```json
{
  "zoom": 9,
  "bbox": [96, 4, 120, 28],
  "tileScale": 1,
  "tileSize": 512,
  "styleId": "openstreetmap",
  "format": "png"
}
```

```json
{
  "zoom": 9.5,
  "bbox": [96, 4, 120, 28],
  "tileScale": 1,
  "tileSize": 512,
  "styleJSON": {
    "version": 8,
    "id": "liberty",
    "name": "Liberty",
    "zoom": 10,
    "center": [105.827799, 21.03116],
    "bearing": 0,
    "pitch": 0,
    "sources": {
      "source": {
        "url": "http://localhost:8080/datas/osm-vietnam.json",
        "type": "vector"
      }
    },
    "sprite": "http://localhost:8080/sprites/liberty/sprite",
    "glyphs": "http://localhost:8080/fonts/{fontstack}/{range}.pbf",
    "layers": [
      {
        "id": "background",
        "type": "background",
        "paint": {
          "background-color": "rgb(239,239,239)"
        }
      },
      {
        "id": "park",
        "type": "fill",
        "source": "source",
        "source-layer": "park",
        "paint": {
          "fill-color": "#d8e8c8",
          "fill-opacity": 0.7,
          "fill-outline-color": "rgba(95, 208, 100, 1)"
        }
      },
      {
        "id": "park_outline",
        "type": "line",
        "source": "source",
        "source-layer": "park",
        "paint": {
          "line-dasharray": [1, 1.5],
          "line-color": "rgba(228, 241, 215, 1)"
        }
      },
      {
        "id": "landuse_residential",
        "type": "fill",
        "source": "source",
        "source-layer": "landuse",
        "maxzoom": 8,
        "filter": ["==", "class", "residential"],
        "paint": {
          "fill-color": {
            "base": 1,
            "stops": [
              [9, "hsla(0, 3%, 85%, 0.84)"],
              [12, "hsla(35, 57%, 88%, 0.49)"]
            ]
          }
        }
      },
      {
        "id": "landcover_wood",
        "type": "fill",
        "source": "source",
        "source-layer": "landcover",
        "filter": ["all", ["==", "class", "wood"]],
        "paint": {
          "fill-antialias": false,
          "fill-color": "hsla(98, 61%, 72%, 0.7)",
          "fill-opacity": 0.4
        }
      },
      {
        "id": "landcover_grass",
        "type": "fill",
        "source": "source",
        "source-layer": "landcover",
        "filter": ["all", ["==", "class", "grass"]],
        "paint": {
          "fill-antialias": false,
          "fill-color": "rgba(176, 213, 154, 1)",
          "fill-opacity": 0.3
        }
      },
      {
        "id": "landcover_ice",
        "type": "fill",
        "source": "source",
        "source-layer": "landcover",
        "filter": ["all", ["==", "class", "ice"]],
        "paint": {
          "fill-antialias": false,
          "fill-color": "rgba(224, 236, 236, 1)",
          "fill-opacity": 0.8
        }
      },
      {
        "id": "landcover_wetland",
        "type": "fill",
        "source": "source",
        "source-layer": "landcover",
        "minzoom": 12,
        "filter": ["all", ["==", "class", "wetland"]],
        "paint": {
          "fill-antialias": true,
          "fill-opacity": 0.8,
          "fill-pattern": "wetland_bg_11",
          "fill-translate-anchor": "map"
        }
      },
      {
        "id": "landuse_pitch",
        "type": "fill",
        "source": "source",
        "source-layer": "landuse",
        "filter": ["==", "class", "pitch"],
        "paint": {
          "fill-color": "#DEE3CD"
        }
      },
      {
        "id": "landuse_track",
        "type": "fill",
        "source": "source",
        "source-layer": "landuse",
        "filter": ["==", "class", "track"],
        "paint": {
          "fill-color": "#DEE3CD"
        }
      },
      {
        "id": "landuse_cemetery",
        "type": "fill",
        "source": "source",
        "source-layer": "landuse",
        "filter": ["==", "class", "cemetery"],
        "paint": {
          "fill-color": "hsl(75, 37%, 81%)"
        }
      },
      {
        "id": "landuse_hospital",
        "type": "fill",
        "source": "source",
        "source-layer": "landuse",
        "filter": ["==", "class", "hospital"],
        "paint": {
          "fill-color": "#fde"
        }
      },
      {
        "id": "landuse_school",
        "type": "fill",
        "source": "source",
        "source-layer": "landuse",
        "filter": ["==", "class", "school"],
        "paint": {
          "fill-color": "rgb(236,238,204)"
        }
      },
      {
        "id": "waterway_tunnel",
        "type": "line",
        "source": "source",
        "source-layer": "waterway",
        "filter": ["all", ["==", "brunnel", "tunnel"]],
        "paint": {
          "line-color": "#a0c8f0",
          "line-dasharray": [3, 3],
          "line-gap-width": {
            "stops": [
              [12, 0],
              [20, 6]
            ]
          },
          "line-opacity": 1,
          "line-width": {
            "base": 1.4,
            "stops": [
              [8, 1],
              [20, 2]
            ]
          }
        }
      },
      {
        "id": "waterway_river",
        "type": "line",
        "source": "source",
        "source-layer": "waterway",
        "filter": ["all", ["==", "class", "river"], ["!=", "brunnel", "tunnel"]],
        "layout": {
          "line-cap": "round"
        },
        "paint": {
          "line-color": "#a0c8f0",
          "line-width": {
            "base": 1.2,
            "stops": [
              [11, 0.5],
              [20, 6]
            ]
          }
        }
      },
      {
        "id": "waterway_other",
        "type": "line",
        "source": "source",
        "source-layer": "waterway",
        "filter": ["all", ["!=", "class", "river"], ["!=", "brunnel", "tunnel"]],
        "layout": {
          "line-cap": "round"
        },
        "paint": {
          "line-color": "#a0c8f0",
          "line-width": {
            "base": 1.3,
            "stops": [
              [13, 0.5],
              [20, 6]
            ]
          }
        }
      },
      {
        "id": "water",
        "type": "fill",
        "source": "source",
        "source-layer": "water",
        "filter": ["all", ["!=", "brunnel", "tunnel"]],
        "paint": {
          "fill-color": "rgb(158,189,255)"
        }
      },
      {
        "id": "landcover_sand",
        "type": "fill",
        "source": "source",
        "source-layer": "landcover",
        "filter": ["all", ["==", "class", "sand"]],
        "paint": {
          "fill-color": "rgba(247, 239, 195, 1)"
        }
      },
      {
        "id": "aeroway_fill",
        "type": "fill",
        "source": "source",
        "source-layer": "aeroway",
        "minzoom": 11,
        "filter": ["==", "$type", "Polygon"],
        "paint": {
          "fill-color": "rgba(229, 228, 224, 1)",
          "fill-opacity": 0.7
        }
      },
      {
        "id": "aeroway_runway",
        "type": "line",
        "source": "source",
        "source-layer": "aeroway",
        "minzoom": 11,
        "filter": ["all", ["==", "$type", "LineString"], ["==", "class", "runway"]],
        "paint": {
          "line-color": "#f0ede9",
          "line-width": {
            "base": 1.2,
            "stops": [
              [11, 3],
              [20, 16]
            ]
          }
        }
      },
      {
        "id": "aeroway_taxiway",
        "type": "line",
        "source": "source",
        "source-layer": "aeroway",
        "minzoom": 11,
        "filter": ["all", ["==", "$type", "LineString"], ["==", "class", "taxiway"]],
        "paint": {
          "line-color": "#f0ede9",
          "line-width": {
            "base": 1.2,
            "stops": [
              [11, 0.5],
              [20, 6]
            ]
          }
        }
      },
      {
        "id": "tunnel_motorway_link_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "class", "motorway"], ["==", "ramp", 1], ["==", "brunnel", "tunnel"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#e9ac77",
          "line-dasharray": [0.5, 0.25],
          "line-width": {
            "base": 1.2,
            "stops": [
              [12, 1],
              [13, 3],
              [14, 4],
              [20, 15]
            ]
          }
        }
      },
      {
        "id": "tunnel_service_track_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "tunnel"], ["in", "class", "service", "track"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#cfcdca",
          "line-dasharray": [0.5, 0.25],
          "line-width": {
            "base": 1.2,
            "stops": [
              [15, 1],
              [16, 4],
              [20, 11]
            ]
          }
        }
      },
      {
        "id": "tunnel_link_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "ramp", 1], ["==", "brunnel", "tunnel"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#e9ac77",
          "line-width": {
            "base": 1.2,
            "stops": [
              [12, 1],
              [13, 3],
              [14, 4],
              [20, 15]
            ]
          }
        }
      },
      {
        "id": "tunnel_street_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "tunnel"], ["in", "class", "street", "street_limited"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#cfcdca",
          "line-opacity": {
            "stops": [
              [12, 0],
              [12.5, 1]
            ]
          },
          "line-width": {
            "base": 1.2,
            "stops": [
              [12, 0.5],
              [13, 1],
              [14, 4],
              [20, 15]
            ]
          }
        }
      },
      {
        "id": "tunnel_secondary_tertiary_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "tunnel"], ["in", "class", "secondary", "tertiary"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#e9ac77",
          "line-width": {
            "base": 1.2,
            "stops": [
              [8, 1.5],
              [20, 17]
            ]
          }
        }
      },
      {
        "id": "tunnel_trunk_primary_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "tunnel"], ["in", "class", "primary", "trunk"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#e9ac77",
          "line-width": {
            "base": 1.2,
            "stops": [
              [5, 0.4],
              [6, 0.7],
              [7, 1.75],
              [20, 22]
            ]
          }
        }
      },
      {
        "id": "tunnel_motorway_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "class", "motorway"], ["!=", "ramp", 1], ["==", "brunnel", "tunnel"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#e9ac77",
          "line-dasharray": [0.5, 0.25],
          "line-width": {
            "base": 1.2,
            "stops": [
              [5, 0.4],
              [6, 0.7],
              [7, 1.75],
              [20, 22]
            ]
          }
        }
      },
      {
        "id": "tunnel_path_pedestrian",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "$type", "LineString"], ["==", "brunnel", "tunnel"], ["in", "class", "path", "pedestrian"]],
        "paint": {
          "line-color": "hsl(0, 0%, 100%)",
          "line-dasharray": [1, 0.75],
          "line-width": {
            "base": 1.2,
            "stops": [
              [14, 0.5],
              [20, 10]
            ]
          }
        }
      },
      {
        "id": "tunnel_motorway_link",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "class", "motorway"], ["==", "ramp", 1], ["==", "brunnel", "tunnel"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fc8",
          "line-width": {
            "base": 1.2,
            "stops": [
              [12.5, 0],
              [13, 1.5],
              [14, 2.5],
              [20, 11.5]
            ]
          }
        }
      },
      {
        "id": "tunnel_service_track",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "tunnel"], ["in", "class", "service", "track"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fff",
          "line-width": {
            "base": 1.2,
            "stops": [
              [15.5, 0],
              [16, 2],
              [20, 7.5]
            ]
          }
        }
      },
      {
        "id": "tunnel_link",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "ramp", 1], ["==", "brunnel", "tunnel"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fff4c6",
          "line-width": {
            "base": 1.2,
            "stops": [
              [12.5, 0],
              [13, 1.5],
              [14, 2.5],
              [20, 11.5]
            ]
          }
        }
      },
      {
        "id": "tunnel_minor",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "tunnel"], ["in", "class", "minor"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fff",
          "line-width": {
            "base": 1.2,
            "stops": [
              [13.5, 0],
              [14, 2.5],
              [20, 11.5]
            ]
          }
        }
      },
      {
        "id": "tunnel_secondary_tertiary",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "tunnel"], ["in", "class", "secondary", "tertiary"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fff4c6",
          "line-width": {
            "base": 1.2,
            "stops": [
              [6.5, 0],
              [7, 0.5],
              [20, 10]
            ]
          }
        }
      },
      {
        "id": "tunnel_trunk_primary",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "tunnel"], ["in", "class", "primary", "trunk"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fff4c6",
          "line-width": {
            "base": 1.2,
            "stops": [
              [5, 0],
              [7, 1],
              [20, 18]
            ]
          }
        }
      },
      {
        "id": "tunnel_motorway",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "class", "motorway"], ["!=", "ramp", 1], ["==", "brunnel", "tunnel"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#ffdaa6",
          "line-width": {
            "base": 1.2,
            "stops": [
              [5, 0],
              [7, 1],
              [20, 18]
            ]
          }
        }
      },
      {
        "id": "tunnel_major_rail",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "tunnel"], ["in", "class", "rail"]],
        "paint": {
          "line-color": "#bbb",
          "line-width": {
            "base": 1.4,
            "stops": [
              [14, 0.4],
              [15, 0.75],
              [20, 2]
            ]
          }
        }
      },
      {
        "id": "tunnel_major_rail_hatching",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "tunnel"], ["==", "class", "rail"]],
        "paint": {
          "line-color": "#bbb",
          "line-dasharray": [0.2, 8],
          "line-width": {
            "base": 1.4,
            "stops": [
              [14.5, 0],
              [15, 3],
              [20, 8]
            ]
          }
        }
      },
      {
        "id": "tunnel_transit_rail",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "tunnel"], ["in", "class", "transit"]],
        "paint": {
          "line-color": "#bbb",
          "line-width": {
            "base": 1.4,
            "stops": [
              [14, 0.4],
              [15, 0.75],
              [20, 2]
            ]
          }
        }
      },
      {
        "id": "tunnel_transit_rail_hatching",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "tunnel"], ["==", "class", "transit"]],
        "paint": {
          "line-color": "#bbb",
          "line-dasharray": [0.2, 8],
          "line-width": {
            "base": 1.4,
            "stops": [
              [14.5, 0],
              [15, 3],
              [20, 8]
            ]
          }
        }
      },
      {
        "id": "road_area_pattern",
        "type": "fill",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "$type", "Polygon"]],
        "paint": {
          "fill-pattern": "pedestrian_polygon"
        }
      },
      {
        "id": "road_motorway_link_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 12,
        "filter": ["all", ["!in", "brunnel", "bridge", "tunnel"], ["==", "class", "motorway"], ["==", "ramp", 1]],
        "layout": {
          "line-cap": "round",
          "line-join": "round"
        },
        "paint": {
          "line-color": "#e9ac77",
          "line-width": {
            "base": 1.2,
            "stops": [
              [12, 1],
              [13, 3],
              [14, 4],
              [20, 15]
            ]
          }
        }
      },
      {
        "id": "road_service_track_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["!in", "brunnel", "bridge", "tunnel"], ["in", "class", "service", "track"]],
        "layout": {
          "line-cap": "round",
          "line-join": "round"
        },
        "paint": {
          "line-color": "#cfcdca",
          "line-width": {
            "base": 1.2,
            "stops": [
              [15, 1],
              [16, 4],
              [20, 11]
            ]
          }
        }
      },
      {
        "id": "road_link_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 13,
        "filter": ["all", ["!in", "brunnel", "bridge", "tunnel"], ["!in", "class", "pedestrian", "path", "track", "service", "motorway"], ["==", "ramp", 1]],
        "layout": {
          "line-cap": "round",
          "line-join": "round"
        },
        "paint": {
          "line-color": "#e9ac77",
          "line-width": {
            "base": 1.2,
            "stops": [
              [12, 1],
              [13, 3],
              [14, 4],
              [20, 15]
            ]
          }
        }
      },
      {
        "id": "road_minor_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "$type", "LineString"], ["!in", "brunnel", "bridge", "tunnel"], ["in", "class", "minor"], ["!=", "ramp", 1]],
        "layout": {
          "line-cap": "round",
          "line-join": "round"
        },
        "paint": {
          "line-color": "#cfcdca",
          "line-opacity": {
            "stops": [
              [12, 0],
              [12.5, 1]
            ]
          },
          "line-width": {
            "base": 1.2,
            "stops": [
              [12, 0.5],
              [13, 1],
              [14, 4],
              [20, 20]
            ]
          }
        }
      },
      {
        "id": "road_secondary_tertiary_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["!in", "brunnel", "bridge", "tunnel"], ["in", "class", "secondary", "tertiary"], ["!=", "ramp", 1]],
        "layout": {
          "line-cap": "round",
          "line-join": "round"
        },
        "paint": {
          "line-color": "#e9ac77",
          "line-width": {
            "base": 1.2,
            "stops": [
              [8, 1.5],
              [20, 17]
            ]
          }
        }
      },
      {
        "id": "road_trunk_primary_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["!in", "brunnel", "bridge", "tunnel"], ["in", "class", "primary", "trunk"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#e9ac77",
          "line-width": {
            "base": 1.2,
            "stops": [
              [5, 0.4],
              [6, 0.7],
              [7, 1.75],
              [20, 22]
            ]
          }
        }
      },
      {
        "id": "road_motorway_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 5,
        "filter": ["all", ["!in", "brunnel", "bridge", "tunnel"], ["==", "class", "motorway"], ["!=", "ramp", 1]],
        "layout": {
          "line-cap": "round",
          "line-join": "round"
        },
        "paint": {
          "line-color": "#e9ac77",
          "line-width": {
            "base": 1.2,
            "stops": [
              [5, 0.4],
              [6, 0.7],
              [7, 1.75],
              [20, 22]
            ]
          }
        }
      },
      {
        "id": "road_path_pedestrian",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 14,
        "filter": ["all", ["==", "$type", "LineString"], ["!in", "brunnel", "bridge", "tunnel"], ["in", "class", "path", "pedestrian"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "hsl(0, 0%, 100%)",
          "line-dasharray": [1, 0.7],
          "line-width": {
            "base": 1.2,
            "stops": [
              [14, 1],
              [20, 10]
            ]
          }
        }
      },
      {
        "id": "road_motorway_link",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 12,
        "filter": ["all", ["!in", "brunnel", "bridge", "tunnel"], ["==", "class", "motorway"], ["==", "ramp", 1]],
        "layout": {
          "line-cap": "round",
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fc8",
          "line-width": {
            "base": 1.2,
            "stops": [
              [12.5, 0],
              [13, 1.5],
              [14, 2.5],
              [20, 11.5]
            ]
          }
        }
      },
      {
        "id": "road_service_track",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["!in", "brunnel", "bridge", "tunnel"], ["in", "class", "service", "track"]],
        "layout": {
          "line-cap": "round",
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fff",
          "line-width": {
            "base": 1.2,
            "stops": [
              [15.5, 0],
              [16, 2],
              [20, 7.5]
            ]
          }
        }
      },
      {
        "id": "road_link",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 13,
        "filter": ["all", ["!in", "brunnel", "bridge", "tunnel"], ["==", "ramp", 1], ["!in", "class", "pedestrian", "path", "track", "service", "motorway"]],
        "layout": {
          "line-cap": "round",
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fea",
          "line-width": {
            "base": 1.2,
            "stops": [
              [12.5, 0],
              [13, 1.5],
              [14, 2.5],
              [20, 11.5]
            ]
          }
        }
      },
      {
        "id": "road_minor",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "$type", "LineString"], ["!in", "brunnel", "bridge", "tunnel"], ["in", "class", "minor"]],
        "layout": {
          "line-cap": "round",
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fff",
          "line-width": {
            "base": 1.2,
            "stops": [
              [13.5, 0],
              [14, 2.5],
              [20, 18]
            ]
          }
        }
      },
      {
        "id": "road_secondary_tertiary",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["!in", "brunnel", "bridge", "tunnel"], ["in", "class", "secondary", "tertiary"]],
        "layout": {
          "line-cap": "round",
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fea",
          "line-width": {
            "base": 1.2,
            "stops": [
              [6.5, 0],
              [8, 0.5],
              [20, 13]
            ]
          }
        }
      },
      {
        "id": "road_trunk_primary",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["!in", "brunnel", "bridge", "tunnel"], ["in", "class", "primary", "trunk"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fea",
          "line-width": {
            "base": 1.2,
            "stops": [
              [5, 0],
              [7, 1],
              [20, 18]
            ]
          }
        }
      },
      {
        "id": "road_motorway",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 5,
        "filter": ["all", ["!in", "brunnel", "bridge", "tunnel"], ["==", "class", "motorway"], ["!=", "ramp", 1]],
        "layout": {
          "line-cap": "round",
          "line-join": "round"
        },
        "paint": {
          "line-color": {
            "base": 1,
            "stops": [
              [5, "hsl(26, 87%, 62%)"],
              [6, "#fc8"]
            ]
          },
          "line-width": {
            "base": 1.2,
            "stops": [
              [5, 0],
              [7, 1],
              [20, 18]
            ]
          }
        }
      },
      {
        "id": "road_major_rail",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["!in", "brunnel", "bridge", "tunnel"], ["==", "class", "rail"]],
        "paint": {
          "line-color": "#bbb",
          "line-width": {
            "base": 1.4,
            "stops": [
              [14, 0.4],
              [15, 0.75],
              [20, 2]
            ]
          }
        }
      },
      {
        "id": "road_major_rail_hatching",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["!in", "brunnel", "bridge", "tunnel"], ["==", "class", "rail"]],
        "paint": {
          "line-color": "#bbb",
          "line-dasharray": [0.2, 8],
          "line-width": {
            "base": 1.4,
            "stops": [
              [14.5, 0],
              [15, 3],
              [20, 8]
            ]
          }
        }
      },
      {
        "id": "road_transit_rail",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["!in", "brunnel", "bridge", "tunnel"], ["==", "class", "transit"]],
        "paint": {
          "line-color": "#bbb",
          "line-width": {
            "base": 1.4,
            "stops": [
              [14, 0.4],
              [15, 0.75],
              [20, 2]
            ]
          }
        }
      },
      {
        "id": "road_transit_rail_hatching",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["!in", "brunnel", "bridge", "tunnel"], ["==", "class", "transit"]],
        "paint": {
          "line-color": "#bbb",
          "line-dasharray": [0.2, 8],
          "line-width": {
            "base": 1.4,
            "stops": [
              [14.5, 0],
              [15, 3],
              [20, 8]
            ]
          }
        }
      },
      {
        "id": "road_one_way_arrow",
        "type": "symbol",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 16,
        "filter": ["==", "oneway", 1],
        "layout": {
          "icon-image": "arrow",
          "symbol-placement": "line"
        }
      },
      {
        "id": "road_one_way_arrow_opposite",
        "type": "symbol",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 16,
        "filter": ["==", "oneway", -1],
        "layout": {
          "icon-image": "arrow",
          "symbol-placement": "line",
          "icon-rotate": 180
        }
      },
      {
        "id": "bridge_motorway_link_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "class", "motorway"], ["==", "ramp", 1], ["==", "brunnel", "bridge"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#e9ac77",
          "line-width": {
            "base": 1.2,
            "stops": [
              [12, 1],
              [13, 3],
              [14, 4],
              [20, 15]
            ]
          }
        }
      },
      {
        "id": "bridge_service_track_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "bridge"], ["in", "class", "service", "track"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#cfcdca",
          "line-width": {
            "base": 1.2,
            "stops": [
              [15, 1],
              [16, 4],
              [20, 11]
            ]
          }
        }
      },
      {
        "id": "bridge_link_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "class", "link"], ["==", "brunnel", "bridge"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#e9ac77",
          "line-width": {
            "base": 1.2,
            "stops": [
              [12, 1],
              [13, 3],
              [14, 4],
              [20, 15]
            ]
          }
        }
      },
      {
        "id": "bridge_street_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "bridge"], ["in", "class", "street", "street_limited"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "hsl(36, 6%, 74%)",
          "line-opacity": {
            "stops": [
              [12, 0],
              [12.5, 1]
            ]
          },
          "line-width": {
            "base": 1.2,
            "stops": [
              [12, 0.5],
              [13, 1],
              [14, 4],
              [20, 25]
            ]
          }
        }
      },
      {
        "id": "bridge_path_pedestrian_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "$type", "LineString"], ["==", "brunnel", "bridge"], ["in", "class", "path", "pedestrian"]],
        "paint": {
          "line-color": "hsl(35, 6%, 80%)",
          "line-dasharray": [1, 0],
          "line-width": {
            "base": 1.2,
            "stops": [
              [14, 1.5],
              [20, 18]
            ]
          }
        }
      },
      {
        "id": "bridge_secondary_tertiary_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "bridge"], ["in", "class", "secondary", "tertiary"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#e9ac77",
          "line-width": {
            "base": 1.2,
            "stops": [
              [8, 1.5],
              [20, 17]
            ]
          }
        }
      },
      {
        "id": "bridge_trunk_primary_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "bridge"], ["in", "class", "primary", "trunk"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#e9ac77",
          "line-width": {
            "base": 1.2,
            "stops": [
              [5, 0.4],
              [6, 0.7],
              [7, 1.75],
              [20, 22]
            ]
          }
        }
      },
      {
        "id": "bridge_motorway_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "class", "motorway"], ["!=", "ramp", 1], ["==", "brunnel", "bridge"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#e9ac77",
          "line-width": {
            "base": 1.2,
            "stops": [
              [5, 0.4],
              [6, 0.7],
              [7, 1.75],
              [20, 22]
            ]
          }
        }
      },
      {
        "id": "bridge_path_pedestrian",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "$type", "LineString"], ["==", "brunnel", "bridge"], ["in", "class", "path", "pedestrian"]],
        "paint": {
          "line-color": "hsl(0, 0%, 100%)",
          "line-dasharray": [1, 0.3],
          "line-width": {
            "base": 1.2,
            "stops": [
              [14, 0.5],
              [20, 10]
            ]
          }
        }
      },
      {
        "id": "bridge_motorway_link",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "class", "motorway"], ["==", "ramp", 1], ["==", "brunnel", "bridge"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fc8",
          "line-width": {
            "base": 1.2,
            "stops": [
              [12.5, 0],
              [13, 1.5],
              [14, 2.5],
              [20, 11.5]
            ]
          }
        }
      },
      {
        "id": "bridge_service_track",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "bridge"], ["in", "class", "service", "track"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fff",
          "line-width": {
            "base": 1.2,
            "stops": [
              [15.5, 0],
              [16, 2],
              [20, 7.5]
            ]
          }
        }
      },
      {
        "id": "bridge_link",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "class", "link"], ["==", "brunnel", "bridge"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fea",
          "line-width": {
            "base": 1.2,
            "stops": [
              [12.5, 0],
              [13, 1.5],
              [14, 2.5],
              [20, 11.5]
            ]
          }
        }
      },
      {
        "id": "bridge_street",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "bridge"], ["in", "class", "minor"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fff",
          "line-width": {
            "base": 1.2,
            "stops": [
              [13.5, 0],
              [14, 2.5],
              [20, 18]
            ]
          }
        }
      },
      {
        "id": "bridge_secondary_tertiary",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "bridge"], ["in", "class", "secondary", "tertiary"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fea",
          "line-width": {
            "base": 1.2,
            "stops": [
              [6.5, 0],
              [7, 0.5],
              [20, 10]
            ]
          }
        }
      },
      {
        "id": "bridge_trunk_primary",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "brunnel", "bridge"], ["in", "class", "primary", "trunk"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fea",
          "line-width": {
            "base": 1.2,
            "stops": [
              [5, 0],
              [7, 1],
              [20, 18]
            ]
          }
        }
      },
      {
        "id": "bridge_motorway",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "class", "motorway"], ["!=", "ramp", 1], ["==", "brunnel", "bridge"]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#fc8",
          "line-width": {
            "base": 1.2,
            "stops": [
              [5, 0],
              [7, 1],
              [20, 18]
            ]
          }
        }
      },
      {
        "id": "bridge_major_rail",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "class", "rail"], ["==", "brunnel", "bridge"]],
        "paint": {
          "line-color": "#bbb",
          "line-width": {
            "base": 1.4,
            "stops": [
              [14, 0.4],
              [15, 0.75],
              [20, 2]
            ]
          }
        }
      },
      {
        "id": "bridge_major_rail_hatching",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "class", "rail"], ["==", "brunnel", "bridge"]],
        "paint": {
          "line-color": "#bbb",
          "line-dasharray": [0.2, 8],
          "line-width": {
            "base": 1.4,
            "stops": [
              [14.5, 0],
              [15, 3],
              [20, 8]
            ]
          }
        }
      },
      {
        "id": "bridge_transit_rail",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "class", "transit"], ["==", "brunnel", "bridge"]],
        "paint": {
          "line-color": "#bbb",
          "line-width": {
            "base": 1.4,
            "stops": [
              [14, 0.4],
              [15, 0.75],
              [20, 2]
            ]
          }
        }
      },
      {
        "id": "bridge_transit_rail_hatching",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "class", "transit"], ["==", "brunnel", "bridge"]],
        "paint": {
          "line-color": "#bbb",
          "line-dasharray": [0.2, 8],
          "line-width": {
            "base": 1.4,
            "stops": [
              [14.5, 0],
              [15, 3],
              [20, 8]
            ]
          }
        }
      },
      {
        "id": "building",
        "type": "fill",
        "source": "source",
        "source-layer": "building",
        "minzoom": 13,
        "maxzoom": 14,
        "paint": {
          "fill-color": "hsl(35, 8%, 85%)",
          "fill-outline-color": {
            "base": 1,
            "stops": [
              [13, "hsla(35, 6%, 79%, 0.32)"],
              [14, "hsl(35, 6%, 79%)"]
            ]
          }
        }
      },
      {
        "id": "building-3d",
        "type": "fill-extrusion",
        "source": "source",
        "source-layer": "building",
        "minzoom": 14,
        "paint": {
          "fill-extrusion-color": "hsl(35, 8%, 85%)",
          "fill-extrusion-height": {
            "property": "render_height",
            "type": "identity"
          },
          "fill-extrusion-base": {
            "property": "render_min_height",
            "type": "identity"
          },
          "fill-extrusion-opacity": 0.8
        }
      },
      {
        "id": "boundary_3",
        "type": "line",
        "source": "source",
        "source-layer": "boundary",
        "minzoom": 8,
        "filter": ["all", ["in", "admin_level", 3, 4]],
        "layout": {
          "line-join": "round"
        },
        "paint": {
          "line-color": "#9e9cab",
          "line-dasharray": [5, 1],
          "line-width": {
            "base": 1,
            "stops": [
              [4, 0.4],
              [5, 1],
              [12, 1.8]
            ]
          }
        }
      },
      {
        "id": "boundary_2_z0-4",
        "type": "line",
        "source": "source",
        "source-layer": "boundary",
        "maxzoom": 5,
        "filter": ["all", ["==", "admin_level", 2], ["!has", "claimed_by"]],
        "layout": {
          "line-cap": "round",
          "line-join": "round"
        },
        "paint": {
          "line-color": "hsl(248, 1%, 41%)",
          "line-opacity": {
            "base": 1,
            "stops": [
              [0, 0.4],
              [4, 1]
            ]
          },
          "line-width": {
            "base": 1,
            "stops": [
              [3, 1],
              [5, 1.2],
              [12, 3]
            ]
          }
        }
      },
      {
        "id": "boundary_2_z5-",
        "type": "line",
        "source": "source",
        "source-layer": "boundary",
        "minzoom": 5,
        "filter": ["all", ["==", "admin_level", 2]],
        "layout": {
          "line-cap": "round",
          "line-join": "round"
        },
        "paint": {
          "line-color": "hsl(248, 1%, 41%)",
          "line-opacity": {
            "base": 1,
            "stops": [
              [0, 0.4],
              [4, 1]
            ]
          },
          "line-width": {
            "base": 1,
            "stops": [
              [3, 1],
              [5, 1.2],
              [12, 3]
            ]
          }
        }
      },
      {
        "id": "water_name_line",
        "type": "symbol",
        "source": "source",
        "source-layer": "waterway",
        "filter": ["all", ["==", "$type", "LineString"]],
        "layout": {
          "text-field": "{name}",
          "text-font": ["Roboto Regular"],
          "text-max-width": 5,
          "text-size": 12,
          "symbol-placement": "line"
        },
        "paint": {
          "text-color": "#5d60be",
          "text-halo-color": "rgba(255,255,255,0.7)",
          "text-halo-width": 1
        }
      },
      {
        "id": "water_name_point",
        "type": "symbol",
        "source": "source",
        "source-layer": "water_name",
        "filter": ["==", "$type", "Point"],
        "layout": {
          "text-field": "{name}",
          "text-font": ["Roboto Regular"],
          "text-max-width": 5,
          "text-size": 12
        },
        "paint": {
          "text-color": "#5d60be",
          "text-halo-color": "rgba(255,255,255,0.7)",
          "text-halo-width": 1
        }
      },
      {
        "id": "poi_z16",
        "type": "symbol",
        "source": "source",
        "source-layer": "poi",
        "minzoom": 16,
        "filter": ["all", ["==", "$type", "Point"], [">=", "rank", 20]],
        "layout": {
          "icon-image": ["match", ["get", "subclass"], ["florist", "furniture", "soccer", "tennis"], ["get", "subclass"], ["get", "class"]],
          "text-anchor": "top",
          "text-field": "{name}",
          "text-font": ["Roboto Condensed Italic"],
          "text-max-width": 9,
          "text-offset": [0, 0.6],
          "text-size": 12
        },
        "paint": {
          "text-color": "#666",
          "text-halo-blur": 0.5,
          "text-halo-color": "#ffffff",
          "text-halo-width": 1
        }
      },
      {
        "id": "poi_z15",
        "type": "symbol",
        "source": "source",
        "source-layer": "poi",
        "minzoom": 15,
        "filter": ["all", ["==", "$type", "Point"], [">=", "rank", 7], ["<", "rank", 20]],
        "layout": {
          "icon-image": ["match", ["get", "subclass"], ["florist", "furniture", "soccer", "tennis"], ["get", "subclass"], ["get", "class"]],
          "text-anchor": "top",
          "text-field": "{name}",
          "text-font": ["Roboto Condensed Italic"],
          "text-max-width": 9,
          "text-offset": [0, 0.6],
          "text-size": 12
        },
        "paint": {
          "text-color": "#666",
          "text-halo-blur": 0.5,
          "text-halo-color": "#ffffff",
          "text-halo-width": 1
        }
      },
      {
        "id": "poi_z14",
        "type": "symbol",
        "source": "source",
        "source-layer": "poi",
        "minzoom": 14,
        "filter": ["all", ["==", "$type", "Point"], [">=", "rank", 1], ["<", "rank", 7]],
        "layout": {
          "icon-image": ["match", ["get", "subclass"], ["florist", "furniture", "soccer", "tennis"], ["get", "subclass"], ["get", "class"]],
          "text-anchor": "top",
          "text-field": "{name}",
          "text-font": ["Roboto Condensed Italic"],
          "text-max-width": 9,
          "text-offset": [0, 0.6],
          "text-size": 12
        },
        "paint": {
          "text-color": "#666",
          "text-halo-blur": 0.5,
          "text-halo-color": "#ffffff",
          "text-halo-width": 1
        }
      },
      {
        "id": "poi_transit",
        "type": "symbol",
        "source": "source",
        "source-layer": "poi",
        "filter": ["all", ["in", "class", "bus", "rail", "airport"]],
        "layout": {
          "icon-image": "{class}",
          "text-anchor": "left",
          "text-field": "{name_en}",
          "text-font": ["Roboto Condensed Italic"],
          "text-max-width": 9,
          "text-offset": [0.9, 0],
          "text-size": 12
        },
        "paint": {
          "text-color": "#4898ff",
          "text-halo-blur": 0.5,
          "text-halo-color": "#ffffff",
          "text-halo-width": 1
        }
      },
      {
        "id": "road_label",
        "type": "symbol",
        "source": "source",
        "source-layer": "transportation_name",
        "filter": ["all"],
        "layout": {
          "symbol-placement": "line",
          "text-anchor": "center",
          "text-field": "{name}",
          "text-font": ["Roboto Regular"],
          "text-offset": [0, 0.15],
          "text-size": {
            "base": 1,
            "stops": [
              [13, 12],
              [14, 13]
            ]
          }
        },
        "paint": {
          "text-color": "#765",
          "text-halo-blur": 0.5,
          "text-halo-width": 1
        }
      },
      {
        "id": "road_shield",
        "type": "symbol",
        "source": "source",
        "source-layer": "transportation_name",
        "minzoom": 7,
        "filter": ["all", ["<=", "ref_length", 6]],
        "layout": {
          "icon-image": "default_{ref_length}",
          "icon-rotation-alignment": "viewport",
          "symbol-placement": {
            "base": 1,
            "stops": [
              [10, "point"],
              [11, "line"]
            ]
          },
          "symbol-spacing": 500,
          "text-field": "{ref}",
          "text-font": ["Roboto Regular"],
          "text-offset": [0, 0.1],
          "text-rotation-alignment": "viewport",
          "text-size": 10,
          "icon-size": 0.8
        }
      },
      {
        "id": "place_other",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "filter": ["all", ["in", "class", "hamlet", "island", "islet", "neighbourhood", "suburb", "quarter"]],
        "layout": {
          "text-field": "{name_en}",
          "text-font": ["Roboto Condensed Italic"],
          "text-letter-spacing": 0.1,
          "text-max-width": 9,
          "text-size": {
            "base": 1.2,
            "stops": [
              [12, 10],
              [15, 14]
            ]
          },
          "text-transform": "uppercase"
        },
        "paint": {
          "text-color": "#633",
          "text-halo-color": "rgba(255,255,255,0.8)",
          "text-halo-width": 1.2
        }
      },
      {
        "id": "place_village",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "filter": ["all", ["==", "class", "village"]],
        "layout": {
          "text-field": "{name_en}",
          "text-font": ["Roboto Regular"],
          "text-max-width": 8,
          "text-size": {
            "base": 1.2,
            "stops": [
              [10, 12],
              [15, 22]
            ]
          }
        },
        "paint": {
          "text-color": "#333",
          "text-halo-color": "rgba(255,255,255,0.8)",
          "text-halo-width": 1.2
        }
      },
      {
        "id": "place_town",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "filter": ["all", ["==", "class", "town"]],
        "layout": {
          "icon-image": {
            "base": 1,
            "stops": [
              [0, "dot_9"],
              [8, ""]
            ]
          },
          "text-anchor": "bottom",
          "text-field": "{name_en}",
          "text-font": ["Roboto Regular"],
          "text-max-width": 8,
          "text-offset": [0, 0],
          "text-size": {
            "base": 1.2,
            "stops": [
              [7, 12],
              [11, 16]
            ]
          }
        },
        "paint": {
          "text-color": "#333",
          "text-halo-color": "rgba(255,255,255,0.8)",
          "text-halo-width": 1.2
        }
      },
      {
        "id": "place_city",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "minzoom": 5,
        "filter": ["all", ["==", "class", "city"]],
        "layout": {
          "icon-image": {
            "base": 1,
            "stops": [
              [0, "dot_9"],
              [8, ""]
            ]
          },
          "text-anchor": "bottom",
          "text-field": "{name_en}",
          "text-font": ["Roboto Medium"],
          "text-max-width": 8,
          "text-offset": [0, 0],
          "text-size": {
            "base": 1.2,
            "stops": [
              [7, 14],
              [11, 24]
            ]
          },
          "icon-allow-overlap": true,
          "icon-optional": false
        },
        "paint": {
          "text-color": "#333",
          "text-halo-color": "rgba(255,255,255,0.8)",
          "text-halo-width": 1.2
        }
      },
      {
        "id": "state",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "maxzoom": 6,
        "filter": ["all", ["==", "class", "state"]],
        "layout": {
          "text-field": "{name_en}",
          "text-font": ["Roboto Condensed Italic"],
          "text-size": {
            "stops": [
              [4, 11],
              [6, 15]
            ]
          },
          "text-transform": "uppercase"
        },
        "paint": {
          "text-color": "#633",
          "text-halo-color": "rgba(255,255,255,0.7)",
          "text-halo-width": 1
        }
      },
      {
        "id": "country_3",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "filter": ["all", [">=", "rank", 3], ["==", "class", "country"]],
        "layout": {
          "text-field": "{name_en}",
          "text-font": ["Roboto Condensed Italic"],
          "text-max-width": 6.25,
          "text-size": {
            "stops": [
              [3, 11],
              [7, 17]
            ]
          },
          "text-transform": "none"
        },
        "paint": {
          "text-color": "#334",
          "text-halo-blur": 1,
          "text-halo-color": "rgba(255,255,255,0.8)",
          "text-halo-width": 1
        }
      },
      {
        "id": "country_2",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "filter": ["all", ["==", "rank", 2], ["==", "class", "country"]],
        "layout": {
          "text-field": "{name_en}",
          "text-font": ["Roboto Condensed Italic"],
          "text-max-width": 6.25,
          "text-size": {
            "stops": [
              [2, 11],
              [5, 17]
            ]
          },
          "text-transform": "none"
        },
        "paint": {
          "text-color": "#334",
          "text-halo-blur": 1,
          "text-halo-color": "rgba(255,255,255,0.8)",
          "text-halo-width": 1
        }
      },
      {
        "id": "country_1",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "filter": ["all", ["==", "rank", 1], ["==", "class", "country"]],
        "layout": {
          "text-field": "{name_en}",
          "text-font": ["Roboto Condensed Italic"],
          "text-max-width": 6.25,
          "text-size": {
            "stops": [
              [1, 11],
              [4, 17]
            ]
          },
          "text-transform": "none"
        },
        "paint": {
          "text-color": "#334",
          "text-halo-blur": 1,
          "text-halo-color": "rgba(255,255,255,0.8)",
          "text-halo-width": 1
        }
      },
      {
        "id": "continent",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "maxzoom": 1,
        "filter": ["all", ["==", "class", "continent"]],
        "layout": {
          "text-field": "{name_en}",
          "text-font": ["Roboto Condensed Italic"],
          "text-size": 13,
          "text-transform": "uppercase",
          "text-justify": "center"
        },
        "paint": {
          "text-color": "#633",
          "text-halo-color": "rgba(255,255,255,0.7)",
          "text-halo-width": 1
        }
      }
    ]
  },
  "format": "png",
  "base64": true
},
{
  "zoom": 9,
  "bbox": [104, 20, 106, 22],
  "tileScale": 1,
  "tileSize": 512,
  "styleJSON": {
    "version": 8,
    "id": "fiord",
    "name": "Fiord",
    "zoom": 10,
    "center": [105.827799, 21.03116],
    "sources": {
      "source": {
        "url": "http://localhost:8080/datas/osm-vietnam.json",
        "type": "vector"
      }
    },
    "sprite": "http://localhost:8080/sprites/fiord/sprite",
    "glyphs": "http://localhost:8080/fonts/{fontstack}/{range}.pbf",
    "layers": [
      {
        "id": "background",
        "type": "background",
        "paint": { "background-color": "#45516E" }
      },
      {
        "id": "water",
        "type": "fill",
        "source": "source",
        "source-layer": "water",
        "filter": ["==", "$type", "Polygon"],
        "layout": { "visibility": "visible" },
        "paint": { "fill-antialias": false, "fill-color": "#38435C" }
      },
      {
        "id": "landcover_ice_shelf",
        "type": "fill",
        "source": "source",
        "source-layer": "landcover",
        "maxzoom": 8,
        "filter": ["all", ["==", "$type", "Polygon"], ["==", "subclass", "ice_shelf"]],
        "layout": { "visibility": "visible" },
        "paint": { "fill-color": "hsl(232, 33%, 34%)", "fill-opacity": 0.4 }
      },
      {
        "id": "landuse_residential",
        "type": "fill",
        "source": "source",
        "source-layer": "landuse",
        "maxzoom": 16,
        "filter": ["all", ["==", "$type", "Polygon"], ["==", "subclass", "residential"]],
        "layout": { "visibility": "visible" },
        "paint": {
          "fill-color": "rgb(234, 234, 230)",
          "fill-opacity": {
            "base": 0.6,
            "stops": [
              [8, 0.8],
              [9, 0.6]
            ]
          }
        }
      },
      {
        "id": "landcover_wood",
        "type": "fill",
        "source": "source",
        "source-layer": "landcover",
        "minzoom": 10,
        "filter": ["all", ["==", "$type", "Polygon"], ["==", "class", "wood"]],
        "layout": { "visibility": "visible" },
        "paint": {
          "fill-color": "hsla(232, 18%, 30%, 0.57)",
          "fill-opacity": {
            "base": 1,
            "stops": [
              [9, 0],
              [12, 1]
            ]
          }
        }
      },
      {
        "id": "park",
        "type": "fill",
        "source": "source",
        "source-layer": "park",
        "filter": ["==", "$type", "Polygon"],
        "layout": { "visibility": "visible" },
        "paint": { "fill-color": "hsl(204, 17%, 35%)", "fill-opacity": 0.3 }
      },
      {
        "id": "park_outline",
        "type": "line",
        "source": "source",
        "source-layer": "park",
        "filter": ["==", "$type", "Polygon"],
        "layout": {},
        "paint": {
          "line-color": "hsl(205, 49%, 31%)",
          "line-dasharray": [2, 2]
        }
      },
      {
        "id": "waterway",
        "type": "line",
        "source": "source",
        "source-layer": "waterway",
        "filter": ["==", "$type", "LineString"],
        "layout": { "visibility": "visible" },
        "paint": { "line-color": "hsl(232, 23%, 28%)", "line-opacity": 0.6 }
      },
      {
        "id": "building",
        "type": "fill",
        "source": "source",
        "source-layer": "building",
        "minzoom": 12,
        "filter": ["==", "$type", "Polygon"],
        "paint": {
          "fill-antialias": false,
          "fill-color": "hsla(232, 47%, 18%, 0.65)",
          "fill-opacity": 0.25
        }
      },
      {
        "id": "tunnel_motorway_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 6,
        "filter": ["all", ["==", "$type", "LineString"], ["all", ["==", "brunnel", "tunnel"], ["==", "class", "motorway"]]],
        "layout": {
          "line-cap": "butt",
          "line-join": "miter",
          "visibility": "visible"
        },
        "paint": {
          "line-color": "#3C4357",
          "line-opacity": 1,
          "line-width": {
            "base": 1.4,
            "stops": [
              [5.8, 0],
              [6, 3],
              [20, 40]
            ]
          }
        }
      },
      {
        "id": "tunnel_motorway_inner",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 6,
        "filter": ["all", ["==", "$type", "LineString"], ["all", ["==", "brunnel", "tunnel"], ["==", "class", "motorway"]]],
        "layout": {
          "line-cap": "round",
          "line-join": "round",
          "visibility": "visible"
        },
        "paint": {
          "line-color": "hsl(224, 18%, 21%)",
          "line-width": {
            "base": 1.4,
            "stops": [
              [4, 2],
              [6, 1.3],
              [20, 30]
            ]
          }
        }
      },
      {
        "id": "aeroway-taxiway",
        "type": "line",
        "source": "source",
        "source-layer": "aeroway",
        "minzoom": 12,
        "filter": ["all", ["in", "class", "taxiway"]],
        "layout": {
          "line-cap": "round",
          "line-join": "round",
          "visibility": "visible"
        },
        "paint": {
          "line-color": "hsl(224, 22%, 45%)",
          "line-opacity": 1,
          "line-width": {
            "base": 1.55,
            "stops": [
              [13, 1.8],
              [20, 20]
            ]
          }
        }
      },
      {
        "id": "aeroway-runway-casing",
        "type": "line",
        "source": "source",
        "source-layer": "aeroway",
        "minzoom": 11,
        "filter": ["all", ["in", "class", "runway"]],
        "layout": {
          "line-cap": "round",
          "line-join": "round",
          "visibility": "visible"
        },
        "paint": {
          "line-color": "hsl(224, 22%, 45%)",
          "line-opacity": 1,
          "line-width": {
            "base": 1.5,
            "stops": [
              [11, 6],
              [17, 55]
            ]
          }
        }
      },
      {
        "id": "aeroway-area",
        "type": "fill",
        "source": "source",
        "source-layer": "aeroway",
        "minzoom": 4,
        "filter": ["all", ["==", "$type", "Polygon"], ["in", "class", "runway", "taxiway"]],
        "layout": { "visibility": "visible" },
        "paint": { "fill-color": "hsl(224, 20%, 29%)", "fill-opacity": 1 }
      },
      {
        "id": "aeroway-runway",
        "type": "line",
        "source": "source",
        "source-layer": "aeroway",
        "minzoom": 11,
        "maxzoom": 24,
        "filter": ["all", ["in", "class", "runway"], ["==", "$type", "LineString"]],
        "layout": {
          "line-cap": "round",
          "line-join": "round",
          "visibility": "visible"
        },
        "paint": {
          "line-color": "hsl(224, 20%, 29%)",
          "line-opacity": 1,
          "line-width": {
            "base": 1.5,
            "stops": [
              [11, 4],
              [17, 50]
            ]
          }
        }
      },
      {
        "id": "road_area_pier",
        "type": "fill",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "$type", "Polygon"], ["==", "class", "pier"]],
        "layout": { "visibility": "visible" },
        "paint": { "fill-antialias": true, "fill-color": "#45516E" }
      },
      {
        "id": "road_pier",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "$type", "LineString"], ["in", "class", "pier"]],
        "layout": { "line-cap": "round", "line-join": "round" },
        "paint": {
          "line-color": "#45516E",
          "line-width": {
            "base": 1.2,
            "stops": [
              [15, 1],
              [17, 4]
            ]
          }
        }
      },
      {
        "id": "highway_path",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "filter": ["all", ["==", "$type", "LineString"], ["==", "class", "path"]],
        "layout": {
          "line-cap": "round",
          "line-join": "round",
          "visibility": "visible"
        },
        "paint": {
          "line-color": "hsl(211, 29%, 38%)",
          "line-dasharray": [2, 2],
          "line-opacity": 1,
          "line-width": {
            "base": 1.2,
            "stops": [
              [12, 0.5],
              [20, 4]
            ]
          }
        }
      },
      {
        "id": "highway_minor",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 8,
        "filter": ["all", ["==", "$type", "LineString"], ["in", "class", "minor", "service", "track"]],
        "layout": {
          "line-cap": "round",
          "line-join": "round",
          "visibility": "visible"
        },
        "paint": {
          "line-color": "hsl(224, 22%, 45%)",
          "line-opacity": 0.9,
          "line-width": {
            "base": 1.55,
            "stops": [
              [13, 1.8],
              [20, 20]
            ]
          }
        }
      },
      {
        "id": "highway_major_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 11,
        "filter": ["all", ["==", "$type", "LineString"], ["in", "class", "primary", "secondary", "tertiary", "trunk"]],
        "layout": {
          "line-cap": "butt",
          "line-join": "miter",
          "visibility": "visible"
        },
        "paint": {
          "line-color": "hsl(224, 22%, 45%)",
          "line-dasharray": [12, 0],
          "line-width": {
            "base": 1.3,
            "stops": [
              [10, 3],
              [20, 23]
            ]
          }
        }
      },
      {
        "id": "highway_major_inner",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 11,
        "filter": ["all", ["==", "$type", "LineString"], ["in", "class", "primary", "secondary", "tertiary", "trunk"]],
        "layout": {
          "line-cap": "round",
          "line-join": "round",
          "visibility": "visible"
        },
        "paint": {
          "line-color": "#3C4357",
          "line-width": {
            "base": 1.3,
            "stops": [
              [10, 2],
              [20, 20]
            ]
          }
        }
      },
      {
        "id": "highway_major_subtle",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "maxzoom": 11,
        "filter": ["all", ["==", "$type", "LineString"], ["in", "class", "primary", "secondary", "tertiary", "trunk"]],
        "layout": {
          "line-cap": "round",
          "line-join": "round",
          "visibility": "visible"
        },
        "paint": {
          "line-color": "#3D4355",
          "line-opacity": 0.6,
          "line-width": 2
        }
      },
      {
        "id": "highway_motorway_casing",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 6,
        "filter": ["all", ["==", "$type", "LineString"], ["all", ["!in", "brunnel", "bridge", "tunnel"], ["==", "class", "motorway"]]],
        "layout": {
          "line-cap": "butt",
          "line-join": "miter",
          "visibility": "visible"
        },
        "paint": {
          "line-color": "hsl(224, 22%, 45%)",
          "line-dasharray": [2, 0],
          "line-opacity": 1,
          "line-width": {
            "base": 1.4,
            "stops": [
              [5.8, 0],
              [6, 3],
              [20, 40]
            ]
          }
        }
      },
      {
        "id": "highway_motorway_inner",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 6,
        "filter": ["all", ["==", "$type", "LineString"], ["all", ["!in", "brunnel", "bridge", "tunnel"], ["==", "class", "motorway"]]],
        "layout": {
          "line-cap": "round",
          "line-join": "round",
          "visibility": "visible"
        },
        "paint": {
          "line-color": {
            "base": 1,
            "stops": [
              [5.8, "hsla(0, 0%, 85%, 0.53)"],
              [6, "hsl(224, 20%, 29%)"]
            ]
          },
          "line-width": {
            "base": 1.4,
            "stops": [
              [4, 2],
              [6, 1.3],
              [20, 30]
            ]
          }
        }
      },
      {
        "id": "highway_motorway_subtle",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "maxzoom": 6,
        "filter": ["all", ["==", "$type", "LineString"], ["==", "class", "motorway"]],
        "layout": {
          "line-cap": "round",
          "line-join": "round",
          "visibility": "visible"
        },
        "paint": {
          "line-color": "hsla(239, 45%, 69%, 0.2)",
          "line-width": {
            "base": 1.4,
            "stops": [
              [4, 2],
              [6, 1.3]
            ]
          }
        }
      },
      {
        "id": "railway_transit",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 16,
        "filter": ["all", ["==", "$type", "LineString"], ["all", ["==", "class", "transit"], ["!in", "brunnel", "tunnel"]]],
        "layout": { "line-join": "round", "visibility": "visible" },
        "paint": { "line-color": "hsl(200, 65%, 11%)", "line-width": 3 }
      },
      {
        "id": "railway_transit_dashline",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 16,
        "filter": ["all", ["==", "$type", "LineString"], ["all", ["==", "class", "transit"], ["!in", "brunnel", "tunnel"]]],
        "layout": { "line-join": "round", "visibility": "visible" },
        "paint": {
          "line-color": "hsl(193, 63%, 26%)",
          "line-dasharray": [3, 3],
          "line-width": 2
        }
      },
      {
        "id": "railway_service",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 16,
        "filter": ["all", ["==", "$type", "LineString"], ["all", ["==", "class", "rail"], ["has", "service"]]],
        "layout": { "line-join": "round", "visibility": "visible" },
        "paint": { "line-color": "hsl(200, 65%, 11%)", "line-width": 3 }
      },
      {
        "id": "railway_service_dashline",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 16,
        "filter": ["all", ["==", "$type", "LineString"], ["all", ["==", "class", "rail"], ["has", "service"]]],
        "layout": { "line-join": "round", "visibility": "visible" },
        "paint": {
          "line-color": "hsl(193, 63%, 26%)",
          "line-dasharray": [3, 3],
          "line-width": 2
        }
      },
      {
        "id": "railway",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 13,
        "filter": ["all", ["==", "$type", "LineString"], ["all", ["!has", "service"], ["==", "class", "rail"]]],
        "layout": { "line-join": "round", "visibility": "visible" },
        "paint": {
          "line-color": "hsl(200, 10%, 18%)",
          "line-width": {
            "base": 1.3,
            "stops": [
              [16, 3],
              [20, 7]
            ]
          }
        }
      },
      {
        "id": "railway_dashline",
        "type": "line",
        "source": "source",
        "source-layer": "transportation",
        "minzoom": 13,
        "filter": ["all", ["==", "$type", "LineString"], ["all", ["!has", "service"], ["==", "class", "rail"]]],
        "layout": { "line-join": "round", "visibility": "visible" },
        "paint": {
          "line-color": "hsl(224, 20%, 41%)",
          "line-dasharray": [3, 3],
          "line-width": {
            "base": 1.3,
            "stops": [
              [16, 1.5],
              [20, 6]
            ]
          }
        }
      },
      {
        "id": "water_name",
        "type": "symbol",
        "source": "source",
        "source-layer": "water_name",
        "filter": ["==", "$type", "LineString"],
        "layout": {
          "symbol-placement": "line",
          "symbol-spacing": 500,
          "text-field": "{name:latin}",
          "text-font": ["Metropolis Medium Italic", "Noto Sans Italic"],
          "text-rotation-alignment": "map",
          "text-size": 12,
          "visibility": "visible"
        },
        "paint": {
          "text-color": "hsl(223, 21%, 52%)",
          "text-halo-blur": 0,
          "text-halo-color": "hsl(232, 5%, 19%)",
          "text-halo-width": 1
        }
      },
      {
        "id": "highway_name_other",
        "type": "symbol",
        "source": "source",
        "source-layer": "transportation_name",
        "filter": ["all", ["!=", "class", "motorway"], ["==", "$type", "LineString"]],
        "layout": {
          "symbol-placement": "line",
          "symbol-spacing": 350,
          "text-field": "{name:latin}",
          "text-font": ["Metropolis Regular", "Noto Sans Regular"],
          "text-max-angle": 30,
          "text-pitch-alignment": "viewport",
          "text-rotation-alignment": "map",
          "text-size": 10,
          "text-transform": "uppercase",
          "visibility": "visible"
        },
        "paint": {
          "text-color": "hsl(223, 31%, 61%)",
          "text-halo-blur": 0,
          "text-halo-color": "hsl(232, 9%, 23%)",
          "text-halo-width": 2,
          "text-opacity": 1,
          "text-translate": [0, 0]
        }
      },
      {
        "id": "highway_ref",
        "type": "symbol",
        "source": "source",
        "source-layer": "transportation_name",
        "filter": ["all", ["==", "$type", "LineString"], ["==", "class", "motorway"]],
        "layout": {
          "symbol-placement": "line",
          "symbol-spacing": 350,
          "text-field": "{ref}",
          "text-font": ["Metropolis Light", "Noto Sans Regular"],
          "text-pitch-alignment": "viewport",
          "text-rotation-alignment": "viewport",
          "text-size": 10,
          "visibility": "none"
        },
        "paint": {
          "text-color": "hsl(215, 57%, 77%)",
          "text-halo-blur": 1,
          "text-halo-color": "hsl(209, 64%, 19%)",
          "text-halo-width": 1,
          "text-opacity": 1,
          "text-translate": [0, 2]
        }
      },
      {
        "id": "boundary_state",
        "type": "line",
        "source": "source",
        "source-layer": "boundary",
        "filter": ["==", "admin_level", 4],
        "layout": {
          "line-cap": "round",
          "line-join": "round",
          "visibility": "visible"
        },
        "paint": {
          "line-blur": 0.4,
          "line-color": "hsla(195, 47%, 62%, 0.26)",
          "line-dasharray": [2, 2],
          "line-opacity": 1,
          "line-width": {
            "base": 1.3,
            "stops": [
              [3, 1],
              [22, 15]
            ]
          }
        }
      },
      {
        "id": "boundary_country_z0-4",
        "type": "line",
        "source": "source",
        "source-layer": "boundary",
        "maxzoom": 5,
        "filter": ["all", ["==", "admin_level", 2], ["!has", "claimed_by"]],
        "layout": { "line-cap": "round", "line-join": "round" },
        "paint": {
          "line-blur": {
            "base": 1,
            "stops": [
              [0, 0.4],
              [22, 4]
            ]
          },
          "line-color": "hsl(214, 63%, 76%)",
          "line-opacity": 0.56,
          "line-width": {
            "base": 1.1,
            "stops": [
              [3, 1],
              [22, 20]
            ]
          }
        }
      },
      {
        "id": "boundary_country_z5-",
        "type": "line",
        "source": "source",
        "source-layer": "boundary",
        "minzoom": 5,
        "filter": ["==", "admin_level", 2],
        "layout": { "line-cap": "round", "line-join": "round" },
        "paint": {
          "line-blur": {
            "base": 1,
            "stops": [
              [0, 0.4],
              [22, 4]
            ]
          },
          "line-color": "hsl(214, 63%, 76%)",
          "line-opacity": 0.56,
          "line-width": {
            "base": 1.1,
            "stops": [
              [3, 1],
              [22, 20]
            ]
          }
        }
      },
      {
        "id": "place_other",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "maxzoom": 14,
        "filter": ["all", ["in", "class", "hamlet", "neighbourhood", "isolated_dwelling"], ["==", "$type", "Point"]],
        "layout": {
          "text-anchor": "center",
          "text-field": "{name:latin}",
          "text-font": ["Metropolis Regular", "Noto Sans Regular"],
          "text-justify": "center",
          "text-offset": [0.5, 0],
          "text-size": 10,
          "text-transform": "uppercase",
          "visibility": "visible"
        },
        "paint": {
          "text-color": "hsl(195, 37%, 73%)",
          "text-halo-blur": 1,
          "text-halo-color": "hsla(228, 60%, 21%, 0.7)",
          "text-halo-width": 1,
          "text-opacity": 0.6
        }
      },
      {
        "id": "place_suburb",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "maxzoom": 15,
        "filter": ["all", ["==", "$type", "Point"], ["==", "class", "suburb"]],
        "layout": {
          "text-anchor": "center",
          "text-field": "{name:latin}",
          "text-font": ["Metropolis Regular", "Noto Sans Regular"],
          "text-justify": "center",
          "text-offset": [0.5, 0],
          "text-size": 10,
          "text-transform": "uppercase",
          "visibility": "visible"
        },
        "paint": {
          "text-color": "hsl(195, 41%, 49%)",
          "text-halo-blur": 1,
          "text-halo-color": "hsla(228, 60%, 21%, 0.7)",
          "text-halo-width": 1
        }
      },
      {
        "id": "place_village",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "maxzoom": 14,
        "filter": ["all", ["==", "$type", "Point"], ["==", "class", "village"]],
        "layout": {
          "icon-size": 0.4,
          "text-anchor": "left",
          "text-field": "{name:latin}",
          "text-font": ["Metropolis Regular", "Noto Sans Regular"],
          "text-justify": "left",
          "text-offset": [0.5, 0.2],
          "text-size": 10,
          "text-transform": "uppercase",
          "visibility": "visible"
        },
        "paint": {
          "icon-opacity": 0.7,
          "text-color": "hsl(195, 41%, 49%)",
          "text-halo-blur": 1,
          "text-halo-color": "hsla(228, 60%, 21%, 0.7)",
          "text-halo-width": 1
        }
      },
      {
        "id": "place_town",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "maxzoom": 15,
        "filter": ["all", ["==", "$type", "Point"], ["==", "class", "town"]],
        "layout": {
          "icon-image": {
            "base": 1,
            "stops": [
              [0, "circle-11"],
              [9, ""]
            ]
          },
          "icon-size": 0.4,
          "text-anchor": {
            "base": 1,
            "stops": [
              [0, "left"],
              [8, "center"]
            ]
          },
          "text-field": "{name:latin}",
          "text-font": ["Metropolis Regular", "Noto Sans Regular"],
          "text-justify": "left",
          "text-offset": [0.5, 0.2],
          "text-size": 10,
          "text-transform": "uppercase",
          "visibility": "visible"
        },
        "paint": {
          "icon-opacity": 0.7,
          "text-color": "hsl(195, 25%, 76%)",
          "text-halo-blur": 1,
          "text-halo-color": "hsla(228, 60%, 21%, 0.7)",
          "text-halo-width": 1
        }
      },
      {
        "id": "place_city",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "maxzoom": 14,
        "filter": ["all", ["==", "$type", "Point"], ["all", ["==", "class", "city"], [">", "rank", 3]]],
        "layout": {
          "icon-size": 0.4,
          "text-anchor": {
            "base": 1,
            "stops": [
              [0, "left"],
              [8, "center"]
            ]
          },
          "text-field": "{name:latin}",
          "text-font": ["Metropolis Regular", "Noto Sans Regular"],
          "text-justify": "left",
          "text-offset": [0.5, 0.2],
          "text-size": 10,
          "text-transform": "uppercase",
          "visibility": "visible"
        },
        "paint": {
          "icon-opacity": 0.7,
          "text-color": "hsl(195, 25%, 76%)",
          "text-halo-blur": 1,
          "text-halo-color": "hsla(228, 60%, 21%, 0.7)",
          "text-halo-width": 1
        }
      },
      {
        "id": "place_city_large",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "maxzoom": 12,
        "filter": ["all", ["==", "$type", "Point"], ["all", ["<=", "rank", 3], ["==", "class", "city"]]],
        "layout": {
          "icon-size": 0.4,
          "text-anchor": {
            "base": 1,
            "stops": [
              [0, "left"],
              [8, "center"]
            ]
          },
          "text-field": "{name:latin}",
          "text-font": ["Metropolis Regular", "Noto Sans Regular"],
          "text-justify": "left",
          "text-offset": [0.5, 0.2],
          "text-size": 14,
          "text-transform": "uppercase",
          "visibility": "visible"
        },
        "paint": {
          "icon-opacity": 0.7,
          "text-color": "hsl(195, 25%, 76%)",
          "text-halo-blur": 1,
          "text-halo-color": "hsla(228, 60%, 21%, 0.7)",
          "text-halo-width": 1
        }
      },
      {
        "id": "place_state",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "maxzoom": 12,
        "filter": ["all", ["==", "$type", "Point"], ["==", "class", "state"]],
        "layout": {
          "text-field": "{name:latin}",
          "text-font": ["Metropolis Regular", "Noto Sans Regular"],
          "text-size": 10,
          "text-transform": "uppercase",
          "visibility": "visible"
        },
        "paint": {
          "text-color": "rgb(113, 129, 144)",
          "text-halo-blur": 1,
          "text-halo-color": "hsla(228, 60%, 21%, 0.7)",
          "text-halo-width": 1
        }
      },
      {
        "id": "place_country_other",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "maxzoom": 8,
        "filter": ["all", ["==", "$type", "Point"], ["==", "class", "country"], ["!has", "iso_a2"]],
        "layout": {
          "text-field": "{name:latin}",
          "text-font": ["Metropolis Light Italic", "Noto Sans Italic"],
          "text-size": {
            "base": 1,
            "stops": [
              [0, 9],
              [6, 11]
            ]
          },
          "text-transform": "uppercase",
          "visibility": "visible"
        },
        "paint": {
          "text-color": {
            "base": 1,
            "stops": [
              [3, "rgb(157,169,177)"],
              [4, "rgb(153, 153, 153)"]
            ]
          },
          "text-halo-color": "hsla(228, 60%, 21%, 0.7)",
          "text-halo-width": 1.4,
          "text-opacity": 1
        }
      },
      {
        "id": "place_country_minor",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "maxzoom": 8,
        "filter": ["all", ["==", "$type", "Point"], ["==", "class", "country"], [">=", "rank", 2], ["has", "iso_a2"]],
        "layout": {
          "text-field": "{name:latin}",
          "text-font": ["Metropolis Regular", "Noto Sans Regular"],
          "text-size": {
            "base": 1,
            "stops": [
              [0, 10],
              [6, 12]
            ]
          },
          "text-transform": "uppercase",
          "visibility": "visible"
        },
        "paint": {
          "text-color": {
            "base": 1,
            "stops": [
              [3, "rgb(157,169,177)"],
              [4, "rgb(153, 153, 153)"]
            ]
          },
          "text-halo-color": "hsla(228, 60%, 21%, 0.7)",
          "text-halo-width": 1.4,
          "text-opacity": 1
        }
      },
      {
        "id": "place_country_major",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "maxzoom": 6,
        "filter": ["all", ["==", "$type", "Point"], ["<=", "rank", 1], ["==", "class", "country"], ["has", "iso_a2"]],
        "layout": {
          "text-anchor": "center",
          "text-field": "{name:latin}",
          "text-font": ["Metropolis Regular", "Noto Sans Regular"],
          "text-size": {
            "base": 1.4,
            "stops": [
              [0, 10],
              [3, 12],
              [4, 14]
            ]
          },
          "text-transform": "uppercase",
          "visibility": "visible"
        },
        "paint": {
          "text-color": {
            "base": 1,
            "stops": [
              [3, "rgb(157,169,177)"],
              [4, "rgb(153, 153, 153)"]
            ]
          },
          "text-halo-color": "hsla(228, 60%, 21%, 0.7)",
          "text-halo-width": 1.4,
          "text-opacity": 1
        }
      },
      {
        "id": "place_continent",
        "type": "symbol",
        "source": "source",
        "source-layer": "place",
        "maxzoom": 6,
        "filter": ["all", ["==", "$type", "Point"], ["==", "class", "continent"]],
        "layout": {
          "text-anchor": "center",
          "text-field": "{name:latin}",
          "text-font": ["Metropolis Regular", "Noto Sans Regular"],
          "text-size": {
            "base": 1.4,
            "stops": [
              [0, 10],
              [3, 12],
              [4, 14]
            ]
          },
          "text-transform": "uppercase",
          "visibility": "visible"
        },
        "paint": {
          "text-color": "hsl(0, 0%, 100%)",
          "text-halo-color": "hsla(228, 60%, 21%, 0.7)",
          "text-halo-width": 1.4,
          "text-opacity": {
            "base": 1,
            "stops": [
              [0, 0.6],
              [3, 0]
            ]
          }
        }
      }
    ]
  },
  "format": "jpg",
  "base64": true
}
```
