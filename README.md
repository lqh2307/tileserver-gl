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

### Run with nodejs - native (on ubuntu 22.04 x86_64 amd)

Install dependencies:

```bash
export DEBIAN_FRONTEND=noninteractive; \
apt-get -y update; \
apt-get -y install \
  wget \
  fontconfig \
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

Install nodejs:

```bash
export NODEJS_VERSION=22.20.0

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
docker build -t tile-server:1.0.0 .
```

Run container:

```bash
docker run --rm -it -p 8080:8080 --name tile-server -v path_to_data_folder:/tile-server/data tile-server:1.0.0
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
    "crimea": {
      "crimea": {
        "geojson": "crimea/crimea.geojson"
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
    "satellite": {
      "mbtiles": "satellite_cache",
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
        "bounds": [96, 4, 120, 28],
        "center": [108, 16, 10],
        "minzoom": 0,
        "maxzoom": 18
      },
      "refreshBefore": {
        "md5": true
      },
      "url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
      "coverages": [
        {
          "bbox": [96, 4, 120, 28],
          "zoom": 0
        },
        {
          "bbox": [96, 4, 120, 28],
          "zoom": 5
        },
        {
          "bbox": [96, 4, 120, 28],
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
        "bounds": [96, 4, 120, 28],
        "center": [108, 16, 10],
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
    },
    "satellite_cache": {
      "metadata": {
        "name": "satellite",
        "description": "satellite",
        "format": "jpeg",
        "bounds": [-180, -90, 180, 90],
        "center": [108, 16, 10],
        "minzoom": 0,
        "maxzoom": 18
      },
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "url": "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
      "coverages": [
        {
          "bbox": [104.4, 8.55, 106.8, 10.42],
          "zoom": 17
        },
        {
          "bbox": [102.5, 20.5, 108, 21.5],
          "zoom": 17
        },
        {
          "bbox": [103.8, 20.15, 106.65, 20.5],
          "zoom": 17
        },
        {
          "bbox": [103.8, 19, 106.39, 20.15],
          "zoom": 17
        },
        {
          "bbox": [104.4, 8.55, 106.8, 10.42],
          "zoom": 18
        },
        {
          "bbox": [102.5, 20.5, 108, 21.5],
          "zoom": 18
        },
        {
          "bbox": [103.8, 20.15, 106.65, 20.5],
          "zoom": 18
        },
        {
          "bbox": [103.8, 19, 106.39, 20.15],
          "zoom": 18
        }
      ],
      "timeout": 180000,
      "concurrency": 30,
      "maxTry": 5,
      "storeType": "mbtiles",
      "storeTransparent": true,
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
      "url": "https://api.maptiler.com/fonts/Roboto Medium/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Roboto Italic": {
      "url": "https://api.maptiler.com/fonts/Roboto Italic/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Roboto Bold": {
      "url": "https://api.maptiler.com/fonts/Roboto Bold/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Roboto Regular": {
      "url": "https://api.maptiler.com/fonts/Roboto Regular/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Noto Sans Regular": {
      "url": "https://api.maptiler.com/fonts/Noto Sans Regular/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Noto Sans Italic": {
      "url": "https://api.maptiler.com/fonts/Noto Sans Italic/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Noto Sans Bold": {
      "url": "https://api.maptiler.com/fonts/Noto Sans Bold/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Open Sans Bold": {
      "url": "https://api.maptiler.com/fonts/Open Sans Bold/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 5,
      "maxTry": 5
    },
    "Open Sans Bold Italic": {
      "url": "https://api.maptiler.com/fonts/Open Sans Bold Italic/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 5,
      "maxTry": 5
    },
    "Open Sans Extrabold": {
      "url": "https://api.maptiler.com/fonts/Open Sans Extrabold/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 5,
      "maxTry": 5
    },
    "Open Sans Extrabold Italic": {
      "url": "https://api.maptiler.com/fonts/Open Sans Extrabold Italic/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 5,
      "maxTry": 5
    },
    "Open Sans Semibold": {
      "url": "https://api.maptiler.com/fonts/Open Sans Semibold/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 5,
      "maxTry": 5
    },
    "Open Sans Semibold Italic": {
      "url": "https://api.maptiler.com/fonts/Open Sans Semibold Italic/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 5,
      "maxTry": 5
    },
    "Open Sans Light": {
      "url": "https://api.maptiler.com/fonts/Open Sans Light/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 5,
      "maxTry": 5
    },
    "Open Sans Light Italic": {
      "url": "https://api.maptiler.com/fonts/Open Sans Light Italic/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 5,
      "maxTry": 5
    },
    "Open Sans Regular": {
      "url": "https://api.maptiler.com/fonts/Open Sans Regular/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 5,
      "maxTry": 5
    },
    "Open Sans Italic": {
      "url": "https://api.maptiler.com/fonts/Open Sans Italic/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 5,
      "maxTry": 5
    },
    "Open Sans Medium": {
      "url": "https://api.maptiler.com/fonts/Open Sans Medium/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 5,
      "maxTry": 5
    },
    "Open Sans Medium Italic": {
      "url": "https://api.maptiler.com/fonts/Open Sans Medium Italic/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 5,
      "maxTry": 5
    },
    "Roboto Condensed Italic": {
      "url": "https://api.maptiler.com/fonts/Roboto Condensed Italic/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Roboto Condensed Regular": {
      "url": "https://api.maptiler.com/fonts/Roboto Condensed Regular/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Roboto Condensed Bold": {
      "url": "https://api.maptiler.com/fonts/Roboto Condensed Bold/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Frutiger Neue Condensed Italic": {
      "url": "https://api.maptiler.com/fonts/Frutiger Neue Condensed Italic/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Frutiger Neue Condensed Regular": {
      "url": "https://api.maptiler.com/fonts/Frutiger Neue Condensed Regular/{range}.pbf",
      "refreshBefore": {
        "time": "2024-10-10T00:00:00"
      },
      "timeout": 60000,
      "concurrency": 50,
      "maxTry": 5
    },
    "Frutiger Neue Condensed Bold": {
      "url": "https://api.maptiler.com/fonts/Frutiger Neue Condensed Bold/{range}.pbf",
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
    "minzoom": 14,
    "maxzoom": 14
  },
  "refreshBefore": {
    "time": "2024-10-10T00:00:00"
  },
  "concurrency": 50,
  "storeType": "mbtiles",
  "storeTransparent": true,
  "createOverview": true
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
    "maxzoom": 17
  },
  "refreshBefore": {
    "time": "2024-10-10T00:00:00"
  },
  "concurrency": 100,
  "storeType": "mbtiles",
  "storeTransparent": true,
  "createOverview": false,
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
    },
    {
      "zoom": 6,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    },
    {
      "zoom": 7,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    },
    {
      "zoom": 8,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    },
    {
      "zoom": 9,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    },
    {
      "zoom": 10,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    },
    {
      "zoom": 11,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    },
    {
      "zoom": 12,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    },
    {
      "zoom": 13,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    },
    {
      "zoom": 14,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    },
    {
      "zoom": 15,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    },
    {
      "zoom": 16,
      "circle": {
        "center": [105.8, 21.0],
        "radius": 50000
      }
    },
    {
      "zoom": 17,
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
[
  {
    "zoom": 8.45,
    "bbox": [104.55758541249514, 20.40418806657307, 106.68188322743384, 22.266067290668104],
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
        },
        "overlay": {
          "type": "image",
          "url": "data:image/jpeg;base64,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",
          "coordinates": [
            [105.75, 21.05],
            [105.85, 21.05],
            [105.85, 21],
            [105.75, 21]
          ]
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
        },
        {
          "id": "overlay",
          "source": "overlay",
          "type": "raster"
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
]
```
