"use strict";

import { renderImageTileData } from "../render_style.js";
import { config, seed } from "../configs/index.js";
import { StatusCodes } from "http-status-codes";
import {
  getAndCacheDataStyleJSON,
  getRenderedStyleJSON,
  validateStyle,
  getStyleMD5,
  getStyle,
} from "../resources/index.js";
import {
  detectContentTypeFromFormat,
  compileHandleBarsTemplate,
  RASTER_TILE_FORMATS,
  createTileMetadata,
  getXYZFromLonLatZ,
  getRequestHost,
  TILE_SIZES,
  isLocalURL,
  gzipAsync,
  printLog,
} from "../utils/index.js";

/**
 * Serve style handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function serveStyleHandler() {
  return async (req, res) => {
    const id = req.params.id;

    try {
      const item = config.styles[id];

      if (!item) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send(`Style id "${id}" does not exist`);
      }

      const compiled = await compileHandleBarsTemplate("viewer", {
        id: id,
        name: item.name,
        base_url: getRequestHost(req),
      });

      return res.status(StatusCodes.OK).send(compiled);
    } catch (error) {
      printLog("error", `Failed to serve style id "${id}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Serve WMTS handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function serveWMTSHandler() {
  return async (req, res) => {
    const id = req.params.id;

    try {
      const item = config.styles[id].tileJSON;

      if (!item) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send(`WMTS of style id "${id}" does not exist`);
      }

      const compiled = await compileHandleBarsTemplate("wmts", {
        id: id,
        name: item.name,
        base_url: getRequestHost(req),
      });

      res.header("content-type", "text/xml");

      return res.status(StatusCodes.OK).send(compiled);
    } catch (error) {
      printLog("error", `Failed to serve WMTS of style id "${id}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get styleJSON handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getStyleHandler() {
  return async (req, res) => {
    const id = req.params.id;

    try {
      /* Check style is used? */
      if (!config.styles[id]) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send(`Style id "${id}" does not exist`);
      }

      /* Get and cache StyleJSON */
      let styleJSON = await getAndCacheDataStyleJSON(id);

      if (req.query.raw !== "true") {
        styleJSON = JSON.parse(styleJSON);

        const requestHost = getRequestHost(req);

        /* Fix sprite url */
        if (styleJSON.sprite) {
          if (styleJSON.sprite.startsWith("sprites://")) {
            styleJSON.sprite = styleJSON.sprite.replace(
              "sprites://",
              `${requestHost}/sprites/`,
            );
          }
        }

        /* Fix font url */
        if (styleJSON.glyphs) {
          if (styleJSON.glyphs.startsWith("fonts://")) {
            styleJSON.glyphs = styleJSON.glyphs.replace(
              "fonts://",
              `${requestHost}/fonts/`,
            );
          }
        }

        /* Fix source urls */
        await Promise.all(
          Object.keys(styleJSON.sources).map(async (id) => {
            const source = styleJSON.sources[id];

            // Fix geoJSON URL
            if (source.data) {
              if (isLocalURL(source.data)) {
                const parts = source.data.split("/");

                source.data = `${requestHost}/geojsons/${parts[2]}/${parts[3]}.geojson`;
              }
            }

            // Fix tileJSON URL
            if (source.url) {
              if (isLocalURL(source.url)) {
                const sourceID = source.url.split("/")[2];

                source.url = `${requestHost}/datas/${sourceID}.json`;
              }
            }

            // Fix tileJSON URLs
            if (source.urls) {
              const urls = new Set(
                source.urls.map((url) => {
                  if (isLocalURL(url)) {
                    const sourceID = url.split("/")[2];

                    url = `${requestHost}/datas/${sourceID}.json`;
                  }

                  return url;
                }),
              );

              source.urls = Array.from(urls);
            }

            // Fix tile URL
            if (source.tiles) {
              const tiles = new Set(
                source.tiles.map((tile) => {
                  if (isLocalURL(tile)) {
                    const sourceID = tile.split("/")[2];
                    const sourceData = config.datas[sourceID];

                    tile = `${requestHost}/datas/${sourceID}/{z}/{x}/{y}.${sourceData.tileJSON.format}`;
                  }

                  return tile;
                }),
              );

              source.tiles = Array.from(tiles);
            }
          }),
        );
      }

      const headers = {
        "content-type": "application/json",
      };

      if (req.query.compression === "true") {
        styleJSON = await gzipAsync(
          req.query.raw !== "true" ? JSON.stringify(styleJSON) : styleJSON,
        );

        headers["content-encoding"] = "gzip";
      }

      res.set(headers);

      return res.status(StatusCodes.OK).send(styleJSON);
    } catch (error) {
      printLog("error", `Failed to get style id "${id}": ${error}`);

      if (error.message.includes("Not Found")) {
        return res.status(StatusCodes.NO_CONTENT).send(error.message);
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
    }
  };
}

/**
 * Get style list handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getStylesListHandler() {
  return async (req, res) => {
    try {
      const requestHost = getRequestHost(req);

      const result = await Promise.all(
        Object.keys(config.styles).map(async (id) => {
          const data = {
            id: id,
            url: `${requestHost}/styles/${id}/style.json`,
          };

          if (config.styles[id].tileJSON) {
            const { name, center } = config.styles[id].tileJSON;

            const [x, y, z] = getXYZFromLonLatZ(
              center[0],
              center[1],
              center[2],
            );

            data.name = name;
            data.thumbnail = `${requestHost}/styles/${id}/${z}/${x}/${y}.png`;
          } else {
            data.name = config.styles[id].name;
          }

          return data;
        }),
      );

      const headers = {
        "content-type": "application/json",
      };

      if (req.query.compression === "true") {
        result = await gzipAsync(JSON.stringify(result));

        headers["content-encoding"] = "gzip";
      }

      res.set(headers);

      return res.status(StatusCodes.OK).send(result);
    } catch (error) {
      printLog("error", `Failed to get styles": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get rendered tile handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getRenderedTileHandler() {
  return async (req, res) => {
    /* Check tile data format */
    if (!RASTER_TILE_FORMATS.has(req.params.format)) {
      return res
        .status(StatusCodes.BAD_REQUEST)
        .send(`Rendered tile format "${req.params.format}" is not support`);
    }

    if (req.query.tileSize && !TILE_SIZES.has(req.query.tileSize)) {
      return res
        .status(StatusCodes.BAD_REQUEST)
        .send(`Tile size "${req.query.tileSize}" is not support`);
    }

    const id = req.params.id;
    const item = config.styles[id];

    /* Check rendered is exist? */
    if (!item || !item.tileJSON) {
      return res
        .status(StatusCodes.NOT_FOUND)
        .send(`Rendered of style id "${id}" does not exist`);
    }

    /* Render tile */
    try {
      const image = await renderImageTileData(
        +req.params.z,
        +req.params.x,
        +req.params.y,
        {
          styleJSON: await getRenderedStyleJSON(item.path),
          tileScale: +req.query.tileScale || 1,
          tileSize: +req.query.tileSize || 256,
          format: req.params.format,
        },
      );

      res.header(
        "content-type",
        detectContentTypeFromFormat(req.params.format),
      );

      return res.status(StatusCodes.OK).send(image);
    } catch (error) {
      printLog(
        "error",
        `Failed to get rendered "${id}" - Tile "${req.params.z}/${req.params.x}/${req.params.y}: ${error}`,
      );

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get rendered tileJSON handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getRenderedHandler() {
  return async (req, res) => {
    const id = req.params.id;
    const queryStrings = [];

    if (req.query.tileSize) {
      if (!TILE_SIZES.has(req.query.tileSize)) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send(`Tile size "${req.query.tileSize}" is not support`);
      } else {
        queryStrings.push(`tileSize=${req.query.tileSize}`);
      }
    }

    if (req.query.tileScale) {
      queryStrings.push(`tileScale=${req.query.tileScale}`);
    }

    try {
      const item = config.styles[id];

      /* Check rendered is exist? */
      if (!item || !item.tileJSON) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send(`Rendered of style id "${id}" does not exist`);
      }

      res.header("content-type", "application/json");

      /* Get render info */
      return res.status(StatusCodes.OK).send({
        ...item.tileJSON,
        tilejson: "2.2.0",
        scheme: "xyz",
        id: id,
        tiles: [
          `${getRequestHost(req)}/styles/${id}/{z}/{x}/{y}.png${
            queryStrings.length ? `?${queryStrings.join("&")}` : ""
          }`,
        ],
      });
    } catch (error) {
      printLog("error", `Failed to get rendered of style id "${id}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get Style MD5 handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getStyleMD5Handler() {
  return async (req, res) => {
    const id = req.params.id;

    try {
      const item = config.styles[id];

      /* Check style is used? */
      if (!item) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send(`Style id "${id}" does not exist`);
      }

      /* Calculate MD5 and Add to header */
      res.set({
        etag: await getStyleMD5(item.path),
      });

      return res.status(StatusCodes.OK).send();
    } catch (error) {
      printLog("error", `Failed to get md5 of style id "${id}": ${error}`);

      if (error.message.includes("Not Found")) {
        return res.status(StatusCodes.NO_CONTENT).send(error.message);
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
    }
  };
}

/**
 * Get rendered list handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getRenderedsListHandler() {
  return async (req, res) => {
    try {
      const requestHost = getRequestHost(req);

      const result = [];

      Object.keys(config.styles).map((id) => {
        const item = config.styles[id].tileJSON;

        if (item) {
          result.push({
            id: id,
            name: item.name,
            url: [
              `${requestHost}/styles/256/${id}.json`,
              `${requestHost}/styles/512/${id}.json`,
            ],
          });
        }
      });

      const headers = {
        "content-type": "application/json",
      };

      if (req.query.compression === "true") {
        result = await gzipAsync(JSON.stringify(result));

        headers["content-encoding"] = "gzip";
      }

      res.set(headers);

      return res.status(StatusCodes.OK).send(result);
    } catch (error) {
      printLog("error", `Failed to get rendereds": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

export const serve_style = {
  /**
   * Register style handlers
   * @param {Express} app Express object
   * @returns {void}
   */
  init: (app) => {
    /**
     * @swagger
     * tags:
     *   - name: Style
     *     description: Style related endpoints
     * /styles/styles.json:
     *   get:
     *     tags:
     *       - Style
     *     summary: Get all styles
     *     parameters:
     *       - in: query
     *         name: compression
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Compressed response
     *     responses:
     *       200:
     *         description: List of all styles
     *         content:
     *           application/json:
     *           schema:
     *             type: array
     *               items:
     *                 type: object
     *                 properties:
     *                   id:
     *                     type: string
     *                   name:
     *                     type: string
     *                   url:
     *                     type: string
     *       404:
     *         description: Not found
     *       503:
     *         description: Server is starting up
     *         content:
     *           text/plain:
     *             schema:
     *               type: string
     *               example: Starting...
     *       500:
     *         description: Internal server error
     */
    app.get("/styles/styles.json", getStylesListHandler());

    /* Serve backend render */
    if (process.env.BACKEND_RENDER === "true") {
      /**
       * @swagger
       * tags:
       *   - name: Style
       *     description: Style related endpoints
       * /styles/{id}/style.json:
       *   get:
       *     tags:
       *       - Style
       *     summary: Get styleJSON
       *     parameters:
       *       - in: path
       *         name: id
       *         schema:
       *           type: string
       *           example: id
       *         required: true
       *         description: ID of the style
       *       - in: query
       *         name: raw
       *         schema:
       *           type: boolean
       *         required: false
       *         description: Use raw
       *       - in: query
       *         name: compression
       *         schema:
       *           type: boolean
       *         required: false
       *         description: Compressed response
       *     responses:
       *       200:
       *         description: StyleJSON
       *         content:
       *           application/json:
       *           schema:
       *             type: object
       *       404:
       *         description: Not found
       *       503:
       *         description: Server is starting up
       *         content:
       *           text/plain:
       *             schema:
       *               type: string
       *               example: Starting...
       *       500:
       *         description: Internal server error
       */
      app.get("/styles/:id/style.json", getStyleHandler());

      /**
       * @swagger
       * tags:
       *   - name: Style
       *     description: Style related endpoints
       * /styles/{id}/wmts.xml:
       *   get:
       *     tags:
       *       - Style
       *     summary: Get WMTS XML of style
       *     parameters:
       *       - in: path
       *         name: id
       *         schema:
       *           type: string
       *           example: id
       *         required: true
       *         description: ID of the style
       *     responses:
       *       200:
       *         description: WMTS XML for the style
       *         content:
       *           text/xml:
       *             schema:
       *               type: string
       *       404:
       *         description: Not found
       *       503:
       *         description: Server is starting up
       *         content:
       *           text/plain:
       *             schema:
       *               type: string
       *               example: Starting...
       *       500:
       *         description: Internal server error
       */
      app.get("/styles/:id/wmts.xml", serveWMTSHandler());

      /**
       * @swagger
       * tags:
       *   - name: Rendered
       *     description: Rendered related endpoints
       * /styles/rendereds.json:
       *   get:
       *     tags:
       *       - Rendered
       *     summary: Get all style rendereds
       *     parameters:
       *       - in: query
       *         name: compression
       *         schema:
       *           type: boolean
       *         required: false
       *         description: Compressed response
       *     responses:
       *       200:
       *         description: List of all style rendereds
       *         content:
       *           application/json:
       *             schema:
       *               type: array
       *               items:
       *                 type: object
       *                 properties:
       *                   id:
       *                     type: string
       *                   name:
       *                     type: string
       *                   url:
       *                     type: array
       *                     items:
       *                       type: string
       *       404:
       *         description: Not found
       *       503:
       *         description: Server is starting up
       *         content:
       *           text/plain:
       *             schema:
       *               type: string
       *               example: Starting...
       *       500:
       *         description: Internal server error
       */
      app.get("/styles/rendereds.json", getRenderedsListHandler());

      /**
       * @swagger
       * tags:
       *   - name: Rendered
       *     description: Rendered related endpoints
       * /styles/{id}.json:
       *   get:
       *     tags:
       *       - Rendered
       *     summary: Get style rendered
       *     parameters:
       *       - in: path
       *         name: id
       *         schema:
       *           type: string
       *           example: id
       *         required: true
       *         description: ID of the style rendered
       *       - in: query
       *         name: tileSize
       *         schema:
       *           type: integer
       *           enum: [256, 512]
       *           example: 256
       *         required: false
       *         description: Tile size
       *       - in: query
       *         name: tileScale
       *         schema:
       *           type: number
       *           example: 1
       *         required: false
       *         description: Tile scale
       *     responses:
       *       200:
       *         description: Style rendered
       *         content:
       *           application/json:
       *             schema:
       *               type: object
       *       404:
       *         description: Not found
       *       503:
       *         description: Server is starting up
       *         content:
       *           text/plain:
       *             schema:
       *               type: string
       *               example: Starting...
       *       500:
       *         description: Internal server error
       */
      app.get("/styles/:id.json", getRenderedHandler());

      /**
       * @swagger
       * tags:
       *   - name: Style
       *     description: Style related endpoints
       * /styles/{id}/md5:
       *   get:
       *     tags:
       *       - Style
       *     summary: Get style md5
       *     parameters:
       *       - in: path
       *         name: id
       *         schema:
       *           type: string
       *           example: id
       *         required: true
       *         description: ID of the style
       *     responses:
       *       200:
       *         description: Style md5
       *       404:
       *         description: Not found
       *       503:
       *         description: Server is starting up
       *         content:
       *           text/plain:
       *             schema:
       *               type: string
       *               example: Starting...
       *       500:
       *         description: Internal server error
       */
      app.get("/styles/:id/md5", getStyleMD5Handler());

      /**
       * @swagger
       * tags:
       *   - name: Rendered
       *     description: Rendered related endpoints
       * /styles/{id}/{z}/{x}/{y}.{format}:
       *   get:
       *     tags:
       *       - Rendered
       *     summary: Get style rendered tile
       *     parameters:
       *       - in: path
       *         name: id
       *         schema:
       *           type: string
       *           example: id
       *         required: true
       *         description: ID of the style
       *       - in: path
       *         name: z
       *         schema:
       *           type: integer
       *         required: true
       *         description: Zoom level
       *       - in: path
       *         name: x
       *         schema:
       *           type: integer
       *         required: true
       *         description: X coordinate
       *       - in: path
       *         name: y
       *         schema:
       *           type: integer
       *         required: true
       *         description: Y coordinate
       *       - in: path
       *         name: format
       *         required: true
       *         schema:
       *           type: string
       *           enum: [jpeg, jpg, png, webp]
       *           example: png
       *         description: Tile format
       *       - in: query
       *         name: tileSize
       *         schema:
       *           type: integer
       *           enum: [256, 512]
       *           example: 256
       *         required: false
       *         description: Tile size
       *       - in: query
       *         name: tileScale
       *         schema:
       *           type: number
       *           example: 1
       *         required: false
       *         description: Tile scale
       *     responses:
       *       200:
       *         description: Style tile
       *         content:
       *           image/png:
       *             schema:
       *               type: string
       *               format: binary
       *       400:
       *         description: Invalid params
       *       404:
       *         description: Not found
       *       503:
       *         description: Server is starting up
       *         content:
       *           text/plain:
       *             schema:
       *               type: string
       *               example: Starting...
       *       500:
       *         description: Internal server error
       */
      app.get("/styles/:id/:z/:x/:y.:format", getRenderedTileHandler());
    }

    if (process.env.SERVE_FRONT_PAGE !== "false") {
      /**
       * @swagger
       * tags:
       *   - name: Style
       *     description: Style related endpoints
       * /styles/{id}:
       *   get:
       *     tags:
       *       - Style
       *     summary: Serve style page
       *     parameters:
       *       - in: path
       *         name: id
       *         schema:
       *           type: string
       *           example: id
       *         required: true
       *         description: ID of the style
       *     responses:
       *       200:
       *         description: Style page
       *         content:
       *           text/html:
       *             schema:
       *               type: string
       *       404:
       *         description: Not found
       *       503:
       *         description: Server is starting up
       *         content:
       *           text/plain:
       *             schema:
       *               type: string
       *               example: Starting...
       *       500:
       *         description: Internal server error
       */
      app.get("/styles/:id", serveStyleHandler());
    }
  },

  /**
   * Add style
   * @returns {void}
   */
  add: async () => {
    if (!config.styles) {
      printLog("info", "No styles in config. Skipping...");
    } else {
      const ids = Object.keys(config.styles);

      printLog("info", `Loading ${ids.length} styles...`);

      const repos = {};

      await Promise.all(
        ids.map(async (id) => {
          const item = config.styles[id];

          let isCanServeRendered = false;

          const styleInfo = {};

          let styleJSON;

          /* Serve style */
          try {
            if (item.cache) {
              styleInfo.path = `${process.env.DATA_DIR}/caches/styles/${item.style}/style.json`;

              const cacheSource = seed.styles?.[item.style];

              if (!cacheSource) {
                throw new Error(`Cache style "${item.style}" is invalid`);
              }

              if (item.cache.forward) {
                styleInfo.sourceURL = cacheSource.url;
                styleInfo.headers = cacheSource.headers;
                styleInfo.storeCache = item.cache.store;
              }
            } else {
              styleInfo.path = `${process.env.DATA_DIR}/styles/${item.style}`;
            }

            try {
              /* Read style.json file */
              styleJSON = JSON.parse(await getStyle(styleInfo.path));

              /* Validate style */
              if (item.validate) {
                await validateStyle(styleJSON);
              }

              /* Store style info */
              styleInfo.name = styleJSON.name ?? "Unknown";
              styleInfo.zoom = styleJSON.zoom ?? 0;
              styleInfo.center = styleJSON.center ?? [0, 0, 0];

              /* Mark to serve rendered */
              isCanServeRendered = true;
            } catch (error) {
              if (item.cache && error.message.includes("Not Found")) {
                const styleSeed = seed.styles[item.style];

                styleInfo.name = styleSeed.metadata.name ?? "Unknown";
                styleInfo.zoom = styleSeed.metadata.zoom ?? 0;
                styleInfo.center = styleSeed.metadata.center ?? [0, 0, 0];

                /* Mark to serve rendered */
                isCanServeRendered = false;
              } else {
                throw error;
              }
            }

            /* Add to repo */
            repos[id] = styleInfo;
          } catch (error) {
            printLog(
              "error",
              `Failed to load style id "${id}": ${error}. Skipping...`,
            );
          }

          /* Serve rendered */
          if (process.env.BACKEND_RENDER === "true" && isCanServeRendered) {
            try {
              /* Rendered info */
              const tileJSON = createTileMetadata({
                name: styleInfo.name,
                description: styleInfo.name,
              });

              /* Fix center */
              if (
                styleJSON.center?.length >= 2 &&
                styleJSON.zoom !== undefined
              ) {
                tileJSON.center = [
                  styleJSON.center[0],
                  styleJSON.center[1],
                  Math.floor(styleJSON.zoom),
                ];
              }

              /* Add to repo */
              repos[id].tileJSON = tileJSON;
            } catch (error) {
              printLog(
                "error",
                `Failed to load rendered of style id "${id}": ${error}. Skipping...`,
              );
            }
          }
        }),
      );

      config.styles = repos;
    }
  },
};
