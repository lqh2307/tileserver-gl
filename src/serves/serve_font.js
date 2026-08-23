"use strict";

import { DEFAULT_CONCURRENCY } from "../defaults/index.js";
import { config, seed } from "../configs/index.js";
import { StatusCodes } from "http-status-codes";
import path from "node:path";
import {
  getAndCacheDataFonts,
  validatePBFFont,
  getFontMD5,
  getFont,
} from "../resources/index.js";
import {
  detectFormatAndHeaders,
  isFileNotModified,
  sendTextResponse,
  runAllWithLimit,
  getRequestHost,
  gzipAsync,
  printLog,
} from "../utils/index.js";

/**
 * Get font handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getFontHandler() {
  return async (req, res) => {
    const ids = req.params.id;
    const fileName = req.url.slice(req.url.lastIndexOf("/") + 1);

    try {
      const fontIDs = ids.split(",");
      if (fontIDs.length === 1) {
        const item = config.fonts[fontIDs[0]];
        const filePath = item ? path.join(item.path, fileName) : undefined;

        if (filePath && (await isFileNotModified(req, res, filePath))) {
          return res.status(StatusCodes.NOT_MODIFIED).end();
        }
      }

      /* Get and cache Fonts */
      let data = await getAndCacheDataFonts(ids, fileName);

      /* Gzip pbf font */
      const headers = detectFormatAndHeaders(data).headers;
      if (!headers["content-encoding"]) {
        data = await gzipAsync(data);

        headers["content-encoding"] = "gzip";
      }

      res.set(headers);

      return res.status(StatusCodes.OK).send(data);
    } catch (error) {
      printLog("error", `Failed to get font "${ids}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get font MD5 handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getFontMD5Handler() {
  return async (req, res) => {
    const id = req.params.id;

    try {
      const item = config.fonts[id];

      /* Check font is used? */
      if (!item) {
        return sendTextResponse(
          res,
          StatusCodes.NOT_FOUND,
          `Font id "${id}" does not exist`,
        );
      }

      /* Calculate MD5 and Add to header */
      res.set("etag", await getFontMD5(item.path));

      return res.status(StatusCodes.OK).send();
    } catch (error) {
      printLog("error", `Failed to get md5 of font id "${id}": ${error}`);

      if (error.message.includes("Not Found")) {
        return sendTextResponse(res, StatusCodes.NO_CONTENT, error.message);
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
    }
  };
}

/**
 * Get font ttf/otf/woff/woff2 handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getFontStaticHandler() {
  return async (req, res) => {
    const id = req.params.id;
    const format = req.params.format;
    const filePath = path.join(
      process.env.DATA_DIR,
      `${format}fonts`,
      id,
      `${req.params.name}.${format}`,
    );

    try {
      if (await isFileNotModified(req, res, filePath)) {
        return res.status(StatusCodes.NOT_MODIFIED).end();
      }

      /* Get static Font */
      let data = await getFont(filePath);

      /* Add header */
      res.set(detectFormatAndHeaders(data).headers);

      return res.status(StatusCodes.OK).send(data);
    } catch (error) {
      printLog("error", `Failed to get font "${format}" id "${id}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get font list handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getFontsListHandler() {
  return async (req, res) => {
    try {
      const requestHost = getRequestHost(req);

      const result = await Promise.all(
        Object.keys(config.fonts).map(async (id) => {
          return {
            id,
            name: id,
            url: `${requestHost}/fonts/${id}/{range}.pbf`,
          };
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
      printLog("error", `Failed to get fonts": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

export const serve_font = {
  /**
   * Register font handlers
   * @param {Express} app Express object
   * @returns {void}
   */
  init: (app) => {
    /**
     * @swagger
     * tags:
     *   - name: Font
     *     description: Font related endpoints
     * /fonts/fonts.json:
     *   get:
     *     tags:
     *       - Font
     *     summary: Get all fonts
     *     parameters:
     *       - in: query
     *         name: compression
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Compressed response
     *     responses:
     *       200:
     *         description: List of fonts
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
    app.get("/fonts/fonts.json", getFontsListHandler());

    /**
     * @swagger
     * tags:
     *   - name: Font
     *     description: Font related endpoints
     * /fonts/{id}/md5:
     *   get:
     *     tags:
     *       - Font
     *     summary: Get font md5
     *     parameters:
     *       - in: path
     *         name: id
     *         schema:
     *           type: string
     *           example: id
     *         required: true
     *         description: ID of the font
     *     responses:
     *       200:
     *         description: Font md5
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
    app.get("/fonts/:id/md5", getFontMD5Handler());

    /**
     * @swagger
     * tags:
     *   - name: Font
     *     description: Font related endpoints
     * /fonts/{id}/{range}.pbf:
     *   get:
     *     tags:
     *       - Font
     *     summary: Get font
     *     parameters:
     *       - in: path
     *         name: id
     *         required: true
     *         schema:
     *           type: string
     *           example: id
     *         description: Font ID
     *       - in: path
     *         name: range
     *         required: true
     *         schema:
     *           type: string
     *           pattern: "\\d{1,5}-\\d{1,5}"
     *           example: 0-255
     *         description: Font range
     *     responses:
     *       200:
     *         description: Font data
     *         content:
     *           application/octet-stream:
     *             schema:
     *               type: string
     *               format: binary
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
    app.get("/fonts/:id/:range.pbf", getFontHandler());

    /**
     * @swagger
     * tags:
     *   - name: Font
     *     description: Font related endpoints
     * /fonts/{id}/{name}.{format}:
     *   get:
     *     tags:
     *       - Font
     *     summary: Get static font
     *     parameters:
     *       - in: path
     *         name: id
     *         required: true
     *         schema:
     *           type: string
     *           example: id
     *         description: Font ID
     *       - in: path
     *         name: name
     *         required: true
     *         schema:
     *           type: string
     *           example: name
     *         description: Font name
     *       - in: path
     *         name: format
     *         required: true
     *         schema:
     *           type: string
     *           enum: [ttf, otf, woff, woff2]
     *           example: ttf
     *         description: Font format
     *     responses:
     *       200:
     *         description: Font static data
     *         content:
     *           application/octet-stream:
     *             schema:
     *               type: string
     *               format: binary
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
    app.get("/fonts/:id/:name.:format", getFontStaticHandler());
  },

  /**
   * Add font
   * @returns {void}
   */
  add: async () => {
    if (!config.fonts) {
      printLog("info", "No fonts in config. Skipping...");
    } else {
      const ids = Object.keys(config.fonts);

      printLog("info", `Loading ${ids.length} fonts...`);

      const repos = {};

      await runAllWithLimit(
        ids.map((id) => {
          return async () => {
            const item = config.fonts[id];
            const fontInfo = {};

            try {
              if (item.cache) {
                fontInfo.path = path.join(
                  process.env.DATA_DIR,
                  "caches/fonts",
                  item.font,
                );

                const cacheSource = seed.fonts?.[item.font];

                if (!cacheSource) {
                  throw new Error(`Cache font "${item.font}" is invalid`);
                }

                if (item.cache.forward) {
                  fontInfo.sourceURL = cacheSource.url;
                  fontInfo.headers = cacheSource.headers;
                  fontInfo.storeCache = item.cache.store;
                }
              } else {
                fontInfo.path = path.join(
                  process.env.DATA_DIR,
                  "fonts",
                  item.font,
                );

                /* Validate font */
                if (item.validate) {
                  await validatePBFFont(fontInfo.path);
                }
              }

              /* Add to repo */
              repos[id] = fontInfo;
            } catch (error) {
              printLog(
                "error",
                `Failed to load font id "${id}": ${error}. Skipping...`,
              );
            }
          };
        }),
        DEFAULT_CONCURRENCY,
      );

      config.fonts = repos;
    }
  },
};
