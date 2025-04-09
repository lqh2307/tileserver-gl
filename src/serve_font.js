"use strict";

import { detectFormatAndHeaders, getRequestHost, gzipAsync } from "./utils.js";
import { getFonts, validateFont } from "./font.js";
import { StatusCodes } from "http-status-codes";
import { printLog } from "./logger.js";
import { config } from "./config.js";
import { seed } from "./seed.js";

/**
 * Get font handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getFontHandler() {
  return async (req, res, next) => {
    const ids = req.params.id;

    try {
      let data = await getFonts(
        ids,
        req.url.slice(req.url.lastIndexOf("/") + 1)
      );

      /* Gzip pbf font */
      const headers = detectFormatAndHeaders(data).headers;
      if (headers["content-encoding"] === undefined) {
        data = await gzipAsync(data);

        res.header("content-encoding", "gzip");
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
 * Get font list handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getFontsListHandler() {
  return async (req, res, next) => {
    try {
      const requestHost = getRequestHost(req);

      const result = await Promise.all(
        Object.keys(config.fonts).map(async (id) => {
          return {
            id: id,
            name: id,
            url: `${requestHost}/fonts/${id}/{range}.pbf`,
          };
        })
      );

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
  },

  add: async () => {
    if (config.fonts === undefined) {
      printLog("info", "No fonts in config. Skipping...");
    } else {
      const ids = Object.keys(config.fonts);

      printLog("info", `Loading ${ids.length} fonts...`);

      const repos = {};

      await Promise.all(
        ids.map(async (id) => {
          const item = config.fonts[id];
          const fontInfo = {};

          try {
            if (item.cache !== undefined) {
              fontInfo.path = `${process.env.DATA_DIR}/caches/fonts/${item.font}`;

              const cacheSource = seed.fonts?.[item.font];

              if (cacheSource === undefined) {
                throw new Error(`Cache font "${item.font}" is invalid`);
              }

              if (item.cache.forward === true) {
                fontInfo.sourceURL = cacheSource.url;
                fontInfo.storeCache = item.cache.store;
              }
            } else {
              fontInfo.path = `${process.env.DATA_DIR}/fonts/${item.font}`;

              /* Validate font */
              await validateFont(fontInfo.path);
            }

            /* Add to repo */
            repos[id] = fontInfo;
          } catch (error) {
            printLog(
              "error",
              `Failed to load font "${id}": ${error}. Skipping...`
            );
          }
        })
      );

      config.fonts = repos;
    }
  },
};
