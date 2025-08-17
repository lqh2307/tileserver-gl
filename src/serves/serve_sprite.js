"use strict";

import { getAndCacheDataSprite } from "../data.js";
import { StatusCodes } from "http-status-codes";
import { validateSprite } from "../sprite.js";
import { config } from "../config.js";
import { seed } from "../seed.js";
import {
  detectContentTypeFromFormat,
  getRequestHost,
  deepClone,
  gzipAsync,
  printLog,
} from "../utils/index.js";

/**
 * Get sprite handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getSpriteHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      /* Check sprite format? */
      if (!["png", "json"].includes(req.params.format)) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send("Sprite format is not support");
      }

      /* Get and cache Sprite */
      const sprite = await getAndCacheDataSprite(
        id,
        req.url.slice(req.url.lastIndexOf("/") + 1)
      );

      res.header(
        "content-type",
        detectContentTypeFromFormat(req.params.format)
      );

      return res.status(StatusCodes.OK).send(sprite);
    } catch (error) {
      printLog("error", `Failed to get sprite "${id}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get sprite list handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getSpritesListHandler() {
  return async (req, res, next) => {
    try {
      const requestHost = getRequestHost(req);

      const result = await Promise.all(
        Object.keys(config.sprites).map(async (id) => {
          return {
            id: id,
            name: id,
            urls: [
              `${requestHost}/sprites/${id}/sprite.json`,
              `${requestHost}/sprites/${id}/sprite.png`,
            ],
          };
        })
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
      printLog("error", `Failed to get sprites": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

export const serve_sprite = {
  /**
   * Register sprite handlers
   * @param {Express} app Express object
   * @returns {void}
   */
  init: (app) => {
    /**
     * @swagger
     * tags:
     *   - name: Sprite
     *     description: Sprite related endpoints
     * /sprites/sprites.json:
     *   get:
     *     tags:
     *       - Sprite
     *     summary: Get all sprites
     *     parameters:
     *       - in: query
     *         name: compression
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Compressed response
     *     responses:
     *       200:
     *         description: List of all sprites
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
     *                   urls:
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
    app.get("/sprites/sprites.json", getSpritesListHandler());

    /**
     * @swagger
     * tags:
     *   - name: Sprite
     *     description: Sprite related endpoints
     * /sprites/{id}/sprite{scale}.{format}:
     *   get:
     *     tags:
     *       - Sprite
     *     summary: Get sprite
     *     parameters:
     *       - in: path
     *         name: id
     *         schema:
     *           type: string
     *           example: id
     *         required: true
     *         description: ID of the sprite
     *       - in: path
     *         name: scale
     *         schema:
     *           type: string
     *         required: false
     *         description: Scale of the sprite (e.g., @2x)
     *       - in: path
     *         name: format
     *         schema:
     *           type: string
     *           enum: [json, png]
     *           example: json
     *         required: true
     *         description: Format of the sprite
     *     responses:
     *       200:
     *         description: Sprite
     *         content:
     *           application/json:
     *             schema:
     *               type: object
     *           image/png:
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
    app.get(
      ["/sprites/:id/sprite.:format", "/sprites/:id/sprite@2x.:format"],
      getSpriteHandler()
    );
  },

  /**
   * Add sprite
   * @returns {void}
   */
  add: async () => {
    if (!config.sprites) {
      printLog("info", "No sprites in config. Skipping...");
    } else {
      const ids = Object.keys(config.sprites);

      printLog("info", `Loading ${ids.length} sprites...`);

      const repos = {};

      await Promise.all(
        ids.map(async (id) => {
          const item = config.sprites[id];
          const spriteInfo = {};

          try {
            if (item.cache) {
              spriteInfo.path = `${process.env.DATA_DIR}/caches/sprites/${item.sprite}`;

              const cacheSource = seed.sprites?.[item.sprite];

              if (!cacheSource) {
                throw new Error(`Cache sprite "${item.sprite}" is invalid`);
              }

              if (item.cache.forward) {
                spriteInfo.sourceURL = cacheSource.url;
                spriteInfo.headers = deepClone(cacheSource.headers);
                spriteInfo.storeCache = item.cache.store;
              }
            } else {
              spriteInfo.path = `${process.env.DATA_DIR}/sprites/${item.sprite}`;

              /* Validate sprite */
              await validateSprite(spriteInfo.path);
            }

            /* Add to repo */
            repos[id] = spriteInfo;
          } catch (error) {
            printLog(
              "error",
              `Failed to load sprite "${id}": ${error}. Skipping...`
            );
          }
        })
      );

      config.sprites = repos;
    }
  },
};
