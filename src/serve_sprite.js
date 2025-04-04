"use strict";

import { getSprite, validateSprite } from "./sprite.js";
import { StatusCodes } from "http-status-codes";
import { getRequestHost } from "./utils.js";
import { printLog } from "./logger.js";
import { config } from "./config.js";
import { seed } from "./seed.js";
import express from "express";

/**
 * Get sprite handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getSpriteHandler() {
  return async (req, res, next) => {
    if (["png", "json"].includes(req.params.format) === false) {
      return res
        .status(StatusCodes.BAD_REQUEST)
        .send("Sprite format is not support");
    }

    const id = req.params.id;

    try {
      const item = config.sprites[id];

      if (item === undefined) {
        return res.status(StatusCodes.NOT_FOUND).send("Sprite does not exist");
      }

      const data = await getSprite(
        id,
        req.url.slice(req.url.lastIndexOf("/") + 1)
      );

      if (req.params.format === "json") {
        res.header("content-type", "application/json");
      } else {
        res.header("content-type", "image/png");
      }

      return res.status(StatusCodes.OK).send(data);
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
  init: () => {
    const app = express().disable("x-powered-by");

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
    app.get("/sprites.json", getSpritesListHandler());

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
    app.get("/:id/sprite:scale(@2x)?.:format", getSpriteHandler());

    return app;
  },

  add: async () => {
    if (config.sprites === undefined) {
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
            if (item.cache !== undefined) {
              spriteInfo.path = `${process.env.DATA_DIR}/caches/sprites/${item.sprite}`;

              const cacheSource = seed.sprites?.[item.sprite];

              if (cacheSource === undefined) {
                throw new Error(`Cache sprite "${item.sprite}" is invalid`);
              }

              if (item.cache.forward === true) {
                spriteInfo.sourceURL = cacheSource.url;
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
