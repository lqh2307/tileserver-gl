"use strict";

import { getRequestHost, getVersion } from "./utils.js";
import swaggerUi from "swagger-ui-express";
import swaggerJsdoc from "swagger-jsdoc";
import express from "express";

/**
 * Serve swagger handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
async function serveSwagger() {
  const version = await getVersion();

  return swaggerUi.setup(
    swaggerJsdoc({
      swaggerDefinition: {
        openapi: "3.0.0",
        info: {
          title: "Tile Server API",
          version: version,
          description: "API for tile server",
        },
      },
      servers: [
        {
          url: getRequestHost(req),
          description: "Tile server",
        },
      ],
      apis: ["src/*.js"],
    })
  );
}

export const serve_swagger = {
  init: () => {
    const app = express().disable("x-powered-by");

    if (process.env.SERVE_SWAGGER !== "false") {
      app.use("/index.html", swaggerUi.serve, serveSwagger());
    }

    return app;
  },
};
