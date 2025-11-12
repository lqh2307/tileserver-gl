"use strict";

import { getRequestHost, getVersion } from "../utils/index.js";
import swaggerUi from "swagger-ui-express";
import swaggerJsdoc from "swagger-jsdoc";

/**
 * Serve swagger handler
 * @returns {(req: Request, res: Response, next: NextFunction) => any}
 */
function serveSwagger() {
  return async (req, res, next) => {
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
      }),
    )(req, res, next);
  };
}

export const serve_swagger = {
  /**
   * Register swagger handlers
   * @param {Express} app Express object
   * @returns {void}
   */
  init: (app) => {
    /* Serve swagger */
    if (process.env.SERVE_SWAGGER !== "false") {
      app.use("/swagger/index.html", swaggerUi.serve, serveSwagger());
    }
  },
};
