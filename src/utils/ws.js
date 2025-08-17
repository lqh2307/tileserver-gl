"use strict";

import { createAdapter } from "@socket.io/cluster-adapter";
import { setupWorker } from "@socket.io/sticky";
import { printLog } from "./logger.js";
import { Server } from "socket.io";
import cluster from "cluster";

let socketServer;

/**
 * Setup WS server
 * @param {object} server Server
 * @returns {void}
 */
export function setupWSServer(server) {
  if (!cluster.isPrimary) {
    socketServer = new Server(server);

    socketServer.adapter(createAdapter());

    setupWorker(socketServer);

    socketServer.on("connection", (socket) => {
      printLog("info", `WS client connected: ${socket.id}`);

      socket
        .on("disconnect", () => {
          printLog("info", `WS client ${socket.id} closed`);
        })
        .on("error", (error) => {
          printLog("error", `WS client ${socket.id} error: ${error}`);
        });
    });
  }
}

/**
 * Emit WS message
 * @param {string} event Event
 * @param {string} message Message
 * @param {string[]} ids ID list
 * @returns {void}
 */
export function emitWSMessage(event = "message", message, ids) {
  if (!socketServer) {
    return;
  }

  if (ids) {
    ids.forEach((id) => socketServer.to(id).emit(event, message));
  } else {
    socketServer.emit(event, message);
  }
}
