"use strict";

import { printLog } from "./logger.js";
import WebSocket, { WebSocketServer } from "ws";
import { v4 } from "uuid";

let wss;

/**
 * Setup WS server
 * @param {object} server Server
 * @returns {void}
 */
export function setupWSServer(server) {
  const wss = new WebSocketServer({
    server,
  });

  wss.on("connection", (ws) => {
    const clientID = v4();

    printLog("info", `WS client connected: ${clientID}`);

    ws.send(clientID);

    ws.on("message", (msg) => {
      printLog("info", `WS client message ${clientID}: ${msg}`);
    })
      .on("close", () => {
        printLog("info", `WS client closed: ${clientID}`);
      })
      .on("error", (err) => {
        printLog("error", `WS client error ${clientID}: ${err.message}`);
      });
  });
}

/**
 * Send message
 * @param {string} message Message
 * @param {string[]} ids ID list
 * @returns {void}
 */
export function broadcast(message, ids) {
  if (!wss) {
    return;
  }

  wss.clients.forEach((client) => {
    if (
      client.readyState === WebSocket.OPEN &&
      (!ids || ids.includes(client.id))
    ) {
      client.send(message);
    }
  });
}
