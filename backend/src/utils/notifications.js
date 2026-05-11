const logger = require('./logger');

// Map from res -> userId
const clients = new Map();
// Map from userId -> timestamp (ms) of last SSE disconnect
const lastSeen = new Map();

function addClient(res, userId) {
  clients.set(res, userId);
  logger.info('SSE client connected', { userId, clientCount: clients.size });
}

function removeClient(res, userId) {
  clients.delete(res);
  // Only mark as lastSeen if the user has no remaining SSE connections
  if (userId && ![...clients.values()].includes(userId)) {
    lastSeen.set(userId, Date.now());
  }
  logger.info('SSE client disconnected', { userId, clientCount: clients.size });
}

function getOnlineUserIds() {
  return new Set(clients.values());
}

function getLastSeen(userId) {
  return lastSeen.get(userId) || null;
}

function broadcast(event) {
  const data = JSON.stringify(event);
  logger.info('SSE broadcast', { type: event.type, clientCount: clients.size });
  for (const [client] of clients) {
    try {
      client.write(`data: ${data}\n\n`);
    } catch {
      clients.delete(client);
    }
  }
}

module.exports = { addClient, removeClient, broadcast, getOnlineUserIds, getLastSeen };
