require("dotenv").config();

const fs = require("fs");
const path = require("path");
const http = require("http");

const express = require("express");
const cors = require("cors");
const { Server } = require("socket.io");
const { Kafka } = require("kafkajs");
const { MongoClient } = require("mongodb");

const PORT = Number(process.env.PORT || 3000);

const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || "crypto-market-api";
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092")
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);

const TOPIC_METRICS = process.env.KAFKA_TOPIC_METRICS || "crypto.metrics";
const TOPIC_ALERTS = process.env.KAFKA_TOPIC_ALERTS || "crypto.alerts";

const MONGO_URI =
  process.env.MONGO_URI || "mongodb://root:root@localhost:27017/?authSource=admin";
const MONGO_DB = process.env.MONGO_DB || "crypto";

const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";

const MAX_RECENT_ALERTS = Number(process.env.MAX_RECENT_ALERTS || 100);
const MAX_LATEST_METRICS = Number(process.env.MAX_LATEST_METRICS || 100);

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: CORS_ORIGIN,
    methods: ["GET", "POST"]
  }
});

app.use(cors({ origin: CORS_ORIGIN }));
app.use(express.json());

const dashboardPath = path.join(__dirname, "..", "dashboard");
app.use(express.static(dashboardPath));

const latestMetrics = new Map();
const recentAlerts = [];

let mongoClient = null;
let db = null;
let tradesCollection = null;
let metricsCollection = null;
let alertsCollection = null;

let metricsConsumer = null;
let alertsConsumer = null;

const runtimeStatus = {
  mongo: "starting",
  kafka: {
    metricsConsumer: "starting",
    alertsConsumer: "starting"
  },
  socket: {
    clients: 0
  }
};

function safeJsonParse(buffer) {
  if (!buffer) {
    return null;
  }

  try {
    return JSON.parse(buffer.toString("utf8"));
  } catch (error) {
    console.error("Message Kafka JSON invalide :", error.message);
    return null;
  }
}

function metricKey(metric) {
  const source = metric.source || "unknown";
  const market = metric.market || metric.symbol || "unknown";

  return `${source}:${market}`;
}

function pushMetric(metric) {
  const key = metricKey(metric);

  latestMetrics.set(key, {
    ...metric,
    api_received_ts: Date.now()
  });

  if (latestMetrics.size > MAX_LATEST_METRICS) {
    const firstKey = latestMetrics.keys().next().value;
    latestMetrics.delete(firstKey);
  }

  return key;
}

function pushAlert(alert) {
  const enrichedAlert = {
    ...alert,
    api_received_ts: Date.now()
  };

  recentAlerts.unshift(enrichedAlert);

  if (recentAlerts.length > MAX_RECENT_ALERTS) {
    recentAlerts.pop();
  }

  return enrichedAlert;
}

function buildMongoFilter(query) {
  const filter = {};

  if (query.source) {
    filter.source = String(query.source);
  }

  if (query.market) {
    filter.market = String(query.market);
  }

  if (query.symbol) {
    filter.symbol = String(query.symbol);
  }

  if (query.type) {
    filter.type = String(query.type);
  }

  if (query.severity) {
    filter.severity = String(query.severity);
  }

  return filter;
}

function parseLimit(value, defaultValue, maxValue) {
  const parsed = Number(value || defaultValue);

  if (Number.isNaN(parsed) || parsed <= 0) {
    return defaultValue;
  }

  return Math.min(parsed, maxValue);
}

function asyncHandler(handler) {
  return async (req, res, next) => {
    try {
      await handler(req, res, next);
    } catch (error) {
      next(error);
    }
  };
}

async function connectMongo() {
  mongoClient = new MongoClient(MONGO_URI);

  await mongoClient.connect();

  db = mongoClient.db(MONGO_DB);

  tradesCollection = db.collection("trades");
  metricsCollection = db.collection("metrics");
  alertsCollection = db.collection("alerts");

  await db.command({ ping: 1 });

  runtimeStatus.mongo = "connected";

  console.log(`MongoDB connecté : ${MONGO_DB}`);
}

async function startKafkaConsumers() {
  const kafka = new Kafka({
    clientId: KAFKA_CLIENT_ID,
    brokers: KAFKA_BROKERS,
    retry: {
      retries: 10,
      initialRetryTime: 1000,
      multiplier: 1.5
    }
  });

  metricsConsumer = kafka.consumer({
    groupId: "api-metrics-websocket-consumer"
  });

  alertsConsumer = kafka.consumer({
    groupId: "api-alerts-websocket-consumer"
  });

  await metricsConsumer.connect();

  await metricsConsumer.subscribe({
    topic: TOPIC_METRICS,
    fromBeginning: false
  });

  runtimeStatus.kafka.metricsConsumer = "connected";

  metricsConsumer
    .run({
      eachMessage: async ({ message }) => {
        const metric = safeJsonParse(message.value);

        if (!metric) {
          return;
        }

        const key = pushMetric(metric);

        io.emit("market:metrics", {
          key,
          metric: latestMetrics.get(key)
        });
      }
    })
    .catch((error) => {
      runtimeStatus.kafka.metricsConsumer = "error";
      console.error("Erreur consumer Kafka metrics :", error);
    });

  await alertsConsumer.connect();

  await alertsConsumer.subscribe({
    topic: TOPIC_ALERTS,
    fromBeginning: false
  });

  runtimeStatus.kafka.alertsConsumer = "connected";

  alertsConsumer
    .run({
      eachMessage: async ({ message }) => {
        const alert = safeJsonParse(message.value);

        if (!alert) {
          return;
        }

        const enrichedAlert = pushAlert(alert);

        io.emit("market:alert", enrichedAlert);
      }
    })
    .catch((error) => {
      runtimeStatus.kafka.alertsConsumer = "error";
      console.error("Erreur consumer Kafka alerts :", error);
    });

  console.log(`Kafka connecté : ${KAFKA_BROKERS.join(", ")}`);
  console.log(`Topic metrics écouté : ${TOPIC_METRICS}`);
  console.log(`Topic alerts écouté : ${TOPIC_ALERTS}`);
}

io.on("connection", (socket) => {
  runtimeStatus.socket.clients += 1;

  console.log(`Client Socket.IO connecté : ${socket.id}`);

  socket.emit("market:snapshot", {
    server_ts: Date.now(),
    metrics: Array.from(latestMetrics.entries()).map(([key, metric]) => ({
      key,
      metric
    })),
    alerts: recentAlerts
  });

  socket.on("disconnect", () => {
    runtimeStatus.socket.clients = Math.max(0, runtimeStatus.socket.clients - 1);
    console.log(`Client Socket.IO déconnecté : ${socket.id}`);
  });
});

app.get("/", (req, res) => {
  const dashboardIndex = path.join(dashboardPath, "index.html");

  if (fs.existsSync(dashboardIndex)) {
    return res.sendFile(dashboardIndex);
  }

  return res.json({
    message: "Crypto Market API is running",
    dashboard: "dashboard/index.html not created yet",
    health: "/api/health",
    latestMetrics: "/api/metrics/latest",
    latestAlerts: "/api/alerts/latest",
    historyMetrics: "/api/history/metrics",
    historyAlerts: "/api/history/alerts",
    latestTrades: "/api/trades/latest"
  });
});

app.get("/api/health", (req, res) => {
  res.json({
    status: "ok",
    service: "crypto-market-api",
    server_ts: Date.now(),
    kafka: runtimeStatus.kafka,
    mongo: runtimeStatus.mongo,
    socket: runtimeStatus.socket,
    topics: {
      metrics: TOPIC_METRICS,
      alerts: TOPIC_ALERTS
    },
    memory: {
      metrics_series: latestMetrics.size,
      recent_alerts: recentAlerts.length
    }
  });
});

app.get(
  "/api/metrics/latest",
  asyncHandler(async (req, res) => {
    const source = req.query.source ? String(req.query.source) : null;
    const market = req.query.market ? String(req.query.market) : null;

    let data = Array.from(latestMetrics.entries()).map(([key, metric]) => ({
      key,
      metric
    }));

    if (source) {
      data = data.filter((item) => item.metric.source === source);
    }

    if (market) {
      data = data.filter((item) => item.metric.market === market);
    }

    if (data.length > 0) {
      return res.json({
        source: "memory",
        count: data.length,
        data
      });
    }

    const limit = parseLimit(req.query.limit, 20, 200);
    const filter = buildMongoFilter(req.query);

    const docs = await metricsCollection
      .find(filter)
      .sort({ computed_time: -1 })
      .limit(limit)
      .toArray();

    return res.json({
      source: "mongodb",
      count: docs.length,
      data: docs
    });
  })
);

app.get(
  "/api/alerts/latest",
  asyncHandler(async (req, res) => {
    const source = req.query.source ? String(req.query.source) : null;
    const market = req.query.market ? String(req.query.market) : null;
    const type = req.query.type ? String(req.query.type) : null;

    let data = recentAlerts;

    if (source) {
      data = data.filter((alert) => alert.source === source);
    }

    if (market) {
      data = data.filter((alert) => alert.market === market);
    }

    if (type) {
      data = data.filter((alert) => alert.type === type);
    }

    if (data.length > 0) {
      const limit = parseLimit(req.query.limit, 50, 200);

      return res.json({
        source: "memory",
        count: data.slice(0, limit).length,
        data: data.slice(0, limit)
      });
    }

    const limit = parseLimit(req.query.limit, 50, 200);
    const filter = buildMongoFilter(req.query);

    const docs = await alertsCollection
      .find(filter)
      .sort({ created_time: -1 })
      .limit(limit)
      .toArray();

    return res.json({
      source: "mongodb",
      count: docs.length,
      data: docs
    });
  })
);

app.get(
  "/api/history/metrics",
  asyncHandler(async (req, res) => {
    const limit = parseLimit(req.query.limit, 300, 2000);
    const filter = buildMongoFilter(req.query);

    const docs = await metricsCollection
      .find(filter)
      .sort({ computed_time: -1 })
      .limit(limit)
      .toArray();

    return res.json({
      count: docs.length,
      data: docs.reverse()
    });
  })
);

app.get(
  "/api/history/alerts",
  asyncHandler(async (req, res) => {
    const limit = parseLimit(req.query.limit, 100, 1000);
    const filter = buildMongoFilter(req.query);

    const docs = await alertsCollection
      .find(filter)
      .sort({ created_time: -1 })
      .limit(limit)
      .toArray();

    return res.json({
      count: docs.length,
      data: docs
    });
  })
);

app.get(
  "/api/trades/latest",
  asyncHandler(async (req, res) => {
    const limit = parseLimit(req.query.limit, 50, 500);
    const filter = buildMongoFilter(req.query);

    const docs = await tradesCollection
      .find(filter)
      .sort({ exchange_time: -1 })
      .limit(limit)
      .toArray();

    return res.json({
      count: docs.length,
      data: docs
    });
  })
);

app.use((req, res) => {
  res.status(404).json({
    error: "Route not found",
    path: req.originalUrl
  });
});

app.use((error, req, res, next) => {
  console.error("Erreur API :", error);

  res.status(500).json({
    error: "Internal server error",
    message: error.message
  });
});

async function startServer() {
  try {
    await connectMongo();
    await startKafkaConsumers();

    server.listen(PORT, () => {
      console.log(`API REST + Socket.IO lancée : http://localhost:${PORT}`);
      console.log(`Healthcheck : http://localhost:${PORT}/api/health`);
      console.log(`Dashboard : http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error("Erreur au démarrage de l'API :", error);
    process.exit(1);
  }
}

async function shutdown() {
  console.log("Arrêt demandé de l'API...");

  try {
    if (metricsConsumer) {
      await metricsConsumer.disconnect();
    }

    if (alertsConsumer) {
      await alertsConsumer.disconnect();
    }

    if (mongoClient) {
      await mongoClient.close();
    }
  } catch (error) {
    console.error("Erreur pendant l'arrêt :", error);
  }

  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

startServer();