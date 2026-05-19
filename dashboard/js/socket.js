import { addAlert, addAlertsPayload, setSocketConnected, upsertMetric } from "./state.js";

export function connectMarketSocket({ onChange, onError }) {
  if (typeof io !== "function") {
    onError?.(new Error("Socket.IO client is not available"));
    return null;
  }

  const socket = io();

  socket.on("connect", () => {
    setSocketConnected(true);
    onChange?.();
  });

  socket.on("disconnect", () => {
    setSocketConnected(false);
    onChange?.();
  });

  socket.on("connect_error", (error) => {
    setSocketConnected(false);
    onError?.(error);
    onChange?.();
  });

  socket.on("market:snapshot", (snapshot) => {
    (snapshot?.metrics || []).forEach((item) => upsertMetric(item.metric, item.key));
    addAlertsPayload(snapshot?.alerts || []);
    onChange?.();
  });

  socket.on("market:metrics", (payload) => {
    upsertMetric(payload?.metric, payload?.key);
    onChange?.();
  });

  socket.on("market:alert", (alert) => {
    addAlert(alert);
    onChange?.();
  });

  return socket;
}
