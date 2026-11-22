import { serviceClients } from "./clients";

export async function fetchInventoryStock(needsCheckOnly = false) {
  const params = needsCheckOnly ? { needsCheck: true } : undefined;
  const { data } = await serviceClients.INVENTORY.get("/stock", { params });
  return Array.isArray(data) ? data : [];
}

export async function createStockReceipt({ ingredientId, quantity }) {
  const { data } = await serviceClients.INVENTORY.post("/stock/receipts", {
    ingredientId,
    quantity: Number(quantity),
  });
  return data;
}

export async function clearIngredientCheck(ingredientId, countedOnHand) {
  const hasCount =
    countedOnHand !== undefined &&
    countedOnHand !== null &&
    String(countedOnHand).trim() !== "";
  const body = hasCount ? { countedOnHand: Number(countedOnHand) } : {};
  const { data } = await serviceClients.INVENTORY.post(
    `/ingredients/${ingredientId}/clear-check`,
    body,
  );
  return data;
}

export async function fetchInventoryMenuSnapshot() {
  const { data } = await serviceClients.INVENTORY.get("/menu/snapshot");
  return {
    epoch: data?.epoch ?? 0,
    seq: data?.seq ?? 0,
    dishes: Array.isArray(data?.dishes) ? data.dishes : [],
  };
}

export async function bumpInventoryEpoch() {
  const { data } = await serviceClients.INVENTORY.post("/admin/epoch/bump");
  return data;
}

export async function fetchInventoryAlerts(kind) {
  const params = kind ? { kind } : undefined;
  const { data } = await serviceClients.INVENTORY.get("/alerts", { params });
  return Array.isArray(data) ? data : [];
}

export async function fetchInventoryMonitor() {
  const { data } = await serviceClients.INVENTORY.get("/monitor");
  return {
    instanceId: data?.instanceId || "inventory",
    kafkaReachable: data?.kafkaReachable ?? data?.kafkaConnected ?? true,
    slowConsumerThresholdSeconds: data?.slowConsumerThresholdSeconds ?? 300,
    slowConsumerWarning: Boolean(
      data?.slowConsumerWarning ?? data?.anySlowConsumerWarning,
    ),
    needsCheckCount: data?.needsCheckCount ?? 0,
    pendingOutboxCount: data?.pendingOutboxCount ?? 0,
    alertsCount: data?.alertsCount ?? 0,
    topics: Array.isArray(data?.topics)
      ? data.topics.map((t) => ({
          topic: t.topic,
          partitionCount: t.partitionCount ?? null,
          totalUnread: t.totalUnread ?? t.unreadCount ?? 0,
          oldestUnreadAgeSeconds: t.oldestUnreadAgeSeconds ?? null,
          slowWarning: Boolean(t.slowWarning ?? t.slowConsumerWarning),
        }))
      : [],
  };
}
