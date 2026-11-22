import { serviceClients } from "./clients";

export async function fetchKitchenTickets(status) {
  const params = status ? { status } : undefined;
  const { data } = await serviceClients.KITCHEN.get("/tickets", { params });
  return Array.isArray(data) ? data : [];
}

export async function startKitchenTicket(orderId) {
  const { data } = await serviceClients.KITCHEN.post(
    `/tickets/${orderId}/start`,
  );
  return data;
}

export async function rejectKitchenTicket(
  orderId,
  { reasonCode, spoiledIngredients = [] },
) {
  const { data } = await serviceClients.KITCHEN.post(
    `/tickets/${orderId}/reject`,
    {
      reasonCode,
      spoiledIngredients,
    },
  );
  return data;
}

export async function completeKitchenTicket(orderId) {
  const { data } = await serviceClients.KITCHEN.post(
    `/tickets/${orderId}/complete`,
  );
  return data;
}

export async function fetchKitchenAlerts(kind) {
  const params = kind ? { kind } : undefined;
  const { data } = await serviceClients.KITCHEN.get("/alerts", { params });
  return Array.isArray(data) ? data : [];
}

export async function fetchKitchenMonitor() {
  const { data } = await serviceClients.KITCHEN.get("/monitor");
  return {
    instanceId: data?.instanceId || "kitchen",
    consumerGroup: data?.consumerGroup || "kitchen",
    kafkaConnected: data?.kafkaConnected ?? data?.kafkaReachable ?? true,
    error: data?.error ?? null,
    slowConsumerThresholdSeconds: data?.slowConsumerThresholdSeconds ?? 300,
    anySlowConsumerWarning: Boolean(
      data?.anySlowConsumerWarning ?? data?.slowConsumerWarning,
    ),
    pendingOutboxCount: data?.pendingOutboxCount ?? 0,
    topics: Array.isArray(data?.topics)
      ? data.topics.map((t) => ({
          topic: t.topic,
          unreadCount: t.unreadCount ?? t.totalUnread ?? 0,
          oldestUnreadTimestamp: t.oldestUnreadTimestamp ?? null,
          oldestUnreadAgeSeconds: t.oldestUnreadAgeSeconds ?? null,
          slowConsumerWarning: Boolean(
            t.slowConsumerWarning ?? t.slowWarning,
          ),
        }))
      : [],
    checkedAt: data?.checkedAt ?? null,
  };
}
