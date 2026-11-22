import { getBranchClient } from "./clients";

export async function fetchBranchMenu(branchId) {
  const { data } = await getBranchClient(branchId).get("/menu");
  if (Array.isArray(data)) {
    return {
      stale: false,
      kafkaConnected: true,
      epoch: 0,
      lastHeardAt: null,
      dishes: data.map((item) => ({
        dishId: item.dishId,
        name: item.name || item.dishName || item.dishId,
        status: item.status || "AVAILABLE",
        epoch: item.epoch ?? 0,
        seq: item.seq ?? 0,
      })),
    };
  }
  const dishes = Array.isArray(data?.dishes) ? data.dishes : [];
  return {
    stale: Boolean(data?.stale),
    kafkaConnected: data?.kafkaConnected !== false,
    epoch: data?.epoch ?? 0,
    lastHeardAt: data?.lastHeardAt ?? null,
    dishes: dishes.map((item) => ({
      dishId: item.dishId,
      name: item.name || item.dishName || item.dishId,
      status: item.status || "AVAILABLE",
      epoch: item.epoch ?? 0,
      seq: item.seq ?? 0,
    })),
  };
}

export async function fetchBranchOrders(branchId) {
  const { data } = await getBranchClient(branchId).get("/orders");
  return Array.isArray(data) ? data : [];
}

export async function fetchBranchOrderDetail(branchId, orderId) {
  const { data } = await getBranchClient(branchId).get(`/orders/${orderId}`);
  return data;
}

export async function createBranchOrder(branchId, payload) {
  const { data } = await getBranchClient(branchId).post("/orders", payload);
  return data;
}

export async function cancelBranchOrder(branchId, orderId, reasonCode) {
  const { data } = await getBranchClient(branchId).post(
    `/orders/${orderId}/cancel`,
    { reasonCode },
  );
  return data;
}

export async function fetchBranchMonitor(branchId) {
  const { data } = await getBranchClient(branchId).get("/monitor");
  return {
    branchId: data?.branchId || branchId,
    kafkaConnected: data?.kafkaConnected !== false,
    lastProbeAt: data?.lastProbeAt ?? null,
    lastProbeError: data?.lastProbeError ?? null,
    pendingOutboxCount: data?.pendingOutboxCount ?? 0,
    pendingInventoryAlerts: Array.isArray(data?.pendingInventoryAlerts)
      ? data.pendingInventoryAlerts
      : [],
    expiringUnsentAlerts: Array.isArray(data?.expiringUnsentAlerts)
      ? data.expiringUnsentAlerts
      : [],
    activeCancelRequests: Array.isArray(data?.activeCancelRequests)
      ? data.activeCancelRequests
      : [],
    alerts: Array.isArray(data?.alerts) ? data.alerts : [],
  };
}
