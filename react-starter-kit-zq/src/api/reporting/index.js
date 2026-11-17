import { reportingClient } from "api/serviceClients";

export async function getReportingSummary() {
  const { data } = await reportingClient.get("/api/reporting/summary");
  return data;
}

export async function listReplicatedOrders({ siteId, status, limit = 100 } = {}) {
  const params = {};
  if (siteId && siteId !== "ALL") params.siteId = siteId;
  if (status && status !== "ALL") params.status = status;
  if (limit) params.limit = limit;

  const { data } = await reportingClient.get("/api/reporting/orders", {
    params,
  });
  return data;
}

export async function getReplicatedOrder(orderId) {
  const { data } = await reportingClient.get(
    `/api/reporting/orders/${orderId}`,
  );
  return data;
}
