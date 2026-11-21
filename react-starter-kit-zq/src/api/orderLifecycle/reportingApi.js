import { serviceClients } from "./clients";

export async function fetchReportingSummary() {
  const { data } = await serviceClients.REPORTING.get("/summary");
  return data;
}

export async function fetchReplicatedOrders(params) {
  const { data } = await serviceClients.REPORTING.get("/orders", { params });
  return data;
}

export async function fetchReplicatedOrderDetails(orderId) {
  const { data } = await serviceClients.REPORTING.get(`/orders/${orderId}`);
  return data;
}
