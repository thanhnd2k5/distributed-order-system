import { getOrderClient } from "api/serviceClients";

export async function listMenu(siteId) {
  const { data } = await getOrderClient(siteId).get("/api/menu");
  return data;
}

export async function createOrder(siteId, payload) {
  const response = await getOrderClient(siteId).post("/api/orders", payload);
  return {
    data: response.data,
    httpStatus: response.status,
    location: response.headers?.location || null,
  };
}

export async function listOrders(siteId) {
  const { data } = await getOrderClient(siteId).get("/api/orders");
  return data;
}

export async function getOrder(siteId, orderId) {
  const { data } = await getOrderClient(siteId).get(`/api/orders/${orderId}`);
  return data;
}
