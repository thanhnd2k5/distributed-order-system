import { kitchenClient } from "api/serviceClients";

export async function listTickets(status) {
  const params = status && status !== "ALL" ? { status } : undefined;
  const { data } = await kitchenClient.get("/api/kitchen/tickets", { params });
  return data;
}

export async function completeTicket(ticketId) {
  const { data } = await kitchenClient.post(
    `/api/kitchen/tickets/${ticketId}/complete`,
  );
  return data;
}
