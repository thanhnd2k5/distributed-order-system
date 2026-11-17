import { inventoryClient } from "api/serviceClients";

export async function listIngredients() {
  const { data } = await inventoryClient.get("/api/ingredients");
  return data;
}

export async function listRecipes() {
  const { data } = await inventoryClient.get("/api/recipes");
  return data;
}

export async function getReservation(orderId) {
  const { data } = await inventoryClient.get(`/api/reservations/${orderId}`);
  return data;
}
