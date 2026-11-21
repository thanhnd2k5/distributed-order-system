import axios from "axios";

const SERVICE_BASE_URLS = {
  POS01: import.meta.env.VITE_POS01_URL || "/api/pos01",
  POS02: import.meta.env.VITE_POS02_URL || "/api/pos02",
  ONLINE: import.meta.env.VITE_ONLINE_URL || "/api/online",
  INVENTORY: import.meta.env.VITE_INVENTORY_URL || "/api/inventory",
  KITCHEN: import.meta.env.VITE_KITCHEN_URL || "/api/kitchen",
  REPORTING: import.meta.env.VITE_REPORTING_URL || "/api/reporting",
};

function createClient(baseURL) {
  return axios.create({
    baseURL,
    timeout: 8000,
    headers: {
      "Content-Type": "application/json",
    },
  });
}

export const serviceClients = {
  POS01: createClient(SERVICE_BASE_URLS.POS01),
  POS02: createClient(SERVICE_BASE_URLS.POS02),
  ONLINE: createClient(SERVICE_BASE_URLS.ONLINE),
  INVENTORY: createClient(SERVICE_BASE_URLS.INVENTORY),
  KITCHEN: createClient(SERVICE_BASE_URLS.KITCHEN),
  REPORTING: createClient(SERVICE_BASE_URLS.REPORTING),
};

export function getBranchClient(branchId) {
  const key = String(branchId || "POS01").toUpperCase();
  return serviceClients[key] || serviceClients.POS01;
}
