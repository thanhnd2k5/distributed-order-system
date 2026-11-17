const trimTrailingSlash = (url) => (url ? url.replace(/\/+$/, "") : "");

export const ORDER_SITES = {
  POS01: {
    siteId: "POS01",
    sourceType: "POS",
    label: "Quầy POS01",
    shortLabel: "POS01",
    baseURL: trimTrailingSlash(
      import.meta.env.VITE_POS01_API_URL || "http://localhost:8081",
    ),
  },
  POS02: {
    siteId: "POS02",
    sourceType: "POS",
    label: "Quầy POS02",
    shortLabel: "POS02",
    baseURL: trimTrailingSlash(
      import.meta.env.VITE_POS02_API_URL || "http://localhost:8082",
    ),
  },
  ONLINE: {
    siteId: "ONLINE",
    sourceType: "ONLINE",
    label: "Kênh Online",
    shortLabel: "ONLINE",
    baseURL: trimTrailingSlash(
      import.meta.env.VITE_ONLINE_API_URL || "http://localhost:8083",
    ),
  },
};

export const POS_SITE_IDS = ["POS01", "POS02"];
export const ALL_SITE_IDS = ["POS01", "POS02", "ONLINE"];

export const CENTRAL_SERVICES = {
  INVENTORY: {
    id: "INVENTORY",
    label: "Kho trung tâm",
    baseURL: trimTrailingSlash(
      import.meta.env.VITE_INVENTORY_API_URL || "http://localhost:8084",
    ),
  },
  KITCHEN: {
    id: "KITCHEN",
    label: "Bếp trung tâm",
    baseURL: trimTrailingSlash(
      import.meta.env.VITE_KITCHEN_API_URL || "http://localhost:8085",
    ),
  },
  QUERY: {
    id: "QUERY",
    label: "Order Query Service",
    baseURL: trimTrailingSlash(
      import.meta.env.VITE_QUERY_API_URL || "http://localhost:8080",
    ),
  },
};

export function getSiteConfig(siteId) {
  const normalized = String(siteId || "").toUpperCase();
  return ORDER_SITES[normalized] || null;
}
