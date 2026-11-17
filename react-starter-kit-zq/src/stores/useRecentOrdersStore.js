import { create } from "zustand";

const STORAGE_KEY = "dos_recent_orders_v1";
const MAX_RECENT = 20;

function toRecentOrder(order) {
  if (!order?.orderId) return null;
  return {
    orderId: String(order.orderId),
    sourceSiteId: order.sourceSiteId || "UNKNOWN",
  };
}

function readInitialOrders() {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.map(toRecentOrder).filter(Boolean).slice(0, MAX_RECENT);
  } catch {
    return [];
  }
}

function persistOrders(orders) {
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(orders));
  } catch {
    // Ignore storage quota errors in demo mode
  }
}

export const useRecentOrdersStore = create((set) => ({
  recentOrders: readInitialOrders(),

  recordOrder: (order) => {
    const recent = toRecentOrder(order);
    if (!recent) return;
    set((state) => {
      const next = [
        recent,
        ...state.recentOrders.filter((item) => item.orderId !== recent.orderId),
      ].slice(0, MAX_RECENT);
      persistOrders(next);
      return { recentOrders: next };
    });
  },
}));
