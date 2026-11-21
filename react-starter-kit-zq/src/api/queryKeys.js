export const authKeys = {
  all: ["auth"],
  me: ["auth", "me"],
};

export const orderLifecycleKeys = {
  branch: (branchId) => ["branch", branchId],
  branchMenu: (branchId) => ["branch", branchId, "menu"],
  branchOrders: (branchId) => ["branch", branchId, "orders"],
  branchOrderDetail: (branchId, orderId) => [
    "branch",
    branchId,
    "orders",
    orderId,
  ],
  branchMonitor: (branchId) => ["branch", branchId, "monitor"],
  kitchenTickets: (status) => ["kitchen", "tickets", status || "ACTIVE"],
  kitchenAlerts: (kind) => ["kitchen", "alerts", kind || "ALL"],
  kitchenMonitor: () => ["kitchen", "monitor"],
  inventoryStock: (needsCheckOnly) => [
    "inventory",
    "stock",
    Boolean(needsCheckOnly),
  ],
  inventoryMenuSnapshot: () => ["inventory", "menu-snapshot"],
  inventoryAlerts: (kind) => ["inventory", "alerts", kind || "ALL"],
  inventoryMonitor: () => ["inventory", "monitor"],
  reportingSummary: () => ["reporting", "summary"],
  reportingOrders: (filters) => ["reporting", "orders", filters || {}],
};
