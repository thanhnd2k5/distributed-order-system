export const authKeys = {
  all: ["auth"],
  me: ["auth", "me"],
};

export const orderKeys = {
  all: ["orders"],
  site: (siteId) => ["orders", siteId],
  menu: (siteId) => ["orders", siteId, "menu"],
  list: (siteId) => ["orders", siteId, "list"],
  detail: (siteId, orderId) => ["orders", siteId, "detail", orderId],
};

export const inventoryKeys = {
  all: ["inventory"],
  ingredients: ["inventory", "ingredients"],
  recipes: ["inventory", "recipes"],
  reservation: (orderId) => ["inventory", "reservation", orderId],
};

export const kitchenKeys = {
  all: ["kitchen"],
  tickets: (status = "ALL") => ["kitchen", "tickets", status || "ALL"],
};

export const reportingKeys = {
  all: ["reporting"],
  summary: ["reporting", "summary"],
  orders: (filters = {}) => ["reporting", "orders", filters],
  detail: (orderId) => ["reporting", "detail", orderId],
};
