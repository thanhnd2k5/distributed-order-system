const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function createUuid() {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

export function isValidUuid(value) {
  return typeof value === "string" && UUID_REGEX.test(value.trim());
}

export function shortUuid(value) {
  if (!value) return "—";
  const str = String(value);
  if (str.length <= 13) return str;
  return `${str.slice(0, 8)}…${str.slice(-4)}`;
}
