import { REASON_LABELS, TERMINAL_ORDER_STATUSES } from "constants/orderLifecycle";

export function generateUuid() {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (char) => {
    const rand = Math.floor(Math.random() * 16);
    const value = char === "x" ? rand : (rand & 0x3) | 0x8;
    return value.toString(16);
  });
}

export function isTerminalOrderStatus(order) {
  if (!order) return false;
  if (TERMINAL_ORDER_STATUSES.has(order.status)) return true;
  return order.statusRank === 2;
}

export function translateReason(code) {
  if (!code) return "—";
  return REASON_LABELS[code] || code;
}

export function formatCancelMessage(message) {
  if (!message) return "";
  const lower = String(message).toLowerCase();
  if (
    lower.includes("started cooking") ||
    lower.includes("đã bắt đầu nấu") ||
    lower.includes("cannot cancel")
  ) {
    return "Không thể hủy: bếp đã bắt đầu nấu";
  }
  return message;
}

export function formatTimeHMS(isoString) {
  if (!isoString) return "—";
  const date = new Date(isoString);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleTimeString("vi-VN", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function formatDateTime(isoString) {
  if (!isoString) return "—";
  const date = new Date(isoString);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleString("vi-VN", {
    hour12: false,
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function formatDurationSeconds(totalSeconds) {
  if (totalSeconds == null || Number.isNaN(Number(totalSeconds))) return "0 giây";
  const safe = Math.max(0, Math.floor(Number(totalSeconds)));
  const hours = Math.floor(safe / 3600);
  const minutes = Math.floor((safe % 3600) / 60);
  const seconds = safe % 60;
  if (hours > 0) {
    return `${hours} giờ ${String(minutes).padStart(2, "0")} phút`;
  }
  if (minutes > 0) {
    return `${minutes} phút ${String(seconds).padStart(2, "0")} giây`;
  }
  return `${seconds} giây`;
}

export function computeElapsedSeconds(isoString, nowMs = Date.now()) {
  if (!isoString) return 0;
  const parsed = Date.parse(isoString);
  if (Number.isNaN(parsed)) return 0;
  return Math.max(0, Math.floor((nowMs - parsed) / 1000));
}

export function formatQuantity(value) {
  if (value === null || value === undefined || value === "") return "0";
  const num = Number(value);
  if (Number.isNaN(num)) return String(value);
  return Number.isInteger(num)
    ? num.toLocaleString("vi-VN")
    : num.toLocaleString("vi-VN", { maximumFractionDigits: 3 });
}

export function shortUuid(uuid) {
  if (!uuid) return "—";
  const str = String(uuid);
  return str.length > 8 ? str.slice(0, 8) : str;
}

export function extractApiError(error) {
  const status = error?.response?.status ?? null;
  const data = error?.response?.data ?? null;
  const currentStatus =
    data && typeof data === "object" ? data.currentStatus ?? null : null;

  let message = "Đã xảy ra lỗi khi gọi dịch vụ.";
  if (data && typeof data === "object") {
    message =
      data.detail ||
      data.message ||
      data.error ||
      data.title ||
      error?.message ||
      message;
  } else if (typeof data === "string" && data.trim()) {
    message = data.trim();
  } else if (error?.message) {
    message = error.message;
  }

  return { status, message, currentStatus, data };
}
