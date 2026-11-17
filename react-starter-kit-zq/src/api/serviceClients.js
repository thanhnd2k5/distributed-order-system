import axios from "axios";
import { CENTRAL_SERVICES, ORDER_SITES } from "configs/endpoints.config";

const STATUS_FALLBACK_MESSAGES = {
  400: "Dữ liệu gửi lên không hợp lệ.",
  404: "Không tìm thấy dữ liệu trên service này.",
  409: "Xung đột dữ liệu: clientRequestId đã được dùng cho nội dung đơn khác.",
  500: "Lỗi xử lý phía server.",
};

function extractErrorMessage(error) {
  const response = error?.response;
  if (!response) {
    return "Không thể kết nối tới service (có thể service đang tắt hoặc bị cô lập mạng).";
  }

  const data = response.data;
  if (typeof data === "string" && data.trim()) {
    return data;
  }

  if (data && typeof data === "object") {
    if (typeof data.detail === "string" && data.detail.trim()) {
      return data.detail;
    }
    if (data.detail && typeof data.detail === "object") {
      const messages = Object.values(data.detail).filter(Boolean);
      if (messages.length > 0) {
        return messages.join("\n");
      }
    }
    if (typeof data.message === "string" && data.message.trim()) {
      return data.message;
    }
    if (typeof data.title === "string" && data.title.trim()) {
      return data.title;
    }
  }

  return (
    STATUS_FALLBACK_MESSAGES[response.status] ||
    error.message ||
    "Yêu cầu thất bại."
  );
}

/**
 * Create an isolated Axios client per backend service/site.
 * Backend CORS allows FRONTEND_ORIGIN without allowCredentials, so withCredentials MUST stay false.
 */
function createServiceClient(baseURL, serviceName) {
  const client = axios.create({
    baseURL,
    withCredentials: false,
    timeout: 8000,
  });

  client.interceptors.response.use(
    (response) => response,
    (error) => {
      const status = error?.response?.status ?? null;
      const normalized = new Error(extractErrorMessage(error));
      normalized.name = "ServiceApiError";
      normalized.status = status;
      normalized.isNetworkError = !error?.response;
      normalized.serviceName = serviceName;
      normalized.baseURL = baseURL;
      normalized.data = error?.response?.data ?? null;
      return Promise.reject(normalized);
    },
  );

  return client;
}

const orderClients = {
  POS01: createServiceClient(ORDER_SITES.POS01.baseURL, "Order POS01"),
  POS02: createServiceClient(ORDER_SITES.POS02.baseURL, "Order POS02"),
  ONLINE: createServiceClient(ORDER_SITES.ONLINE.baseURL, "Order ONLINE"),
};

export function getOrderClient(siteId) {
  const normalized = String(siteId || "").toUpperCase();
  const client = orderClients[normalized];
  if (!client) {
    throw new Error(`Unsupported siteId: ${siteId}`);
  }
  return client;
}

export const inventoryClient = createServiceClient(
  CENTRAL_SERVICES.INVENTORY.baseURL,
  CENTRAL_SERVICES.INVENTORY.label,
);

export const kitchenClient = createServiceClient(
  CENTRAL_SERVICES.KITCHEN.baseURL,
  CENTRAL_SERVICES.KITCHEN.label,
);

export const reportingClient = createServiceClient(
  CENTRAL_SERVICES.QUERY.baseURL,
  CENTRAL_SERVICES.QUERY.label,
);
