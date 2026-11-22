export const POLLING_INTERVAL_MS = 2000;

export const BRANCH_OPTIONS = [
  {
    id: "POS01",
    label: "POS01 - Quầy 1",
    shortLabel: "POS01",
    channel: "POS",
    port: 8081,
  },
  {
    id: "POS02",
    label: "POS02 - Quầy 2",
    shortLabel: "POS02",
    channel: "POS",
    port: 8082,
  },
  {
    id: "ONLINE",
    label: "ONLINE - Trực tuyến",
    shortLabel: "ONLINE",
    channel: "ONLINE",
    port: 8083,
  },
];

export const ORDER_STATUS_LIST = [
  "PENDING_INVENTORY",
  "RESERVED",
  "DONE",
  "CANCELLED",
  "FAILED",
];

export const TERMINAL_ORDER_STATUSES = new Set(["DONE", "CANCELLED", "FAILED"]);

export const ORDER_STATUS_META = {
  PENDING_INVENTORY: { label: "Chờ Kho", color: "warning", rank: 0 },
  RESERVED: { label: "Đã giữ", color: "info", rank: 1 },
  DONE: { label: "Hoàn tất", color: "success", rank: 2 },
  CANCELLED: { label: "Đã hủy", color: "neutral", rank: 2 },
  FAILED: { label: "Thất bại", color: "error", rank: 2 },
};

export const KITCHEN_STATUS_META = {
  NOT_EXISTS: { label: "Chưa có", color: "neutral" },
  CANCEL_PENDING: { label: "Hủy chờ đơn", color: "warning" },
  QUEUED: { label: "Chờ nấu", color: "warning" },
  COOKING: { label: "Đang nấu", color: "info" },
  DONE: { label: "Hoàn tất", color: "success" },
  CANCELLED: { label: "Đã hủy", color: "neutral" },
  FAILED: { label: "Thất bại", color: "error" },
};

export const DISH_STATUS_META = {
  AVAILABLE: { label: "Còn", color: "success" },
  SOLD_OUT: { label: "Hết", color: "error" },
  HIDDEN: { label: "Ẩn", color: "neutral" },
};

export const DELIVERY_STATUS_META = {
  NOT_SENT: { label: "Chưa gửi, bếp chưa nhận", color: "warning" },
  SENDING: { label: "Đang gửi, chưa rõ bếp đã nhận", color: "info" },
  CANCEL_NOT_SENT: {
    label: "Yêu cầu hủy chưa gửi, chưa chắc hủy được",
    color: "warning",
  },
  SENT: { label: "Đã gửi tới Kafka", color: "success" },
};

export const CUSTOMER_CANCEL_REASONS = [
  { code: "CUSTOMER_CHANGED_MIND", label: "Khách đổi ý" },
  { code: "CUSTOMER_WAITED_TOO_LONG", label: "Khách chờ quá lâu" },
  { code: "WRONG_ORDER_ENTERED", label: "Thu ngân nhập sai đơn" },
];

export const KITCHEN_REJECTION_REASONS = [
  {
    code: "EQUIPMENT_FAILURE",
    label: "Hỏng thiết bị",
    requiresSpoiledIngredient: false,
  },
  {
    code: "SHORT_STAFFED",
    label: "Thiếu người",
    requiresSpoiledIngredient: false,
  },
  {
    code: "SPOILED_INGREDIENT",
    label: "Nguyên liệu hỏng",
    requiresSpoiledIngredient: true,
  },
];

export const REASON_LABELS = {
  INSUFFICIENT_INGREDIENTS: "Thiếu nguyên liệu",
  INSUFFICIENT_INGREDIENT: "Thiếu nguyên liệu",
  INGREDIENT_AWAITING_STOCK_CHECK: "Nguyên liệu đang chờ kiểm kho",
  DISH_HAS_NO_RECIPE: "Món chưa có công thức",
  RECIPE_NOT_FOUND: "Món chưa có công thức",
  ORDER_EXPIRED: "Đơn quá hạn",
  INVALID_CREATION_TIME: "Thời điểm tạo không hợp lệ (ở tương lai)",
  INVALID_ORDER_DATA: "Dữ liệu đơn không hợp lệ",
  EQUIPMENT_FAILURE: "Hỏng thiết bị",
  SHORT_STAFFED: "Thiếu người",
  SPOILED_INGREDIENT: "Nguyên liệu hỏng",
  CUSTOMER_CHANGED_MIND: "Khách đổi ý",
  CUSTOMER_WAITED_TOO_LONG: "Khách chờ quá lâu",
  WRONG_ORDER_ENTERED: "Thu ngân nhập sai đơn",
};

export const ALERT_KIND_META = {
  NEEDS_REVIEW: { label: "Cần kiểm tra (DLQ)", color: "error" },
  UNMATCHED_COMMAND: {
    label: "Lệnh hoàn không khớp bản ghi giữ",
    color: "warning",
  },
  WRONG_BRANCH_CANCEL: { label: "Hủy sai chi nhánh", color: "warning" },
};

export const DEMO_SCENARIOS = [
  {
    code: "KB-1",
    title: "Yêu cầu hủy tới trước đơn",
    summary:
      "Tạo đơn khi Kho chậm/dừng hoặc hủy ngay khi đơn đang Chờ Kho -> Bếp lập Hủy chờ đơn (bếp sẽ không nấu), khi đơn tới Bếp chuyển Đã hủy và Kho hoàn lại.",
  },
  {
    code: "KB-3",
    title: "Tranh chấp Hủy vs Bắt đầu nấu",
    summary:
      "Đơn ở trạng thái Đã giữ / Chờ nấu -> Bếp bấm Bắt đầu nấu và Thu ngân bấm Hủy cùng lúc; đúng 1 bên thắng, bên thua nhận 409 hoặc 'Không thể hủy: bếp đã bắt đầu nấu'.",
  },
  {
    code: "KB-4",
    title: "Từ chối phiếu khi Kho đang dừng",
    summary:
      "Bếp từ chối phiếu Chờ nấu (hoặc báo Nguyên liệu hỏng) khi Inventory tạm dừng -> Chi nhánh thấy Đã hủy ngay, khi Inventory chạy lại tự hoàn/hao hụt và đánh dấu kiểm kho.",
  },
  {
    code: "KB-10",
    title: "Hủy cục bộ khi mất mạng (chưa thử gửi)",
    summary:
      "Ngắt Kafka cho POS01 -> Tạo đơn (Chưa gửi) -> Bấm Hủy -> POS01 khép Đã hủy cục bộ ngay, khi có mạng lại không gửi sang Kho/Bếp.",
  },
  {
    code: "KB-36",
    title: "Hủy sau khi đã thử gửi & Bếp đã nấu",
    summary:
      "Đặt đơn online (đã gửi), ngắt mạng POS01 -> Bấm Hủy (Đang gửi yêu cầu hủy...) -> Bếp Bắt đầu nấu -> Mở mạng POS01 -> Nhận 'Không thể hủy: bếp đã bắt đầu nấu'.",
  },
];
