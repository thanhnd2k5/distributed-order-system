import PropTypes from "prop-types";
import { Badge } from "components/ui";

const STATUS_META = {
  PENDING_INVENTORY: {
    label: "Chờ Kho xác nhận",
    color: "warning",
  },
  RESERVED: {
    label: "Đã giữ nguyên liệu",
    color: "info",
  },
  WAITING: {
    label: "Chờ Bếp xử lý",
    color: "warning",
  },
  DONE: {
    label: "Hoàn tất",
    color: "success",
  },
  FAILED: {
    label: "Thất bại",
    color: "error",
  },
};

export function OrderStatusBadge({ status, className }) {
  const normalized = String(status || "").toUpperCase();
  const meta = STATUS_META[normalized] || {
    label: status || "Không rõ",
    color: "neutral",
  };

  return (
    <Badge variant="soft" color={meta.color} className={className}>
      {meta.label}
    </Badge>
  );
}

OrderStatusBadge.propTypes = {
  status: PropTypes.string,
  className: PropTypes.string,
};
