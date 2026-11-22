import { Badge } from "components/ui";
import {
  ALERT_KIND_META,
  DELIVERY_STATUS_META,
  DISH_STATUS_META,
  KITCHEN_STATUS_META,
  ORDER_STATUS_META,
} from "constants/orderLifecycle";

export function OrderStatusBadge({ status, className = "" }) {
  const meta = ORDER_STATUS_META[status] || {
    label: status || "Không rõ",
    color: "neutral",
  };
  return (
    <Badge
      color={meta.color}
      variant="soft"
      data-status={status}
      className={`rounded-full px-2.5 py-0.5 text-xs font-semibold ${className}`}
    >
      {meta.label}
    </Badge>
  );
}

export function KitchenStatusBadge({ status, className = "" }) {
  const meta = KITCHEN_STATUS_META[status] || {
    label: status || "Không rõ",
    color: "neutral",
  };
  return (
    <Badge
      color={meta.color}
      variant="soft"
      data-kitchen-status={status}
      className={`rounded-full px-2.5 py-0.5 text-xs font-semibold ${className}`}
    >
      {meta.label}
    </Badge>
  );
}

export function DishStatusBadge({ status, staleOrOffline = false, className = "" }) {
  if (status === "SOLD_OUT" && staleOrOffline) {
    return (
      <Badge
        color="warning"
        variant="soft"
        data-dish-status={status}
        className={`rounded-full px-2.5 py-0.5 text-xs font-semibold ${className}`}
      >
        Có thể đã hết
      </Badge>
    );
  }
  const meta = DISH_STATUS_META[status] || {
    label: status || "Không rõ",
    color: "neutral",
  };
  return (
    <Badge
      color={meta.color}
      variant="soft"
      data-dish-status={status}
      className={`rounded-full px-2.5 py-0.5 text-xs font-semibold ${className}`}
    >
      {meta.label}
    </Badge>
  );
}

export function DeliveryStatusBadge({ deliveryStatus, className = "" }) {
  if (!deliveryStatus) return null;
  const meta = DELIVERY_STATUS_META[deliveryStatus] || {
    label: deliveryStatus,
    color: "neutral",
  };
  return (
    <Badge
      color={meta.color}
      variant="outlined"
      className={`rounded-full px-2 py-0.5 text-[11px] font-medium ${className}`}
    >
      {meta.label}
    </Badge>
  );
}

export function AlertKindBadge({ kind, className = "" }) {
  const meta = ALERT_KIND_META[kind] || {
    label: kind || "Cảnh báo",
    color: "warning",
  };
  return (
    <Badge
      color={meta.color}
      variant="soft"
      className={`rounded-full px-2.5 py-0.5 text-xs font-semibold ${className}`}
    >
      {meta.label}
    </Badge>
  );
}
