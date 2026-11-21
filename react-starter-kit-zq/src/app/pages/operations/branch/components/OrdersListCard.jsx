import { useState } from "react";
import {
  ChevronDownIcon,
  ChevronUpIcon,
  ClipboardDocumentListIcon,
} from "@heroicons/react/24/outline";
import { Badge, Button, Card } from "components/ui";
import { useBranchOrderDetailQuery } from "hooks/queries/useBranchQueries";
import { LiveElapsedTimer } from "app/pages/operations/shared/LiveElapsedTimer";
import {
  DeliveryStatusBadge,
  OrderStatusBadge,
} from "app/pages/operations/shared/StatusBadge";
import {
  formatCancelMessage,
  formatTimeHMS,
  isTerminalOrderStatus,
  shortUuid,
  translateReason,
} from "utils/orderFormatting";

function OrderItemsDrawer({ branchId, orderId }) {
  const { data, isLoading, isError } = useBranchOrderDetailQuery(
    branchId,
    orderId,
  );
  if (isLoading) {
    return (
      <p className="mt-2 text-[11px] text-gray-400">
        Đang tải chi tiết món...
      </p>
    );
  }
  if (isError || !data) {
    return (
      <p className="mt-2 text-[11px] text-error">
        Không thể tải chi tiết đơn.
      </p>
    );
  }
  const items = Array.isArray(data.items) ? data.items : [];
  return (
    <div className="mt-2 rounded border border-gray-200 bg-gray-50 p-2 text-[11px] dark:border-dark-600 dark:bg-dark-800/70">
      <p className="font-semibold text-gray-600 dark:text-dark-200">
        Chi tiết món:
      </p>
      <ul className="mt-1 space-y-0.5">
        {items.map((item) => (
          <li
            key={`${item.lineNo}-${item.dishId}`}
            className="flex justify-between text-gray-700 dark:text-dark-200"
          >
            <span>
              {item.dishName || item.dishId}{" "}
              <span className="font-mono text-gray-400">({item.dishId})</span>
            </span>
            <span className="font-mono font-semibold">×{item.quantity}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

export function OrdersListCard({
  branchId,
  orders = [],
  isLoading,
  isError,
  onRequestCancel,
}) {
  const [expandedOrderId, setExpandedOrderId] = useState(null);

  return (
    <Card className="p-4">
      <div className="mb-3 flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <ClipboardDocumentListIcon className="size-5 text-primary-500" />
          <h3 className="text-sm font-semibold text-gray-800 dark:text-dark-100">
            Đơn hàng tại {branchId}
          </h3>
          <Badge color="neutral" variant="soft">
            {orders.length}
          </Badge>
        </div>
        <span className="text-[11px] text-gray-400">Tự làm mới mỗi 2s</span>
      </div>

      {isLoading ? (
        <p className="text-xs text-gray-400">Đang tải danh sách đơn...</p>
      ) : isError ? (
        <p className="text-xs text-error">
          Không thể tải đơn hàng từ {branchId}.
        </p>
      ) : orders.length === 0 ? (
        <p className="py-4 text-center text-xs text-gray-500">
          Chưa có đơn hàng nào tại chi nhánh {branchId}.
        </p>
      ) : (
        <div className="space-y-2.5 max-h-[34rem] overflow-y-auto pr-1">
          {orders.map((order) => {
            const isTerminal = isTerminalOrderStatus(order);
            const reasonCode = order.closeReason || order.failureReason;
            const cancelMsg = formatCancelMessage(order.cancelMessage);
            const hasActiveCancel =
              order.cancelFlag && order.cancelFlag !== "NONE";
            const isExpanded = expandedOrderId === order.orderId;

            return (
              <div
                key={order.orderId}
                data-testid={`order-row-${order.orderId}`}
                className="rounded-xl border border-gray-200 bg-white p-3 text-xs transition-colors dark:border-dark-600 dark:bg-dark-800"
              >
                <div className="flex flex-wrap items-start justify-between gap-2">
                  <div className="min-w-0 space-y-1">
                    <div className="flex flex-wrap items-center gap-2">
                      <span
                        className="font-mono font-semibold text-gray-900 dark:text-dark-50"
                        title={order.orderId}
                      >
                        #{shortUuid(order.orderId)}
                      </span>
                      <OrderStatusBadge status={order.status} />
                      <DeliveryStatusBadge deliveryStatus={order.deliveryStatus} />
                      <span className="text-[11px] text-gray-400">
                        {formatTimeHMS(order.createdAt)}
                      </span>
                    </div>

                    {reasonCode && (
                      <p className="text-gray-600 dark:text-dark-200">
                        Lý do:{" "}
                        <span className="font-medium">
                          {translateReason(reasonCode)}
                        </span>{" "}
                        <span className="font-mono text-[11px] text-gray-400">
                          ({reasonCode})
                        </span>
                      </p>
                    )}

                    {order.cancelFlag === "KITCHEN_WILL_NOT_COOK" && (
                      <div className="inline-flex items-center gap-1.5 rounded-md bg-warning/15 px-2 py-1 text-[11px] font-semibold text-warning-darker dark:text-warning-lighter">
                        <span>Yêu cầu hủy: bếp sẽ không nấu</span>
                        {order.cancelRequestedAt && (
                          <span>
                            (đã chờ{" "}
                            <LiveElapsedTimer sinceIso={order.cancelRequestedAt} />
                            )
                          </span>
                        )}
                      </div>
                    )}

                    {order.cancelFlag === "REQUESTED" && (
                      <div className="inline-flex items-center gap-1.5 rounded-md bg-info/15 px-2 py-1 text-[11px] font-semibold text-info-darker dark:text-info-lighter">
                        <span>Đang gửi yêu cầu hủy...</span>
                        {order.cancelRequestedAt && (
                          <span>
                            (<LiveElapsedTimer sinceIso={order.cancelRequestedAt} />)
                          </span>
                        )}
                      </div>
                    )}

                    {cancelMsg && (
                      <div
                        role="status"
                        className="inline-flex items-center gap-1.5 rounded-md bg-error/10 px-2 py-1 text-[11px] font-semibold text-error"
                      >
                        {cancelMsg}
                      </div>
                    )}
                  </div>

                  <div className="flex shrink-0 items-center gap-1.5">
                    <Button
                      size="sm"
                      variant="flat"
                      color="neutral"
                      onClick={() =>
                        setExpandedOrderId(isExpanded ? null : order.orderId)
                      }
                      className="px-2 py-1 text-[11px]"
                    >
                      {isExpanded ? (
                        <ChevronUpIcon className="size-4" />
                      ) : (
                        <ChevronDownIcon className="size-4" />
                      )}
                      <span className="ml-1">Chi tiết</span>
                    </Button>

                    {!isTerminal && (
                      <Button
                        size="sm"
                        color="error"
                        variant={hasActiveCancel ? "outlined" : "soft"}
                        disabled={hasActiveCancel}
                        onClick={() => onRequestCancel(order)}
                        className="px-2.5 py-1 text-xs font-medium"
                      >
                        {hasActiveCancel ? "Đã yêu cầu hủy" : "Hủy đơn"}
                      </Button>
                    )}
                  </div>
                </div>

                {isExpanded && (
                  <OrderItemsDrawer
                    branchId={branchId}
                    orderId={order.orderId}
                  />
                )}
              </div>
            );
          })}
        </div>
      )}
    </Card>
  );
}
