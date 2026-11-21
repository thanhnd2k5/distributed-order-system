import {
  ExclamationTriangleIcon,
  FireIcon,
} from "@heroicons/react/24/outline";
import { Badge, Button, Card } from "components/ui";
import { KitchenStatusBadge } from "app/pages/operations/shared/StatusBadge";
import {
  formatQuantity,
  formatTimeHMS,
  shortUuid,
} from "utils/orderFormatting";

export function KitchenTicketsList({
  tickets = [],
  isLoading,
  isError,
  onStart,
  onReject,
  onComplete,
  busyOrderId,
}) {
  return (
    <Card className="p-4">
      <div className="mb-3 flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <FireIcon className="size-5 text-warning" />
          <h3 className="text-sm font-semibold text-gray-800 dark:text-dark-100">
            Phiếu bếp đang xử lý (Chờ nấu & Đang nấu)
          </h3>
          <Badge color="neutral" variant="soft">
            {tickets.length}
          </Badge>
        </div>
        <span className="text-[11px] text-gray-400">Tự làm mới mỗi 2s</span>
      </div>

      {isLoading ? (
        <p className="text-xs text-gray-400">Đang tải danh sách phiếu bếp...</p>
      ) : isError ? (
        <p className="text-xs text-error">
          Không thể tải phiếu từ Kitchen Service.
        </p>
      ) : tickets.length === 0 ? (
        <p className="py-6 text-center text-xs text-gray-500">
          Hiện không có phiếu nào đang Chờ nấu hoặc Đang nấu.
        </p>
      ) : (
        <div className="space-y-3 max-h-[38rem] overflow-y-auto pr-1">
          {tickets.map((ticket) => {
            const flaggedSet = new Set(ticket.flaggedIngredients || []);
            const hasWarning =
              Boolean(ticket.warning) || flaggedSet.size > 0;
            const isQueued = ticket.status === "QUEUED";
            const isCooking = ticket.status === "COOKING";
            const isBusy = busyOrderId === ticket.orderId;
            const items = Array.isArray(ticket.items) ? ticket.items : [];
            const reserved = Array.isArray(ticket.reserved)
              ? ticket.reserved
              : [];

            return (
              <div
                key={ticket.orderId}
                data-testid={`kitchen-ticket-${ticket.orderId}`}
                className={`rounded-xl border p-3.5 text-xs ${
                  hasWarning
                    ? "border-error/60 bg-error/5 dark:border-error/50"
                    : "border-gray-200 bg-white dark:border-dark-600 dark:bg-dark-800"
                }`}
              >
                <div className="flex flex-wrap items-start justify-between gap-2">
                  <div className="space-y-1">
                    <div className="flex flex-wrap items-center gap-2">
                      <span
                        className="font-mono font-bold text-gray-900 dark:text-dark-50"
                        title={ticket.orderId}
                      >
                        #{shortUuid(ticket.orderId)}
                      </span>
                      <KitchenStatusBadge status={ticket.status} />
                      {ticket.originBranch && (
                        <Badge color="primary" variant="outlined">
                          {ticket.originBranch}
                        </Badge>
                      )}
                      <span className="text-[11px] text-gray-400">
                        {formatTimeHMS(
                          ticket.orderCreatedAt || ticket.updatedAt,
                        )}
                      </span>
                    </div>

                    {hasWarning && (
                      <div
                        role="alert"
                        data-testid={`ticket-warning-${ticket.orderId}`}
                        className="inline-flex items-center gap-1.5 rounded-md bg-error px-2.5 py-1 text-[11px] font-semibold text-white"
                      >
                        <ExclamationTriangleIcon className="size-4 shrink-0" />
                        <span>
                          Cảnh báo: có nguyên liệu đang chờ kiểm kho
                          {flaggedSet.size > 0 &&
                            ` (${Array.from(flaggedSet).join(", ")})`}
                        </span>
                      </div>
                    )}
                  </div>

                  <div className="flex shrink-0 flex-wrap gap-1.5">
                    {isQueued && (
                      <>
                        <Button
                          size="sm"
                          color="primary"
                          disabled={isBusy}
                          onClick={() => onStart(ticket.orderId)}
                          className="px-3 py-1.5 text-xs font-semibold"
                        >
                          Bắt đầu nấu
                        </Button>
                        <Button
                          size="sm"
                          color="error"
                          variant="soft"
                          disabled={isBusy}
                          onClick={() => onReject(ticket)}
                          className="px-3 py-1.5 text-xs font-semibold"
                        >
                          Từ chối
                        </Button>
                      </>
                    )}
                    {isCooking && (
                      <Button
                        size="sm"
                        color="success"
                        disabled={isBusy}
                        onClick={() => onComplete(ticket.orderId)}
                        className="px-3 py-1.5 text-xs font-semibold"
                      >
                        Hoàn thành
                      </Button>
                    )}
                  </div>
                </div>

                <div className="mt-3 grid gap-2 sm:grid-cols-2">
                  <div className="rounded-lg bg-gray-50 p-2 dark:bg-dark-750">
                    <p className="mb-1 text-[11px] font-semibold uppercase text-gray-500 dark:text-dark-300">
                      Món ăn
                    </p>
                    {items.length === 0 ? (
                      <p className="text-[11px] text-gray-400">—</p>
                    ) : (
                      <ul className="space-y-0.5">
                        {items.map((item, idx) => (
                          <li
                            key={`${item.dishId}-${idx}`}
                            className="flex justify-between text-gray-800 dark:text-dark-100"
                          >
                            <span>{item.dishName || item.dishId}</span>
                            <span className="font-mono font-semibold">
                              ×{item.quantity}
                            </span>
                          </li>
                        ))}
                      </ul>
                    )}
                  </div>

                  <div className="rounded-lg bg-gray-50 p-2 dark:bg-dark-750">
                    <p className="mb-1 text-[11px] font-semibold uppercase text-gray-500 dark:text-dark-300">
                      Nguyên liệu đã giữ
                    </p>
                    {reserved.length === 0 ? (
                      <p className="text-[11px] text-gray-400">—</p>
                    ) : (
                      <ul className="space-y-0.5">
                        {reserved.map((ing) => {
                          const isFlagged = flaggedSet.has(ing.ingredientId);
                          return (
                            <li
                              key={ing.ingredientId}
                              className={`flex justify-between ${
                                isFlagged
                                  ? "font-semibold text-error"
                                  : "text-gray-700 dark:text-dark-200"
                              }`}
                            >
                              <span className="font-mono">
                                {ing.ingredientId}
                                {isFlagged && " [Kiểm kho]"}
                              </span>
                              <span className="font-mono">
                                {formatQuantity(ing.quantity)}
                              </span>
                            </li>
                          );
                        })}
                      </ul>
                    )}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </Card>
  );
}
