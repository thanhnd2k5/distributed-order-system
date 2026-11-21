import {
  ClockIcon,
  ExclamationTriangleIcon,
  WifiIcon,
} from "@heroicons/react/24/outline";
import { Badge, Button, Card } from "components/ui";
import { LiveElapsedTimer } from "app/pages/operations/shared/LiveElapsedTimer";
import {
  formatDateTime,
  formatDurationSeconds,
  shortUuid,
} from "utils/orderFormatting";

export function BranchMonitorPanel({
  branchId,
  monitor,
  isLoading,
  isError,
  onRequestCancelByOrderId,
}) {
  const kafkaConnected = monitor ? monitor.kafkaConnected !== false : false;
  const pendingInventoryAlerts = monitor?.pendingInventoryAlerts || [];
  const expiringUnsentAlerts = monitor?.expiringUnsentAlerts || [];
  const activeCancelRequests = monitor?.activeCancelRequests || [];

  return (
    <Card className="p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <WifiIcon className="size-5 text-primary-500" />
          <h3 className="text-sm font-semibold text-gray-800 dark:text-dark-100">
            Luồng D — Giám sát chi nhánh {branchId}
          </h3>
        </div>
        {monitor && (
          <div className="flex flex-wrap items-center gap-2">
            <Badge
              color={kafkaConnected ? "success" : "error"}
              variant="soft"
              data-testid="branch-kafka-status"
            >
              {kafkaConnected ? "Đang kết nối Kafka" : "Mất kết nối Kafka"}
            </Badge>
            <Badge
              color={(monitor.pendingOutboxCount ?? 0) > 0 ? "warning" : "neutral"}
              variant="outlined"
              data-testid="branch-outbox-count"
            >
              Gói chờ gửi: {monitor.pendingOutboxCount ?? 0}
            </Badge>
          </div>
        )}
      </div>

      {isLoading ? (
        <p className="text-xs text-gray-400">Đang tải thông số giám sát...</p>
      ) : isError || !monitor ? (
        <p className="text-xs text-error">
          Không thể đọc trạng thái /monitor từ chi nhánh {branchId}.
        </p>
      ) : (
        <div className="space-y-3 text-xs">
          {!kafkaConnected && monitor.lastProbeError && (
            <div className="rounded-lg border border-error/40 bg-error/10 px-3 py-2 text-error">
              Mất kết nối Kafka ({monitor.lastProbeError})
            </div>
          )}

          {pendingInventoryAlerts.length > 0 && (
            <div className="rounded-lg border border-warning/40 bg-warning/10 p-3">
              <div className="mb-2 flex items-center gap-1.5 font-semibold text-warning-darker dark:text-warning-lighter">
                <ExclamationTriangleIcon className="size-4 shrink-0" />
                <span>
                  Cảnh báo đơn ở Chờ Kho quá ngưỡng (
                  {pendingInventoryAlerts.length})
                </span>
              </div>
              <div className="space-y-1.5">
                {pendingInventoryAlerts.map((item) => (
                  <div
                    key={item.orderId}
                    className="flex flex-wrap items-center justify-between gap-2 rounded bg-white/80 px-2.5 py-1.5 dark:bg-dark-800"
                  >
                    <div>
                      <span className="font-mono font-semibold">
                        #{shortUuid(item.orderId)}
                      </span>
                      <span className="ml-2 text-gray-500">
                        Chờ Kho:{" "}
                        {item.waitingSeconds != null
                          ? formatDurationSeconds(item.waitingSeconds)
                          : formatDateTime(item.createdAt)}
                      </span>
                    </div>
                    <Button
                      size="sm"
                      color="error"
                      variant="soft"
                      onClick={() => onRequestCancelByOrderId(item.orderId)}
                      className="px-2.5 py-1 text-xs font-medium"
                    >
                      Yêu cầu hủy
                    </Button>
                  </div>
                ))}
              </div>
            </div>
          )}

          {expiringUnsentAlerts.length > 0 && (
            <div className="rounded-lg border border-error/40 bg-error/10 p-3">
              <div className="mb-2 flex items-center gap-1.5 font-semibold text-error">
                <ClockIcon className="size-4 shrink-0" />
                <span>
                  Đơn trong hàng chờ gửi sắp quá hạn X giờ (
                  {expiringUnsentAlerts.length})
                </span>
              </div>
              <div className="space-y-1">
                {expiringUnsentAlerts.map((item) => (
                  <div
                    key={`${item.orderId}-${item.kind}`}
                    className="flex flex-wrap items-center justify-between gap-2 rounded bg-white/80 px-2.5 py-1.5 dark:bg-dark-800"
                  >
                    <span className="font-mono font-semibold">
                      #{shortUuid(item.orderId)} ({item.kind})
                    </span>
                    <span className="text-error">
                      Còn{" "}
                      {item.remainingSeconds != null
                        ? formatDurationSeconds(item.remainingSeconds)
                        : formatDateTime(item.expiresAt)}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {activeCancelRequests.length > 0 && (
            <div className="rounded-lg border border-info/40 bg-info/10 p-3">
              <p className="mb-1.5 font-semibold text-info-darker dark:text-info-lighter">
                Đơn đang có cờ Yêu cầu hủy ({activeCancelRequests.length})
              </p>
              <div className="space-y-1">
                {activeCancelRequests.map((item) => (
                  <div
                    key={item.orderId}
                    className="flex flex-wrap items-center justify-between gap-2 rounded bg-white/80 px-2.5 py-1.5 dark:bg-dark-800"
                  >
                    <span className="font-mono font-semibold">
                      #{shortUuid(item.orderId)} ·{" "}
                      {item.cancelFlag === "KITCHEN_WILL_NOT_COOK"
                        ? "Bếp sẽ không nấu"
                        : "Đang gửi yêu cầu hủy"}
                    </span>
                    <span className="text-gray-600 dark:text-dark-200">
                      Đã chờ:{" "}
                      <LiveElapsedTimer
                        sinceIso={item.cancelRequestedAt}
                        initialSeconds={item.waitedSeconds}
                      />
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {pendingInventoryAlerts.length === 0 &&
            expiringUnsentAlerts.length === 0 &&
            activeCancelRequests.length === 0 && (
              <p className="text-gray-500 dark:text-dark-300">
                Không có cảnh báo chờ Kho, sắp quá hạn, hay yêu cầu hủy treo.
              </p>
            )}
        </div>
      )}
    </Card>
  );
}
