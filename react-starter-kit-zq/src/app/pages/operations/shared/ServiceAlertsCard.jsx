import { ExclamationTriangleIcon } from "@heroicons/react/24/outline";
import { Badge, Card } from "components/ui";
import { formatDateTime, shortUuid } from "utils/orderFormatting";
import { AlertKindBadge } from "./StatusBadge";

export function ServiceAlertsCard({
  title = "Cảnh báo Luồng D",
  alerts = [],
  isLoading = false,
  isError = false,
  emptyText = "Không có cảnh báo bất thường.",
  kindFilter,
  onKindFilterChange,
  filterOptions = [],
}) {
  return (
    <Card className="p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <ExclamationTriangleIcon className="size-5 text-warning" />
          <h3 className="text-sm font-semibold text-gray-800 dark:text-dark-100">
            {title}
          </h3>
          <Badge color={alerts.length > 0 ? "error" : "neutral"} variant="soft">
            {alerts.length}
          </Badge>
        </div>
        {filterOptions.length > 0 && onKindFilterChange && (
          <div className="flex flex-wrap gap-1.5">
            {filterOptions.map((opt) => (
              <button
                key={opt.value || "ALL"}
                type="button"
                onClick={() => onKindFilterChange(opt.value)}
                className={`rounded-lg px-2.5 py-1 text-xs font-medium transition-colors ${
                  kindFilter === opt.value
                    ? "bg-primary-600 text-white"
                    : "bg-gray-100 text-gray-600 hover:bg-gray-200 dark:bg-dark-600 dark:text-dark-200 dark:hover:bg-dark-500"
                }`}
              >
                {opt.label}
              </button>
            ))}
          </div>
        )}
      </div>

      {isLoading ? (
        <p className="text-xs text-gray-400">Đang tải cảnh báo...</p>
      ) : isError ? (
        <p className="text-xs text-error">
          Không thể tải cảnh báo từ dịch vụ.
        </p>
      ) : alerts.length === 0 ? (
        <p className="text-xs text-gray-500 dark:text-dark-300">{emptyText}</p>
      ) : (
        <div className="space-y-2 max-h-64 overflow-y-auto pr-1">
          {alerts.map((alert, idx) => (
            <div
              key={alert.id ?? `${alert.sourceTopic}-${alert.sourceOffset}-${idx}`}
              className="rounded-lg border border-gray-200 bg-gray-50/70 p-2.5 text-xs dark:border-dark-600 dark:bg-dark-800/60"
            >
              <div className="flex flex-wrap items-center justify-between gap-2">
                <AlertKindBadge kind={alert.kind} />
                <span className="text-gray-400 dark:text-dark-300">
                  {formatDateTime(alert.createdAt)}
                </span>
              </div>
              <div className="mt-1.5 flex flex-wrap gap-x-4 gap-y-1 text-gray-700 dark:text-dark-200">
                {alert.orderId && (
                  <span>
                    Mã đơn:{" "}
                    <span className="font-mono font-semibold" title={alert.orderId}>
                      {shortUuid(alert.orderId)}
                    </span>
                  </span>
                )}
                {alert.sourceTopic && (
                  <span>
                    Topic: <span className="font-mono">{alert.sourceTopic}</span>
                    {alert.sourcePartition !== undefined &&
                      `[${alert.sourcePartition}@${alert.sourceOffset}]`}
                  </span>
                )}
              </div>
              {alert.detail && (
                <pre className="mt-1.5 overflow-x-auto rounded bg-gray-100 p-1.5 text-[11px] text-gray-600 dark:bg-dark-700 dark:text-dark-200">
                  {typeof alert.detail === "string"
                    ? alert.detail
                    : JSON.stringify(alert.detail)}
                </pre>
              )}
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}
