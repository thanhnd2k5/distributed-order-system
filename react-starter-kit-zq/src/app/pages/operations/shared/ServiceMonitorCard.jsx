import { SignalIcon } from "@heroicons/react/24/outline";
import { Badge, Card } from "components/ui";
import { formatDurationSeconds } from "utils/orderFormatting";

export function ServiceMonitorCard({
  title = "Giám sát Consumer & Outbox",
  monitor,
  isLoading = false,
  isError = false,
}) {
  const kafkaOk = monitor
    ? (monitor.kafkaConnected ?? monitor.kafkaReachable ?? true)
    : false;
  const slowWarning = Boolean(
    monitor?.slowConsumerWarning ?? monitor?.anySlowConsumerWarning,
  );
  const topics = Array.isArray(monitor?.topics) ? monitor.topics : [];

  return (
    <Card className="p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <SignalIcon className="size-5 text-primary-500" />
          <h3 className="text-sm font-semibold text-gray-800 dark:text-dark-100">
            {title}
          </h3>
        </div>
        {monitor && (
          <div className="flex flex-wrap items-center gap-2">
            <Badge color={kafkaOk ? "success" : "error"} variant="soft">
              {kafkaOk ? "Kafka: Kết nối" : "Kafka: Mất kết nối"}
            </Badge>
            <Badge color="neutral" variant="outlined">
              Outbox chờ gửi: {monitor.pendingOutboxCount ?? 0}
            </Badge>
          </div>
        )}
      </div>

      {isLoading ? (
        <p className="text-xs text-gray-400">Đang tải thông số giám sát...</p>
      ) : isError || !monitor ? (
        <p className="text-xs text-error">
          Không thể kết nối tới endpoint /monitor của dịch vụ.
        </p>
      ) : (
        <div className="space-y-3">
          {slowWarning && (
            <div className="rounded-lg border border-error/40 bg-error/10 px-3 py-2 text-xs font-medium text-error">
              Cảnh báo consumer chậm: có tin chưa đọc cũ hơn ngưỡng{" "}
              {formatDurationSeconds(monitor.slowConsumerThresholdSeconds ?? 300)}
              .
            </div>
          )}

          {monitor.error && (
            <div className="rounded-lg border border-warning/40 bg-warning/10 px-3 py-2 text-xs text-warning-darker dark:text-warning-lighter">
              Lỗi kết nối Kafka: {monitor.error}
            </div>
          )}

          {topics.length > 0 && (
            <div className="overflow-x-auto">
              <table className="min-w-full text-left text-xs">
                <thead>
                  <tr className="border-b border-gray-200 text-gray-500 dark:border-dark-600 dark:text-dark-300">
                    <th className="py-1.5 pr-3 font-medium">Topic</th>
                    <th className="py-1.5 pr-3 font-medium">Tin chưa đọc</th>
                    <th className="py-1.5 pr-3 font-medium">Tuổi tin cũ nhất</th>
                    <th className="py-1.5 font-medium">Trạng thái</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100 dark:divide-dark-700">
                  {topics.map((t) => {
                    const unread = t.unreadCount ?? t.totalUnread ?? 0;
                    const age = t.oldestUnreadAgeSeconds;
                    const isSlow = Boolean(
                      t.slowConsumerWarning ?? t.slowWarning,
                    );
                    return (
                      <tr key={t.topic}>
                        <td className="py-1.5 pr-3 font-mono text-gray-800 dark:text-dark-100">
                          {t.topic}
                        </td>
                        <td className="py-1.5 pr-3 font-mono">{unread}</td>
                        <td className="py-1.5 pr-3 font-mono">
                          {age != null ? formatDurationSeconds(age) : "—"}
                        </td>
                        <td className="py-1.5">
                          <Badge
                            color={isSlow ? "error" : "success"}
                            variant="soft"
                            className="text-[11px]"
                          >
                            {isSlow ? "Chậm" : "Bình thường"}
                          </Badge>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </Card>
  );
}
