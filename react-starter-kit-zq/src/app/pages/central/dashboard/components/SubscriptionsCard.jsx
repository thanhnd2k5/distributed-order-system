import PropTypes from "prop-types";
import { Badge, Card, Spinner, Table, TBody, Td, Th, THead, Tr } from "components/ui";
import { formatDateTime } from "utils/time";

export function SubscriptionsCard({
  subscriptions = [],
  isLoading = false,
  isError = false,
}) {
  return (
    <Card className="overflow-hidden">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-gray-200 px-5 py-3.5 dark:border-dark-600">
        <div>
          <h2 className="text-base font-semibold text-gray-900 dark:text-dark-50">
            Trạng thái PostgreSQL Logical Replication (Kịch bản D11)
          </h2>
          <p className="text-xs text-gray-500 dark:text-dark-300">
            Theo dõi workerPid và dấu thời gian nhận WAL từ từng publisher về `central-db`
          </p>
        </div>
        <Badge variant="soft" color="neutral">
          {subscriptions.length} subscriptions
        </Badge>
      </div>

      {isLoading && subscriptions.length === 0 ? (
        <div className="flex items-center gap-2 p-5 text-sm text-gray-500">
          <Spinner className="size-4" color="primary" />
          <span>Đang đọc pg_subscription từ central-db...</span>
        </div>
      ) : isError && subscriptions.length === 0 ? (
        <div className="p-5 text-sm text-error dark:text-error-light">
          Không đọc được trạng thái subscription. Đây là lỗi Order Query Service, không phải danh sách subscription rỗng.
        </div>
      ) : subscriptions.length === 0 ? (
        <div className="p-5 text-sm text-gray-500 dark:text-dark-300">
          central-db chưa có subscription nào.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <Table className="w-full text-left">
            <THead>
              <Tr>
                <Th>Subscription</Th>
                <Th>Trạng thái worker</Th>
                <Th>Worker PID</Th>
                <Th>Send Time</Th>
                <Th>Receipt Time</Th>
                <Th>Latest End Time</Th>
              </Tr>
            </THead>
            <TBody>
              {subscriptions.map((sub) => {
                const hasExplicitEnabled = typeof sub.enabled === "boolean";
                const isRunning = Boolean(sub.workerPid);
                const isEnabled = hasExplicitEnabled ? sub.enabled : isRunning;

                return (
                  <Tr key={sub.subscriptionName}>
                    <Td className="font-mono text-xs font-semibold text-gray-900 dark:text-dark-100">
                      {sub.subscriptionName}
                    </Td>
                    <Td>
                      <Badge
                        variant="soft"
                        color={isEnabled && isRunning ? "success" : "warning"}
                      >
                        {isEnabled && isRunning
                          ? "Đang hoạt động"
                          : isEnabled
                            ? "Đang chờ worker"
                            : "Tạm dừng / Disabled"}
                      </Badge>
                    </Td>
                    <Td className="font-mono text-xs">
                      {isRunning ? (
                        <span className="text-success font-medium">
                          PID {sub.workerPid}
                        </span>
                      ) : (
                        <span className="text-error dark:text-error-light font-medium">
                          Không chạy (null)
                        </span>
                      )}
                    </Td>
                    <Td className="whitespace-nowrap text-xs text-gray-600 dark:text-dark-300">
                      {formatDateTime(sub.lastMessageSendTime)}
                    </Td>
                    <Td className="whitespace-nowrap text-xs text-gray-600 dark:text-dark-300">
                      {formatDateTime(sub.lastMessageReceiptTime)}
                    </Td>
                    <Td className="whitespace-nowrap text-xs text-gray-600 dark:text-dark-300">
                      {formatDateTime(sub.latestEndTime)}
                    </Td>
                  </Tr>
                );
              })}
            </TBody>
          </Table>
        </div>
      )}
    </Card>
  );
}

SubscriptionsCard.propTypes = {
  subscriptions: PropTypes.array,
  isLoading: PropTypes.bool,
  isError: PropTypes.bool,
};
