import { Badge, Card } from "components/ui";
import { ORDER_STATUS_LIST, ORDER_STATUS_META } from "constants/orderLifecycle";
import { OrderStatusBadge } from "app/pages/operations/shared/StatusBadge";
import { formatTimeHMS, translateReason } from "utils/orderFormatting";

function ReasonBreakdownList({ title, counts = {}, emptyText }) {
  const entries = Object.entries(counts || {}).filter(([, val]) => Number(val) > 0);
  return (
    <div className="rounded-lg bg-gray-50 p-2.5 text-xs dark:bg-dark-800/70">
      <p className="mb-1.5 font-semibold text-gray-700 dark:text-dark-200">
        {title}
      </p>
      {entries.length === 0 ? (
        <p className="text-[11px] text-gray-400">{emptyText}</p>
      ) : (
        <ul className="space-y-1">
          {entries.map(([code, count]) => (
            <li
              key={code}
              className="flex items-center justify-between gap-2 text-gray-700 dark:text-dark-200"
            >
              <span>
                {translateReason(code)}{" "}
                <span className="font-mono text-[10px] text-gray-400">
                  ({code})
                </span>
              </span>
              <span className="font-mono font-bold">{count}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export function BranchSummaryCard({ branch }) {
  const dataAsOfText = formatTimeHMS(branch.dataAsOf);

  return (
    <Card
      data-testid={`branch-report-${branch.branchId}`}
      className="flex flex-col gap-3 p-4"
    >
      <div className="flex flex-wrap items-start justify-between gap-2 border-b border-gray-100 pb-3 dark:border-dark-600">
        <div>
          <h3 className="text-base font-bold text-gray-900 dark:text-dark-50">
            {branch.branchId}
          </h3>
          <p className="mt-0.5 text-xs font-medium text-primary-600 dark:text-primary-400">
            Dữ liệu tính đến {dataAsOfText}
          </p>
        </div>
        <Badge color="primary" variant="soft" className="font-mono text-xs">
          Tổng: {branch.totalOrders ?? 0} đơn
        </Badge>
      </div>

      <div className="grid grid-cols-2 gap-2 sm:grid-cols-3">
        {ORDER_STATUS_LIST.map((status) => (
          <div
            key={status}
            className="flex items-center justify-between rounded-lg border border-gray-200 px-2.5 py-1.5 text-xs dark:border-dark-600"
          >
            <span className="text-gray-600 dark:text-dark-200">
              {ORDER_STATUS_META[status]?.label || status}
            </span>
            <span className="font-mono font-bold text-gray-900 dark:text-dark-50">
              {branch.byStatus?.[status] ?? 0}
            </span>
          </div>
        ))}
      </div>

      <div className="grid gap-2 sm:grid-cols-2">
        <ReasonBreakdownList
          title="Lý do Đã hủy"
          counts={branch.cancelledByReason}
          emptyText="Chưa có đơn hủy"
        />
        <ReasonBreakdownList
          title="Lý do Thất bại"
          counts={branch.failedByReason}
          emptyText="Chưa có đơn thất bại"
        />
      </div>
    </Card>
  );
}

export function ChainOverviewPanel({ summary }) {
  const totalOrders = summary?.totalOrders ?? 0;
  const byStatus = summary?.byStatus || {};

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        <Card className="p-3.5">
          <p className="text-[11px] font-semibold uppercase text-gray-500">
            Tổng toàn chuỗi
          </p>
          <p className="mt-1 font-mono text-2xl font-bold text-gray-900 dark:text-dark-50">
            {totalOrders}
          </p>
        </Card>
        {ORDER_STATUS_LIST.map((status) => (
          <Card key={status} className="p-3.5">
            <OrderStatusBadge status={status} />
            <p className="mt-2 font-mono text-2xl font-bold text-gray-900 dark:text-dark-50">
              {byStatus[status] ?? 0}
            </p>
          </Card>
        ))}
      </div>

      <div className="grid gap-4 md:grid-cols-2">
        <Card className="p-4">
          <ReasonBreakdownList
            title="Tổng hợp lý do Đã hủy toàn chuỗi"
            counts={summary?.byCancelledReason}
            emptyText="Chưa có đơn Đã hủy."
          />
        </Card>
        <Card className="p-4">
          <ReasonBreakdownList
            title="Tổng hợp lý do Thất bại toàn chuỗi"
            counts={summary?.byFailedReason}
            emptyText="Chưa có đơn Thất bại."
          />
        </Card>
      </div>
    </div>
  );
}

export function ReplicationStatusCard({ subscriptions = [] }) {
  return (
    <Card className="p-4">
      <h3 className="mb-3 text-sm font-semibold text-gray-800 dark:text-dark-100">
        Trạng thái nhân bản logic PostgreSQL (Central Order Replica)
      </h3>
      {subscriptions.length === 0 ? (
        <p className="text-xs text-gray-500">
          Chưa có subscription nhân bản nào được ghi nhận.
        </p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-left text-xs">
            <thead>
              <tr className="border-b border-gray-200 text-gray-500 dark:border-dark-600 dark:text-dark-300">
                <th className="py-2 pr-3 font-semibold">Subscription</th>
                <th className="py-2 pr-3 font-semibold">Trạng thái</th>
                <th className="py-2 pr-3 font-semibold">Worker PID</th>
                <th className="py-2 pr-3 font-semibold">Nhận tin cuối</th>
                <th className="py-2 font-semibold">Xử lý cuối</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100 dark:divide-dark-700">
              {subscriptions.map((sub) => {
                const running = sub.enabled && sub.workerPid != null;
                return (
                  <tr key={sub.subscriptionName}>
                    <td className="py-2 pr-3 font-mono font-semibold text-gray-900 dark:text-dark-50">
                      {sub.subscriptionName}
                    </td>
                    <td className="py-2 pr-3">
                      <Badge
                        color={running ? "success" : "error"}
                        variant="soft"
                      >
                        {running
                          ? "Đang đồng bộ"
                          : sub.enabled
                            ? "Mất kết nối worker"
                            : "Đã tắt"}
                      </Badge>
                    </td>
                    <td className="py-2 pr-3 font-mono">
                      {sub.workerPid ?? "—"}
                    </td>
                    <td className="py-2 pr-3 font-mono">
                      {formatTimeHMS(sub.lastMessageReceiptTime)}
                    </td>
                    <td className="py-2 font-mono">
                      {formatTimeHMS(sub.latestEndTime)}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}
