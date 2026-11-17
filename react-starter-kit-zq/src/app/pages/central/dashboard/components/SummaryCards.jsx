import PropTypes from "prop-types";
import { Card, Spinner } from "components/ui";
import { SiteBadge } from "components/shared/SiteBadge";
import { OrderStatusBadge } from "components/shared/OrderStatusBadge";

const SITES = ["POS01", "POS02", "ONLINE"];
const STATUSES = ["PENDING_INVENTORY", "RESERVED", "DONE", "FAILED"];

export function SummaryCards({ summary, isLoading, isError, error }) {
  if (!summary && isLoading) {
    return (
      <Card className="flex items-center gap-2 p-5 text-sm text-gray-500">
        <Spinner className="size-4" color="primary" />
        <span>Đang đọc số liệu từ Central Order Replica...</span>
      </Card>
    );
  }

  if (!summary) {
    return (
      <Card className="p-5 text-sm text-error dark:text-error-light">
        {isError
          ? `Không đọc được số đơn tổng hợp${error?.message ? `: ${error.message}` : "."}`
          : "Chưa có số liệu tổng hợp từ Order Query Service."}
      </Card>
    );
  }

  const totalOrders = summary.totalOrders ?? 0;
  const bySite = summary?.bySite || {};
  const byStatus = summary?.byStatus || {};

  return (
    <div className="grid grid-cols-1 gap-4 lg:grid-cols-12">
      <Card className="flex flex-col justify-between p-5 lg:col-span-3">
        <span className="text-xs font-semibold tracking-wider text-gray-500 uppercase dark:text-dark-300">
          Tổng đơn tại Trung tâm
        </span>
        <div className="mt-3 flex items-baseline gap-2">
          <span className="font-mono text-4xl font-bold text-gray-900 dark:text-dark-50">
            {totalOrders.toLocaleString("vi-VN")}
          </span>
          <span className="text-xs text-gray-500 dark:text-dark-300">
            đơn hợp nhất
          </span>
        </div>
        <p className="mt-2 text-[11px] text-gray-400 dark:text-dark-300">
          UNION ALL từ `pos01`, `pos02`, `online`
        </p>
      </Card>

      <Card className="p-5 lg:col-span-4">
        <span className="text-xs font-semibold tracking-wider text-gray-500 uppercase dark:text-dark-300">
          Theo mảnh nguồn (Site)
        </span>
        <div className="mt-3 grid grid-cols-3 gap-3">
          {SITES.map((siteId) => (
            <div
              key={siteId}
              className="rounded-xl border border-gray-100 bg-gray-50 p-3 dark:border-dark-600 dark:bg-dark-800/70"
            >
              <SiteBadge siteId={siteId} />
              <p className="mt-2 font-mono text-2xl font-bold text-gray-900 dark:text-dark-50">
                {(bySite[siteId] ?? 0).toLocaleString("vi-VN")}
              </p>
            </div>
          ))}
        </div>
      </Card>

      <Card className="p-5 lg:col-span-5">
        <span className="text-xs font-semibold tracking-wider text-gray-500 uppercase dark:text-dark-300">
          Theo trạng thái đơn
        </span>
        <div className="mt-3 grid grid-cols-2 gap-2.5">
          {STATUSES.map((status) => (
            <div
              key={status}
              className="flex items-center justify-between gap-2 rounded-xl border border-gray-100 bg-gray-50 px-3 py-2.5 dark:border-dark-600 dark:bg-dark-800/70"
            >
              <OrderStatusBadge status={status} />
              <span className="font-mono text-xl font-bold text-gray-900 dark:text-dark-50">
                {(byStatus[status] ?? 0).toLocaleString("vi-VN")}
              </span>
            </div>
          ))}
        </div>
      </Card>
    </div>
  );
}

SummaryCards.propTypes = {
  summary: PropTypes.object,
  isLoading: PropTypes.bool,
  isError: PropTypes.bool,
  error: PropTypes.object,
};
