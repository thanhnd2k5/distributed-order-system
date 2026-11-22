import { ChartBarIcon } from "@heroicons/react/24/outline";
import { Page } from "components/shared/Page";
import { Badge, Card } from "components/ui";
import { useReportingSummaryQuery } from "hooks/queries/useReportingQueries";
import { formatTimeHMS } from "utils/orderFormatting";
import {
  BranchSummaryCard,
  ChainOverviewPanel,
  ReplicationStatusCard,
} from "./components/ReportingCards";

export default function ReportingPage() {
  const summaryQuery = useReportingSummaryQuery();
  const summary = summaryQuery.data;
  const branches = Array.isArray(summary?.branches) ? summary.branches : [];

  return (
    <Page title="Báo cáo quản lý">
      <div className="mx-auto w-full max-w-[1440px] space-y-5 px-4 py-6 sm:px-6 lg:px-8">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <div className="flex items-center gap-2">
              <ChartBarIcon className="size-6 text-primary-500" />
              <h1 className="text-2xl font-bold text-gray-900 dark:text-dark-50">
                Báo cáo Quản lý (Central Order Replica)
              </h1>
            </div>
            <p className="mt-1 text-sm text-gray-500 dark:text-dark-300">
              Tổng hợp đơn hàng theo chi nhánh, trạng thái và lý do hủy/thất bại
              từ bản sao bất đồng bộ.
            </p>
          </div>
          {summary?.queriedAt && (
            <Badge color="neutral" variant="outlined" className="text-xs">
              Truy vấn lúc {formatTimeHMS(summary.queriedAt)} · Tự làm mới 2s
            </Badge>
          )}
        </div>

        {summary?.replicaNote && (
          <div className="rounded-xl border border-info/30 bg-info/10 px-4 py-2.5 text-xs text-info-darker dark:text-info-lighter">
            {summary.replicaNote}
          </div>
        )}

        {summaryQuery.isLoading ? (
          <Card className="p-6 text-center text-xs text-gray-400">
            Đang tải báo cáo tổng hợp từ Order Query Service...
          </Card>
        ) : summaryQuery.isError || !summary ? (
          <Card className="p-6 text-center text-xs text-error">
            Không thể kết nối tới Order Query Service (port 8080).
          </Card>
        ) : (
          <>
            <ChainOverviewPanel summary={summary} />

            <div>
              <h2 className="mb-3 text-base font-semibold text-gray-800 dark:text-dark-100">
                Chi tiết theo chi nhánh
              </h2>
              <div className="grid gap-4 lg:grid-cols-3">
                {branches.map((branch) => (
                  <BranchSummaryCard key={branch.branchId} branch={branch} />
                ))}
              </div>
            </div>

            <ReplicationStatusCard subscriptions={summary.subscriptions || []} />
          </>
        )}
      </div>
    </Page>
  );
}
