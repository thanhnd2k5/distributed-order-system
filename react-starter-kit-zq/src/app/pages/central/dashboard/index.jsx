import { useState } from "react";
import { ChartBarSquareIcon } from "@heroicons/react/24/outline";
import { Page } from "components/shared/Page";
import { PollingStatusBar } from "components/shared/PollingStatusBar";
import { CENTRAL_SERVICES } from "configs/endpoints.config";
import { ReplicationBanner } from "./components/ReplicationBanner";
import { ReplicatedOrderDetailModal } from "./components/ReplicatedOrderDetailModal";
import { ReplicatedOrdersTable } from "./components/ReplicatedOrdersTable";
import { SubscriptionsCard } from "./components/SubscriptionsCard";
import { SummaryCards } from "./components/SummaryCards";
import {
  useReportingOrderDetailQuery,
  useReportingOrdersQuery,
  useReportingSummaryQuery,
} from "./hooks/useReportingQueries";

export default function CentralDashboardPage() {
  const [autoPoll, setAutoPoll] = useState(true);
  const [filters, setFilters] = useState({
    siteId: "ALL",
    status: "ALL",
    limit: 100,
  });
  const [selectedOrderId, setSelectedOrderId] = useState(null);

  const summaryQuery = useReportingSummaryQuery({ autoPoll });
  const ordersQuery = useReportingOrdersQuery(filters, { autoPoll });
  const detailQuery = useReportingOrderDetailQuery(selectedOrderId, {
    autoPoll,
  });

  const isConnected = !summaryQuery.isError && !ordersQuery.isError;
  const connectionError = summaryQuery.error || ordersQuery.error;

  const handleRefresh = () => {
    summaryQuery.refetch();
    ordersQuery.refetch();
    if (selectedOrderId) {
      detailQuery.refetch();
    }
  };

  return (
    <Page title="Dashboard Tổng hợp">
      <div className="transition-content mx-auto flex w-full max-w-[1600px] flex-col gap-5 px-4 py-6 sm:px-6 lg:px-8">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="bg-primary-500/10 text-primary-600 dark:text-primary-400 flex size-11 items-center justify-center rounded-xl">
              <ChartBarSquareIcon className="size-6" />
            </div>
            <div>
              <h1 className="text-xl font-bold text-gray-900 dark:text-dark-50">
                Dashboard Tổng hợp Trung tâm
              </h1>
              <p className="text-xs text-gray-500 dark:text-dark-300">
                Đọc dữ liệu nhân bản từ `POS01`, `POS02`, `ONLINE` qua `order-query-service` (user `query_reader`)
              </p>
            </div>
          </div>
        </div>

        <PollingStatusBar
          serviceLabel={CENTRAL_SERVICES.QUERY.label}
          endpointUrl={CENTRAL_SERVICES.QUERY.baseURL}
          isConnected={isConnected}
          isFetching={summaryQuery.isFetching || ordersQuery.isFetching}
          autoPoll={autoPoll}
          onToggleAutoPoll={setAutoPoll}
          onRefresh={handleRefresh}
          lastUpdatedAt={ordersQuery.dataUpdatedAt || summaryQuery.dataUpdatedAt}
          error={connectionError}
        />

        <ReplicationBanner
          replicaNote={summaryQuery.data?.replicaNote || ordersQuery.data?.replicaNote}
          queriedAt={ordersQuery.data?.queriedAt || summaryQuery.data?.queriedAt}
        />

        <SummaryCards
          summary={summaryQuery.data}
          isLoading={summaryQuery.isLoading}
          isError={summaryQuery.isError}
          error={summaryQuery.error}
        />

        <SubscriptionsCard
          subscriptions={summaryQuery.data?.subscriptions || []}
          isLoading={summaryQuery.isLoading}
          isError={summaryQuery.isError}
        />

        <ReplicatedOrdersTable
          filters={filters}
          onChangeFilter={setFilters}
          ordersData={ordersQuery.data}
          isLoading={ordersQuery.isLoading}
          isError={ordersQuery.isError}
          error={ordersQuery.error}
          onSelectOrder={setSelectedOrderId}
        />

        <ReplicatedOrderDetailModal
          orderId={selectedOrderId}
          detailQuery={detailQuery}
          onClose={() => setSelectedOrderId(null)}
        />
      </div>
    </Page>
  );
}
