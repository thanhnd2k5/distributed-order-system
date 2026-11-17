import { useCallback, useEffect, useState } from "react";
import { Link, useParams, useSearchParams } from "react-router";
import { BuildingStorefrontIcon } from "@heroicons/react/24/outline";
import { Page } from "components/shared/Page";
import { PollingStatusBar } from "components/shared/PollingStatusBar";
import { Card } from "components/ui";
import { getSiteConfig, POS_SITE_IDS } from "configs/endpoints.config";
import {
  useSiteOrderDetailQuery,
  useSiteOrdersQuery,
} from "hooks/queries/useOrderQueries";
import { isValidUuid } from "utils/uuid";
import { OrderComposerCard } from "../shared/OrderComposerCard";
import { OrderDetailPanel } from "../shared/OrderDetailPanel";
import { OrdersTable } from "../shared/OrdersTable";
import { useOrderComposer } from "../shared/useOrderComposer";

export default function PosOrderPage() {
  const { siteId: rawSiteId } = useParams();
  const normalizedSiteId = String(rawSiteId || "POS01").toUpperCase();
  const isValidPosSite = POS_SITE_IDS.includes(normalizedSiteId);
  const siteConfig = isValidPosSite ? getSiteConfig(normalizedSiteId) : null;

  const [searchParams, setSearchParams] = useSearchParams();
  const urlOrderId = searchParams.get("orderId") || "";
  const [autoPoll, setAutoPoll] = useState(true);
  const [selectedOrderId, setSelectedOrderId] = useState(
    isValidUuid(urlOrderId) ? urlOrderId.trim() : null,
  );

  const selectOrder = useCallback(
    (orderId) => {
      if (!orderId) return;
      setSelectedOrderId(orderId);
      setSearchParams({ orderId });
    },
    [setSearchParams],
  );

  useEffect(() => {
    if (isValidUuid(urlOrderId)) {
      setSelectedOrderId(urlOrderId.trim());
      return;
    }
    setSelectedOrderId(null);
  }, [normalizedSiteId, urlOrderId]);

  const composer = useOrderComposer(isValidPosSite ? normalizedSiteId : null, {
    onOrderCreated: (order) => {
      if (order?.orderId) {
        selectOrder(order.orderId);
      }
    },
  });

  const ordersQuery = useSiteOrdersQuery(
    isValidPosSite ? normalizedSiteId : null,
    { autoPoll },
  );

  const detailQuery = useSiteOrderDetailQuery(
    isValidPosSite ? normalizedSiteId : null,
    selectedOrderId,
    { autoPoll, enabled: Boolean(selectedOrderId && isValidPosSite) },
  );

  if (!siteConfig) {
    return (
      <Page title="POS không hợp lệ">
        <div className="mx-auto max-w-2xl px-4 py-10">
          <Card className="p-6 text-center">
            <h1 className="text-lg font-semibold text-gray-900 dark:text-dark-50">
              Không tìm thấy quầy POS &ldquo;{rawSiteId}&rdquo;
            </h1>
            <p className="mt-2 text-sm text-gray-500 dark:text-dark-300">
              Hệ thống hiện có hai điểm bán mẫu là POS01 và POS02.
            </p>
            <div className="mt-4 flex justify-center gap-3">
              {POS_SITE_IDS.map((id) => (
                <Link
                  key={id}
                  to={`/pos/${id}`}
                  className="bg-primary-600 hover:bg-primary-700 rounded-lg px-4 py-2 text-sm font-medium text-white"
                >
                  Mở {id}
                </Link>
              ))}
            </div>
          </Card>
        </div>
      </Page>
    );
  }

  const isConnected = !ordersQuery.isError && !composer.menuQuery.isError;
  const connectionError = ordersQuery.error || composer.menuQuery.error;

  const handleManualRefresh = () => {
    composer.menuQuery.refetch();
    ordersQuery.refetch();
    if (selectedOrderId) {
      detailQuery.refetch();
    }
  };

  return (
    <Page title={siteConfig.label}>
      <div className="transition-content mx-auto flex w-full max-w-[1600px] flex-col gap-5 px-4 py-6 sm:px-6 lg:px-8">
        {/* Page Header + Site Switcher */}
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="bg-primary-500/10 text-primary-600 dark:text-primary-400 flex size-11 items-center justify-center rounded-xl">
              <BuildingStorefrontIcon className="size-6" />
            </div>
            <div>
              <h1 className="text-xl font-bold text-gray-900 dark:text-dark-50">
                {siteConfig.label}
              </h1>
              <p className="text-xs text-gray-500 dark:text-dark-300">
                Order Service độc lập cho mảnh {siteConfig.siteId} — lưu đơn cục bộ ngay cả khi Kafka tạm ngừng
              </p>
            </div>
          </div>

          <div className="flex items-center gap-1.5 rounded-xl border border-gray-200 bg-white p-1 dark:border-dark-600 dark:bg-dark-800">
            {POS_SITE_IDS.map((id) => {
              const active = id === siteConfig.siteId;
              return (
                <Link
                  key={id}
                  to={`/pos/${id}`}
                  className={`rounded-lg px-3.5 py-1.5 text-xs font-semibold transition-colors ${
                    active
                      ? "bg-primary-600 text-white shadow-xs"
                      : "text-gray-600 hover:bg-gray-100 dark:text-dark-200 dark:hover:bg-dark-700"
                  }`}
                >
                  {id}
                </Link>
              );
            })}
          </div>
        </div>

        <PollingStatusBar
          serviceLabel={`Order Service (${siteConfig.siteId})`}
          endpointUrl={siteConfig.baseURL}
          isConnected={isConnected}
          isFetching={ordersQuery.isFetching || detailQuery.isFetching}
          autoPoll={autoPoll}
          onToggleAutoPoll={setAutoPoll}
          onRefresh={handleManualRefresh}
          lastUpdatedAt={ordersQuery.dataUpdatedAt}
          error={connectionError}
        />

        <div className="grid grid-cols-1 gap-6 lg:grid-cols-12">
          <div className="lg:col-span-5 xl:col-span-4">
            <OrderComposerCard siteConfig={siteConfig} composer={composer} />
          </div>

          <div className="flex flex-col gap-5 lg:col-span-7 xl:col-span-8">
            <OrdersTable
              siteId={siteConfig.siteId}
              orders={ordersQuery.data || []}
              isLoading={ordersQuery.isLoading}
              isError={ordersQuery.isError}
              error={ordersQuery.error}
              selectedOrderId={selectedOrderId}
              onSelectOrder={selectOrder}
            />

            <OrderDetailPanel
              siteId={siteConfig.siteId}
              orderId={selectedOrderId}
              detailQuery={detailQuery}
              title={`Chi tiết đơn tại ${siteConfig.siteId}`}
            />
          </div>
        </div>
      </div>
    </Page>
  );
}
