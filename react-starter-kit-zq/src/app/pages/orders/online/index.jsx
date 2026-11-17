import { useEffect, useState } from "react";
import { useSearchParams } from "react-router";
import {
  CheckCircleIcon,
  ClockIcon,
  GlobeAltIcon,
  MagnifyingGlassIcon,
  MinusCircleIcon,
  XCircleIcon,
} from "@heroicons/react/24/outline";
import { Page } from "components/shared/Page";
import { PollingStatusBar } from "components/shared/PollingStatusBar";
import { OrderStatusBadge } from "components/shared/OrderStatusBadge";
import { Button, Card, Input } from "components/ui";
import { getSiteConfig } from "configs/endpoints.config";
import {
  useSiteOrderDetailQuery,
  useSiteOrdersQuery,
} from "hooks/queries/useOrderQueries";
import { isValidUuid } from "utils/uuid";
import { OrderComposerCard } from "../shared/OrderComposerCard";
import { OrderDetailPanel } from "../shared/OrderDetailPanel";
import { OrdersTable } from "../shared/OrdersTable";
import { useOrderComposer } from "../shared/useOrderComposer";

const SITE_ID = "ONLINE";

function OnlineOrderProgress({ status, failureReason }) {
  if (!status) return null;

  const isFailed = status === "FAILED";
  const isReservedOrDone = status === "RESERVED" || status === "DONE";
  const isDone = status === "DONE";

  const steps = [
    {
      key: "created",
      label: "1. Đã nhận đơn tại Online DB",
      state: "completed",
    },
    {
      key: "inventory",
      label: isFailed
        ? "2. Kho từ chối giữ nguyên liệu"
        : isReservedOrDone
          ? "2. Kho đã giữ nguyên liệu (Reserved)"
          : "2. Chờ Kho trung tâm xác nhận",
      state: isFailed ? "error" : isReservedOrDone ? "completed" : "pending",
    },
    {
      key: "kitchen",
      label: isDone
        ? "3. Bếp đã hoàn tất (Done)"
        : isFailed
          ? "3. Không tạo phiếu bếp"
          : isReservedOrDone
            ? "3. Bếp đang chế biến (WAITING)"
            : "3. Chờ bước Kho",
      state: isDone ? "completed" : isFailed ? "skipped" : "pending",
    },
  ];

  return (
    <div className="rounded-xl border border-gray-200 bg-gray-50/70 p-3.5 dark:border-dark-600 dark:bg-dark-800/60">
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
        {steps.map((step) => {
          const icon =
            step.state === "completed" ? (
              <CheckCircleIcon className="size-4.5 shrink-0 text-success" />
            ) : step.state === "error" ? (
              <XCircleIcon className="size-4.5 shrink-0 text-error" />
            ) : step.state === "skipped" ? (
              <MinusCircleIcon className="size-4.5 shrink-0 text-gray-400" />
            ) : (
              <ClockIcon className="size-4.5 shrink-0 text-warning" />
            );
          return (
            <div
              key={step.key}
              className="flex items-start gap-2 rounded-lg bg-white p-2.5 text-xs shadow-2xs dark:bg-dark-750"
            >
              {icon}
              <span className="font-medium text-gray-800 dark:text-dark-100">
                {step.label}
              </span>
            </div>
          );
        })}
      </div>
      {failureReason && (
        <p className="mt-2 text-xs text-error dark:text-error-light">
          Lý do từ Kho: {failureReason}
        </p>
      )}
    </div>
  );
}

export default function OnlineOrderPage() {
  const siteConfig = getSiteConfig(SITE_ID);
  const [searchParams, setSearchParams] = useSearchParams();
  const initialQueryOrderId = searchParams.get("orderId") || "";

  const [autoPoll, setAutoPoll] = useState(true);
  const [lookupInput, setLookupInput] = useState(initialQueryOrderId);
  const [trackedOrderId, setTrackedOrderId] = useState(
    isValidUuid(initialQueryOrderId) ? initialQueryOrderId.trim() : "",
  );
  const [lookupValidationError, setLookupValidationError] = useState("");

  const urlOrderId = searchParams.get("orderId") || "";

  const selectOrderForTracking = (orderId) => {
    if (!orderId) return;
    setLookupInput(orderId);
    setTrackedOrderId(orderId);
    setLookupValidationError("");
    setSearchParams({ orderId });
  };

  useEffect(() => {
    if (urlOrderId && isValidUuid(urlOrderId)) {
      const trimmed = urlOrderId.trim();
      setLookupInput(trimmed);
      setTrackedOrderId(trimmed);
      setLookupValidationError("");
      return;
    }
    if (!urlOrderId) {
      setTrackedOrderId("");
      setLookupInput("");
    }
  }, [urlOrderId]);

  const composer = useOrderComposer(SITE_ID, {
    onOrderCreated: (order) => {
      if (order?.orderId) {
        selectOrderForTracking(order.orderId);
      }
    },
  });

  const ordersQuery = useSiteOrdersQuery(SITE_ID, { autoPoll });
  const detailQuery = useSiteOrderDetailQuery(SITE_ID, trackedOrderId, {
    autoPoll,
    enabled: Boolean(trackedOrderId),
  });

  const isConnected = !ordersQuery.isError && !composer.menuQuery.isError;
  const connectionError = ordersQuery.error || composer.menuQuery.error;

  const handleLookupSubmit = (event) => {
    event.preventDefault();
    const trimmed = lookupInput.trim();
    if (!trimmed) {
      setTrackedOrderId("");
      setLookupInput("");
      setLookupValidationError("Nhập mã đơn (UUID) để tra cứu.");
      setSearchParams({}, { replace: true });
      return;
    }
    if (!isValidUuid(trimmed)) {
      setLookupValidationError("Mã đơn phải đúng định dạng UUID.");
      return;
    }
    selectOrderForTracking(trimmed);
  };

  const handleManualRefresh = () => {
    composer.menuQuery.refetch();
    ordersQuery.refetch();
    if (trackedOrderId) {
      detailQuery.refetch();
    }
  };

  const trackedOrder = detailQuery.data?.order;

  return (
    <Page title={siteConfig.label}>
      <div className="transition-content mx-auto flex w-full max-w-[1600px] flex-col gap-5 px-4 py-6 sm:px-6 lg:px-8">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="bg-info/10 text-info flex size-11 items-center justify-center rounded-xl">
              <GlobeAltIcon className="size-6" />
            </div>
            <div>
              <h1 className="text-xl font-bold text-gray-900 dark:text-dark-50">
                Đặt hàng Online
              </h1>
              <p className="text-xs text-gray-500 dark:text-dark-300">
                Kênh ONLINE (mảnh `online`) — gửi đơn và theo dõi trạng thái xử lý bất đồng bộ qua mã đơn
              </p>
            </div>
          </div>
        </div>

        <PollingStatusBar
          serviceLabel="Order Service (ONLINE)"
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
            {/* Order Status Lookup by orderId */}
            <Card className="flex flex-col gap-4 p-5">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div>
                  <h2 className="text-base font-semibold text-gray-900 dark:text-dark-50">
                    Tra cứu trạng thái đơn Online
                  </h2>
                  <p className="text-xs text-gray-500 dark:text-dark-300">
                    Nhập mã đơn UUID hoặc chọn đơn từ danh sách bên dưới để theo dõi tự động mỗi 2s
                  </p>
                </div>
                {trackedOrder?.status && (
                  <OrderStatusBadge status={trackedOrder.status} />
                )}
              </div>

              <form
                onSubmit={handleLookupSubmit}
                className="flex flex-col gap-2 sm:flex-row"
              >
                <Input
                  value={lookupInput}
                  onChange={(event) => {
                    setLookupInput(event.target.value);
                    setLookupValidationError("");
                  }}
                  placeholder="Nhập orderId UUID (ví dụ: d967e06a-090d-4abc-9fb2-c5ec54306e08)"
                  error={lookupValidationError}
                  classNames={{ root: "flex-1" }}
                  prefix={<MagnifyingGlassIcon className="size-4" />}
                />
                <Button type="submit" color="primary" className="h-9.5 shrink-0 gap-1.5">
                  <MagnifyingGlassIcon className="size-4" />
                  <span>Tra cứu</span>
                </Button>
              </form>

              {trackedOrder && (
                <OnlineOrderProgress
                  status={trackedOrder.status}
                  failureReason={trackedOrder.failureReason}
                />
              )}
            </Card>

            <OrderDetailPanel
              siteId={SITE_ID}
              orderId={trackedOrderId || null}
              detailQuery={detailQuery}
              title="Chi tiết đơn đang theo dõi"
            />

            <OrdersTable
              siteId={SITE_ID}
              orders={ordersQuery.data || []}
              isLoading={ordersQuery.isLoading}
              isError={ordersQuery.isError}
              error={ordersQuery.error}
              selectedOrderId={trackedOrderId}
              onSelectOrder={selectOrderForTracking}
            />
          </div>
        </div>
      </div>
    </Page>
  );
}
