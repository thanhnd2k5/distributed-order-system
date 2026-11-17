import { useMemo, useState } from "react";
import { Link } from "react-router";
import {
  CheckCircleIcon,
  FireIcon,
} from "@heroicons/react/24/outline";
import { Page } from "components/shared/Page";
import { PollingStatusBar } from "components/shared/PollingStatusBar";
import { OrderIdText } from "components/shared/OrderIdText";
import { OrderStatusBadge } from "components/shared/OrderStatusBadge";
import { SiteBadge } from "components/shared/SiteBadge";
import { Badge, Button, Card, Spinner } from "components/ui";
import { CENTRAL_SERVICES } from "configs/endpoints.config";
import { formatDateTime } from "utils/time";
import {
  useCompleteTicketMutation,
  useKitchenTicketsQuery,
} from "./hooks/useKitchenQueries";

const STATUS_TABS = [
  { key: "ALL", label: "Tất cả" },
  { key: "WAITING", label: "Đang chờ (WAITING)" },
  { key: "DONE", label: "Đã xong (DONE)" },
];

export default function KitchenPage() {
  const [autoPoll, setAutoPoll] = useState(true);
  const [statusFilter, setStatusFilter] = useState("ALL");

  const ticketsQuery = useKitchenTicketsQuery(statusFilter, { autoPoll });
  const completeMutation = useCompleteTicketMutation();

  const tickets = useMemo(() => ticketsQuery.data || [], [ticketsQuery.data]);

  const counts = useMemo(() => {
    let waiting = 0;
    let done = 0;
    for (const ticket of tickets) {
      if (ticket.status === "WAITING") waiting += 1;
      if (ticket.status === "DONE") done += 1;
    }
    return { waiting, done, total: tickets.length };
  }, [tickets]);

  return (
    <Page title="Bếp trung tâm">
      <div className="transition-content mx-auto flex w-full max-w-[1600px] flex-col gap-5 px-4 py-6 sm:px-6 lg:px-8">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="bg-rose-500/10 text-rose-600 dark:text-rose-400 flex size-11 items-center justify-center rounded-xl">
              <FireIcon className="size-6" />
            </div>
            <div>
              <h1 className="text-xl font-bold text-gray-900 dark:text-dark-50">
                Bếp trung tâm (Kitchen Service)
              </h1>
              <p className="text-xs text-gray-500 dark:text-dark-300">
                Chỉ tạo phiếu từ sự kiện `Reserved` — bấm Hoàn tất để phát `Done` về đúng nguồn đơn
              </p>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-1.5 rounded-xl border border-gray-200 bg-white p-1 dark:border-dark-600 dark:bg-dark-800">
            {STATUS_TABS.map((tab) => {
              const active = statusFilter === tab.key;
              return (
                <button
                  key={tab.key}
                  type="button"
                  onClick={() => setStatusFilter(tab.key)}
                  className={`cursor-pointer rounded-lg px-3 py-1.5 text-xs font-semibold transition-colors ${
                    active
                      ? "bg-primary-600 text-white shadow-xs"
                      : "text-gray-600 hover:bg-gray-100 dark:text-dark-200 dark:hover:bg-dark-700"
                  }`}
                >
                  {tab.label}
                </button>
              );
            })}
          </div>
        </div>

        <PollingStatusBar
          serviceLabel={CENTRAL_SERVICES.KITCHEN.label}
          endpointUrl={CENTRAL_SERVICES.KITCHEN.baseURL}
          isConnected={!ticketsQuery.isError}
          isFetching={ticketsQuery.isFetching}
          autoPoll={autoPoll}
          onToggleAutoPoll={setAutoPoll}
          onRefresh={() => ticketsQuery.refetch()}
          lastUpdatedAt={ticketsQuery.dataUpdatedAt}
          error={ticketsQuery.error}
        />

        {/* Quick Counters */}
        <div className="flex flex-wrap items-center gap-3 text-xs">
          <Badge variant="soft" color="neutral">
            Hiển thị: {counts.total} phiếu
          </Badge>
          {statusFilter === "ALL" && (
            <>
              <Badge variant="soft" color="warning">
                Đang chờ: {counts.waiting}
              </Badge>
              <Badge variant="soft" color="success">
                Đã hoàn tất: {counts.done}
              </Badge>
            </>
          )}
        </div>

        {/* Tickets Grid */}
        {ticketsQuery.isLoading ? (
          <Card className="flex items-center justify-center gap-2 py-12 text-sm text-gray-500">
            <Spinner className="size-4" color="primary" />
            <span>Đang tải danh sách phiếu bếp...</span>
          </Card>
        ) : ticketsQuery.isError ? (
          <Card className="p-6 text-sm text-error dark:text-error-light">
            Không tải được danh sách phiếu từ Kitchen Service: {ticketsQuery.error?.message}
          </Card>
        ) : tickets.length === 0 ? (
          <Card className="p-10 text-center text-sm text-gray-500 dark:text-dark-300">
            Chưa có phiếu bếp nào ở bộ lọc hiện tại. Chỉ các đơn được Kho xác nhận `Reserved` mới xuất hiện tại đây.
          </Card>
        ) : (
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
            {tickets.map((ticket) => {
              const isWaiting = ticket.status === "WAITING";
              const isCompletingThis =
                completeMutation.isPending &&
                completeMutation.variables === ticket.ticketId;

              return (
                <Card
                  key={ticket.ticketId}
                  className={`flex flex-col justify-between gap-4 p-5 ${
                    isWaiting ? "border-warning/50" : ""
                  }`}
                >
                  <div className="space-y-3">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <SiteBadge siteId={ticket.sourceSiteId} />
                      <OrderStatusBadge status={ticket.status} />
                    </div>

                    <div className="space-y-1 text-xs">
                      <div className="flex items-center justify-between gap-2">
                        <span className="text-gray-500 dark:text-dark-300">
                          Mã phiếu:
                        </span>
                        <OrderIdText value={ticket.ticketId} />
                      </div>
                      <div className="flex items-center justify-between gap-2">
                        <span className="text-gray-500 dark:text-dark-300">
                          Mã đơn:
                        </span>
                        <OrderIdText value={ticket.orderId} />
                      </div>
                    </div>

                    <div className="space-y-1.5 rounded-xl bg-gray-50 p-3 dark:bg-dark-800/70">
                      {(ticket.items || []).map((item) => (
                        <div
                          key={item.lineNo}
                          className="flex items-center justify-between text-xs"
                        >
                          <span className="font-medium text-gray-900 dark:text-dark-100">
                            {item.dishName}{" "}
                            <span className="font-mono text-gray-400">
                              ({item.dishId})
                            </span>
                          </span>
                          <span className="font-mono font-bold text-gray-900 dark:text-dark-50">
                            ×{item.quantity}
                          </span>
                        </div>
                      ))}
                    </div>

                    <div className="space-y-0.5 text-[11px] text-gray-500 dark:text-dark-300">
                      <p>Nhận lúc: {formatDateTime(ticket.createdAt)}</p>
                      {ticket.completedAt && (
                        <p>Hoàn tất: {formatDateTime(ticket.completedAt)}</p>
                      )}
                    </div>
                  </div>

                  <div className="flex items-center justify-between gap-2 border-t border-gray-100 pt-3 dark:border-dark-600">
                    <Link
                      to={`/inventory?orderId=${ticket.orderId}`}
                      className="text-xs font-medium text-gray-500 hover:text-primary-600 dark:text-dark-300 dark:hover:text-primary-400"
                    >
                      Xem giữ kho
                    </Link>

                    {isWaiting ? (
                      <Button
                        color="success"
                        className="h-8.5 gap-1.5 px-3 text-xs font-medium"
                        disabled={completeMutation.isPending}
                        onClick={() => completeMutation.mutate(ticket.ticketId)}
                      >
                        {isCompletingThis ? (
                          <>
                            <Spinner className="size-3.5" color="neutral" />
                            <span>Đang hoàn tất...</span>
                          </>
                        ) : (
                          <>
                            <CheckCircleIcon className="size-4" />
                            <span>Hoàn tất món</span>
                          </>
                        )}
                      </Button>
                    ) : (
                      <span className="text-success inline-flex items-center gap-1 text-xs font-medium">
                        <CheckCircleIcon className="size-4" />
                        <span>Đã phát Done</span>
                      </span>
                    )}
                  </div>
                </Card>
              );
            })}
          </div>
        )}
      </div>
    </Page>
  );
}
