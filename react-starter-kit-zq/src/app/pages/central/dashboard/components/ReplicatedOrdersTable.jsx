import PropTypes from "prop-types";
import {
  Badge,
  Card,
  Select,
  Spinner,
  Table,
  TBody,
  Td,
  Th,
  THead,
  Tr,
} from "components/ui";
import { OrderIdText } from "components/shared/OrderIdText";
import { OrderStatusBadge } from "components/shared/OrderStatusBadge";
import { SiteBadge } from "components/shared/SiteBadge";
import { formatDateTime } from "utils/time";

const SITE_OPTIONS = [
  { label: "Tất cả nguồn", value: "ALL" },
  { label: "POS01", value: "POS01" },
  { label: "POS02", value: "POS02" },
  { label: "ONLINE", value: "ONLINE" },
];

const STATUS_OPTIONS = [
  { label: "Tất cả trạng thái", value: "ALL" },
  { label: "PENDING_INVENTORY", value: "PENDING_INVENTORY" },
  { label: "RESERVED", value: "RESERVED" },
  { label: "DONE", value: "DONE" },
  { label: "FAILED", value: "FAILED" },
];

const LIMIT_OPTIONS = [
  { label: "50 dòng", value: "50" },
  { label: "100 dòng", value: "100" },
  { label: "200 dòng", value: "200" },
  { label: "500 dòng", value: "500" },
];

export function ReplicatedOrdersTable({
  filters,
  onChangeFilter,
  ordersData,
  isLoading,
  isError,
  error,
  onSelectOrder,
}) {
  const orders = ordersData?.orders || [];

  return (
    <Card className="overflow-hidden">
      <div className="flex flex-wrap items-center justify-between gap-4 border-b border-gray-200 px-5 py-4 dark:border-dark-600">
        <div>
          <div className="flex items-center gap-2">
            <h2 className="text-base font-semibold text-gray-900 dark:text-dark-50">
              Đơn hàng hợp nhất (`reporting.all_orders`)
            </h2>
            <Badge variant="soft" color={isError ? "error" : "primary"}>
              {isError ? "Không đọc được" : `${ordersData?.count ?? orders.length} đơn`}
            </Badge>
          </div>
          <p className="mt-0.5 text-xs text-gray-500 dark:text-dark-300">
            Bấm vào từng dòng để xem chi tiết món trên bản sao trung tâm
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2.5">
          <div className="w-40">
            <Select
              aria-label="Lọc theo nguồn"
              data={SITE_OPTIONS}
              value={filters.siteId}
              onChange={(event) =>
                onChangeFilter({ ...filters, siteId: event.target.value })
              }
            />
          </div>
          <div className="w-48">
            <Select
              aria-label="Lọc theo trạng thái"
              data={STATUS_OPTIONS}
              value={filters.status}
              onChange={(event) =>
                onChangeFilter({ ...filters, status: event.target.value })
              }
            />
          </div>
          <div className="w-32">
            <Select
              aria-label="Số lượng hiển thị"
              data={LIMIT_OPTIONS}
              value={String(filters.limit)}
              onChange={(event) =>
                onChangeFilter({
                  ...filters,
                  limit: Number(event.target.value) || 100,
                })
              }
            />
          </div>
        </div>
      </div>

      {isLoading ? (
        <div className="flex items-center justify-center gap-2 py-12 text-sm text-gray-500">
          <Spinner className="size-4" color="primary" />
          <span>Đang truy vấn Central Order Replica...</span>
        </div>
      ) : isError ? (
        <div className="p-6 text-sm text-error dark:text-error-light">
          Không thể tải đơn hợp nhất: {error?.message}
        </div>
      ) : orders.length === 0 ? (
        <div className="p-10 text-center text-sm text-gray-500 dark:text-dark-300">
          Không có đơn hàng nào khớp bộ lọc trên bản sao trung tâm.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <Table hoverable className="w-full text-left">
            <THead>
              <Tr>
                <Th>Mã đơn (orderId)</Th>
                <Th>Nguồn (Site)</Th>
                <Th>Kênh</Th>
                <Th>Trạng thái</Th>
                <Th>Version</Th>
                <Th>Lý do lỗi</Th>
                <Th>Tạo lúc</Th>
                <Th>Cập nhật</Th>
              </Tr>
            </THead>
            <TBody>
              {orders.map((order) => (
                <Tr
                  key={order.orderId}
                  onClick={() => onSelectOrder(order.orderId)}
                  className="cursor-pointer"
                >
                  <Td>
                    <OrderIdText value={order.orderId} />
                  </Td>
                  <Td>
                    <SiteBadge siteId={order.sourceSiteId} />
                  </Td>
                  <Td className="font-mono text-xs text-gray-600 dark:text-dark-300">
                    {order.sourceType}
                  </Td>
                  <Td>
                    <OrderStatusBadge status={order.status} />
                  </Td>
                  <Td className="font-mono text-xs">v{order.version}</Td>
                  <Td className="max-w-[220px] truncate text-xs text-gray-600 dark:text-dark-300">
                    {order.failureReason || "—"}
                  </Td>
                  <Td className="whitespace-nowrap text-xs text-gray-600 dark:text-dark-300">
                    {formatDateTime(order.createdAt)}
                  </Td>
                  <Td className="whitespace-nowrap text-xs text-gray-600 dark:text-dark-300">
                    {formatDateTime(order.updatedAt)}
                  </Td>
                </Tr>
              ))}
            </TBody>
          </Table>
        </div>
      )}
    </Card>
  );
}

ReplicatedOrdersTable.propTypes = {
  filters: PropTypes.object.isRequired,
  onChangeFilter: PropTypes.func.isRequired,
  ordersData: PropTypes.object,
  isLoading: PropTypes.bool,
  isError: PropTypes.bool,
  error: PropTypes.object,
  onSelectOrder: PropTypes.func.isRequired,
};
