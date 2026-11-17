import PropTypes from "prop-types";
import { Link } from "react-router";
import { ArrowTopRightOnSquareIcon } from "@heroicons/react/24/outline";
import { Badge, Card, Spinner, Table, TBody, Td, Th, THead, Tr } from "components/ui";
import { OrderIdText } from "components/shared/OrderIdText";
import { OrderStatusBadge } from "components/shared/OrderStatusBadge";
import { formatDateTime } from "utils/time";

export function OrdersTable({
  siteId,
  orders = [],
  isLoading,
  isError,
  error,
  selectedOrderId,
  onSelectOrder,
}) {
  return (
    <Card className="overflow-hidden">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-gray-200 px-5 py-3.5 dark:border-dark-600">
        <div>
          <h2 className="text-base font-semibold text-gray-900 dark:text-dark-50">
            Đơn hàng tại {siteId}
          </h2>
          <p className="text-xs text-gray-500 dark:text-dark-300">
            Tối đa 100 đơn gần nhất từ DB riêng của {siteId} (polling 2s)
          </p>
        </div>
        <Badge variant="soft" color="primary">
          {orders.length} đơn
        </Badge>
      </div>

      {isLoading ? (
        <div className="flex items-center justify-center gap-2 py-10 text-sm text-gray-500">
          <Spinner className="size-4" color="primary" />
          <span>Đang tải danh sách đơn...</span>
        </div>
      ) : isError ? (
        <div className="p-5 text-sm text-error dark:text-error-light">
          Không đọc được danh sách đơn của {siteId}: {error?.message}
        </div>
      ) : orders.length === 0 ? (
        <div className="p-8 text-center text-sm text-gray-500 dark:text-dark-300">
          Chưa có đơn nào tại {siteId}. Hãy tạo đơn đầu tiên ở bên cạnh.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <Table hoverable className="w-full text-left">
            <THead>
              <Tr>
                <Th>Mã đơn (orderId)</Th>
                <Th>Trạng thái</Th>
                <Th>Lý do / Ghi chú</Th>
                <Th>Thời gian tạo</Th>
                <Th className="text-right">Đối chứng</Th>
              </Tr>
            </THead>
            <TBody>
              {orders.map((order) => {
                const isSelected = order.orderId === selectedOrderId;
                return (
                  <Tr
                    key={order.orderId}
                    onClick={() => onSelectOrder?.(order.orderId)}
                    className={`cursor-pointer transition-colors ${
                      isSelected
                        ? "bg-primary-500/8 dark:bg-primary-400/10"
                        : ""
                    }`}
                  >
                    <Td>
                      <OrderIdText value={order.orderId} />
                    </Td>
                    <Td>
                      <OrderStatusBadge status={order.status} />
                    </Td>
                    <Td className="max-w-[240px] truncate text-xs text-gray-600 dark:text-dark-300">
                      {order.failureReason ||
                        (order.status === "PENDING_INVENTORY"
                          ? "Đang chờ Kafka & Kho"
                          : "—")}
                    </Td>
                    <Td className="whitespace-nowrap text-xs text-gray-600 dark:text-dark-300">
                      {formatDateTime(order.createdAt)}
                    </Td>
                    <Td className="text-right">
                      <Link
                        to={`/inventory?orderId=${order.orderId}`}
                        onClick={(event) => event.stopPropagation()}
                        className="text-primary-600 hover:text-primary-700 dark:text-primary-400 inline-flex items-center gap-1 text-xs font-medium"
                      >
                        <span>Kho</span>
                        <ArrowTopRightOnSquareIcon className="size-3.5" />
                      </Link>
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

OrdersTable.propTypes = {
  siteId: PropTypes.string.isRequired,
  orders: PropTypes.array,
  isLoading: PropTypes.bool,
  isError: PropTypes.bool,
  error: PropTypes.object,
  selectedOrderId: PropTypes.string,
  onSelectOrder: PropTypes.func,
};
