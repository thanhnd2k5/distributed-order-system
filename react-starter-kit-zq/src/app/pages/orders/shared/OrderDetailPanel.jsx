import PropTypes from "prop-types";
import { Link } from "react-router";
import { ArrowTopRightOnSquareIcon } from "@heroicons/react/24/outline";
import { Badge, Card, Spinner, Table, TBody, Td, Th, THead, Tr } from "components/ui";
import { OrderIdText } from "components/shared/OrderIdText";
import { OrderStatusBadge } from "components/shared/OrderStatusBadge";
import { SiteBadge } from "components/shared/SiteBadge";
import { formatDateTime } from "utils/time";

export function OrderDetailPanel({
  siteId,
  orderId,
  detailQuery,
  title = "Chi tiết đơn hàng",
}) {
  if (!orderId) {
    return (
      <Card className="p-5 text-sm text-gray-500 dark:text-dark-300">
        Chọn một đơn trong danh sách để xem các dòng món snapshot và đối chứng trạng thái.
      </Card>
    );
  }

  const data = detailQuery.data;
  const order = data?.order;
  const items = data?.items || [];

  return (
    <Card className="flex flex-col gap-4 p-5">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h3 className="text-base font-semibold text-gray-900 dark:text-dark-50">
            {title}
          </h3>
          <div className="mt-1 flex flex-wrap items-center gap-2">
            <SiteBadge siteId={order?.sourceSiteId || siteId} />
            <OrderIdText value={orderId} full />
          </div>
        </div>

        <div className="flex items-center gap-2">
          {order?.status && <OrderStatusBadge status={order.status} />}
          <Link
            to={`/inventory?orderId=${orderId}`}
            className="border-gray-300 hover:bg-gray-100 dark:border-dark-500 dark:hover:bg-dark-700 inline-flex items-center gap-1 rounded-lg border px-2.5 py-1.5 text-xs font-medium text-gray-700 dark:text-dark-100"
          >
            <span>Tra cứu Kho</span>
            <ArrowTopRightOnSquareIcon className="size-3.5" />
          </Link>
        </div>
      </div>

      {detailQuery.isLoading ? (
        <div className="flex items-center gap-2 py-4 text-sm text-gray-500">
          <Spinner className="size-4" color="primary" />
          <span>Đang tải chi tiết đơn...</span>
        </div>
      ) : detailQuery.isError ? (
        <div className="rounded-lg border border-error/40 bg-error/5 p-3 text-xs text-error dark:text-error-light">
          {detailQuery.error?.status === 404
            ? `Không tìm thấy đơn ${orderId} tại ${siteId} (mỗi site chỉ lưu đơn của chính nó).`
            : detailQuery.error?.message}
        </div>
      ) : order ? (
        <>
          <div className="grid grid-cols-1 gap-3 rounded-xl bg-gray-50 p-3 text-xs sm:grid-cols-3 dark:bg-dark-800/70">
            <div>
              <span className="text-gray-500 dark:text-dark-300">Thời gian tạo</span>
              <p className="mt-0.5 font-medium text-gray-900 dark:text-dark-100">
                {formatDateTime(order.createdAt)}
              </p>
            </div>
            <div>
              <span className="text-gray-500 dark:text-dark-300">Trạng thái hiện tại</span>
              <p className="mt-0.5 font-medium text-gray-900 dark:text-dark-100">
                {order.status}
              </p>
            </div>
            <div>
              <span className="text-gray-500 dark:text-dark-300">Lý do thất bại</span>
              <p className="mt-0.5 font-medium text-gray-900 dark:text-dark-100">
                {order.failureReason || "—"}
              </p>
            </div>
          </div>

          <div className="overflow-x-auto">
            <Table className="w-full text-left">
              <THead>
                <Tr>
                  <Th>#</Th>
                  <Th>Mã món</Th>
                  <Th>Tên món (snapshot)</Th>
                  <Th className="text-right">Số lượng</Th>
                </Tr>
              </THead>
              <TBody>
                {items.map((item) => (
                  <Tr key={item.lineNo}>
                    <Td className="font-mono text-xs">{item.lineNo}</Td>
                    <Td>
                      <Badge variant="soft" color="neutral" className="font-mono">
                        {item.dishId}
                      </Badge>
                    </Td>
                    <Td className="font-medium text-gray-900 dark:text-dark-100">
                      {item.dishName}
                    </Td>
                    <Td className="text-right font-mono font-semibold">
                      {item.quantity}
                    </Td>
                  </Tr>
                ))}
              </TBody>
            </Table>
          </div>
        </>
      ) : null}
    </Card>
  );
}

OrderDetailPanel.propTypes = {
  siteId: PropTypes.string.isRequired,
  orderId: PropTypes.string,
  detailQuery: PropTypes.object.isRequired,
  title: PropTypes.string,
};
