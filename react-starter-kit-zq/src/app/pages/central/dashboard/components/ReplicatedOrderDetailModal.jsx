import PropTypes from "prop-types";
import { Fragment } from "react";
import { Link } from "react-router";
import {
  Dialog,
  DialogPanel,
  DialogTitle,
  Transition,
  TransitionChild,
} from "@headlessui/react";
import {
  ArrowTopRightOnSquareIcon,
  XMarkIcon,
} from "@heroicons/react/24/outline";
import {
  Badge,
  Button,
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
import { POS_SITE_IDS } from "configs/endpoints.config";
import { formatDateTime } from "utils/time";

function getSourceRoute(order) {
  if (!order?.orderId || !order.sourceSiteId) return "/dashboard";
  if (order.sourceSiteId === "ONLINE") {
    return `/online?orderId=${order.orderId}`;
  }
  if (POS_SITE_IDS.includes(order.sourceSiteId)) {
    return `/pos/${order.sourceSiteId}?orderId=${order.orderId}`;
  }
  return "/dashboard";
}

export function ReplicatedOrderDetailModal({ orderId, detailQuery, onClose }) {
  const isOpen = Boolean(orderId);
  const data = detailQuery.data;
  const order = data?.order;
  const items = data?.items || [];

  return (
    <Transition appear show={isOpen} as={Fragment}>
      <Dialog
        as="div"
        className="fixed inset-0 z-100 flex items-center justify-center overflow-y-auto px-4 py-6"
        onClose={onClose}
      >
        <TransitionChild
          as={Fragment}
          enter="ease-out duration-200"
          enterFrom="opacity-0"
          enterTo="opacity-100"
          leave="ease-in duration-150"
          leaveFrom="opacity-100"
          leaveTo="opacity-0"
        >
          <div className="fixed inset-0 bg-gray-900/50 backdrop-blur-xs dark:bg-black/50" />
        </TransitionChild>

        <TransitionChild
          as={Fragment}
          enter="ease-out duration-200"
          enterFrom="opacity-0 scale-95"
          enterTo="opacity-100 scale-100"
          leave="ease-in duration-150"
          leaveFrom="opacity-100 scale-100"
          leaveTo="opacity-0 scale-95"
        >
          <DialogPanel className="relative z-10 w-full max-w-2xl rounded-2xl border border-gray-200 bg-white p-6 shadow-xl dark:border-dark-600 dark:bg-dark-750">
            <div className="flex items-start justify-between gap-4 border-b border-gray-200 pb-4 dark:border-dark-600">
              <div>
                <DialogTitle className="text-lg font-bold text-gray-900 dark:text-dark-50">
                  Chi tiết đơn trên Central Replica
                </DialogTitle>
                <div className="mt-1.5 flex flex-wrap items-center gap-2">
                  {order?.sourceSiteId && (
                    <SiteBadge siteId={order.sourceSiteId} />
                  )}
                  <OrderIdText value={orderId} full />
                </div>
              </div>

              <Button
                variant="flat"
                color="neutral"
                isIcon
                className="size-8 rounded-full"
                onClick={onClose}
                aria-label="Đóng"
              >
                <XMarkIcon className="size-5" />
              </Button>
            </div>

            <div className="mt-4 space-y-4">
              {detailQuery.isLoading ? (
                <div className="flex items-center justify-center gap-2 py-8 text-sm text-gray-500">
                  <Spinner className="size-4" color="primary" />
                  <span>Đang đọc từ Central Order Replica...</span>
                </div>
              ) : detailQuery.isError ? (
                <div className="rounded-xl border border-error/40 bg-error/5 p-4 text-xs text-error dark:text-error-light">
                  {detailQuery.error?.status === 404
                    ? "Đơn chưa có trên bản sao trung tâm (có thể subscription đang trễ hoặc tạm dừng)."
                    : detailQuery.error?.message}
                </div>
              ) : order ? (
                <>
                  <div className="grid grid-cols-2 gap-3 rounded-xl bg-gray-50 p-3.5 text-xs sm:grid-cols-3 dark:bg-dark-800/70">
                    <div>
                      <span className="text-gray-500 dark:text-dark-300">
                        Trạng thái
                      </span>
                      <div className="mt-1">
                        <OrderStatusBadge status={order.status} />
                      </div>
                    </div>
                    <div>
                      <span className="text-gray-500 dark:text-dark-300">
                        Phiên bản (version)
                      </span>
                      <p className="mt-1 font-mono font-semibold text-gray-900 dark:text-dark-100">
                        v{order.version}
                      </p>
                    </div>
                    <div>
                      <span className="text-gray-500 dark:text-dark-300">
                        Cập nhật lúc
                      </span>
                      <p className="mt-1 font-medium text-gray-900 dark:text-dark-100">
                        {formatDateTime(order.updatedAt)}
                      </p>
                    </div>
                    {order.failureReason && (
                      <div className="col-span-full">
                        <span className="text-gray-500 dark:text-dark-300">
                          Lý do lỗi
                        </span>
                        <p className="mt-0.5 font-medium text-error dark:text-error-light">
                          {order.failureReason}
                        </p>
                      </div>
                    )}
                  </div>

                  <div className="overflow-x-auto">
                    <Table className="w-full text-left">
                      <THead>
                        <Tr>
                          <Th>#</Th>
                          <Th>Mã món</Th>
                          <Th>Tên món</Th>
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
                            <Td className="font-medium">{item.dishName}</Td>
                            <Td className="text-right font-mono font-semibold">
                              {item.quantity}
                            </Td>
                          </Tr>
                        ))}
                      </TBody>
                    </Table>
                  </div>

                  <div className="flex flex-wrap items-center justify-end gap-2 border-t border-gray-200 pt-3 dark:border-dark-600">
                    <Link
                      to={getSourceRoute(order)}
                      onClick={onClose}
                      className="inline-flex items-center gap-1 rounded-lg border border-gray-300 px-3 py-1.5 text-xs font-medium text-gray-700 hover:bg-gray-100 dark:border-dark-500 dark:text-dark-100 dark:hover:bg-dark-700"
                    >
                      <span>Mở site nguồn ({order.sourceSiteId})</span>
                      <ArrowTopRightOnSquareIcon className="size-3.5" />
                    </Link>
                    <Link
                      to={`/inventory?orderId=${order.orderId}`}
                      onClick={onClose}
                      className="bg-primary-600 hover:bg-primary-700 inline-flex items-center gap-1 rounded-lg px-3 py-1.5 text-xs font-medium text-white"
                    >
                      <span>Đối chứng ở Kho</span>
                      <ArrowTopRightOnSquareIcon className="size-3.5" />
                    </Link>
                  </div>
                </>
              ) : null}
            </div>
          </DialogPanel>
        </TransitionChild>
      </Dialog>
    </Transition>
  );
}

ReplicatedOrderDetailModal.propTypes = {
  orderId: PropTypes.string,
  detailQuery: PropTypes.object.isRequired,
  onClose: PropTypes.func.isRequired,
};
