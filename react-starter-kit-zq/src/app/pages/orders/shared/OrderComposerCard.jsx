import PropTypes from "prop-types";
import { useState } from "react";
import {
  ArrowPathIcon,
  ChevronDownIcon,
  ExclamationTriangleIcon,
  MinusIcon,
  PlusIcon,
  ShoppingBagIcon,
} from "@heroicons/react/24/outline";
import { Badge, Button, Card, Collapse, Input, Spinner } from "components/ui";
import { OrderIdText } from "components/shared/OrderIdText";
import { OrderStatusBadge } from "components/shared/OrderStatusBadge";

export function OrderComposerCard({ siteConfig, composer }) {
  const [showDemoTools, setShowDemoTools] = useState(false);

  const {
    menuQuery,
    isSubmitting,
    quantities,
    selectedItems,
    clientRequestId,
    lastSubmission,
    submissionFeedback,
    setDishQuantity,
    incrementDish,
    decrementDish,
    regenerateRequestId,
    requestIdLocked,
    submitOrder,
    retryLastOrderSamePayload,
    testConflictWithLastRequestId,
  } = composer;

  const totalDishesCount = selectedItems.reduce(
    (sum, item) => sum + item.quantity,
    0,
  );

  return (
    <Card className="flex flex-col gap-4 p-5">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2.5">
          <div className="bg-primary-500/10 text-primary-600 dark:text-primary-400 flex size-9 items-center justify-center rounded-lg">
            <ShoppingBagIcon className="size-5" />
          </div>
          <div>
            <h2 className="text-base font-semibold text-gray-900 dark:text-dark-50">
              Tạo đơn mới ({siteConfig.label})
            </h2>
            <p className="text-xs text-gray-500 dark:text-dark-300">
              Ghi cục bộ vào DB của {siteConfig.siteId} kèm Outbox `OrderCreated`
            </p>
          </div>
        </div>

        <Badge variant="soft" color="neutral">
          Menu cục bộ
        </Badge>
      </div>

      {/* Menu selection */}
      {menuQuery.isLoading ? (
        <div className="flex items-center gap-2 py-6 text-sm text-gray-500">
          <Spinner className="size-4" color="primary" />
          <span>Đang tải menu từ {siteConfig.baseURL}...</span>
        </div>
      ) : menuQuery.isError ? (
        <div className="border-error/40 bg-error/5 text-error dark:text-error-light rounded-lg border p-3 text-xs">
          <p className="font-semibold">Không tải được menu từ {siteConfig.siteId}</p>
          <p className="mt-1">{menuQuery.error?.message}</p>
          <Button
            variant="outlined"
            color="error"
            className="mt-2 h-7 px-2.5 text-xs"
            onClick={() => menuQuery.refetch()}
          >
            Thử tải lại menu
          </Button>
        </div>
      ) : (
        <div className="space-y-2.5">
          {(menuQuery.data || []).map((dish) => {
            const qty = quantities[dish.dishId] || 0;
            return (
              <div
                key={dish.dishId}
                className={`flex items-center justify-between gap-3 rounded-xl border p-3 transition-colors ${
                  qty > 0
                    ? "border-primary-500/60 bg-primary-500/5 dark:border-primary-400/50"
                    : "border-gray-200 dark:border-dark-600"
                }`}
              >
                <div className="min-w-0">
                  <p className="font-medium text-gray-900 dark:text-dark-50">
                    {dish.name}
                  </p>
                  <p className="font-mono text-xs text-gray-500 dark:text-dark-300">
                    {dish.dishId}
                  </p>
                </div>

                <div className="flex items-center gap-1.5">
                  <Button
                    variant="outlined"
                    color="neutral"
                    isIcon
                    className="size-8 rounded-lg"
                    disabled={qty <= 0 || isSubmitting}
                    onClick={() => decrementDish(dish.dishId)}
                    aria-label={`Giảm ${dish.name}`}
                  >
                    <MinusIcon className="size-4" />
                  </Button>
                  <Input
                    type="number"
                    min={0}
                    max={999}
                    value={qty}
                    disabled={isSubmitting}
                    onChange={(event) =>
                      setDishQuantity(dish.dishId, event.target.value)
                    }
                    className="h-8 w-14 text-center font-mono text-sm"
                    aria-label={`Số lượng ${dish.name}`}
                  />
                  <Button
                    variant="soft"
                    color="primary"
                    isIcon
                    className="size-8 rounded-lg"
                    disabled={isSubmitting}
                    onClick={() => incrementDish(dish.dishId)}
                    aria-label={`Tăng ${dish.name}`}
                  >
                    <PlusIcon className="size-4" />
                  </Button>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Client Request ID & Submit */}
      <div className="rounded-xl border border-gray-200 bg-gray-50/70 p-3 dark:border-dark-600 dark:bg-dark-800/60">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <span className="text-xs font-medium text-gray-600 dark:text-dark-200">
            clientRequestId (giữ nguyên nếu retry lỗi):
          </span>
          <div className="flex items-center gap-2">
            <OrderIdText value={clientRequestId} />
            <Button
              variant="flat"
              color="neutral"
              className="h-6 gap-1 px-2 text-xs"
              disabled={isSubmitting || requestIdLocked}
              onClick={regenerateRequestId}
              title={
                requestIdLocked
                  ? "Chưa biết request trước đã ghi đơn hay chưa, nên không đổi ID"
                  : "Tạo mã request mới cho đơn mới"
              }
            >
              <ArrowPathIcon className="size-3.5" />
              <span>Đổi ID mới</span>
            </Button>
          </div>
        </div>
      </div>

      <Button
        color="primary"
        className="h-10 w-full gap-2 font-medium"
        disabled={selectedItems.length === 0 || isSubmitting}
        onClick={submitOrder}
      >
        {isSubmitting ? (
          <>
            <Spinner className="size-4" color="neutral" />
            <span>Đang gửi đơn tới {siteConfig.siteId}...</span>
          </>
        ) : (
          <span>
            Gửi đơn ({totalDishesCount} phần)
          </span>
        )}
      </Button>

      {/* Feedback Banner */}
      {submissionFeedback && (
        <div
          className={`rounded-xl border p-3 text-xs ${
            submissionFeedback.kind === "success"
              ? "border-success/40 bg-success/5 text-gray-800 dark:text-dark-100"
              : "border-error/40 bg-error/5 text-error dark:text-error-light"
          }`}
        >
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="flex items-center gap-2 font-semibold">
              {submissionFeedback.kind === "error" && (
                <ExclamationTriangleIcon className="size-4 shrink-0" />
              )}
              <span>
                {submissionFeedback.httpStatus
                  ? `HTTP ${submissionFeedback.httpStatus}`
                  : "Lỗi kết nối mạng"}
              </span>
            </div>
            {submissionFeedback.order?.status && (
              <OrderStatusBadge status={submissionFeedback.order.status} />
            )}
          </div>
          <p className="mt-1.5 leading-relaxed">{submissionFeedback.message}</p>
          {submissionFeedback.kind === "error" && (
            <p className="mt-1.5 text-[11px] opacity-85">
              {submissionFeedback.lockRequestId
                ? "Chưa có phản hồi HTTP. Giữ nguyên clientRequestId và bấm Gửi đơn. Đổi ID lúc này có thể tạo đơn thứ hai nếu server đã ghi đơn."
                : "Server đã trả lời request này. Có thể gửi lại cùng ID, hoặc đổi ID nếu muốn tạo đơn khác."}
            </p>
          )}
        </div>
      )}

      {/* Collapsible D13 Idempotency / 409 Demo Tools */}
      <div className="border-t border-gray-200 pt-3 dark:border-dark-600">
        <button
          type="button"
          onClick={() => setShowDemoTools((prev) => !prev)}
          className="flex w-full cursor-pointer items-center justify-between text-xs font-medium text-gray-600 hover:text-gray-900 dark:text-dark-300 dark:hover:text-dark-100"
        >
          <span>Kiểm chứng Retry & 409 (Kịch bản D13)</span>
          <ChevronDownIcon
            className={`size-4 transition-transform ${
              showDemoTools ? "rotate-180" : ""
            }`}
          />
        </button>

        <Collapse in={showDemoTools}>
          <div className="mt-3 space-y-2.5 rounded-xl border border-dashed border-gray-300 p-3 text-xs dark:border-dark-500">
            {!lastSubmission ? (
              <p className="text-gray-500 dark:text-dark-300">
                Hãy tạo ít nhất 1 đơn trước, sau đó bạn có thể thử gửi lại cùng{" "}
                <code className="font-mono">clientRequestId</code> (nhận 200) hoặc đổi món giữ nguyên ID (nhận 409).
              </p>
            ) : (
              <>
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span className="text-gray-600 dark:text-dark-200">
                    Request vừa gửi:
                  </span>
                  <OrderIdText value={lastSubmission.clientRequestId} />
                </div>
                <div className="flex flex-wrap gap-2">
                  <Button
                    variant="outlined"
                    color="info"
                    className="h-8 flex-1 text-xs"
                    disabled={isSubmitting}
                    onClick={retryLastOrderSamePayload}
                  >
                    Gửi lại y hệt (Test 200)
                  </Button>
                  <Button
                    variant="outlined"
                    color="warning"
                    className="h-8 flex-1 text-xs"
                    disabled={isSubmitting}
                    onClick={testConflictWithLastRequestId}
                  >
                    Đổi món, giữ ID (Test 409)
                  </Button>
                </div>
              </>
            )}
          </div>
        </Collapse>
      </div>
    </Card>
  );
}

OrderComposerCard.propTypes = {
  siteConfig: PropTypes.object.isRequired,
  composer: PropTypes.object.isRequired,
};
