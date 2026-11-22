import { useState } from "react";
import { toast } from "sonner";
import { Badge } from "components/ui";
import { BRANCH_OPTIONS } from "constants/orderLifecycle";
import {
  useBranchMenuQuery,
  useBranchMonitorQuery,
  useBranchOrdersQuery,
  useCancelBranchOrderMutation,
  useCreateBranchOrderMutation,
} from "hooks/queries/useBranchQueries";
import {
  extractApiError,
  generateUuid,
  shortUuid,
} from "utils/orderFormatting";
import { BranchMonitorPanel } from "./BranchMonitorPanel";
import { CancelOrderModal } from "./CancelOrderModal";
import { MenuOrderCard } from "./MenuOrderCard";
import { OrdersListCard } from "./OrdersListCard";

export function BranchWorkspace({
  initialBranch = "POS01",
  allowBranchSwitch = true,
  compact = false,
}) {
  const [branchId, setBranchId] = useState(initialBranch);
  const [cancelTarget, setCancelTarget] = useState(null);
  const [submitError, setSubmitError] = useState(null);
  const [cancelModalError, setCancelModalError] = useState(null);
  const [conflictBanner, setConflictBanner] = useState(null);

  const menuQuery = useBranchMenuQuery(branchId);
  const ordersQuery = useBranchOrdersQuery(branchId);
  const monitorQuery = useBranchMonitorQuery(branchId);

  const createOrderMutation = useCreateBranchOrderMutation(branchId);
  const cancelOrderMutation = useCancelBranchOrderMutation(branchId);

  const handleBranchChange = (nextBranch) => {
    setBranchId(nextBranch);
    setCancelTarget(null);
    setSubmitError(null);
    setCancelModalError(null);
    setConflictBanner(null);
  };

  const handleSubmitOrder = async (items) => {
    setSubmitError(null);
    try {
      const created = await createOrderMutation.mutateAsync({
        clientRequestId: generateUuid(),
        items,
      });
      toast.success(
        `Đã tạo đơn #${shortUuid(created?.orderId)} tại ${branchId} (Chờ Kho).`,
      );
      return true;
    } catch (error) {
      const parsed = extractApiError(error);
      setSubmitError(parsed.message);
      toast.error(parsed.message);
      return false;
    }
  };

  const handleOpenCancelByOrderId = (orderId) => {
    const found = (ordersQuery.data || []).find((o) => o.orderId === orderId);
    setCancelModalError(null);
    setCancelTarget(found || { orderId });
  };

  const handleConfirmCancel = async (orderId, reasonCode) => {
    setCancelModalError(null);
    setConflictBanner(null);
    try {
      const result = await cancelOrderMutation.mutateAsync({
        orderId,
        reasonCode,
      });
      setCancelTarget(null);
      if (result?.status === "CANCELLED") {
        toast.success(
          `Đã hủy cục bộ đơn #${shortUuid(orderId)} (chưa gửi tới Kafka).`,
        );
      } else {
        toast.success(`Đã gửi yêu cầu hủy đơn #${shortUuid(orderId)} tới Bếp.`);
      }
    } catch (error) {
      const parsed = extractApiError(error);
      if (parsed.status === 409) {
        const msg =
          "Không thể hủy: đơn đã ở trạng thái cuối (409). Đã tải lại trạng thái mới nhất.";
        setCancelTarget(null);
        setConflictBanner(msg);
        toast.warning(msg);
        await Promise.all([ordersQuery.refetch(), monitorQuery.refetch()]);
      } else {
        setCancelModalError(parsed.message);
        toast.error(parsed.message);
      }
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-2">
          {allowBranchSwitch ? (
            BRANCH_OPTIONS.map((opt) => (
              <button
                key={opt.id}
                type="button"
                onClick={() => handleBranchChange(opt.id)}
                className={`rounded-xl px-3.5 py-2 text-xs font-semibold transition-all ${
                  branchId === opt.id
                    ? "bg-primary-600 text-white shadow-sm"
                    : "border border-gray-200 bg-white text-gray-700 hover:bg-gray-50 dark:border-dark-600 dark:bg-dark-800 dark:text-dark-200 dark:hover:bg-dark-700"
                }`}
              >
                {opt.label}
              </button>
            ))
          ) : (
            <Badge color="primary" variant="soft" className="px-3 py-1 text-xs">
              Chi nhánh: {branchId}
            </Badge>
          )}
        </div>

        <div className="flex items-center gap-2 text-xs">
          <Badge
            color={
              monitorQuery.data?.kafkaConnected !== false ? "success" : "error"
            }
            variant="soft"
          >
            {monitorQuery.data?.kafkaConnected !== false
              ? "Kafka: Online"
              : "Kafka: Offline"}
          </Badge>
          <Badge color="neutral" variant="outlined">
            Chờ gửi: {monitorQuery.data?.pendingOutboxCount ?? 0}
          </Badge>
        </div>
      </div>

      {conflictBanner && (
        <div
          role="alert"
          className="flex items-center justify-between gap-2 rounded-xl border border-warning/40 bg-warning/10 px-4 py-2.5 text-xs font-medium text-warning-darker dark:text-warning-lighter"
        >
          <span>{conflictBanner}</span>
          <button
            type="button"
            onClick={() => setConflictBanner(null)}
            className="text-xs underline"
          >
            Đóng
          </button>
        </div>
      )}

      <MenuOrderCard
        branchId={branchId}
        menuData={menuQuery.data}
        isLoading={menuQuery.isLoading}
        isError={menuQuery.isError}
        monitorKafkaConnected={monitorQuery.data?.kafkaConnected}
        onSubmitOrder={handleSubmitOrder}
        isSubmitting={createOrderMutation.isPending}
        submitError={submitError}
        compact={compact}
      />

      <OrdersListCard
        branchId={branchId}
        orders={ordersQuery.data || []}
        isLoading={ordersQuery.isLoading}
        isError={ordersQuery.isError}
        onRequestCancel={(order) => {
          setCancelModalError(null);
          setCancelTarget(order);
        }}
      />

      <BranchMonitorPanel
        branchId={branchId}
        monitor={monitorQuery.data}
        isLoading={monitorQuery.isLoading}
        isError={monitorQuery.isError}
        onRequestCancelByOrderId={handleOpenCancelByOrderId}
      />

      <CancelOrderModal
        isOpen={Boolean(cancelTarget)}
        order={cancelTarget}
        onClose={() => setCancelTarget(null)}
        onConfirm={handleConfirmCancel}
        isSubmitting={cancelOrderMutation.isPending}
        errorMessage={cancelModalError}
      />
    </div>
  );
}
