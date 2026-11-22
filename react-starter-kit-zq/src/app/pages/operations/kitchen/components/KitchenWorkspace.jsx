import { useState } from "react";
import { toast } from "sonner";
import { KITCHEN_STATUS_META } from "constants/orderLifecycle";
import {
  useCompleteKitchenTicketMutation,
  useKitchenAlertsQuery,
  useKitchenMonitorQuery,
  useKitchenTicketsQuery,
  useRejectKitchenTicketMutation,
  useStartKitchenTicketMutation,
} from "hooks/queries/useKitchenQueries";
import { ServiceAlertsCard } from "app/pages/operations/shared/ServiceAlertsCard";
import { ServiceMonitorCard } from "app/pages/operations/shared/ServiceMonitorCard";
import { extractApiError, shortUuid } from "utils/orderFormatting";
import { KitchenTicketsList } from "./KitchenTicketsList";
import { RejectTicketModal } from "./RejectTicketModal";

const KITCHEN_ALERT_FILTERS = [
  { value: "", label: "Tất cả" },
  { value: "NEEDS_REVIEW", label: "Cần kiểm tra (DLQ)" },
  { value: "WRONG_BRANCH_CANCEL", label: "Hủy sai chi nhánh" },
];

export function KitchenWorkspace() {
  const [alertKind, setAlertKind] = useState("");
  const [rejectTarget, setRejectTarget] = useState(null);
  const [rejectModalError, setRejectModalError] = useState(null);
  const [conflictBanner, setConflictBanner] = useState(null);
  const [busyOrderId, setBusyOrderId] = useState(null);

  const ticketsQuery = useKitchenTicketsQuery();
  const alertsQuery = useKitchenAlertsQuery(alertKind || undefined);
  const monitorQuery = useKitchenMonitorQuery();

  const startMutation = useStartKitchenTicketMutation();
  const rejectMutation = useRejectKitchenTicketMutation();
  const completeMutation = useCompleteKitchenTicketMutation();

  const handleConflict = async (orderId, parsedError) => {
    const statusLabel = parsedError.currentStatus
      ? KITCHEN_STATUS_META[parsedError.currentStatus]?.label ||
        parsedError.currentStatus
      : "khác";
    const msg = `Xung đột trạng thái (409) trên phiếu #${shortUuid(orderId)}: phiếu hiện đang ở "${statusLabel}". Đã tải lại danh sách phiếu mới nhất.`;
    setConflictBanner(msg);
    toast.warning(msg);
    await Promise.all([ticketsQuery.refetch(), alertsQuery.refetch()]);
  };

  const handleStart = async (orderId) => {
    setConflictBanner(null);
    setBusyOrderId(orderId);
    try {
      await startMutation.mutateAsync(orderId);
      toast.success(`Phiếu #${shortUuid(orderId)} đã chuyển sang Đang nấu.`);
    } catch (error) {
      const parsed = extractApiError(error);
      if (parsed.status === 409) {
        await handleConflict(orderId, parsed);
      } else {
        toast.error(parsed.message);
      }
    } finally {
      setBusyOrderId(null);
    }
  };

  const handleComplete = async (orderId) => {
    setConflictBanner(null);
    setBusyOrderId(orderId);
    try {
      await completeMutation.mutateAsync(orderId);
      toast.success(`Phiếu #${shortUuid(orderId)} đã Hoàn thành.`);
    } catch (error) {
      const parsed = extractApiError(error);
      if (parsed.status === 409) {
        await handleConflict(orderId, parsed);
      } else {
        toast.error(parsed.message);
      }
    } finally {
      setBusyOrderId(null);
    }
  };

  const handleConfirmReject = async ({
    orderId,
    reasonCode,
    spoiledIngredients,
  }) => {
    setRejectModalError(null);
    setConflictBanner(null);
    setBusyOrderId(orderId);
    try {
      await rejectMutation.mutateAsync({
        orderId,
        reasonCode,
        spoiledIngredients,
      });
      setRejectTarget(null);
      toast.success(`Đã từ chối phiếu #${shortUuid(orderId)}.`);
    } catch (error) {
      const parsed = extractApiError(error);
      if (parsed.status === 409) {
        setRejectTarget(null);
        await handleConflict(orderId, parsed);
      } else {
        setRejectModalError(parsed.message);
        toast.error(parsed.message);
      }
    } finally {
      setBusyOrderId(null);
    }
  };

  return (
    <div className="space-y-4">
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

      <KitchenTicketsList
        tickets={ticketsQuery.data || []}
        isLoading={ticketsQuery.isLoading}
        isError={ticketsQuery.isError}
        onStart={handleStart}
        onReject={(ticket) => {
          setRejectModalError(null);
          setRejectTarget(ticket);
        }}
        onComplete={handleComplete}
        busyOrderId={busyOrderId}
      />

      <ServiceAlertsCard
        title="Cảnh báo Bếp (Luồng D)"
        alerts={alertsQuery.data || []}
        isLoading={alertsQuery.isLoading}
        isError={alertsQuery.isError}
        emptyText="Không có cảnh báo DLQ hay hủy sai chi nhánh."
        kindFilter={alertKind}
        onKindFilterChange={setAlertKind}
        filterOptions={KITCHEN_ALERT_FILTERS}
      />

      <ServiceMonitorCard
        title="Giám sát Consumer & Outbox Bếp"
        monitor={monitorQuery.data}
        isLoading={monitorQuery.isLoading}
        isError={monitorQuery.isError}
      />

      <RejectTicketModal
        isOpen={Boolean(rejectTarget)}
        ticket={rejectTarget}
        onClose={() => setRejectTarget(null)}
        onConfirm={handleConfirmReject}
        isSubmitting={rejectMutation.isPending}
        errorMessage={rejectModalError}
      />
    </div>
  );
}
