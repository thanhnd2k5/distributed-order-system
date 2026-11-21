import { useState } from "react";
import { ArrowPathIcon, ExclamationTriangleIcon } from "@heroicons/react/24/outline";
import { toast } from "sonner";
import { Badge, Button, Card } from "components/ui";
import {
  useBumpInventoryEpochMutation,
  useClearIngredientCheckMutation,
  useInventoryAlertsQuery,
  useInventoryMenuSnapshotQuery,
  useInventoryMonitorQuery,
  useInventoryStockQuery,
  useStockReceiptMutation,
} from "hooks/queries/useInventoryQueries";
import { ServiceAlertsCard } from "app/pages/operations/shared/ServiceAlertsCard";
import { ServiceMonitorCard } from "app/pages/operations/shared/ServiceMonitorCard";
import { DishStatusBadge } from "app/pages/operations/shared/StatusBadge";
import { extractApiError } from "utils/orderFormatting";
import { ClearCheckModal } from "./ClearCheckModal";
import { StockReceiptForm } from "./StockReceiptForm";
import { StockTableCard } from "./StockTableCard";

const INVENTORY_ALERT_FILTERS = [
  { value: "", label: "Tất cả" },
  { value: "NEEDS_REVIEW", label: "Cần kiểm tra (DLQ)" },
  { value: "UNMATCHED_COMMAND", label: "Lệnh không khớp" },
];

export function InventoryWorkspace({ compact = false }) {
  const [alertKind, setAlertKind] = useState("");
  const [clearTarget, setClearTarget] = useState(null);
  const [clearModalError, setClearModalError] = useState(null);
  const [conflictBanner, setConflictBanner] = useState(null);

  const stockQuery = useInventoryStockQuery();
  const snapshotQuery = useInventoryMenuSnapshotQuery();
  const alertsQuery = useInventoryAlertsQuery(alertKind || undefined);
  const monitorQuery = useInventoryMonitorQuery();

  const receiptMutation = useStockReceiptMutation();
  const clearCheckMutation = useClearIngredientCheckMutation();
  const bumpEpochMutation = useBumpInventoryEpochMutation();

  const stock = stockQuery.data || [];
  const flaggedStock = stock.filter((item) => item.needsCheck);
  const snapshotDishes = snapshotQuery.data?.dishes || [];

  const handleSubmitReceipt = async (payload) => {
    try {
      await receiptMutation.mutateAsync(payload);
      toast.success(`Đã nhập kho cho nguyên liệu ${payload.ingredientId}.`);
      return true;
    } catch (error) {
      const parsed = extractApiError(error);
      toast.error(parsed.message);
      return false;
    }
  };

  const handleConfirmClearCheck = async (ingredientId, countedOnHand) => {
    setClearModalError(null);
    setConflictBanner(null);
    try {
      await clearCheckMutation.mutateAsync({ ingredientId, countedOnHand });
      setClearTarget(null);
      toast.success(`Đã xác nhận kiểm kho cho ${ingredientId}.`);
    } catch (error) {
      const parsed = extractApiError(error);
      if (parsed.status === 409) {
        const msg =
          parsed.message ||
          "Không thể xác nhận kiểm kho (409): số lượng kiểm thực tế nhỏ hơn lượng đang giữ.";
        setClearModalError(msg);
        setConflictBanner(
          `${msg} Đã tải lại tồn kho mới nhất — hãy từ chối phiếu liên quan trước nếu cần.`,
        );
        toast.warning(msg);
        await stockQuery.refetch();
      } else {
        setClearModalError(parsed.message);
        toast.error(parsed.message);
      }
    }
  };

  const handleBumpEpoch = async () => {
    try {
      const res = await bumpEpochMutation.mutateAsync();
      toast.success(
        `Đã tăng Epoch thành công (Epoch mới: ${res?.epoch ?? "—"}, Seq: ${res?.seq ?? "—"}).`,
      );
    } catch (error) {
      const parsed = extractApiError(error);
      toast.error(parsed.message);
    }
  };

  return (
    <div className="space-y-4">
      <Card className="p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="space-y-1">
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-sm font-semibold text-gray-800 dark:text-dark-100">
                Snapshot trạng thái món tại Kho
              </span>
              {snapshotQuery.data && (
                <Badge color="primary" variant="soft">
                  Epoch {snapshotQuery.data.epoch} · Seq{" "}
                  {snapshotQuery.data.seq}
                </Badge>
              )}
            </div>
            {snapshotDishes.length > 0 && (
              <div className="flex flex-wrap gap-1.5 pt-1">
                {snapshotDishes.map((dish) => (
                  <div
                    key={dish.dishId}
                    className="inline-flex items-center gap-1.5 rounded-lg border border-gray-200 px-2 py-1 text-xs dark:border-dark-600"
                  >
                    <span className="font-medium text-gray-700 dark:text-dark-200">
                      {dish.dishName || dish.dishId}
                    </span>
                    <DishStatusBadge status={dish.status} />
                  </div>
                ))}
              </div>
            )}
          </div>

          <Button
            size="sm"
            color="warning"
            variant="soft"
            disabled={bumpEpochMutation.isPending}
            onClick={handleBumpEpoch}
            className="gap-1.5 px-3 py-2 text-xs font-semibold"
          >
            <ArrowPathIcon className="size-4" />
            <span>
              {bumpEpochMutation.isPending
                ? "Đang tăng Epoch..."
                : "Tăng Epoch (Khôi phục sao lưu)"}
            </span>
          </Button>
        </div>
      </Card>

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

      {flaggedStock.length > 0 && (
        <Card className="border-error/40 bg-error/5 p-4">
          <div className="mb-2 flex items-center gap-2 text-xs font-semibold text-error">
            <ExclamationTriangleIcon className="size-4" />
            <span>
              Luồng D — Nguyên liệu đang chờ kiểm kho ({flaggedStock.length})
            </span>
          </div>
          <div className="space-y-2">
            {flaggedStock.map((item) => (
              <div
                key={item.ingredientId}
                className="flex flex-wrap items-center justify-between gap-2 rounded-lg bg-white px-3 py-2 text-xs dark:bg-dark-800"
              >
                <div>
                  <span className="font-semibold text-gray-900 dark:text-dark-50">
                    {item.name}
                  </span>{" "}
                  <span className="font-mono text-gray-400">
                    ({item.ingredientId})
                  </span>
                </div>
                <Button
                  size="sm"
                  color="primary"
                  onClick={() => {
                    setClearModalError(null);
                    setClearTarget(item);
                  }}
                  className="px-2.5 py-1 text-xs"
                >
                  Xác nhận kiểm kho
                </Button>
              </div>
            ))}
          </div>
        </Card>
      )}

      <StockTableCard
        stock={stock}
        isLoading={stockQuery.isLoading}
        isError={stockQuery.isError}
        onOpenClearCheck={(item) => {
          setClearModalError(null);
          setClearTarget(item);
        }}
      />

      <StockReceiptForm
        stock={stock}
        onSubmitReceipt={handleSubmitReceipt}
        isSubmitting={receiptMutation.isPending}
      />

      {!compact ? (
        <div className="grid gap-4 lg:grid-cols-2">
          <ServiceAlertsCard
            title="Cảnh báo Kho (Luồng D)"
            alerts={alertsQuery.data || []}
            isLoading={alertsQuery.isLoading}
            isError={alertsQuery.isError}
            emptyText="Không có cảnh báo DLQ hay lệnh hoàn không khớp."
            kindFilter={alertKind}
            onKindFilterChange={setAlertKind}
            filterOptions={INVENTORY_ALERT_FILTERS}
          />
          <ServiceMonitorCard
            title="Giám sát Consumer & Outbox Kho"
            monitor={monitorQuery.data}
            isLoading={monitorQuery.isLoading}
            isError={monitorQuery.isError}
          />
        </div>
      ) : (
        <>
          <ServiceAlertsCard
            title="Cảnh báo Kho (Luồng D)"
            alerts={alertsQuery.data || []}
            isLoading={alertsQuery.isLoading}
            isError={alertsQuery.isError}
            emptyText="Không có cảnh báo DLQ hay lệnh hoàn không khớp."
            kindFilter={alertKind}
            onKindFilterChange={setAlertKind}
            filterOptions={INVENTORY_ALERT_FILTERS}
          />
          <ServiceMonitorCard
            title="Giám sát Consumer & Outbox Kho"
            monitor={monitorQuery.data}
            isLoading={monitorQuery.isLoading}
            isError={monitorQuery.isError}
          />
        </>
      )}

      <ClearCheckModal
        isOpen={Boolean(clearTarget)}
        ingredient={clearTarget}
        onClose={() => setClearTarget(null)}
        onConfirm={handleConfirmClearCheck}
        isSubmitting={clearCheckMutation.isPending}
        errorMessage={clearModalError}
      />
    </div>
  );
}
