import { useState } from "react";
import {
  ExclamationTriangleIcon,
  MinusIcon,
  PlusIcon,
  ShoppingBagIcon,
  TrashIcon,
} from "@heroicons/react/24/outline";
import { Badge, Button, Card } from "components/ui";
import { DishStatusBadge } from "app/pages/operations/shared/StatusBadge";
import { formatTimeHMS } from "utils/orderFormatting";

export function MenuOrderCard({
  branchId,
  menuData,
  isLoading,
  isError,
  monitorKafkaConnected,
  onSubmitOrder,
  isSubmitting,
  submitError,
  compact = false,
}) {
  const [cart, setCart] = useState([]);
  const [validationError, setValidationError] = useState(null);

  const isStaleOrOffline =
    Boolean(menuData?.stale) ||
    menuData?.kafkaConnected === false ||
    monitorKafkaConnected === false;

  const visibleDishes = (menuData?.dishes || []).filter(
    (dish) => dish.status !== "HIDDEN",
  );

  const handleAddDish = (dish) => {
    setValidationError(null);
    setCart((prev) => {
      const existing = prev.find((item) => item.dishId === dish.dishId);
      if (existing) {
        const currentQty = Number(existing.quantity) || 0;
        return prev.map((item) =>
          item.dishId === dish.dishId
            ? { ...item, quantity: Math.max(1, currentQty + 1) }
            : item,
        );
      }
      return [
        ...prev,
        { dishId: dish.dishId, name: dish.name || dish.dishId, quantity: 1 },
      ];
    });
  };

  const handleQtyChange = (dishId, rawValue) => {
    setValidationError(null);
    setCart((prev) =>
      prev.map((item) =>
        item.dishId === dishId ? { ...item, quantity: rawValue } : item,
      ),
    );
  };

  const handleStepQty = (dishId, delta) => {
    setValidationError(null);
    setCart((prev) =>
      prev
        .map((item) => {
          if (item.dishId !== dishId) return item;
          const next = (Number(item.quantity) || 0) + delta;
          return { ...item, quantity: next };
        })
        .filter((item) => Number(item.quantity) > 0),
    );
  };

  const handleRemoveDish = (dishId) => {
    setValidationError(null);
    setCart((prev) => prev.filter((item) => item.dishId !== dishId));
  };

  const handleSubmit = async (event) => {
    event.preventDefault();
    setValidationError(null);

    if (cart.length === 0) {
      setValidationError("Vui lòng chọn ít nhất một món trước khi đặt đơn.");
      return;
    }

    const parsedItems = [];
    for (const item of cart) {
      const qty = Number(item.quantity);
      if (!Number.isFinite(qty) || !Number.isInteger(qty) || qty <= 0) {
        setValidationError("Số lượng mỗi món phải là số nguyên lớn hơn 0.");
        return;
      }
      if (!isStaleOrOffline) {
        const dishInfo = visibleDishes.find((d) => d.dishId === item.dishId);
        if (dishInfo?.status === "SOLD_OUT") {
          setValidationError(
            `Món "${item.name}" vừa hết hàng, không thể đặt khi dữ liệu đang mới.`,
          );
          return;
        }
      }
      parsedItems.push({ dishId: item.dishId, quantity: qty });
    }

    const ok = await onSubmitOrder(parsedItems);
    if (ok) {
      setCart([]);
    }
  };

  return (
    <Card className="p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <ShoppingBagIcon className="size-5 text-primary-500" />
          <h3 className="text-sm font-semibold text-gray-800 dark:text-dark-100">
            Thực đơn & Tạo đơn ({branchId})
          </h3>
        </div>
        {menuData && (
          <div className="flex items-center gap-2 text-xs text-gray-500 dark:text-dark-300">
            <span>Epoch {menuData.epoch ?? 0}</span>
            {menuData.lastHeardAt && (
              <span>· Nghe Kho: {formatTimeHMS(menuData.lastHeardAt)}</span>
            )}
          </div>
        )}
      </div>

      {isStaleOrOffline && (
        <div
          role="alert"
          className="mb-3 flex items-start gap-2 rounded-lg border border-warning/40 bg-warning/10 px-3 py-2 text-xs font-medium text-warning-darker dark:text-warning-lighter"
        >
          <ExclamationTriangleIcon className="mt-0.5 size-4 shrink-0" />
          <div>
            <p className="font-semibold">Trạng thái món có thể đã thay đổi</p>
            <p className="mt-0.5 text-[11px] opacity-90">
              {menuData?.kafkaConnected === false ||
              monitorKafkaConnected === false
                ? "Chi nhánh đang mất kết nối Kafka; món hết vẫn cho phép đặt để Kho kiểm tra sau."
                : "Chưa nhận heartbeat từ Kho trong ngưỡng mới; món hết vẫn cho phép đặt với cảnh báo."}
            </p>
          </div>
        </div>
      )}

      <div className={compact ? "space-y-4" : "grid gap-4 lg:grid-cols-2"}>
        <div>
          <p className="mb-2 text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-dark-300">
            Danh sách món ({visibleDishes.length})
          </p>
          {isLoading ? (
            <p className="text-xs text-gray-400">Đang tải thực đơn...</p>
          ) : isError ? (
            <p className="text-xs text-error">
              Không thể tải thực đơn từ chi nhánh {branchId}.
            </p>
          ) : visibleDishes.length === 0 ? (
            <p className="text-xs text-gray-500">Không có món khả dụng.</p>
          ) : (
            <div className="space-y-2">
              {visibleDishes.map((dish) => {
                const isSoldOut = dish.status === "SOLD_OUT";
                const isBlocked = isSoldOut && !isStaleOrOffline;
                return (
                  <div
                    key={dish.dishId}
                    data-testid={`menu-dish-${dish.dishId}`}
                    className={`flex items-center justify-between gap-2 rounded-lg border p-2.5 text-xs ${
                      isBlocked
                        ? "border-gray-200 bg-gray-50 opacity-70 dark:border-dark-600 dark:bg-dark-800/40"
                        : "border-gray-200 bg-white dark:border-dark-600 dark:bg-dark-800"
                    }`}
                  >
                    <div className="min-w-0">
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="font-semibold text-gray-800 dark:text-dark-100">
                          {dish.name}
                        </span>
                        <DishStatusBadge
                          status={dish.status}
                          staleOrOffline={isStaleOrOffline}
                        />
                      </div>
                      <p className="mt-0.5 font-mono text-[11px] text-gray-400">
                        {dish.dishId} · v({dish.epoch},{dish.seq})
                      </p>
                    </div>
                    <Button
                      size="sm"
                      color={isSoldOut && isStaleOrOffline ? "warning" : "primary"}
                      variant={isBlocked ? "outlined" : "soft"}
                      disabled={isBlocked}
                      onClick={() => handleAddDish(dish)}
                      aria-label={`Chọn món ${dish.name}`}
                      className="shrink-0 px-3 py-1.5 text-xs"
                    >
                      {isBlocked ? "Đã hết" : "+ Chọn"}
                    </Button>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        <form onSubmit={handleSubmit} noValidate className="flex flex-col">
          <div className="mb-2 flex items-center justify-between">
            <p className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-dark-300">
              Đơn hàng đang tạo ({cart.length})
            </p>
            {cart.length > 0 && (
              <button
                type="button"
                onClick={() => {
                  setCart([]);
                  setValidationError(null);
                }}
                className="text-xs text-gray-400 hover:text-error"
              >
                Xóa hết
              </button>
            )}
          </div>

          <div className="flex-1 rounded-lg border border-dashed border-gray-300 bg-gray-50/50 p-3 dark:border-dark-600 dark:bg-dark-800/40">
            {cart.length === 0 ? (
              <p className="py-6 text-center text-xs text-gray-400">
                Chọn món từ thực đơn bên cạnh để tạo đơn mới.
              </p>
            ) : (
              <div className="space-y-2">
                {cart.map((item) => {
                  const dishMeta = visibleDishes.find(
                    (d) => d.dishId === item.dishId,
                  );
                  const soldOutWhileFresh =
                    !isStaleOrOffline && dishMeta?.status === "SOLD_OUT";
                  return (
                    <div
                      key={item.dishId}
                      className="flex items-center justify-between gap-2 rounded-lg bg-white p-2 text-xs shadow-2xs dark:bg-dark-750"
                    >
                      <div className="min-w-0 flex-1">
                        <p className="truncate font-medium text-gray-800 dark:text-dark-100">
                          {item.name}
                        </p>
                        {soldOutWhileFresh && (
                          <Badge color="error" variant="soft" className="mt-0.5 text-[10px]">
                            Hết món
                          </Badge>
                        )}
                      </div>
                      <div className="flex items-center gap-1">
                        <button
                          type="button"
                          onClick={() => handleStepQty(item.dishId, -1)}
                          aria-label={`Giảm ${item.name}`}
                          className="rounded border border-gray-200 p-1 text-gray-600 hover:bg-gray-100 dark:border-dark-500 dark:text-dark-200"
                        >
                          <MinusIcon className="size-3.5" />
                        </button>
                        <input
                          type="number"
                          min="1"
                          step="1"
                          aria-label={`Số lượng ${item.name}`}
                          value={item.quantity}
                          onChange={(e) =>
                            handleQtyChange(item.dishId, e.target.value)
                          }
                          className="w-14 rounded border border-gray-300 bg-white px-2 py-1 text-center font-mono text-xs text-gray-900 dark:border-dark-500 dark:bg-dark-700 dark:text-dark-50"
                        />
                        <button
                          type="button"
                          onClick={() => handleStepQty(item.dishId, 1)}
                          aria-label={`Tăng ${item.name}`}
                          className="rounded border border-gray-200 p-1 text-gray-600 hover:bg-gray-100 dark:border-dark-500 dark:text-dark-200"
                        >
                          <PlusIcon className="size-3.5" />
                        </button>
                        <button
                          type="button"
                          onClick={() => handleRemoveDish(item.dishId)}
                          aria-label={`Xóa ${item.name}`}
                          className="ml-1 rounded p-1 text-gray-400 hover:bg-error/10 hover:text-error"
                        >
                          <TrashIcon className="size-3.5" />
                        </button>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {(validationError || submitError) && (
            <p
              role="alert"
              className="mt-2 rounded-lg bg-error/10 px-3 py-2 text-xs font-medium text-error"
            >
              {validationError || submitError}
            </p>
          )}

          <Button
            type="submit"
            color="primary"
            disabled={isSubmitting}
            className="mt-3 w-full justify-center py-2 text-xs font-semibold"
          >
            {isSubmitting ? "Đang gửi đơn..." : "Đặt đơn"}
          </Button>
        </form>
      </div>
    </Card>
  );
}
