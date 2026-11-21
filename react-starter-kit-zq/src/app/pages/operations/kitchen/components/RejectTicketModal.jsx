import { useEffect, useState } from "react";
import {
  Dialog,
  DialogPanel,
  DialogTitle,
  Transition,
  TransitionChild,
} from "@headlessui/react";
import { FiX } from "react-icons/fi";
import { Button } from "components/ui";
import { KITCHEN_REJECTION_REASONS } from "constants/orderLifecycle";
import { formatQuantity, shortUuid } from "utils/orderFormatting";

export function RejectTicketModal({
  isOpen,
  ticket,
  onClose,
  onConfirm,
  isSubmitting,
  errorMessage,
}) {
  const [reasonCode, setReasonCode] = useState("");
  const [selectedSpoiled, setSelectedSpoiled] = useState([]);
  const [localError, setLocalError] = useState(null);

  useEffect(() => {
    if (isOpen) {
      setReasonCode("");
      setSelectedSpoiled([]);
      setLocalError(null);
    }
  }, [isOpen, ticket?.orderId]);

  const reservedList = Array.isArray(ticket?.reserved) ? ticket.reserved : [];
  const selectedReason = KITCHEN_REJECTION_REASONS.find(
    (r) => r.code === reasonCode,
  );
  const requiresSpoiled = Boolean(selectedReason?.requiresSpoiledIngredient);
  const missingRequiredSpoiled =
    requiresSpoiled && selectedSpoiled.length === 0;

  const toggleIngredient = (ingredientId) => {
    setLocalError(null);
    setSelectedSpoiled((prev) =>
      prev.includes(ingredientId)
        ? prev.filter((id) => id !== ingredientId)
        : [...prev, ingredientId],
    );
  };

  const handleSubmit = (event) => {
    event.preventDefault();
    if (!ticket || !reasonCode) return;
    if (requiresSpoiled && selectedSpoiled.length === 0) {
      setLocalError(
        "Vui lòng chọn ít nhất một nguyên liệu bị hỏng từ danh sách đã giữ.",
      );
      return;
    }
    onConfirm({
      orderId: ticket.orderId,
      reasonCode,
      spoiledIngredients: requiresSpoiled ? selectedSpoiled : [],
    });
  };

  return (
    <Transition appear show={isOpen}>
      <Dialog as="div" className="relative z-50" onClose={onClose}>
        <TransitionChild
          enter="ease-out duration-200"
          enterFrom="opacity-0"
          enterTo="opacity-100"
          leave="ease-in duration-150"
          leaveFrom="opacity-100"
          leaveTo="opacity-0"
        >
          <div className="fixed inset-0 bg-gray-900/50 backdrop-blur-xs dark:bg-black/60" />
        </TransitionChild>

        <div className="fixed inset-0 overflow-y-auto">
          <div className="flex min-h-full items-center justify-center p-4">
            <TransitionChild
              enter="ease-out duration-200"
              enterFrom="opacity-0 scale-95"
              enterTo="opacity-100 scale-100"
              leave="ease-in duration-150"
              leaveFrom="opacity-100 scale-100"
              leaveTo="opacity-0 scale-95"
            >
              <DialogPanel className="w-full max-w-md overflow-hidden rounded-2xl bg-white shadow-xl dark:bg-dark-700">
                <div className="flex items-center justify-between border-b border-gray-100 px-5 py-4 dark:border-dark-600">
                  <DialogTitle
                    as="h3"
                    className="text-base font-semibold text-gray-900 dark:text-dark-50"
                  >
                    Từ chối phiếu #{shortUuid(ticket?.orderId)}
                  </DialogTitle>
                  <button
                    type="button"
                    onClick={onClose}
                    aria-label="Đóng hộp thoại từ chối phiếu"
                    className="rounded-lg p-1.5 text-gray-400 hover:bg-gray-100 hover:text-gray-600 dark:hover:bg-dark-600"
                  >
                    <FiX className="size-4" />
                  </button>
                </div>

                <form onSubmit={handleSubmit} className="space-y-4 px-5 py-4">
                  <div>
                    <label
                      htmlFor="reject-reason-select"
                      className="block text-xs font-semibold text-gray-700 dark:text-dark-100"
                    >
                      Lý do từ chối (bắt buộc)
                    </label>
                    <select
                      id="reject-reason-select"
                      value={reasonCode}
                      onChange={(e) => {
                        setReasonCode(e.target.value);
                        setLocalError(null);
                      }}
                      required
                      className="mt-1.5 w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm text-gray-900 focus:border-primary-600 focus:outline-none dark:border-dark-500 dark:bg-dark-800 dark:text-dark-50"
                    >
                      <option value="">-- Chọn lý do từ chối --</option>
                      {KITCHEN_REJECTION_REASONS.map((reason) => (
                        <option key={reason.code} value={reason.code}>
                          {reason.label} ({reason.code})
                        </option>
                      ))}
                    </select>
                  </div>

                  {requiresSpoiled && (
                    <div className="rounded-xl border border-error/30 bg-error/5 p-3">
                      <p className="text-xs font-semibold text-error">
                        Chọn nguyên liệu bị hỏng từ danh sách đã giữ của phiếu
                        (bắt buộc):
                      </p>
                      {reservedList.length === 0 ? (
                        <p className="mt-2 text-xs text-gray-500">
                          Phiếu này chưa có danh sách nguyên liệu đã giữ.
                        </p>
                      ) : (
                        <div className="mt-2 space-y-1.5">
                          {reservedList.map((ing) => {
                            const checked = selectedSpoiled.includes(
                              ing.ingredientId,
                            );
                            return (
                              <label
                                key={ing.ingredientId}
                                className="flex cursor-pointer items-center justify-between rounded-lg border border-gray-200 bg-white px-3 py-2 text-xs dark:border-dark-500 dark:bg-dark-800"
                              >
                                <span className="flex items-center gap-2 font-medium text-gray-800 dark:text-dark-100">
                                  <input
                                    type="checkbox"
                                    checked={checked}
                                    onChange={() =>
                                      toggleIngredient(ing.ingredientId)
                                    }
                                    className="rounded border-gray-300 text-error focus:ring-error"
                                  />
                                  <span className="font-mono font-semibold">
                                    {ing.ingredientId}
                                  </span>
                                </span>
                                <span className="font-mono text-gray-500">
                                  Đã giữ: {formatQuantity(ing.quantity)}
                                </span>
                              </label>
                            );
                          })}
                        </div>
                      )}
                      {missingRequiredSpoiled && (
                        <p className="mt-2 text-[11px] font-medium text-error">
                          Bắt buộc chọn ít nhất 1 nguyên liệu bị hỏng trước khi
                          xác nhận.
                        </p>
                      )}
                    </div>
                  )}

                  {(localError || errorMessage) && (
                    <div
                      role="alert"
                      className="rounded-lg border border-error/40 bg-error/10 px-3 py-2 text-xs font-medium text-error"
                    >
                      {localError || errorMessage}
                    </div>
                  )}

                  <div className="flex justify-end gap-2 border-t border-gray-100 pt-3 dark:border-dark-600">
                    <Button variant="outlined" color="neutral" onClick={onClose}>
                      Đóng
                    </Button>
                    <Button
                      type="submit"
                      color="error"
                      disabled={
                        !reasonCode || missingRequiredSpoiled || isSubmitting
                      }
                    >
                      {isSubmitting ? "Đang xử lý..." : "Xác nhận từ chối"}
                    </Button>
                  </div>
                </form>
              </DialogPanel>
            </TransitionChild>
          </div>
        </div>
      </Dialog>
    </Transition>
  );
}
