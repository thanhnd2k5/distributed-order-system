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
import { CUSTOMER_CANCEL_REASONS } from "constants/orderLifecycle";
import { shortUuid } from "utils/orderFormatting";

export function CancelOrderModal({
  isOpen,
  order,
  onClose,
  onConfirm,
  isSubmitting,
  errorMessage,
}) {
  const [reasonCode, setReasonCode] = useState("");

  useEffect(() => {
    if (isOpen) {
      setReasonCode("");
    }
  }, [isOpen, order?.orderId]);

  const handleSubmit = (event) => {
    event.preventDefault();
    if (!reasonCode || !order) return;
    onConfirm(order.orderId, reasonCode);
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
                    Yêu cầu hủy đơn #{shortUuid(order?.orderId)}
                  </DialogTitle>
                  <button
                    type="button"
                    onClick={onClose}
                    aria-label="Đóng hộp thoại hủy đơn"
                    className="rounded-lg p-1.5 text-gray-400 hover:bg-gray-100 hover:text-gray-600 dark:hover:bg-dark-600"
                  >
                    <FiX className="size-4" />
                  </button>
                </div>

                <form onSubmit={handleSubmit} className="space-y-4 px-5 py-4">
                  <p className="text-xs text-gray-600 dark:text-dark-200">
                    Nếu gói tạo đơn chưa từng thử gửi tới Kafka, chi nhánh sẽ
                    hủy cục bộ ngay. Ngược lại, yêu cầu sẽ được gửi tới Bếp để
                    quyết định.
                  </p>

                  <div>
                    <label
                      htmlFor="cancel-reason-select"
                      className="block text-xs font-semibold text-gray-700 dark:text-dark-100"
                    >
                      Lý do hủy đơn (bắt buộc)
                    </label>
                    <select
                      id="cancel-reason-select"
                      value={reasonCode}
                      onChange={(e) => setReasonCode(e.target.value)}
                      required
                      className="mt-1.5 w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm text-gray-900 focus:border-primary-600 focus:outline-none dark:border-dark-500 dark:bg-dark-800 dark:text-dark-50"
                    >
                      <option value="">-- Chọn lý do hủy --</option>
                      {CUSTOMER_CANCEL_REASONS.map((reason) => (
                        <option key={reason.code} value={reason.code}>
                          {reason.label} ({reason.code})
                        </option>
                      ))}
                    </select>
                  </div>

                  {errorMessage && (
                    <div
                      role="alert"
                      className="rounded-lg border border-error/40 bg-error/10 px-3 py-2 text-xs font-medium text-error"
                    >
                      {errorMessage}
                    </div>
                  )}

                  <div className="flex justify-end gap-2 border-t border-gray-100 pt-3 dark:border-dark-600">
                    <Button variant="outlined" color="neutral" onClick={onClose}>
                      Đóng
                    </Button>
                    <Button
                      type="submit"
                      color="error"
                      disabled={!reasonCode || isSubmitting}
                    >
                      {isSubmitting ? "Đang xử lý..." : "Xác nhận hủy đơn"}
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
