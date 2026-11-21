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
import { formatQuantity } from "utils/orderFormatting";

export function ClearCheckModal({
  isOpen,
  ingredient,
  onClose,
  onConfirm,
  isSubmitting,
  errorMessage,
}) {
  const [countedOnHand, setCountedOnHand] = useState("");
  const [localError, setLocalError] = useState(null);

  useEffect(() => {
    if (isOpen) {
      setCountedOnHand("");
      setLocalError(null);
    }
  }, [isOpen, ingredient?.ingredientId]);

  const handleSubmit = (event) => {
    event.preventDefault();
    setLocalError(null);
    if (!ingredient) return;

    const trimmed = String(countedOnHand).trim();
    if (trimmed !== "") {
      const parsed = Number(trimmed);
      if (!Number.isFinite(parsed) || parsed < 0) {
        setLocalError("Số lượng kiểm thực tế không được âm.");
        return;
      }
      onConfirm(ingredient.ingredientId, parsed);
    } else {
      onConfirm(ingredient.ingredientId, undefined);
    }
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
                    Xác nhận kiểm kho: {ingredient?.name || ingredient?.ingredientId}
                  </DialogTitle>
                  <button
                    type="button"
                    onClick={onClose}
                    aria-label="Đóng hộp thoại kiểm kho"
                    className="rounded-lg p-1.5 text-gray-400 hover:bg-gray-100 hover:text-gray-600 dark:hover:bg-dark-600"
                  >
                    <FiX className="size-4" />
                  </button>
                </div>

                <form onSubmit={handleSubmit} noValidate className="space-y-4 px-5 py-4">
                  <div className="grid grid-cols-2 gap-2 rounded-xl bg-gray-50 p-3 text-xs dark:bg-dark-800">
                    <div>
                      <p className="text-gray-500">Khả dụng hiện tại</p>
                      <p className="font-mono font-semibold text-gray-900 dark:text-dark-50">
                        {formatQuantity(ingredient?.available)} {ingredient?.unit}
                      </p>
                    </div>
                    <div>
                      <p className="text-gray-500">Đang giữ (Reserved)</p>
                      <p className="font-mono font-semibold text-warning-darker dark:text-warning-lighter">
                        {formatQuantity(ingredient?.reserved)} {ingredient?.unit}
                      </p>
                    </div>
                  </div>

                  <div>
                    <label
                      htmlFor="counted-on-hand-input"
                      className="block text-xs font-semibold text-gray-700 dark:text-dark-100"
                    >
                      Tồn thực tế tại kho (tùy chọn, {ingredient?.unit || "đơn vị"})
                    </label>
                    <input
                      id="counted-on-hand-input"
                      type="number"
                      step="any"
                      min="0"
                      placeholder="Bỏ trống để giữ nguyên tồn khả dụng"
                      value={countedOnHand}
                      onChange={(e) => {
                        setCountedOnHand(e.target.value);
                        setLocalError(null);
                      }}
                      className="mt-1.5 w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm text-gray-900 focus:border-primary-600 focus:outline-none dark:border-dark-500 dark:bg-dark-800 dark:text-dark-50"
                    />
                    <p className="mt-1 text-[11px] text-gray-500 dark:text-dark-300">
                      Tồn thực tế = Khả dụng + Đang giữ. Nếu nhập nhỏ hơn lượng
                      đang giữ, Kho sẽ từ chối (409) cho tới khi các phiếu liên
                      quan được từ chối.
                    </p>
                  </div>

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
                      color="primary"
                      disabled={isSubmitting}
                    >
                      {isSubmitting ? "Đang xác nhận..." : "Xác nhận kiểm kho"}
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
