import { useEffect, useState } from "react";
import { PlusCircleIcon } from "@heroicons/react/24/outline";
import { Button, Card } from "components/ui";

export function StockReceiptForm({ stock = [], onSubmitReceipt, isSubmitting }) {
  const [ingredientId, setIngredientId] = useState("");
  const [quantity, setQuantity] = useState("");
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!ingredientId && stock.length > 0) {
      setIngredientId(stock[0].ingredientId);
    }
  }, [stock, ingredientId]);

  const handleSubmit = async (event) => {
    event.preventDefault();
    setError(null);

    if (!ingredientId) {
      setError("Vui lòng chọn nguyên liệu.");
      return;
    }
    const qty = Number(quantity);
    if (!Number.isFinite(qty) || qty <= 0) {
      setError("Số lượng nhập kho phải lớn hơn 0.");
      return;
    }

    const ok = await onSubmitReceipt({ ingredientId, quantity: qty });
    if (ok) {
      setQuantity("");
    }
  };

  return (
    <Card className="p-4">
      <div className="mb-3 flex items-center gap-2">
        <PlusCircleIcon className="size-5 text-success" />
        <h3 className="text-sm font-semibold text-gray-800 dark:text-dark-100">
          Nhập kho nguyên liệu
        </h3>
      </div>

      <form
        onSubmit={handleSubmit}
        noValidate
        className="flex flex-wrap items-end gap-3"
      >
        <div className="min-w-[180px] flex-1">
          <label
            htmlFor="receipt-ingredient-select"
            className="block text-xs font-medium text-gray-600 dark:text-dark-200"
          >
            Nguyên liệu
          </label>
          <select
            id="receipt-ingredient-select"
            value={ingredientId}
            onChange={(e) => {
              setIngredientId(e.target.value);
              setError(null);
            }}
            className="mt-1 w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-xs text-gray-900 focus:border-primary-600 focus:outline-none dark:border-dark-500 dark:bg-dark-800 dark:text-dark-50"
          >
            {stock.length === 0 ? (
              <option value="">-- Chưa có nguyên liệu --</option>
            ) : (
              stock.map((item) => (
                <option key={item.ingredientId} value={item.ingredientId}>
                  {item.name} ({item.ingredientId} · {item.unit})
                </option>
              ))
            )}
          </select>
        </div>

        <div className="w-36">
          <label
            htmlFor="receipt-quantity-input"
            className="block text-xs font-medium text-gray-600 dark:text-dark-200"
          >
            Số lượng nhập
          </label>
          <input
            id="receipt-quantity-input"
            type="number"
            min="0.001"
            step="any"
            placeholder="VD: 500"
            value={quantity}
            onChange={(e) => {
              setQuantity(e.target.value);
              setError(null);
            }}
            className="mt-1 w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-xs text-gray-900 focus:border-primary-600 focus:outline-none dark:border-dark-500 dark:bg-dark-800 dark:text-dark-50"
          />
        </div>

        <Button
          type="submit"
          color="success"
          disabled={isSubmitting || stock.length === 0}
          className="px-4 py-2 text-xs font-semibold"
        >
          {isSubmitting ? "Đang nhập..." : "Nhập kho"}
        </Button>
      </form>

      {error && (
        <p role="alert" className="mt-2 text-xs font-medium text-error">
          {error}
        </p>
      )}
    </Card>
  );
}
