import { ArchiveBoxIcon } from "@heroicons/react/24/outline";
import { Badge, Button, Card } from "components/ui";
import { formatQuantity } from "utils/orderFormatting";

export function StockTableCard({
  stock = [],
  isLoading,
  isError,
  onOpenClearCheck,
}) {
  return (
    <Card className="p-4">
      <div className="mb-3 flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <ArchiveBoxIcon className="size-5 text-primary-500" />
          <h3 className="text-sm font-semibold text-gray-800 dark:text-dark-100">
            Tồn kho toàn chuỗi
          </h3>
          <Badge color="neutral" variant="soft">
            {stock.length}
          </Badge>
        </div>
        <span className="text-[11px] text-gray-400">Tự làm mới mỗi 2s</span>
      </div>

      {isLoading ? (
        <p className="text-xs text-gray-400">Đang tải dữ liệu tồn kho...</p>
      ) : isError ? (
        <p className="text-xs text-error">
          Không thể tải dữ liệu từ Inventory Service.
        </p>
      ) : stock.length === 0 ? (
        <p className="py-4 text-center text-xs text-gray-500">
          Chưa có dữ liệu nguyên liệu.
        </p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-left text-xs">
            <thead>
              <tr className="border-b border-gray-200 text-gray-500 dark:border-dark-600 dark:text-dark-300">
                <th className="py-2 pr-3 font-semibold">Nguyên liệu</th>
                <th className="py-2 pr-3 text-right font-semibold">Khả dụng</th>
                <th className="py-2 pr-3 text-right font-semibold">Đang giữ</th>
                <th className="py-2 pr-3 text-right font-semibold">Đã dùng</th>
                <th className="py-2 pr-3 text-right font-semibold">Hao hụt</th>
                <th className="py-2 pr-3 font-semibold">Kiểm kho</th>
                <th className="py-2 text-right font-semibold">Thao tác</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100 dark:divide-dark-700">
              {stock.map((item) => (
                <tr
                  key={item.ingredientId}
                  data-testid={`stock-row-${item.ingredientId}`}
                  className={
                    item.needsCheck
                      ? "bg-error/5 dark:bg-error/10"
                      : "hover:bg-gray-50/70 dark:hover:bg-dark-800/50"
                  }
                >
                  <td className="py-2.5 pr-3">
                    <p className="font-semibold text-gray-900 dark:text-dark-50">
                      {item.name}
                    </p>
                    <p className="font-mono text-[11px] text-gray-400">
                      {item.ingredientId} ({item.unit})
                    </p>
                  </td>
                  <td className="py-2.5 pr-3 text-right font-mono font-semibold text-gray-900 dark:text-dark-50">
                    {formatQuantity(item.available)}
                  </td>
                  <td className="py-2.5 pr-3 text-right font-mono text-warning-darker dark:text-warning-lighter">
                    {formatQuantity(item.reserved)}
                  </td>
                  <td className="py-2.5 pr-3 text-right font-mono text-gray-700 dark:text-dark-200">
                    {formatQuantity(item.consumed)}
                  </td>
                  <td className="py-2.5 pr-3 text-right font-mono text-error">
                    {formatQuantity(item.wasted)}
                  </td>
                  <td className="py-2.5 pr-3">
                    {item.needsCheck ? (
                      <Badge color="error" variant="filled">
                        Cần kiểm kho
                      </Badge>
                    ) : (
                      <Badge color="success" variant="soft">
                        Bình thường
                      </Badge>
                    )}
                  </td>
                  <td className="py-2.5 text-right">
                    {item.needsCheck && (
                      <Button
                        size="sm"
                        color="primary"
                        variant="soft"
                        onClick={() => onOpenClearCheck(item)}
                        className="px-2.5 py-1 text-xs font-medium"
                      >
                        Xác nhận kiểm kho
                      </Button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}
