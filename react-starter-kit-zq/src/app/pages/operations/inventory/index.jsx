import { Page } from "components/shared/Page";
import { InventoryWorkspace } from "./components/InventoryWorkspace";

export default function InventoryPage() {
  return (
    <Page title="Kho nguyên liệu">
      <div className="mx-auto w-full max-w-[1440px] space-y-5 px-4 py-6 sm:px-6 lg:px-8">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-dark-50">
            Màn hình Kho nguyên liệu (Inventory)
          </h1>
          <p className="mt-1 text-sm text-gray-500 dark:text-dark-300">
            Theo dõi tồn kho khả dụng / đang giữ / đã dùng / hao hụt, xác nhận
            kiểm kho, nhập hàng, tăng epoch và giám sát Luồng D.
          </p>
        </div>

        <InventoryWorkspace />
      </div>
    </Page>
  );
}
