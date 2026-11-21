import { Page } from "components/shared/Page";
import { KitchenWorkspace } from "./components/KitchenWorkspace";

export default function KitchenPage() {
  return (
    <Page title="Bếp">
      <div className="mx-auto w-full max-w-[1440px] space-y-5 px-4 py-6 sm:px-6 lg:px-8">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-dark-50">
            Màn hình Bếp (Kitchen)
          </h1>
          <p className="mt-1 text-sm text-gray-500 dark:text-dark-300">
            Nhận phiếu Chờ nấu, bắt đầu nấu, từ chối phiếu (kèm chọn nguyên liệu
            hỏng), hoàn thành món và theo dõi cảnh báo Luồng D.
          </p>
        </div>

        <KitchenWorkspace />
      </div>
    </Page>
  );
}
