import { Page } from "components/shared/Page";
import { BranchWorkspace } from "./components/BranchWorkspace";

export default function BranchPage() {
  return (
    <Page title="Chi nhánh (POS / Online)">
      <div className="mx-auto w-full max-w-[1440px] space-y-5 px-4 py-6 sm:px-6 lg:px-8">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-dark-50">
            Màn hình Chi nhánh (POS / Online)
          </h1>
          <p className="mt-1 text-sm text-gray-500 dark:text-dark-300">
            Đặt đơn, theo dõi trạng thái xử lý từ Kho/Bếp, gửi yêu cầu hủy và
            giám sát hàng chờ gửi Luồng D.
          </p>
        </div>

        <BranchWorkspace initialBranch="POS01" allowBranchSwitch />
      </div>
    </Page>
  );
}
