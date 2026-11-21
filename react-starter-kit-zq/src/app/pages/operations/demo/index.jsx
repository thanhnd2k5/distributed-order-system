import { useState } from "react";
import { ChevronDownIcon, ChevronUpIcon } from "@heroicons/react/24/outline";
import { Page } from "components/shared/Page";
import { Badge, Card } from "components/ui";
import { DEMO_SCENARIOS } from "constants/orderLifecycle";
import { BranchWorkspace } from "app/pages/operations/branch/components/BranchWorkspace";
import { KitchenWorkspace } from "app/pages/operations/kitchen/components/KitchenWorkspace";
import { InventoryWorkspace } from "app/pages/operations/inventory/components/InventoryWorkspace";

export default function DemoPage() {
  const [showGuide, setShowGuide] = useState(true);

  return (
    <Page title="Demo trực tiếp">
      <div className="mx-auto w-full max-w-[1800px] space-y-4 px-4 py-5 sm:px-6">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <div className="flex flex-wrap items-center gap-2">
              <h1 className="text-xl font-bold text-gray-900 dark:text-dark-50 sm:text-2xl">
                Màn hình Demo song song (POS01 + Bếp + Kho)
              </h1>
              <Badge color="primary" variant="soft">
                KB-1 · KB-3 · KB-4 · KB-10 · KB-36
              </Badge>
            </div>
            <p className="mt-0.5 text-xs text-gray-500 dark:text-dark-300">
              Quan sát trực tiếp vòng đời đơn hàng, tranh chấp hủy vs nấu, báo
              hỏng nguyên liệu và hủy cục bộ khi mất mạng trên cùng một trang.
            </p>
          </div>

          <button
            type="button"
            onClick={() => setShowGuide((prev) => !prev)}
            className="inline-flex items-center gap-1.5 rounded-xl border border-gray-200 bg-white px-3 py-1.5 text-xs font-medium text-gray-700 hover:bg-gray-50 dark:border-dark-600 dark:bg-dark-800 dark:text-dark-200"
          >
            <span>Hướng dẫn 5 kịch bản demo</span>
            {showGuide ? (
              <ChevronUpIcon className="size-4" />
            ) : (
              <ChevronDownIcon className="size-4" />
            )}
          </button>
        </div>

        {showGuide && (
          <Card className="p-3.5">
            <div className="grid gap-2.5 sm:grid-cols-2 lg:grid-cols-5">
              {DEMO_SCENARIOS.map((sc) => (
                <div
                  key={sc.code}
                  className="rounded-lg border border-gray-200 bg-gray-50/70 p-2.5 text-xs dark:border-dark-600 dark:bg-dark-800/60"
                >
                  <div className="flex items-center gap-1.5">
                    <Badge color="primary" variant="soft" className="text-[11px]">
                      {sc.code}
                    </Badge>
                    <span className="font-semibold text-gray-800 dark:text-dark-100">
                      {sc.title}
                    </span>
                  </div>
                  <p className="mt-1 text-[11px] leading-relaxed text-gray-600 dark:text-dark-300">
                    {sc.summary}
                  </p>
                </div>
              ))}
            </div>
          </Card>
        )}

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
          <section
            aria-label="Khu vực Chi nhánh POS01"
            className="space-y-3 rounded-2xl border border-gray-200 bg-gray-50/40 p-3 dark:border-dark-600 dark:bg-dark-900/40"
          >
            <div className="flex items-center justify-between border-b border-gray-200 pb-2 dark:border-dark-600">
              <h2 className="text-sm font-bold text-gray-900 dark:text-dark-50">
                1. Chi nhánh (POS01)
              </h2>
              <Badge color="info" variant="soft">
                :8081
              </Badge>
            </div>
            <BranchWorkspace initialBranch="POS01" allowBranchSwitch compact />
          </section>

          <section
            aria-label="Khu vực Bếp"
            className="space-y-3 rounded-2xl border border-gray-200 bg-gray-50/40 p-3 dark:border-dark-600 dark:bg-dark-900/40"
          >
            <div className="flex items-center justify-between border-b border-gray-200 pb-2 dark:border-dark-600">
              <h2 className="text-sm font-bold text-gray-900 dark:text-dark-50">
                2. Bếp (Kitchen)
              </h2>
              <Badge color="warning" variant="soft">
                :8085
              </Badge>
            </div>
            <KitchenWorkspace />
          </section>

          <section
            aria-label="Khu vực Kho nguyên liệu"
            className="space-y-3 rounded-2xl border border-gray-200 bg-gray-50/40 p-3 dark:border-dark-600 dark:bg-dark-900/40"
          >
            <div className="flex items-center justify-between border-b border-gray-200 pb-2 dark:border-dark-600">
              <h2 className="text-sm font-bold text-gray-900 dark:text-dark-50">
                3. Kho (Inventory)
              </h2>
              <Badge color="success" variant="soft">
                :8084
              </Badge>
            </div>
            <InventoryWorkspace compact />
          </section>
        </div>
      </div>
    </Page>
  );
}
