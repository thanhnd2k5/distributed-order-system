import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { CubeIcon, MagnifyingGlassIcon } from "@heroicons/react/24/outline";
import { Page } from "components/shared/Page";
import { PollingStatusBar } from "components/shared/PollingStatusBar";
import { OrderIdText } from "components/shared/OrderIdText";
import { OrderStatusBadge } from "components/shared/OrderStatusBadge";
import { SiteBadge } from "components/shared/SiteBadge";
import {
  Badge,
  Button,
  Card,
  Input,
  Spinner,
  Table,
  TBody,
  Td,
  Th,
  THead,
  Tr,
} from "components/ui";
import { CENTRAL_SERVICES } from "configs/endpoints.config";
import { reportingKeys } from "api/queryKeys";
import { listReplicatedOrders } from "api/reporting";
import { useRecentOrdersStore } from "stores/useRecentOrdersStore";
import { formatDateTime } from "utils/time";
import { isValidUuid, shortUuid } from "utils/uuid";
import {
  useIngredientsQuery,
  useRecipesQuery,
  useReservationQuery,
} from "./hooks/useInventoryQueries";

export default function InventoryPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const urlOrderId = searchParams.get("orderId") || "";

  const [autoPoll, setAutoPoll] = useState(true);
  const [lookupInput, setLookupInput] = useState(urlOrderId);
  const [selectedOrderId, setSelectedOrderId] = useState(
    isValidUuid(urlOrderId) ? urlOrderId.trim() : "",
  );
  const [validationError, setValidationError] = useState("");

  useEffect(() => {
    if (urlOrderId && isValidUuid(urlOrderId)) {
      const trimmed = urlOrderId.trim();
      setLookupInput(trimmed);
      setSelectedOrderId(trimmed);
      setValidationError("");
    }
  }, [urlOrderId]);

  const ingredientsQuery = useIngredientsQuery({ autoPoll });
  const recipesQuery = useRecipesQuery();
  const lowStockPortionByIngredient = useMemo(() => {
    const portions = new Map();
    for (const recipe of recipesQuery.data || []) {
      for (const ingredient of recipe.ingredients || []) {
        const perDish = Number(ingredient.quantityPerDish) || 0;
        const current = portions.get(ingredient.ingredientId) || 0;
        if (perDish > current) {
          portions.set(ingredient.ingredientId, perDish);
        }
      }
    }
    return portions;
  }, [recipesQuery.data]);
  const reservationQuery = useReservationQuery(selectedOrderId, { autoPoll });

  const localRecentOrders = useRecentOrdersStore((state) => state.recentOrders);

  // Best-effort helper list from central query service for quick demo selection
  const centralRecentQuery = useQuery({
    queryKey: reportingKeys.orders({ source: "inventory-quick-pick", limit: 8 }),
    queryFn: () => listReplicatedOrders({ limit: 8 }),
    staleTime: 10_000,
    retry: false,
  });

  const quickPickOrders = useMemo(() => {
    const merged = new Map();
    for (const order of localRecentOrders) {
      if (order?.orderId) {
        merged.set(order.orderId, {
          orderId: order.orderId,
          sourceSiteId: order.sourceSiteId,
        });
      }
    }
    for (const order of centralRecentQuery.data?.orders || []) {
      if (order?.orderId && !merged.has(order.orderId)) {
        merged.set(order.orderId, {
          orderId: order.orderId,
          sourceSiteId: order.sourceSiteId,
        });
      }
    }
    return Array.from(merged.values()).slice(0, 8);
  }, [centralRecentQuery.data, localRecentOrders]);

  const selectOrder = (orderId) => {
    const trimmed = orderId.trim();
    setLookupInput(trimmed);
    setSelectedOrderId(trimmed);
    setValidationError("");
    setSearchParams({ orderId: trimmed }, { replace: true });
  };

  const handleLookupSubmit = (event) => {
    event.preventDefault();
    const trimmed = lookupInput.trim();
    if (!trimmed) {
      setSelectedOrderId("");
      setValidationError("Nhập orderId (UUID) để kiểm tra quyết định tại Kho.");
      return;
    }
    if (!isValidUuid(trimmed)) {
      setValidationError("orderId phải đúng định dạng UUID.");
      return;
    }
    selectOrder(trimmed);
  };

  const handleRefresh = () => {
    ingredientsQuery.refetch();
    recipesQuery.refetch();
    if (selectedOrderId) {
      reservationQuery.refetch();
    }
  };

  const isConnected = !ingredientsQuery.isError;
  const reservation = reservationQuery.data;

  return (
    <Page title="Kho trung tâm">
      <div className="transition-content mx-auto flex w-full max-w-[1600px] flex-col gap-5 px-4 py-6 sm:px-6 lg:px-8">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="bg-amber-500/10 text-amber-600 dark:text-amber-400 flex size-11 items-center justify-center rounded-xl">
              <CubeIcon className="size-6" />
            </div>
            <div>
              <h1 className="text-xl font-bold text-gray-900 dark:text-dark-50">
                Kho nguyên liệu trung tâm
              </h1>
              <p className="text-xs text-gray-500 dark:text-dark-300">
                Nơi duy nhất giữ nguyên liệu nguyên tử và phát quyết định `Reserved` / `Failed`
              </p>
            </div>
          </div>
        </div>

        <PollingStatusBar
          serviceLabel={CENTRAL_SERVICES.INVENTORY.label}
          endpointUrl={CENTRAL_SERVICES.INVENTORY.baseURL}
          isConnected={isConnected}
          isFetching={ingredientsQuery.isFetching || reservationQuery.isFetching}
          autoPoll={autoPoll}
          onToggleAutoPoll={setAutoPoll}
          onRefresh={handleRefresh}
          lastUpdatedAt={ingredientsQuery.dataUpdatedAt}
          error={ingredientsQuery.error}
        />

        {/* Ingredient Stock Cards */}
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {(ingredientsQuery.data || []).map((item) => {
            const portion = lowStockPortionByIngredient.get(item.ingredientId) || 0;
            const isOut = item.availableQuantity <= 0;
            const isLow = !isOut && portion > 0 && item.availableQuantity < portion * 2;
            return (
              <Card key={item.ingredientId} className="p-5">
                <div className="flex items-start justify-between gap-2">
                  <div>
                    <span className="font-mono text-xs text-gray-500 dark:text-dark-300">
                      {item.ingredientId}
                    </span>
                    <h2 className="mt-0.5 text-base font-semibold text-gray-900 dark:text-dark-50">
                      {item.name}
                    </h2>
                  </div>
                  <Badge
                    variant="soft"
                    color={isOut ? "error" : isLow ? "warning" : "success"}
                    title={
                      isLow
                        ? `Còn dưới 2 phần. Món dùng nhiều nhất cần ${portion} ${item.unit}/phần.`
                        : undefined
                    }
                  >
                    {isOut ? "Hết tồn" : isLow ? "Sắp hết" : "Khả dụng"}
                  </Badge>
                </div>

                <div className="mt-4 flex items-baseline gap-1.5">
                  <span className="font-mono text-3xl font-bold text-gray-900 dark:text-dark-50">
                    {item.availableQuantity.toLocaleString("vi-VN")}
                  </span>
                  <span className="text-sm text-gray-500 dark:text-dark-300">
                    {item.unit}
                  </span>
                </div>
                <p className="mt-2 text-[11px] text-gray-400 dark:text-dark-300">
                  Cập nhật: {formatDateTime(item.updatedAt)}
                </p>
              </Card>
            );
          })}
        </div>

        <div className="grid grid-cols-1 gap-6 lg:grid-cols-12">
          {/* Recipes */}
          <Card className="overflow-hidden lg:col-span-5">
            <div className="border-b border-gray-200 px-5 py-3.5 dark:border-dark-600">
              <h2 className="text-base font-semibold text-gray-900 dark:text-dark-50">
                Công thức định lượng mẫu
              </h2>
              <p className="text-xs text-gray-500 dark:text-dark-300">
                Inventory quy đổi món ăn sang nguyên liệu để trừ nguyên tử
              </p>
            </div>

            {recipesQuery.isLoading ? (
              <div className="flex items-center gap-2 p-5 text-sm text-gray-500">
                <Spinner className="size-4" color="primary" />
                <span>Đang tải công thức...</span>
              </div>
            ) : recipesQuery.isError ? (
              <div className="p-5 text-sm text-error dark:text-error-light">
                {recipesQuery.error?.message}
              </div>
            ) : (
              <div className="divide-y divide-gray-200 dark:divide-dark-600">
                {(recipesQuery.data || []).map((recipe) => (
                  <div key={recipe.dishId} className="p-4">
                    <div className="mb-2 flex items-center justify-between">
                      <Badge variant="soft" color="primary" className="font-mono">
                        {recipe.dishId}
                      </Badge>
                      <span className="text-xs text-gray-500 dark:text-dark-300">
                        {recipe.ingredients.length} nguyên liệu
                      </span>
                    </div>
                    <div className="space-y-1.5">
                      {recipe.ingredients.map((ing) => (
                        <div
                          key={ing.ingredientId}
                          className="flex items-center justify-between rounded-lg bg-gray-50 px-3 py-1.5 text-xs dark:bg-dark-800/70"
                        >
                          <span className="font-medium text-gray-700 dark:text-dark-100">
                            {ing.ingredientName}{" "}
                            <span className="font-mono text-gray-400">
                              ({ing.ingredientId})
                            </span>
                          </span>
                          <span className="font-mono font-semibold text-gray-900 dark:text-dark-50">
                            {ing.quantityPerDish} {ing.unit}
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Card>

          {/* Reservation Lookup by orderId */}
          <Card className="flex flex-col gap-4 p-5 lg:col-span-7">
            <div>
              <h2 className="text-base font-semibold text-gray-900 dark:text-dark-50">
                Tra cứu quyết định giữ nguyên liệu theo mã đơn
              </h2>
              <p className="text-xs text-gray-500 dark:text-dark-300">
                Gọi <code className="font-mono">GET /api/reservations/&#123;orderId&#125;</code> để đối chứng `RESERVED` hoặc `FAILED`
              </p>
            </div>

            <form
              onSubmit={handleLookupSubmit}
              className="flex flex-col gap-2 sm:flex-row"
            >
              <Input
                value={lookupInput}
                onChange={(event) => {
                  setLookupInput(event.target.value);
                  setValidationError("");
                }}
                placeholder="Nhập orderId UUID..."
                error={validationError}
                classNames={{ root: "flex-1" }}
                prefix={<MagnifyingGlassIcon className="size-4" />}
              />
              <Button type="submit" color="primary" className="h-9.5 shrink-0 gap-1.5">
                <MagnifyingGlassIcon className="size-4" />
                <span>Tra cứu</span>
              </Button>
            </form>

            {quickPickOrders.length > 0 && (
              <div className="flex flex-wrap items-center gap-1.5 text-xs">
                <span className="text-gray-500 dark:text-dark-300">
                  Chọn nhanh:
                </span>
                {quickPickOrders.map((item) => {
                  const isActive = item.orderId === selectedOrderId;
                  return (
                    <button
                      key={item.orderId}
                      type="button"
                      onClick={() => selectOrder(item.orderId)}
                      className={`inline-flex cursor-pointer items-center gap-1.5 rounded-lg border px-2 py-1 font-mono text-[11px] transition-colors ${
                        isActive
                          ? "border-primary-500 bg-primary-500/10 text-primary-700 dark:text-primary-300"
                          : "border-gray-200 hover:bg-gray-100 dark:border-dark-600 dark:hover:bg-dark-700"
                      }`}
                    >
                      <span className="font-sans font-semibold">
                        {item.sourceSiteId}
                      </span>
                      <span>{shortUuid(item.orderId)}</span>
                    </button>
                  );
                })}
              </div>
            )}

            {!selectedOrderId ? (
              <div className="rounded-xl border border-dashed border-gray-200 p-6 text-center text-sm text-gray-500 dark:border-dark-600 dark:text-dark-300">
                Chọn một mã đơn ở trên hoặc bấm link &ldquo;Kho&rdquo; từ màn POS / Online / Dashboard để xem chi tiết quyết định giữ hàng.
              </div>
            ) : reservationQuery.isLoading ? (
              <div className="flex items-center gap-2 py-6 text-sm text-gray-500">
                <Spinner className="size-4" color="primary" />
                <span>Đang tra cứu quyết định cho {selectedOrderId}...</span>
              </div>
            ) : reservationQuery.isError ? (
              <div className="rounded-xl border border-warning/40 bg-warning/5 p-4 text-xs text-gray-800 dark:text-dark-100">
                <p className="font-semibold">
                  {reservationQuery.error?.status === 404
                    ? "Chưa có quyết định tại Kho cho mã đơn này (404)"
                    : "Lỗi khi tra cứu Inventory"}
                </p>
                <p className="mt-1 text-gray-600 dark:text-dark-300">
                  {reservationQuery.error?.status === 404
                    ? "Đơn có thể đang chờ Outbox/Kafka (ví dụ đang thử mất kết nối POS01 hoặc tắt Kafka) hoặc mã đơn không tồn tại."
                    : reservationQuery.error?.message}
                </p>
              </div>
            ) : reservation ? (
              <div className="space-y-4">
                <div className="grid grid-cols-1 gap-3 rounded-xl bg-gray-50 p-3.5 text-xs sm:grid-cols-2 dark:bg-dark-800/70">
                  <div>
                    <span className="text-gray-500 dark:text-dark-300">
                      Mã đơn (orderId)
                    </span>
                    <div className="mt-1">
                      <OrderIdText value={reservation.orderId} full />
                    </div>
                  </div>
                  <div className="flex flex-wrap items-center gap-2 sm:justify-end">
                    <SiteBadge siteId={reservation.sourceSiteId} />
                    <OrderStatusBadge status={reservation.decision} />
                  </div>
                  <div>
                    <span className="text-gray-500 dark:text-dark-300">
                      Mã lý do / Thông điệp
                    </span>
                    <p className="mt-0.5 font-medium text-gray-900 dark:text-dark-100">
                      {reservation.reasonCode || "—"}
                      {reservation.reasonMessage
                        ? ` — ${reservation.reasonMessage}`
                        : ""}
                    </p>
                  </div>
                  <div>
                    <span className="text-gray-500 dark:text-dark-300">
                      Thời gian xử lý
                    </span>
                    <p className="mt-0.5 font-medium text-gray-900 dark:text-dark-100">
                      {formatDateTime(reservation.createdAt)}
                    </p>
                  </div>
                </div>

                {reservation.items.length > 0 ? (
                  <div className="overflow-x-auto">
                    <Table className="w-full text-left">
                      <THead>
                        <Tr>
                          <Th>Mã nguyên liệu</Th>
                          <Th>Tên nguyên liệu</Th>
                          <Th className="text-right">Đã giữ</Th>
                        </Tr>
                      </THead>
                      <TBody>
                        {reservation.items.map((item) => (
                          <Tr key={item.ingredientId}>
                            <Td className="font-mono text-xs">
                              {item.ingredientId}
                            </Td>
                            <Td className="font-medium">
                              {item.ingredientName}
                            </Td>
                            <Td className="text-right font-mono font-semibold">
                              {item.reservedQuantity} {item.unit}
                            </Td>
                          </Tr>
                        ))}
                      </TBody>
                    </Table>
                  </div>
                ) : (
                  <p className="text-xs text-gray-500 dark:text-dark-300">
                    Không có dòng giữ nguyên liệu (quyết định từ chối nên toàn bộ giao dịch không trừ tồn).
                  </p>
                )}
              </div>
            ) : null}
          </Card>
        </div>
      </div>
    </Page>
  );
}
