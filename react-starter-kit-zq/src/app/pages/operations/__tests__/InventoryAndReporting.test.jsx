import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { InventoryWorkspace } from "app/pages/operations/inventory/components/InventoryWorkspace";
import ReportingPage from "app/pages/operations/reporting";
import { renderWithProviders } from "test/renderWithProviders";

const server = setupServer();

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("InventoryWorkspace & ReportingPage rules", () => {
  it("handles 409 conflict on clear-check when countedOnHand < reserved and reloads stock", async () => {
    const user = userEvent.setup();
    let stockFetchCount = 0;

    server.use(
      http.get("*/api/inventory/stock", () => {
        stockFetchCount += 1;
        return HttpResponse.json([
          {
            ingredientId: "THIT_GA",
            name: "Thịt gà",
            unit: "gram",
            available: 500,
            reserved: 300,
            consumed: 200,
            wasted: 150,
            needsCheck: true,
          },
        ]);
      }),
      http.get("*/api/inventory/menu/snapshot", () =>
        HttpResponse.json({
          epoch: 2,
          seq: 10,
          dishes: [
            { dishId: "COM_GA", dishName: "Cơm gà", status: "SOLD_OUT", epoch: 2, seq: 10 },
          ],
        }),
      ),
      http.get("*/api/inventory/alerts", () => HttpResponse.json([])),
      http.get("*/api/inventory/monitor", () =>
        HttpResponse.json({
          instanceId: "inventory-1",
          kafkaReachable: true,
          slowConsumerThresholdSeconds: 300,
          slowConsumerWarning: false,
          needsCheckCount: 1,
          pendingOutboxCount: 0,
          alertsCount: 0,
          topics: [],
        }),
      ),
      http.post("*/api/inventory/ingredients/THIT_GA/clear-check", () =>
        HttpResponse.json(
          {
            status: 409,
            detail: "Counted on hand (100) is less than reserved (300)",
          },
          { status: 409 },
        ),
      ),
    );

    renderWithProviders(<InventoryWorkspace />);

    const stockRow = await screen.findByTestId("stock-row-THIT_GA");
    expect(within(stockRow).getByText("Cần kiểm kho")).toBeInTheDocument();

    await user.click(
      within(stockRow).getByRole("button", { name: "Xác nhận kiểm kho" }),
    );

    const countedInput = await screen.findByLabelText(/Tồn thực tế tại kho/i);
    await user.type(countedInput, "100");

    const beforeCount = stockFetchCount;
    const dialog = screen.getByRole("dialog");
    await user.click(
      within(dialog).getByRole("button", { name: "Xác nhận kiểm kho" }),
    );

    expect(
      await within(dialog).findByText(/Counted on hand \(100\) is less than reserved \(300\)/i),
    ).toBeInTheDocument();

    await waitFor(() => {
      expect(stockFetchCount).toBeGreaterThan(beforeCount);
    });
  });

  it("renders branch dataAsOf hh:mm:ss and reason breakdowns on ReportingPage", async () => {
    server.use(
      http.get("*/api/reporting/summary", () =>
        HttpResponse.json({
          replicaNote: "Central Order Replica (async)",
          queriedAt: "2026-11-22T01:05:00Z",
          totalOrders: 5,
          bySite: { POS01: 3, POS02: 1, ONLINE: 1 },
          byStatus: {
            PENDING_INVENTORY: 1,
            RESERVED: 1,
            DONE: 1,
            CANCELLED: 1,
            FAILED: 1,
          },
          byCancelledReason: { CUSTOMER_CHANGED_MIND: 1 },
          byFailedReason: { INSUFFICIENT_INGREDIENTS: 1 },
          branches: [
            {
              branchId: "POS01",
              dataAsOf: "2026-11-22T01:04:55Z",
              totalOrders: 3,
              byStatus: {
                PENDING_INVENTORY: 1,
                RESERVED: 0,
                DONE: 1,
                CANCELLED: 1,
                FAILED: 0,
              },
              cancelledByReason: { CUSTOMER_CHANGED_MIND: 1 },
              failedByReason: {},
            },
            {
              branchId: "POS02",
              dataAsOf: null,
              totalOrders: 1,
              byStatus: {
                PENDING_INVENTORY: 0,
                RESERVED: 1,
                DONE: 0,
                CANCELLED: 0,
                FAILED: 0,
              },
              cancelledByReason: {},
              failedByReason: {},
            },
          ],
          subscriptions: [
            {
              subscriptionName: "sub_pos01",
              enabled: true,
              workerPid: 123,
              lastMessageReceiptTime: "2026-11-22T01:04:55Z",
              latestEndTime: "2026-11-22T01:04:56Z",
            },
          ],
        }),
      ),
    );

    renderWithProviders(<ReportingPage />);

    const pos01Card = await screen.findByTestId("branch-report-POS01");
    expect(within(pos01Card).getByText(/Dữ liệu tính đến \d{2}:\d{2}:\d{2}/)).toBeInTheDocument();
    expect(within(pos01Card).getByText(/Khách đổi ý/)).toBeInTheDocument();

    const pos02Card = screen.getByTestId("branch-report-POS02");
    expect(within(pos02Card).getByText("Dữ liệu tính đến —")).toBeInTheDocument();

    expect(screen.getByText(/Thiếu nguyên liệu/)).toBeInTheDocument();
    expect(screen.getByText("sub_pos01")).toBeInTheDocument();
  });
});
