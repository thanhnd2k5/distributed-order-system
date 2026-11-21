import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import DemoPage from "app/pages/operations/demo";
import { renderWithProviders } from "test/renderWithProviders";

const server = setupServer();

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("DemoPage side-by-side view", () => {
  it("renders Branch (POS01), Kitchen, and Inventory workspaces together on one page", async () => {
    const user = userEvent.setup();

    server.use(
      http.get("*/api/pos01/menu", () =>
        HttpResponse.json({
          stale: false,
          kafkaConnected: true,
          epoch: 1,
          lastHeardAt: "2026-11-22T01:00:00Z",
          dishes: [
            { dishId: "COM_GA", name: "Cơm gà", status: "AVAILABLE", epoch: 1, seq: 1 },
          ],
        }),
      ),
      http.get("*/api/pos01/orders", () => HttpResponse.json([])),
      http.get("*/api/pos01/monitor", () =>
        HttpResponse.json({
          branchId: "POS01",
          kafkaConnected: false,
          lastProbeError: "broker unreachable",
          pendingOutboxCount: 1,
          pendingInventoryAlerts: [
            {
              orderId: "77777777-1111-1111-1111-111111111111",
              createdAt: "2026-11-22T00:40:00Z",
              waitingSeconds: 900,
            },
          ],
          expiringUnsentAlerts: [
            {
              orderId: "88888888-1111-1111-1111-111111111111",
              kind: "CREATE_ORDER",
              remainingSeconds: 1800,
            },
          ],
          activeCancelRequests: [],
        }),
      ),
      http.get("*/api/kitchen/tickets", () => HttpResponse.json([])),
      http.get("*/api/kitchen/alerts", () => HttpResponse.json([])),
      http.get("*/api/kitchen/monitor", () =>
        HttpResponse.json({
          instanceId: "kitchen-1",
          kafkaConnected: true,
          anySlowConsumerWarning: false,
          pendingOutboxCount: 0,
          topics: [],
        }),
      ),
      http.get("*/api/inventory/stock", () =>
        HttpResponse.json([
          {
            ingredientId: "GAO",
            name: "Gạo",
            unit: "gram",
            available: 1000,
            reserved: 0,
            consumed: 0,
            wasted: 0,
            needsCheck: false,
          },
        ]),
      ),
      http.get("*/api/inventory/menu/snapshot", () =>
        HttpResponse.json({ epoch: 1, seq: 1, dishes: [] }),
      ),
      http.get("*/api/inventory/alerts", () => HttpResponse.json([])),
      http.get("*/api/inventory/monitor", () =>
        HttpResponse.json({
          instanceId: "inventory-1",
          kafkaReachable: true,
          slowConsumerWarning: false,
          pendingOutboxCount: 0,
          topics: [],
        }),
      ),
    );

    renderWithProviders(<DemoPage />);

    const branchSection = screen.getByRole("region", {
      name: "Khu vực Chi nhánh POS01",
    });
    const kitchenSection = screen.getByRole("region", {
      name: "Khu vực Bếp",
    });
    const inventorySection = screen.getByRole("region", {
      name: "Khu vực Kho nguyên liệu",
    });

    expect(
      await within(branchSection).findByText("Cơm gà"),
    ).toBeInTheDocument();
    expect(
      await within(kitchenSection).findByText(/Phiếu bếp đang xử lý/i),
    ).toBeInTheDocument();
    expect(
      await within(inventorySection).findByTestId("stock-row-GAO"),
    ).toBeInTheDocument();

    // Flow D Branch Monitor inline "Yêu cầu hủy" button opens cancel modal
    const inlineCancelBtn = await within(branchSection).findByRole("button", {
      name: "Yêu cầu hủy",
    });
    await user.click(inlineCancelBtn);

    await waitFor(() => {
      expect(
        screen.getByText(/Yêu cầu hủy đơn #77777777/i),
      ).toBeInTheDocument();
    });
  });
});
