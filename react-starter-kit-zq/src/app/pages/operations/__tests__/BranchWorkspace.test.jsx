import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { BranchWorkspace } from "app/pages/operations/branch/components/BranchWorkspace";
import { renderWithProviders } from "test/renderWithProviders";

const server = setupServer();

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

function setupBranchHandlers({
  menuResponse,
  ordersResponse = [],
  monitorResponse,
  onPostOrder,
  onPostCancel,
} = {}) {
  server.use(
    http.get("*/api/pos01/menu", () =>
      HttpResponse.json(
        menuResponse ?? {
          stale: false,
          kafkaConnected: true,
          epoch: 1,
          lastHeardAt: "2026-11-22T01:00:00Z",
          dishes: [
            { dishId: "COM_GA", name: "Cơm gà", status: "AVAILABLE", epoch: 1, seq: 1 },
            { dishId: "CHAO_GA", name: "Cháo gà", status: "SOLD_OUT", epoch: 1, seq: 2 },
            { dishId: "PHO_BO", name: "Phở bò", status: "HIDDEN", epoch: 1, seq: 3 },
          ],
        },
      ),
    ),
    http.get("*/api/pos01/orders", () =>
      HttpResponse.json(
        typeof ordersResponse === "function" ? ordersResponse() : ordersResponse,
      ),
    ),
    http.get("*/api/pos01/monitor", () =>
      HttpResponse.json(
        monitorResponse ?? {
          branchId: "POS01",
          kafkaConnected: true,
          lastProbeAt: "2026-11-22T01:00:00Z",
          lastProbeError: null,
          pendingOutboxCount: 0,
          pendingInventoryAlerts: [],
          expiringUnsentAlerts: [],
          activeCancelRequests: [],
          alerts: [],
        },
      ),
    ),
    http.post("*/api/pos01/orders", async ({ request }) => {
      const body = await request.json();
      if (onPostOrder) {
        return onPostOrder(body);
      }
      return HttpResponse.json(
        {
          orderId: "11111111-1111-1111-1111-111111111111",
          sourceSiteId: "POS01",
          status: "PENDING_INVENTORY",
          statusRank: 0,
          cancelFlag: "NONE",
          createdAt: "2026-11-22T01:00:01Z",
        },
        { status: 201 },
      );
    }),
    http.post("*/api/pos01/orders/:orderId/cancel", async ({ params, request }) => {
      const body = await request.json();
      if (onPostCancel) {
        return onPostCancel(params.orderId, body);
      }
      return HttpResponse.json({
        orderId: params.orderId,
        sourceSiteId: "POS01",
        status: "PENDING_INVENTORY",
        statusRank: 0,
        cancelFlag: "REQUESTED",
      }, { status: 202 });
    }),
  );
}

describe("BranchWorkspace display & interaction rules", () => {
  it("blocks SOLD_OUT and hides HIDDEN when online and data is fresh", async () => {
    setupBranchHandlers();
    renderWithProviders(<BranchWorkspace initialBranch="POS01" />);

    const availableDish = await screen.findByTestId("menu-dish-COM_GA");
    const soldOutDish = screen.getByTestId("menu-dish-CHAO_GA");

    expect(within(availableDish).getByText("Còn")).toBeInTheDocument();
    expect(
      within(availableDish).getByRole("button", { name: /Chọn món Cơm gà/i }),
    ).toBeEnabled();

    expect(within(soldOutDish).getByText("Hết")).toBeInTheDocument();
    expect(
      within(soldOutDish).getByRole("button", { name: /Chọn món Cháo gà/i }),
    ).toBeDisabled();

    expect(screen.queryByTestId("menu-dish-PHO_BO")).not.toBeInTheDocument();
    expect(
      screen.queryByText("Trạng thái món có thể đã thay đổi"),
    ).not.toBeInTheDocument();
  });

  it("shows stale banner and allows selecting SOLD_OUT with warning when stale or offline", async () => {
    const user = userEvent.setup();
    let submittedPayload = null;

    setupBranchHandlers({
      menuResponse: {
        stale: true,
        kafkaConnected: false,
        epoch: 1,
        lastHeardAt: null,
        dishes: [
          { dishId: "COM_GA", name: "Cơm gà", status: "AVAILABLE", epoch: 1, seq: 1 },
          { dishId: "CHAO_GA", name: "Cháo gà", status: "SOLD_OUT", epoch: 1, seq: 2 },
          { dishId: "PHO_BO", name: "Phở bò", status: "HIDDEN", epoch: 1, seq: 3 },
        ],
      },
      monitorResponse: {
        branchId: "POS01",
        kafkaConnected: false,
        pendingOutboxCount: 2,
        pendingInventoryAlerts: [],
        expiringUnsentAlerts: [],
        activeCancelRequests: [],
      },
      onPostOrder: (body) => {
        submittedPayload = body;
        return HttpResponse.json(
          {
            orderId: "22222222-2222-2222-2222-222222222222",
            sourceSiteId: "POS01",
            status: "PENDING_INVENTORY",
            statusRank: 0,
            cancelFlag: "NONE",
          },
          { status: 201 },
        );
      },
    });

    renderWithProviders(<BranchWorkspace initialBranch="POS01" />);

    expect(
      await screen.findByText("Trạng thái món có thể đã thay đổi"),
    ).toBeInTheDocument();
    expect(screen.queryByTestId("menu-dish-PHO_BO")).not.toBeInTheDocument();

    const soldOutDish = screen.getByTestId("menu-dish-CHAO_GA");
    expect(within(soldOutDish).getByText("Có thể đã hết")).toBeInTheDocument();
    const selectSoldOutBtn = within(soldOutDish).getByRole("button", {
      name: /Chọn món Cháo gà/i,
    });
    expect(selectSoldOutBtn).toBeEnabled();

    await user.click(selectSoldOutBtn);
    await user.click(screen.getByRole("button", { name: "Đặt đơn" }));

    await waitFor(() => {
      expect(submittedPayload).not.toBeNull();
    });
    expect(submittedPayload.items).toEqual([{ dishId: "CHAO_GA", quantity: 1 }]);
  });

  it("validates quantity > 0 before submitting order", async () => {
    const user = userEvent.setup();
    let postCalled = false;

    setupBranchHandlers({
      onPostOrder: () => {
        postCalled = true;
        return HttpResponse.json({}, { status: 201 });
      },
    });

    renderWithProviders(<BranchWorkspace initialBranch="POS01" />);
    const selectComGa = await screen.findByRole("button", {
      name: /Chọn món Cơm gà/i,
    });
    await user.click(selectComGa);

    const qtyInput = screen.getByLabelText("Số lượng Cơm gà");
    await user.clear(qtyInput);
    await user.type(qtyInput, "0");
    await user.click(screen.getByRole("button", { name: "Đặt đơn" }));

    expect(
      await screen.findByText("Số lượng mỗi món phải là số nguyên lớn hơn 0."),
    ).toBeInTheDocument();
    expect(postCalled).toBe(false);
  });

  it("hides Cancel button on terminal statuses and displays auxiliary cancel labels", async () => {
    setupBranchHandlers({
      ordersResponse: [
        {
          orderId: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
          sourceSiteId: "POS01",
          status: "PENDING_INVENTORY",
          statusRank: 0,
          cancelFlag: "KITCHEN_WILL_NOT_COOK",
          cancelRequestedAt: "2026-11-22T01:00:00Z",
          createdAt: "2026-11-22T00:59:00Z",
        },
        {
          orderId: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
          sourceSiteId: "POS01",
          status: "RESERVED",
          statusRank: 1,
          cancelFlag: "NONE",
          cancelMessage: "Cannot cancel: the kitchen has started cooking",
          createdAt: "2026-11-22T00:58:00Z",
        },
        {
          orderId: "cccccccc-cccc-cccc-cccc-cccccccccccc",
          sourceSiteId: "POS01",
          status: "DONE",
          statusRank: 2,
          cancelFlag: "NONE",
          createdAt: "2026-11-22T00:57:00Z",
        },
        {
          orderId: "dddddddd-dddd-dddd-dddd-dddddddddddd",
          sourceSiteId: "POS01",
          status: "CANCELLED",
          statusRank: 2,
          closeReason: "CUSTOMER_CHANGED_MIND",
          cancelFlag: "NONE",
          createdAt: "2026-11-22T00:56:00Z",
        },
        {
          orderId: "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
          sourceSiteId: "POS01",
          status: "FAILED",
          statusRank: 2,
          closeReason: "INSUFFICIENT_INGREDIENTS",
          cancelFlag: "NONE",
          createdAt: "2026-11-22T00:55:00Z",
        },
      ],
    });

    renderWithProviders(<BranchWorkspace initialBranch="POS01" />);

    const pendingRow = await screen.findByTestId(
      "order-row-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    );
    expect(
      within(pendingRow).getByText("Yêu cầu hủy: bếp sẽ không nấu"),
    ).toBeInTheDocument();

    const reservedRow = screen.getByTestId(
      "order-row-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    );
    expect(
      within(reservedRow).getByText("Không thể hủy: bếp đã bắt đầu nấu"),
    ).toBeInTheDocument();
    expect(
      within(reservedRow).getByRole("button", { name: "Hủy đơn" }),
    ).toBeEnabled();

    for (const terminalId of [
      "cccccccc-cccc-cccc-cccc-cccccccccccc",
      "dddddddd-dddd-dddd-dddd-dddddddddddd",
      "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
    ]) {
      const row = screen.getByTestId(`order-row-${terminalId}`);
      expect(within(row).queryByRole("button", { name: /Hủy/i })).toBeNull();
    }
  });

  it("reloads orders and shows message when cancel returns 409 Conflict", async () => {
    const user = userEvent.setup();
    let currentOrders = [
      {
        orderId: "ffffffff-ffff-ffff-ffff-ffffffffffff",
        sourceSiteId: "POS01",
        status: "RESERVED",
        statusRank: 1,
        cancelFlag: "NONE",
        createdAt: "2026-11-22T00:55:00Z",
      },
    ];

    setupBranchHandlers({
      ordersResponse: () => currentOrders,
      onPostCancel: () => {
        currentOrders = [
          {
            orderId: "ffffffff-ffff-ffff-ffff-ffffffffffff",
            sourceSiteId: "POS01",
            status: "DONE",
            statusRank: 2,
            cancelFlag: "NONE",
            createdAt: "2026-11-22T00:55:00Z",
          },
        ];
        return HttpResponse.json(
          {
            status: 409,
            detail: "Order already completed",
            currentStatus: "DONE",
          },
          { status: 409 },
        );
      },
    });

    renderWithProviders(<BranchWorkspace initialBranch="POS01" />);

    const row = await screen.findByTestId(
      "order-row-ffffffff-ffff-ffff-ffff-ffffffffffff",
    );
    await user.click(within(row).getByRole("button", { name: "Hủy đơn" }));

    const confirmBtn = await screen.findByRole("button", {
      name: "Xác nhận hủy đơn",
    });
    expect(confirmBtn).toBeDisabled();

    await user.selectOptions(
      screen.getByLabelText("Lý do hủy đơn (bắt buộc)"),
      "CUSTOMER_CHANGED_MIND",
    );
    expect(confirmBtn).toBeEnabled();
    await user.click(confirmBtn);

    expect(
      await screen.findByText(/đơn đã ở trạng thái cuối \(409\)/i),
    ).toBeInTheDocument();

    await waitFor(() => {
      const updatedRow = screen.getByTestId(
        "order-row-ffffffff-ffff-ffff-ffff-ffffffffffff",
      );
      expect(within(updatedRow).getByText("Hoàn tất")).toBeInTheDocument();
      expect(
        within(updatedRow).queryByRole("button", { name: /Hủy/i }),
      ).toBeNull();
    });
  });
});
