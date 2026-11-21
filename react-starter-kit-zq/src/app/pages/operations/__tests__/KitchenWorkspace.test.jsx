import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { KitchenWorkspace } from "app/pages/operations/kitchen/components/KitchenWorkspace";
import { renderWithProviders } from "test/renderWithProviders";

const server = setupServer();

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("KitchenWorkspace display & interaction rules", () => {
  it("shows red warning badge on flagged queued ticket and requires spoiled ingredient when SPOILED_INGREDIENT is selected", async () => {
    const user = userEvent.setup();
    let rejectRequestBody = null;

    server.use(
      http.get("*/api/kitchen/tickets", () =>
        HttpResponse.json([
          {
            orderId: "12345678-1111-1111-1111-111111111111",
            status: "QUEUED",
            originBranch: "POS01",
            warning: true,
            flaggedIngredients: ["THIT_GA"],
            items: [
              { lineNo: 1, dishId: "COM_GA", dishName: "Cơm gà", quantity: 2 },
            ],
            reserved: [
              { ingredientId: "THIT_GA", quantity: 300 },
              { ingredientId: "GAO", quantity: 200 },
            ],
            orderCreatedAt: "2026-11-22T01:00:00Z",
          },
        ]),
      ),
      http.get("*/api/kitchen/alerts", () => HttpResponse.json([])),
      http.get("*/api/kitchen/monitor", () =>
        HttpResponse.json({
          instanceId: "kitchen-1",
          kafkaConnected: true,
          slowConsumerThresholdSeconds: 300,
          anySlowConsumerWarning: false,
          pendingOutboxCount: 0,
          topics: [],
        }),
      ),
      http.post(
        "*/api/kitchen/tickets/:orderId/reject",
        async ({ request }) => {
          rejectRequestBody = await request.json();
          return HttpResponse.json({
            orderId: "12345678-1111-1111-1111-111111111111",
            status: "CANCELLED",
          });
        },
      ),
    );

    renderWithProviders(<KitchenWorkspace />);

    const warningBadge = await screen.findByTestId(
      "ticket-warning-12345678-1111-1111-1111-111111111111",
    );
    expect(warningBadge).toHaveTextContent(
      /Cảnh báo: có nguyên liệu đang chờ kiểm kho/i,
    );

    const ticketCard = screen.getByTestId(
      "kitchen-ticket-12345678-1111-1111-1111-111111111111",
    );
    await user.click(within(ticketCard).getByRole("button", { name: "Từ chối" }));

    const confirmBtn = await screen.findByRole("button", {
      name: "Xác nhận từ chối",
    });
    expect(confirmBtn).toBeDisabled();

    await user.selectOptions(
      screen.getByLabelText("Lý do từ chối (bắt buộc)"),
      "SPOILED_INGREDIENT",
    );

    // Must still be disabled until an ingredient from ticket.reserved is checked
    expect(confirmBtn).toBeDisabled();
    expect(
      screen.getByText(/Bắt buộc chọn ít nhất 1 nguyên liệu bị hỏng/i),
    ).toBeInTheDocument();

    const thitGaCheckbox = screen.getByRole("checkbox", {
      name: /THIT_GA/i,
    });
    await user.click(thitGaCheckbox);
    expect(confirmBtn).toBeEnabled();

    await user.click(confirmBtn);
    await waitFor(() => {
      expect(rejectRequestBody).toEqual({
        reasonCode: "SPOILED_INGREDIENT",
        spoiledIngredients: ["THIT_GA"],
      });
    });
  });

  it("reloads tickets and displays conflict banner on 409 when starting ticket", async () => {
    const user = userEvent.setup();
    let ticketsState = [
      {
        orderId: "99999999-1111-1111-1111-111111111111",
        status: "QUEUED",
        originBranch: "POS01",
        warning: false,
        flaggedIngredients: [],
        items: [{ lineNo: 1, dishId: "COM_GA", dishName: "Cơm gà", quantity: 1 }],
        reserved: [{ ingredientId: "THIT_GA", quantity: 150 }],
        orderCreatedAt: "2026-11-22T01:00:00Z",
      },
    ];

    server.use(
      http.get("*/api/kitchen/tickets", () => HttpResponse.json(ticketsState)),
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
      http.post("*/api/kitchen/tickets/:orderId/start", () => {
        ticketsState = [];
        return HttpResponse.json(
          {
            status: 409,
            detail: "Ticket was cancelled by branch",
            currentStatus: "CANCELLED",
          },
          { status: 409 },
        );
      }),
    );

    renderWithProviders(<KitchenWorkspace />);

    const ticketCard = await screen.findByTestId(
      "kitchen-ticket-99999999-1111-1111-1111-111111111111",
    );
    await user.click(
      within(ticketCard).getByRole("button", { name: "Bắt đầu nấu" }),
    );

    expect(
      await screen.findByText(/Xung đột trạng thái \(409\)/i),
    ).toBeInTheDocument();

    await waitFor(() => {
      expect(
        screen.queryByTestId(
          "kitchen-ticket-99999999-1111-1111-1111-111111111111",
        ),
      ).not.toBeInTheDocument();
    });
  });
});
