import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { completeTicket, listTickets } from "api/kitchen";
import { kitchenKeys } from "api/queryKeys";
import { DEFAULT_POLL_INTERVAL_MS } from "hooks/queries/useOrderQueries";

export function useKitchenTicketsQuery(statusFilter = "ALL", { autoPoll = true } = {}) {
  return useQuery({
    queryKey: kitchenKeys.tickets(statusFilter),
    queryFn: () => listTickets(statusFilter),
    refetchInterval: autoPoll ? DEFAULT_POLL_INTERVAL_MS : false,
    retry: false,
  });
}

export function useCompleteTicketMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (ticketId) => completeTicket(ticketId),
    onSuccess: (updatedTicket) => {
      queryClient.invalidateQueries({ queryKey: kitchenKeys.all });
      toast.success(
        `Đã hoàn tất phiếu bếp và ghi Outbox Done cho đơn ${updatedTicket?.orderId || ""}`,
      );
    },
    onError: (error) => {
      toast.error(error?.message || "Không thể hoàn tất phiếu bếp.");
    },
  });
}
