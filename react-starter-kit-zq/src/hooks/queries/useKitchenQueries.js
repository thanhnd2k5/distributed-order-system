import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  completeKitchenTicket,
  fetchKitchenAlerts,
  fetchKitchenMonitor,
  fetchKitchenTickets,
  rejectKitchenTicket,
  startKitchenTicket,
} from "api/orderLifecycle";
import { orderLifecycleKeys } from "api/queryKeys";
import { POLLING_INTERVAL_MS } from "constants/orderLifecycle";

const ACTIVE_POLL_MS =
  import.meta.env.MODE === "test" ? false : POLLING_INTERVAL_MS;

export function useKitchenTicketsQuery(status, options = {}) {
  return useQuery({
    queryKey: orderLifecycleKeys.kitchenTickets(status),
    queryFn: () => fetchKitchenTickets(status),
    refetchInterval: ACTIVE_POLL_MS,
    refetchIntervalInBackground: false,
    ...options,
  });
}

export function useKitchenAlertsQuery(kind, options = {}) {
  return useQuery({
    queryKey: orderLifecycleKeys.kitchenAlerts(kind),
    queryFn: () => fetchKitchenAlerts(kind),
    refetchInterval: ACTIVE_POLL_MS,
    refetchIntervalInBackground: false,
    ...options,
  });
}

export function useKitchenMonitorQuery(options = {}) {
  return useQuery({
    queryKey: orderLifecycleKeys.kitchenMonitor(),
    queryFn: () => fetchKitchenMonitor(),
    refetchInterval: ACTIVE_POLL_MS,
    refetchIntervalInBackground: false,
    ...options,
  });
}

function invalidateAfterKitchenAction(queryClient) {
  queryClient.invalidateQueries({ queryKey: ["kitchen"] });
  queryClient.invalidateQueries({ queryKey: ["branch"] });
  queryClient.invalidateQueries({ queryKey: ["inventory"] });
  queryClient.invalidateQueries({
    queryKey: orderLifecycleKeys.reportingSummary(),
  });
}

export function useStartKitchenTicketMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (orderId) => startKitchenTicket(orderId),
    onSettled: () => invalidateAfterKitchenAction(queryClient),
  });
}

export function useRejectKitchenTicketMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ orderId, reasonCode, spoiledIngredients }) =>
      rejectKitchenTicket(orderId, { reasonCode, spoiledIngredients }),
    onSettled: () => invalidateAfterKitchenAction(queryClient),
  });
}

export function useCompleteKitchenTicketMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (orderId) => completeKitchenTicket(orderId),
    onSettled: () => invalidateAfterKitchenAction(queryClient),
  });
}
