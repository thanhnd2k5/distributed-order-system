import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  bumpInventoryEpoch,
  clearIngredientCheck,
  createStockReceipt,
  fetchInventoryAlerts,
  fetchInventoryMenuSnapshot,
  fetchInventoryMonitor,
  fetchInventoryStock,
} from "api/orderLifecycle";
import { orderLifecycleKeys } from "api/queryKeys";
import { POLLING_INTERVAL_MS } from "constants/orderLifecycle";

const ACTIVE_POLL_MS =
  import.meta.env.MODE === "test" ? false : POLLING_INTERVAL_MS;

export function useInventoryStockQuery(needsCheckOnly = false, options = {}) {
  return useQuery({
    queryKey: orderLifecycleKeys.inventoryStock(needsCheckOnly),
    queryFn: () => fetchInventoryStock(needsCheckOnly),
    refetchInterval: ACTIVE_POLL_MS,
    refetchIntervalInBackground: false,
    ...options,
  });
}

export function useInventoryMenuSnapshotQuery(options = {}) {
  return useQuery({
    queryKey: orderLifecycleKeys.inventoryMenuSnapshot(),
    queryFn: () => fetchInventoryMenuSnapshot(),
    refetchInterval: ACTIVE_POLL_MS,
    refetchIntervalInBackground: false,
    ...options,
  });
}

export function useInventoryAlertsQuery(kind, options = {}) {
  return useQuery({
    queryKey: orderLifecycleKeys.inventoryAlerts(kind),
    queryFn: () => fetchInventoryAlerts(kind),
    refetchInterval: ACTIVE_POLL_MS,
    refetchIntervalInBackground: false,
    ...options,
  });
}

export function useInventoryMonitorQuery(options = {}) {
  return useQuery({
    queryKey: orderLifecycleKeys.inventoryMonitor(),
    queryFn: () => fetchInventoryMonitor(),
    refetchInterval: ACTIVE_POLL_MS,
    refetchIntervalInBackground: false,
    ...options,
  });
}

function invalidateAfterInventoryAction(queryClient) {
  queryClient.invalidateQueries({ queryKey: ["inventory"] });
  queryClient.invalidateQueries({ queryKey: ["kitchen"] });
  queryClient.invalidateQueries({ queryKey: ["branch"] });
}

export function useStockReceiptMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (payload) => createStockReceipt(payload),
    onSettled: () => invalidateAfterInventoryAction(queryClient),
  });
}

export function useClearIngredientCheckMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ ingredientId, countedOnHand }) =>
      clearIngredientCheck(ingredientId, countedOnHand),
    onSettled: () => invalidateAfterInventoryAction(queryClient),
  });
}

export function useBumpInventoryEpochMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: () => bumpInventoryEpoch(),
    onSettled: () => invalidateAfterInventoryAction(queryClient),
  });
}
