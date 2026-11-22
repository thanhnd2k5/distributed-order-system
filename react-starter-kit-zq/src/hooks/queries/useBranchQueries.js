import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  cancelBranchOrder,
  createBranchOrder,
  fetchBranchMenu,
  fetchBranchMonitor,
  fetchBranchOrderDetail,
  fetchBranchOrders,
} from "api/orderLifecycle";
import { orderLifecycleKeys } from "api/queryKeys";
import { POLLING_INTERVAL_MS } from "constants/orderLifecycle";

const ACTIVE_POLL_MS =
  import.meta.env.MODE === "test" ? false : POLLING_INTERVAL_MS;

export function useBranchMenuQuery(branchId, options = {}) {
  return useQuery({
    queryKey: orderLifecycleKeys.branchMenu(branchId),
    queryFn: () => fetchBranchMenu(branchId),
    refetchInterval: ACTIVE_POLL_MS,
    refetchIntervalInBackground: false,
    ...options,
  });
}

export function useBranchOrdersQuery(branchId, options = {}) {
  return useQuery({
    queryKey: orderLifecycleKeys.branchOrders(branchId),
    queryFn: () => fetchBranchOrders(branchId),
    refetchInterval: ACTIVE_POLL_MS,
    refetchIntervalInBackground: false,
    ...options,
  });
}

export function useBranchOrderDetailQuery(branchId, orderId, options = {}) {
  return useQuery({
    queryKey: orderLifecycleKeys.branchOrderDetail(branchId, orderId),
    queryFn: () => fetchBranchOrderDetail(branchId, orderId),
    enabled: Boolean(orderId),
    ...options,
  });
}

export function useBranchMonitorQuery(branchId, options = {}) {
  return useQuery({
    queryKey: orderLifecycleKeys.branchMonitor(branchId),
    queryFn: () => fetchBranchMonitor(branchId),
    refetchInterval: ACTIVE_POLL_MS,
    refetchIntervalInBackground: false,
    ...options,
  });
}

export function useCreateBranchOrderMutation(branchId) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (payload) => createBranchOrder(branchId, payload),
    onSettled: () => {
      queryClient.invalidateQueries({
        queryKey: orderLifecycleKeys.branchOrders(branchId),
      });
      queryClient.invalidateQueries({
        queryKey: orderLifecycleKeys.branchMonitor(branchId),
      });
      queryClient.invalidateQueries({
        queryKey: orderLifecycleKeys.reportingSummary(),
      });
    },
  });
}

export function useCancelBranchOrderMutation(branchId) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ orderId, reasonCode }) =>
      cancelBranchOrder(branchId, orderId, reasonCode),
    onSettled: () => {
      queryClient.invalidateQueries({
        queryKey: orderLifecycleKeys.branchOrders(branchId),
      });
      queryClient.invalidateQueries({
        queryKey: orderLifecycleKeys.branchMonitor(branchId),
      });
      queryClient.invalidateQueries({
        queryKey: ["kitchen"],
      });
      queryClient.invalidateQueries({
        queryKey: orderLifecycleKeys.reportingSummary(),
      });
    },
  });
}
