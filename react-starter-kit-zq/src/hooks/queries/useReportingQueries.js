import { useQuery } from "@tanstack/react-query";
import {
  fetchReplicatedOrderDetails,
  fetchReplicatedOrders,
  fetchReportingSummary,
} from "api/orderLifecycle";
import { orderLifecycleKeys } from "api/queryKeys";
import { POLLING_INTERVAL_MS } from "constants/orderLifecycle";

const ACTIVE_POLL_MS =
  import.meta.env.MODE === "test" ? false : POLLING_INTERVAL_MS;

export function useReportingSummaryQuery(options = {}) {
  return useQuery({
    queryKey: orderLifecycleKeys.reportingSummary(),
    queryFn: () => fetchReportingSummary(),
    refetchInterval: ACTIVE_POLL_MS,
    refetchIntervalInBackground: false,
    ...options,
  });
}

export function useReplicatedOrdersQuery(filters = {}, options = {}) {
  return useQuery({
    queryKey: orderLifecycleKeys.reportingOrders(filters),
    queryFn: () => fetchReplicatedOrders(filters),
    refetchInterval: ACTIVE_POLL_MS,
    refetchIntervalInBackground: false,
    ...options,
  });
}

export function useReplicatedOrderDetailsQuery(orderId, options = {}) {
  return useQuery({
    queryKey: ["reporting", "orders", orderId],
    queryFn: () => fetchReplicatedOrderDetails(orderId),
    enabled: Boolean(orderId),
    ...options,
  });
}
