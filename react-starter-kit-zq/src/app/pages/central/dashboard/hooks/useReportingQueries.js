import { useQuery } from "@tanstack/react-query";
import { reportingKeys } from "api/queryKeys";
import {
  getReportingSummary,
  getReplicatedOrder,
  listReplicatedOrders,
} from "api/reporting";
import { DEFAULT_POLL_INTERVAL_MS } from "hooks/queries/useOrderQueries";

export function useReportingSummaryQuery({ autoPoll = true } = {}) {
  return useQuery({
    queryKey: reportingKeys.summary,
    queryFn: getReportingSummary,
    refetchInterval: autoPoll ? DEFAULT_POLL_INTERVAL_MS : false,
    retry: false,
  });
}

export function useReportingOrdersQuery(filters, { autoPoll = true } = {}) {
  return useQuery({
    queryKey: reportingKeys.orders(filters),
    queryFn: () => listReplicatedOrders(filters),
    refetchInterval: autoPoll ? DEFAULT_POLL_INTERVAL_MS : false,
    retry: false,
  });
}

export function useReportingOrderDetailQuery(orderId, { autoPoll = true } = {}) {
  return useQuery({
    queryKey: reportingKeys.detail(orderId),
    queryFn: () => getReplicatedOrder(orderId),
    enabled: Boolean(orderId),
    refetchInterval: autoPoll ? DEFAULT_POLL_INTERVAL_MS : false,
    retry: false,
  });
}
