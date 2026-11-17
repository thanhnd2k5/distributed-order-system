import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createOrder, getOrder, listMenu, listOrders } from "api/orders";
import { orderKeys } from "api/queryKeys";
import { useRecentOrdersStore } from "stores/useRecentOrdersStore";

export const DEFAULT_POLL_INTERVAL_MS = 2000;

export function useSiteMenuQuery(siteId, options = {}) {
  return useQuery({
    queryKey: orderKeys.menu(siteId),
    queryFn: () => listMenu(siteId),
    enabled: Boolean(siteId),
    staleTime: 60_000,
    retry: false,
    ...options,
  });
}

export function useSiteOrdersQuery(siteId, { autoPoll = true } = {}) {
  return useQuery({
    queryKey: orderKeys.list(siteId),
    queryFn: () => listOrders(siteId),
    enabled: Boolean(siteId),
    refetchInterval: autoPoll ? DEFAULT_POLL_INTERVAL_MS : false,
    retry: false,
  });
}

export function useSiteOrderDetailQuery(
  siteId,
  orderId,
  { autoPoll = true, enabled = true } = {},
) {
  return useQuery({
    queryKey: orderKeys.detail(siteId, orderId),
    queryFn: () => getOrder(siteId, orderId),
    enabled: Boolean(siteId && orderId && enabled),
    refetchInterval: autoPoll ? DEFAULT_POLL_INTERVAL_MS : false,
    retry: false,
  });
}

export function useCreateSiteOrderMutation() {
  const queryClient = useQueryClient();
  const recordOrder = useRecentOrdersStore((state) => state.recordOrder);

  return useMutation({
    mutationFn: ({ siteId, payload }) => createOrder(siteId, payload),
    onSuccess: (result, variables) => {
      if (result?.data) {
        recordOrder(result.data);
      }
      if (variables?.siteId) {
        queryClient.invalidateQueries({
          queryKey: orderKeys.list(variables.siteId),
        });
      }
    },
  });
}
