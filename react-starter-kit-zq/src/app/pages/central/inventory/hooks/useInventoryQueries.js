import { useQuery } from "@tanstack/react-query";
import { getReservation, listIngredients, listRecipes } from "api/inventory";
import { inventoryKeys } from "api/queryKeys";
import { DEFAULT_POLL_INTERVAL_MS } from "hooks/queries/useOrderQueries";

export function useIngredientsQuery({ autoPoll = true } = {}) {
  return useQuery({
    queryKey: inventoryKeys.ingredients,
    queryFn: listIngredients,
    refetchInterval: autoPoll ? DEFAULT_POLL_INTERVAL_MS : false,
    retry: false,
  });
}

export function useRecipesQuery() {
  return useQuery({
    queryKey: inventoryKeys.recipes,
    queryFn: listRecipes,
    staleTime: 60_000,
    retry: false,
  });
}

export function useReservationQuery(orderId, { autoPoll = true } = {}) {
  return useQuery({
    queryKey: inventoryKeys.reservation(orderId),
    queryFn: () => getReservation(orderId),
    enabled: Boolean(orderId),
    refetchInterval: autoPoll ? DEFAULT_POLL_INTERVAL_MS : false,
    retry: false,
  });
}
