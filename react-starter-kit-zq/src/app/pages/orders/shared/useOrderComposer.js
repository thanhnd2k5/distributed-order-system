import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { toast } from "sonner";
import { useCreateSiteOrderMutation, useSiteMenuQuery } from "hooks/queries/useOrderQueries";
import { createUuid } from "utils/uuid";

function buildCartItems(quantities, menuItems) {
  const menuById = new Map((menuItems || []).map((item) => [item.dishId, item]));
  return Object.entries(quantities)
    .map(([dishId, qty]) => ({
      dishId,
      name: menuById.get(dishId)?.name || dishId,
      quantity: Number(qty),
    }))
    .filter((item) => Number.isInteger(item.quantity) && item.quantity > 0);
}

function canonicalItems(items) {
  return items
    .map(({ dishId, quantity }) => ({
      dishId,
      quantity: Number(quantity),
    }))
    .sort((left, right) => String(left.dishId).localeCompare(String(right.dishId)));
}

function sameOrderItems(left, right) {
  return (
    JSON.stringify(canonicalItems(left)) === JSON.stringify(canonicalItems(right))
  );
}

function buildConflictItems(baseItems, selectedItems) {
  const cartPayload = selectedItems.map(({ dishId, quantity }) => ({
    dishId,
    quantity,
  }));
  if (cartPayload.length > 0 && !sameOrderItems(cartPayload, baseItems)) {
    return cartPayload;
  }

  const [first, ...rest] = canonicalItems(baseItems);
  return [{ dishId: first.dishId, quantity: first.quantity + 1 }, ...rest];
}

export function useOrderComposer(siteId, { onOrderCreated } = {}) {
  const menuQuery = useSiteMenuQuery(siteId);
  const { mutateAsync, isPending } = useCreateSiteOrderMutation();

  const [quantities, setQuantities] = useState({});
  const [clientRequestId, setClientRequestId] = useState(() => createUuid());
  const [lastSubmission, setLastSubmission] = useState(null);
  const [submissionFeedback, setSubmissionFeedback] = useState(null);

  const siteIdRef = useRef(siteId);
  const mountedRef = useRef(true);
  const onOrderCreatedRef = useRef(onOrderCreated);
  const pendingBySiteRef = useRef({});
  const feedbackRef = useRef(submissionFeedback);

  siteIdRef.current = siteId;
  onOrderCreatedRef.current = onOrderCreated;
  feedbackRef.current = submissionFeedback;

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  useEffect(() => {
    setQuantities({});
    setLastSubmission(null);

    const pending = siteId ? pendingBySiteRef.current[siteId] : null;
    if (pending) {
      setClientRequestId(pending.requestId);
      setSubmissionFeedback(
        pending.feedback || {
          kind: "error",
          isNetworkError: true,
          lockRequestId: true,
          clientRequestId: pending.requestId,
          message:
            "Request trước chưa có phản hồi từ service. Giữ nguyên clientRequestId và bấm Gửi đơn để thử lại.",
        },
      );
      return;
    }

    setClientRequestId(createUuid());
    setSubmissionFeedback(null);
  }, [siteId]);

  const selectedItems = useMemo(
    () => buildCartItems(quantities, menuQuery.data),
    [quantities, menuQuery.data],
  );

  const setDishQuantity = useCallback((dishId, nextQty) => {
    const parsed = Math.max(0, Math.min(999, Math.floor(Number(nextQty) || 0)));
    setQuantities((prev) => {
      if (parsed <= 0) {
        const copy = { ...prev };
        delete copy[dishId];
        return copy;
      }
      return { ...prev, [dishId]: parsed };
    });
  }, []);

  const incrementDish = useCallback((dishId) => {
    setQuantities((prev) => ({
      ...prev,
      [dishId]: Math.min(999, (prev[dishId] || 0) + 1),
    }));
  }, []);

  const decrementDish = useCallback((dishId) => {
    setQuantities((prev) => {
      const next = (prev[dishId] || 0) - 1;
      if (next <= 0) {
        const copy = { ...prev };
        delete copy[dishId];
        return copy;
      }
      return { ...prev, [dishId]: next };
    });
  }, []);

  const regenerateRequestId = useCallback(() => {
    if (feedbackRef.current?.lockRequestId) return;
    setClientRequestId(createUuid());
    setSubmissionFeedback(null);
  }, []);

  const executeOrderRequest = useCallback(
    async ({ requestId, items, mode }) => {
      const requestSiteId = siteIdRef.current;
      if (!requestSiteId) return null;

      const stillOnRequestSite = () =>
        mountedRef.current && siteIdRef.current === requestSiteId;

      if (stillOnRequestSite()) {
        setSubmissionFeedback(null);
      }

      const payload = {
        clientRequestId: requestId,
        items: canonicalItems(items),
      };

      if (mode === "normal") {
        pendingBySiteRef.current[requestSiteId] = { requestId };
      }

      try {
        const result = await mutateAsync({ siteId: requestSiteId, payload });
        const order = result.data;
        const isIdempotentReplay = result.httpStatus === 200;

        if (mode === "normal") {
          delete pendingBySiteRef.current[requestSiteId];
        }

        toast.success(
          isIdempotentReplay
            ? `Nhận lại đơn cũ tại ${requestSiteId} (${result.httpStatus})`
            : `Tạo đơn thành công tại ${requestSiteId}`,
        );

        if (!stillOnRequestSite()) {
          return order;
        }

        setLastSubmission({
          clientRequestId: requestId,
          items: payload.items,
          order,
        });

        setSubmissionFeedback({
          kind: "success",
          httpStatus: result.httpStatus,
          order,
          clientRequestId: requestId,
          message: isIdempotentReplay
            ? `HTTP 200 (Idempotent): Trả về đúng đơn đã lưu (${order.orderId}), không tạo đơn trùng.`
            : `HTTP 201 (Created): Đã ghi đơn ${order.orderId} tại ${order.sourceSiteId} ở trạng thái ${order.status}.`,
        });

        if (mode === "normal") {
          setQuantities({});
          setClientRequestId(createUuid());
        }

        onOrderCreatedRef.current?.(order);
        return order;
      } catch (error) {
        const isNetworkError = Boolean(error.isNetworkError);
        const lockRequestId = mode === "normal" && isNetworkError;
        const feedback = {
          kind: "error",
          httpStatus: error.status,
          isNetworkError,
          lockRequestId,
          clientRequestId: requestId,
          message: error.message,
        };

        if (mode === "normal" && isNetworkError) {
          pendingBySiteRef.current[requestSiteId] = { requestId, feedback };
        } else if (mode === "normal") {
          delete pendingBySiteRef.current[requestSiteId];
        }

        if (!stillOnRequestSite()) {
          return null;
        }

        setSubmissionFeedback(feedback);
        toast.error(error.message);
        return null;
      }
    },
    [mutateAsync],
  );

  const submitOrder = useCallback(async () => {
    if (selectedItems.length === 0) {
      toast.warning("Vui lòng chọn ít nhất một món trước khi gửi đơn.");
      return null;
    }
    return executeOrderRequest({
      requestId: clientRequestId,
      items: selectedItems,
      mode: "normal",
    });
  }, [clientRequestId, executeOrderRequest, selectedItems]);

  const retryLastOrderSamePayload = useCallback(async () => {
    if (!lastSubmission) return null;
    return executeOrderRequest({
      requestId: lastSubmission.clientRequestId,
      items: lastSubmission.items,
      mode: "test-replay",
    });
  }, [executeOrderRequest, lastSubmission]);

  const testConflictWithLastRequestId = useCallback(async () => {
    if (!lastSubmission?.items?.length) return null;
    return executeOrderRequest({
      requestId: lastSubmission.clientRequestId,
      items: buildConflictItems(lastSubmission.items, selectedItems),
      mode: "test-conflict",
    });
  }, [executeOrderRequest, lastSubmission, selectedItems]);

  return {
    menuQuery,
    isSubmitting: isPending,
    quantities,
    selectedItems,
    clientRequestId,
    lastSubmission,
    submissionFeedback,
    requestIdLocked: Boolean(submissionFeedback?.lockRequestId),
    setDishQuantity,
    incrementDish,
    decrementDish,
    regenerateRequestId,
    submitOrder,
    retryLastOrderSamePayload,
    testConflictWithLastRequestId,
  };
}
