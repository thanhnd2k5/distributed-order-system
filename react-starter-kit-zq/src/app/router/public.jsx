import { Navigate } from "react-router";
import { DynamicLayout } from "app/layouts/DynamicLayout";

const publicRoutes = {
  id: "public",
  children: [
    {
      Component: DynamicLayout,
      children: [
        {
          index: true,
          element: <Navigate to="/dashboard" replace />,
        },
        {
          path: "dashboard",
          lazy: async () => ({
            Component: (await import("app/pages/central/dashboard")).default,
          }),
        },
        {
          path: "pos",
          element: <Navigate to="/pos/POS01" replace />,
        },
        {
          path: "pos/:siteId",
          lazy: async () => ({
            Component: (await import("app/pages/orders/pos")).default,
          }),
        },
        {
          path: "online",
          lazy: async () => ({
            Component: (await import("app/pages/orders/online")).default,
          }),
        },
        {
          path: "inventory",
          lazy: async () => ({
            Component: (await import("app/pages/central/inventory")).default,
          }),
        },
        {
          path: "kitchen",
          lazy: async () => ({
            Component: (await import("app/pages/central/kitchen")).default,
          }),
        },
      ],
    },
    {
      path: "*",
      lazy: async () => ({
        Component: (await import("app/pages/errors/404")).default,
      }),
    },
  ],
};

export { publicRoutes };
