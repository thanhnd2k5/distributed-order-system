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
          element: <Navigate to="/ops/branch" replace />,
        },
        {
          path: "ops",
          children: [
            {
              index: true,
              element: <Navigate to="/ops/branch" replace />,
            },
            {
              path: "branch",
              lazy: async () => ({
                Component: (await import("app/pages/operations/branch"))
                  .default,
              }),
            },
            {
              path: "kitchen",
              lazy: async () => ({
                Component: (await import("app/pages/operations/kitchen"))
                  .default,
              }),
            },
            {
              path: "inventory",
              lazy: async () => ({
                Component: (await import("app/pages/operations/inventory"))
                  .default,
              }),
            },
            {
              path: "reporting",
              lazy: async () => ({
                Component: (await import("app/pages/operations/reporting"))
                  .default,
              }),
            },
            {
              path: "demo",
              lazy: async () => ({
                Component: (await import("app/pages/operations/demo")).default,
              }),
            },
          ],
        },
        {
          path: "branch",
          element: <Navigate to="/ops/branch" replace />,
        },
        {
          path: "kitchen",
          element: <Navigate to="/ops/kitchen" replace />,
        },
        {
          path: "inventory",
          element: <Navigate to="/ops/inventory" replace />,
        },
        {
          path: "reporting",
          element: <Navigate to="/ops/reporting" replace />,
        },
        {
          path: "demo",
          element: <Navigate to="/ops/demo" replace />,
        },
      ],
    },
  ],
};

export { publicRoutes };
