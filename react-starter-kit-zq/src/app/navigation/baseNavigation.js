import { NAV_TYPE_ITEM } from "constants/app.constant";
import DashboardsIcon from "assets/dualicons/dashboards.svg?react";

export const baseNavigation = [
  {
    id: "operations",
    type: NAV_TYPE_ITEM,
    path: "/ops",
    title: "Vận hành đơn hàng",
    transKey: "nav.operations.root",
    Icon: DashboardsIcon,
  },
];
