import {
  CubeIcon,
  FireIcon,
  HomeIcon,
} from "@heroicons/react/24/outline";
import DashboardsIcon from "assets/dualicons/dashboards.svg?react";
import { NAV_TYPE_ITEM, NAV_TYPE_ROOT } from "constants/app.constant";

export const central = {
  id: "central",
  type: NAV_TYPE_ROOT,
  path: "/central",
  title: "Trung tâm vận hành",
  transKey: "nav.central.root",
  Icon: DashboardsIcon,
  childs: [
    {
      id: "central.dashboard",
      path: "/dashboard",
      type: NAV_TYPE_ITEM,
      title: "Dashboard Tổng hợp",
      transKey: "nav.central.dashboard",
      Icon: HomeIcon,
    },
    {
      id: "central.kitchen",
      path: "/kitchen",
      type: NAV_TYPE_ITEM,
      title: "Bếp trung tâm",
      transKey: "nav.central.kitchen",
      Icon: FireIcon,
    },
    {
      id: "central.inventory",
      path: "/inventory",
      type: NAV_TYPE_ITEM,
      title: "Kho nguyên liệu",
      transKey: "nav.central.inventory",
      Icon: CubeIcon,
    },
  ],
};
