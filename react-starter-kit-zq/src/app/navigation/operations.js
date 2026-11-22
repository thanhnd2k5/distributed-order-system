import {
  ArchiveBoxIcon,
  BuildingStorefrontIcon,
  ChartBarIcon,
  FireIcon,
  Squares2X2Icon,
} from "@heroicons/react/24/outline";
import DashboardsIcon from "assets/dualicons/dashboards.svg?react";
import { NAV_TYPE_ITEM, NAV_TYPE_ROOT } from "constants/app.constant";

const ROOT_OPS = "/ops";

const path = (root, item) => `${root}${item}`;

export const operations = {
  id: "operations",
  type: NAV_TYPE_ROOT,
  path: ROOT_OPS,
  title: "Vận hành đơn hàng",
  transKey: "nav.operations.root",
  Icon: DashboardsIcon,
  childs: [
    {
      id: "operations.branch",
      path: path(ROOT_OPS, "/branch"),
      type: NAV_TYPE_ITEM,
      title: "Chi nhánh (POS / Online)",
      transKey: "nav.operations.branch",
      Icon: BuildingStorefrontIcon,
    },
    {
      id: "operations.kitchen",
      path: path(ROOT_OPS, "/kitchen"),
      type: NAV_TYPE_ITEM,
      title: "Bếp (Kitchen)",
      transKey: "nav.operations.kitchen",
      Icon: FireIcon,
    },
    {
      id: "operations.inventory",
      path: path(ROOT_OPS, "/inventory"),
      type: NAV_TYPE_ITEM,
      title: "Kho nguyên liệu",
      transKey: "nav.operations.inventory",
      Icon: ArchiveBoxIcon,
    },
    {
      id: "operations.reporting",
      path: path(ROOT_OPS, "/reporting"),
      type: NAV_TYPE_ITEM,
      title: "Báo cáo quản lý",
      transKey: "nav.operations.reporting",
      Icon: ChartBarIcon,
    },
    {
      id: "operations.demo",
      path: path(ROOT_OPS, "/demo"),
      type: NAV_TYPE_ITEM,
      title: "Demo song song",
      transKey: "nav.operations.demo",
      Icon: Squares2X2Icon,
    },
  ],
};
