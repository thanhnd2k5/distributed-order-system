import { NAV_TYPE_ITEM } from "constants/app.constant";
import DashboardsIcon from "assets/dualicons/dashboards.svg?react";
import ApplicationsIcon from "assets/dualicons/applications.svg?react";

export const baseNavigation = [
  {
    id: "central",
    type: NAV_TYPE_ITEM,
    path: "/dashboard",
    title: "Trung tâm vận hành",
    transKey: "nav.central.root",
    Icon: DashboardsIcon,
  },
  {
    id: "channels",
    type: NAV_TYPE_ITEM,
    path: "/pos/POS01",
    title: "Kênh nhận đơn",
    transKey: "nav.channels.root",
    Icon: ApplicationsIcon,
  },
];
