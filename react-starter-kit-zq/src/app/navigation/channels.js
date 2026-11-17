import {
  BuildingStorefrontIcon,
  GlobeAltIcon,
} from "@heroicons/react/24/outline";
import ApplicationsIcon from "assets/dualicons/applications.svg?react";
import { NAV_TYPE_ITEM, NAV_TYPE_ROOT } from "constants/app.constant";

export const channels = {
  id: "channels",
  type: NAV_TYPE_ROOT,
  path: "/channels",
  title: "Kênh nhận đơn",
  transKey: "nav.channels.root",
  Icon: ApplicationsIcon,
  childs: [
    {
      id: "channels.pos01",
      path: "/pos/POS01",
      type: NAV_TYPE_ITEM,
      title: "Quầy POS01",
      transKey: "nav.channels.pos01",
      Icon: BuildingStorefrontIcon,
    },
    {
      id: "channels.pos02",
      path: "/pos/POS02",
      type: NAV_TYPE_ITEM,
      title: "Quầy POS02",
      transKey: "nav.channels.pos02",
      Icon: BuildingStorefrontIcon,
    },
    {
      id: "channels.online",
      path: "/online",
      type: NAV_TYPE_ITEM,
      title: "Đặt hàng Online",
      transKey: "nav.channels.online",
      Icon: GlobeAltIcon,
    },
  ],
};
