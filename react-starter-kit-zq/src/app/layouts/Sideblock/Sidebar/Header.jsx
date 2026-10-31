import { Link } from "react-router";
import { clsx } from "clsx";
import { useTranslation } from "react-i18next";
import {
  ChevronLeftIcon,
  ChevronRightIcon,
  XMarkIcon,
} from "@heroicons/react/24/outline";
import { useHotkeys } from "react-hotkeys-hook";

import Logo from "assets/appLogo.svg?react";
import LogoType from "assets/logotype.svg?react";
import { useSidebarContext } from "app/contexts/sidebar/context";
import { useBreakpointsContext } from "app/contexts/breakpoint/context";

const iconBtnFocus =
  "outline-hidden focus-visible:ring-2 focus-visible:ring-primary-500/50";

export function Header() {
  const { t } = useTranslation();
  const { isExpanded, close, toggle } = useSidebarContext();
  const { lgAndDown } = useBreakpointsContext();
  const homeLabel = t("nav.dashboards.home");

  useHotkeys(
    "mod+b",
    () => toggle(),
    { enableOnFormTags: false, preventDefault: true },
    [toggle],
  );

  return (
    <header
      className={clsx(
        "relative flex shrink-0 items-center border-b border-gray-150 transition-all duration-300 dark:border-dark-600",
        isExpanded
          ? "h-[70px] justify-between px-4"
          : "flex-col justify-center gap-2 px-0 py-3.5",
      )}
    >
      {isExpanded ? (
        <>
          <Link
            to="/"
            aria-label={homeLabel}
            className="flex items-center gap-3 overflow-hidden transition-all duration-300"
          >
            <Logo className="size-9 shrink-0 text-primary-600 dark:text-primary-400" />
            <LogoType
              aria-hidden
              className="h-4.5 w-auto animate-in text-gray-800 fade-in duration-300 dark:text-dark-50"
            />
          </Link>

          {lgAndDown ? (
            <button
              type="button"
              onClick={close}
              aria-label={t("common.closeMenu")}
              className={clsx(
                "flex size-8 cursor-pointer items-center justify-center rounded-lg text-gray-500 hover:bg-gray-100 hover:text-gray-700 dark:text-dark-300 dark:hover:bg-dark-700 dark:hover:text-dark-100",
                iconBtnFocus,
              )}
            >
              <XMarkIcon className="size-5" />
            </button>
          ) : (
            <button
              type="button"
              onClick={toggle}
              aria-label={t("common.collapseSidebar")}
              data-tooltip="true"
              data-tooltip-content={t("common.collapseSidebar")}
              data-tooltip-place="right"
              className={clsx(
                "flex size-8 cursor-pointer items-center justify-center rounded-lg text-gray-500 transition-colors hover:bg-gray-100 hover:text-gray-900 dark:text-dark-300 dark:hover:bg-dark-700 dark:hover:text-dark-100",
                iconBtnFocus,
              )}
            >
              <ChevronLeftIcon className="size-5 stroke-[2] rtl:rotate-180" />
            </button>
          )}
        </>
      ) : (
        <>
          <Link
            to="/"
            aria-label={homeLabel}
            className="flex size-11 items-center justify-center rounded-xl outline-hidden transition-all duration-200 hover:bg-gray-100 dark:hover:bg-dark-700"
          >
            <Logo className="size-10 shrink-0 text-primary-600 dark:text-primary-400" />
          </Link>

          <button
            type="button"
            onClick={toggle}
            aria-label={t("common.expandSidebar")}
            data-tooltip="true"
            data-tooltip-content={t("common.expandSidebar")}
            data-tooltip-place="right"
            className={clsx(
              "flex size-9 cursor-pointer items-center justify-center rounded-lg border border-gray-200/90 bg-gray-50 text-gray-600 shadow-2xs transition-all hover:border-primary-300 hover:bg-primary-50 hover:text-primary-600 dark:border-dark-600 dark:bg-dark-800 dark:text-dark-200 dark:hover:border-primary-500/40 dark:hover:bg-primary-500/10 dark:hover:text-primary-400",
              iconBtnFocus,
            )}
          >
            <ChevronRightIcon className="size-4.5 stroke-[2] rtl:rotate-180" />
          </button>
        </>
      )}
    </header>
  );
}
