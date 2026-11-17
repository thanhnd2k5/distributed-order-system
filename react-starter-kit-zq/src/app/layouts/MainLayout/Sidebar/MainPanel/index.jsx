// Import Dependencies
import PropTypes from "prop-types";
import { Link } from "react-router";
import clsx from "clsx";

// Local Imports
import Logo from "assets/appLogo.svg?react";
import { Menu } from "./Menu";
import { useThemeContext } from "app/contexts/theme/context";

// ----------------------------------------------------------------------

export function MainPanel({ nav, setActiveSegment, activeSegment }) {
  const { cardSkin } = useThemeContext();
  return (
    <div className="main-panel">
      <div
        className={clsx(
          "flex h-full w-full flex-col items-center border-gray-150 bg-white dark:border-dark-600/80 ltr:border-r rtl:border-l",
          cardSkin === "shadow" ? "dark:bg-dark-750" : "dark:bg-dark-900",
        )}
      >
        {/* Application Logo */}
        <div className="flex pt-3.5">
          <Link to="/dashboard">
            <Logo className="size-10 text-primary-600 dark:text-primary-400" />
          </Link>
        </div>

        <Menu
          nav={nav}
          activeSegment={activeSegment}
          setActiveSegment={setActiveSegment}
        />

        {/* Bottom Area (No-Auth Demo Mode) */}
        <div className="flex flex-col items-center py-3">
          <span className="rounded-md bg-gray-100 px-1.5 py-0.5 text-[10px] font-semibold tracking-wider text-gray-500 uppercase dark:bg-dark-700 dark:text-dark-300">
            Demo
          </span>
        </div>
      </div>
    </div>
  );
}

MainPanel.propTypes = {
  nav: PropTypes.array,
  setActiveSegment: PropTypes.func,
  activeSegment: PropTypes.string,
};
