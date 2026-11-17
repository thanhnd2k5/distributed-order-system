import PropTypes from "prop-types";
import { Badge } from "components/ui";

const SITE_COLOR = {
  POS01: "primary",
  POS02: "secondary",
  ONLINE: "info",
};

export function SiteBadge({ siteId, className }) {
  const normalized = String(siteId || "").toUpperCase();
  const color = SITE_COLOR[normalized] || "neutral";

  return (
    <Badge variant="outlined" color={color} className={className}>
      {normalized || "N/A"}
    </Badge>
  );
}

SiteBadge.propTypes = {
  siteId: PropTypes.string,
  className: PropTypes.string,
};
