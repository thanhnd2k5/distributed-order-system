import PropTypes from "prop-types";
import { ArrowPathIcon } from "@heroicons/react/24/outline";
import { Badge, Button, Switch } from "components/ui";
import { formatTimeOnly } from "utils/time";

export function PollingStatusBar({
  serviceLabel,
  endpointUrl,
  isConnected,
  isFetching,
  autoPoll,
  onToggleAutoPoll,
  onRefresh,
  lastUpdatedAt,
  error,
}) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-gray-200 bg-white px-4 py-2.5 text-xs dark:border-dark-600 dark:bg-dark-800">
      <div className="flex flex-wrap items-center gap-2.5">
        <Badge variant="soft" color={isConnected ? "success" : "error"}>
          <span
            className={`mr-1.5 inline-block size-2 rounded-full ${
              isConnected ? "bg-success" : "bg-error"
            }`}
          />
          {isConnected ? "Đang kết nối" : "Mất kết nối"}
        </Badge>

        {serviceLabel && (
          <span className="font-medium text-gray-700 dark:text-dark-100">
            {serviceLabel}
          </span>
        )}

        {endpointUrl && (
          <code className="rounded bg-gray-100 px-1.5 py-0.5 font-mono text-[11px] text-gray-600 dark:bg-dark-700 dark:text-dark-200">
            {endpointUrl}
          </code>
        )}

        {lastUpdatedAt ? (
          <span className="text-gray-500 dark:text-dark-300">
            Cập nhật lúc {formatTimeOnly(lastUpdatedAt)}
          </span>
        ) : null}

        {!isConnected && error?.message && (
          <span className="text-error dark:text-error-light font-medium">
            {error.message}
          </span>
        )}
      </div>

      <div className="flex items-center gap-3">
        <Switch
          checked={autoPoll}
          onChange={(event) => onToggleAutoPoll(event.target.checked)}
          label="Auto-poll 2s"
          classNames={{
            labelText: "text-xs text-gray-600 dark:text-dark-200 select-none",
          }}
        />
        <Button
          variant="outlined"
          color="neutral"
          className="h-7 gap-1.5 px-2.5 text-xs"
          onClick={onRefresh}
          disabled={isFetching}
        >
          <ArrowPathIcon
            className={`size-3.5 ${isFetching ? "animate-spin" : ""}`}
          />
          <span>Làm mới</span>
        </Button>
      </div>
    </div>
  );
}

PollingStatusBar.propTypes = {
  serviceLabel: PropTypes.string,
  endpointUrl: PropTypes.string,
  isConnected: PropTypes.bool,
  isFetching: PropTypes.bool,
  autoPoll: PropTypes.bool,
  onToggleAutoPoll: PropTypes.func.isRequired,
  onRefresh: PropTypes.func.isRequired,
  lastUpdatedAt: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
  error: PropTypes.object,
};
