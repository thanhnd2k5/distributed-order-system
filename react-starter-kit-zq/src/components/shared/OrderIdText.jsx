import PropTypes from "prop-types";
import { CheckIcon, DocumentDuplicateIcon } from "@heroicons/react/24/outline";
import { CopyButton } from "components/ui";
import { shortUuid } from "utils/uuid";

export function OrderIdText({ value, full = false, className = "" }) {
  if (!value) {
    return <span className="text-gray-400">—</span>;
  }

  return (
    <span className={`inline-flex items-center gap-1.5 font-mono text-xs ${className}`}>
      <span title={value}>{full ? value : shortUuid(value)}</span>
      <CopyButton value={String(value)}>
        {({ copy, copied }) => (
          <button
            type="button"
            onClick={(event) => {
              event.stopPropagation();
              copy();
            }}
            title={copied ? "Đã sao chép" : "Sao chép UUID"}
            className="inline-flex size-5 cursor-pointer items-center justify-center rounded text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-700 dark:hover:bg-dark-600 dark:hover:text-dark-100"
          >
            {copied ? (
              <CheckIcon className="size-3.5 text-success" />
            ) : (
              <DocumentDuplicateIcon className="size-3.5" />
            )}
          </button>
        )}
      </CopyButton>
    </span>
  );
}

OrderIdText.propTypes = {
  value: PropTypes.string,
  full: PropTypes.bool,
  className: PropTypes.string,
};
