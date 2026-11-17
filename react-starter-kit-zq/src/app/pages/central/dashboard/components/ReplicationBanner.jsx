import PropTypes from "prop-types";
import { InformationCircleIcon } from "@heroicons/react/24/outline";
import { Badge } from "components/ui";
import { formatDateTime } from "utils/time";

export function ReplicationBanner({ replicaNote, queriedAt }) {
  return (
    <div className="border-info/35 bg-info/8 flex flex-wrap items-start justify-between gap-3 rounded-xl border px-4 py-3 text-xs">
      <div className="flex items-start gap-2.5">
        <InformationCircleIcon className="text-info mt-0.5 size-5 shrink-0" />
        <div className="space-y-0.5">
          <p className="font-semibold text-gray-900 dark:text-dark-50">
            Bản sao tổng hợp trung tâm (PostgreSQL Logical Replication — Read-Only)
          </p>
          <p className="text-gray-600 dark:text-dark-200">
            {replicaNote ||
              "Dữ liệu được nhân bản bất đồng bộ từ POS01, POS02, ONLINE về central-db và có thể trễ so với DB nguồn."}
          </p>
        </div>
      </div>

      {queriedAt && (
        <Badge variant="soft" color="info" className="shrink-0 font-mono">
          API queriedAt: {formatDateTime(queriedAt)}
        </Badge>
      )}
    </div>
  );
}

ReplicationBanner.propTypes = {
  replicaNote: PropTypes.string,
  queriedAt: PropTypes.string,
};
