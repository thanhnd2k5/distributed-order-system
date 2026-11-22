import { useState } from "react";
import { useNowTick } from "hooks/useNowTick";
import {
  computeElapsedSeconds,
  formatDurationSeconds,
} from "utils/orderFormatting";

export function LiveElapsedTimer({ sinceIso, initialSeconds }) {
  const [mountedAt] = useState(() => Date.now());
  const nowMs = useNowTick(1000, true);

  let elapsed = 0;
  if (sinceIso) {
    elapsed = computeElapsedSeconds(sinceIso, nowMs);
  } else if (typeof initialSeconds === "number") {
    elapsed =
      Math.max(0, Math.floor(initialSeconds)) +
      Math.max(0, Math.floor((nowMs - mountedAt) / 1000));
  }

  return (
    <span className="font-mono text-xs tabular-nums">
      {formatDurationSeconds(elapsed)}
    </span>
  );
}
