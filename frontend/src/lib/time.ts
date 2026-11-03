// Formatter built once at module load (constructing Intl formatters is costly) and reused for every call.
const FORMAT = new Intl.DateTimeFormat(undefined, {
  // undefined locale above = the user's own locale.
  // Two-digit hour/minute/second keep columns aligned.
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  // Millisecond precision matters for comparing pipeline latencies.
  fractionalSecondDigits: 3,
  // 24-hour clock avoids AM/PM clutter in the log.
  hour12: false,
});

/** Local wall-clock time with milliseconds, e.g. 23:41:04.603. */
// null (e.g. an unprocessed row's processed_at) renders as a dash; strings are ISO timestamps parsed to Dates.
export const clockTime = (value: Date | string | null): string =>
  value === null ? "—" : FORMAT.format(typeof value === "string" ? new Date(value) : value);
