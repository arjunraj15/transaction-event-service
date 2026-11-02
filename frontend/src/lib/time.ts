const FORMAT = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  fractionalSecondDigits: 3,
  hour12: false,
});

/** Local wall-clock time with milliseconds, e.g. 23:41:04.603. */
export const clockTime = (value: Date | string | null): string =>
  value === null ? "—" : FORMAT.format(typeof value === "string" ? new Date(value) : value);
