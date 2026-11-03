// One parsed SSE message.
export interface SSEFrame {
  event: string; // "message" when the frame has no `event:` field
  // The data payload (multiple data lines joined with newlines).
  data: string;
}

/**
 * Incremental Server-Sent-Events parser for fetch() streams (EventSource can't send auth headers).
 * Feed it decoded text chunks of any size; it emits one frame per blank-line-terminated block and
 * ignores comments (heartbeats) and `retry:` lines.
 */
export function createSSEParser(onFrame: (frame: SSEFrame) => void): (chunk: string) => void {
  // Holds text received so far that has not yet formed a complete frame (chunks can split anywhere).
  let buffer = "";
  return (chunk: string) => {
    // Normalize CRLF/CR line endings to LF so frame boundaries are always "\n\n".
    buffer += chunk.replace(/\r\n?/g, "\n");
    // Look for the blank line that terminates a frame.
    let boundary = buffer.indexOf("\n\n");
    // Drain every complete frame currently in the buffer.
    while (boundary !== -1) {
      // The frame text without its terminator.
      const block = buffer.slice(0, boundary);
      // Drop the consumed frame (and the two newlines) from the buffer.
      buffer = buffer.slice(boundary + 2);
      // Per the SSE spec the default event name is "message".
      let event = "message";
      // A frame may carry several data: lines, which are joined below.
      const data: string[] = [];
      // Parse the frame line by line.
      for (const line of block.split("\n")) {
        // Empty lines and lines starting with ":" are comments/heartbeats; skip them.
        if (!line || line.startsWith(":")) continue;
        // Split "field: value" at the first colon.
        const colon = line.indexOf(":");
        // A line with no colon is a field name with an empty value.
        const field = colon === -1 ? line : line.slice(0, colon);
        // The spec strips a single leading space after the colon.
        const value = colon === -1 ? "" : line.slice(colon + 1).replace(/^ /, "");
        // Capture a custom event name.
        if (field === "event") event = value;
        // Collect data lines; other fields (id, retry) are intentionally ignored.
        else if (field === "data") data.push(value);
      }
      // Only emit frames that actually carried data.
      if (data.length) onFrame({ event, data: data.join("\n") });
      // Check for another complete frame in the remaining buffer.
      boundary = buffer.indexOf("\n\n");
    }
  };
}
