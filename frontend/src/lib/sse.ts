export interface SSEFrame {
  event: string; // "message" when the frame has no `event:` field
  data: string;
}

/**
 * Incremental Server-Sent-Events parser for fetch() streams (EventSource can't send auth headers).
 * Feed it decoded text chunks of any size; it emits one frame per blank-line-terminated block and
 * ignores comments (heartbeats) and `retry:` lines.
 */
export function createSSEParser(onFrame: (frame: SSEFrame) => void): (chunk: string) => void {
  let buffer = "";
  return (chunk: string) => {
    buffer += chunk.replace(/\r\n?/g, "\n");
    let boundary = buffer.indexOf("\n\n");
    while (boundary !== -1) {
      const block = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      let event = "message";
      const data: string[] = [];
      for (const line of block.split("\n")) {
        if (!line || line.startsWith(":")) continue;
        const colon = line.indexOf(":");
        const field = colon === -1 ? line : line.slice(0, colon);
        const value = colon === -1 ? "" : line.slice(colon + 1).replace(/^ /, "");
        if (field === "event") event = value;
        else if (field === "data") data.push(value);
      }
      if (data.length) onFrame({ event, data: data.join("\n") });
      boundary = buffer.indexOf("\n\n");
    }
  };
}
