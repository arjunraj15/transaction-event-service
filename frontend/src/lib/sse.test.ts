import { describe, expect, it } from "vitest";
import { createSSEParser, type SSEFrame } from "./sse";

function parseAll(chunks: string[]): SSEFrame[] {
  const frames: SSEFrame[] = [];
  const push = createSSEParser((f) => frames.push(f));
  chunks.forEach(push);
  return frames;
}

describe("createSSEParser", () => {
  it("parses named and default events, skipping retry and heartbeat comments", () => {
    const frames = parseAll([
      "retry: 3000\n\n",
      'event: hello\ndata: {"connected": true}\n\n',
      ": heartbeat\n\n",
      'data: {"type":"processed"}\n\n',
    ]);
    expect(frames).toEqual([
      { event: "hello", data: '{"connected": true}' },
      { event: "message", data: '{"type":"processed"}' },
    ]);
  });

  it("handles frames split across arbitrary chunk boundaries and CRLF", () => {
    const frames = parseAll(['da', 'ta: {"a"', ":1}\r", "\n\r\ndata: x\n", "\n"]);
    expect(frames).toEqual([
      { event: "message", data: '{"a":1}' },
      { event: "message", data: "x" },
    ]);
  });

  it("joins multi-line data and keeps an incomplete frame buffered", () => {
    const frames: SSEFrame[] = [];
    const push = createSSEParser((f) => frames.push(f));
    push("data: line1\ndata: line2\n\ndata: pending");
    expect(frames).toEqual([{ event: "message", data: "line1\nline2" }]);
    push("\n\n");
    expect(frames[1]).toEqual({ event: "message", data: "pending" });
  });
});
