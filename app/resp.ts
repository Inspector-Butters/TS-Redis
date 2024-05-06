export function parseRespCommand(cmd: string): string[] {
  const lines = cmd.split("\r\n");
  const parts: string[] = [];
  const len = parseInt(lines[0].split("*")[1]);
  for (let i = 1; i <= 2 * len; i += 2) {
    const argLen = parseInt(lines[i].split("$")[1]);
    parts.push(lines[i + 1].slice(0, argLen));
  }
  return parts;
}

export function parseMultiRespCommand(cmd: string): string[][] {
  const lines = cmd.split("\r\n");
  const parts: string[][] = [];
  let tmp: string[] = [];
  for (let i = 1; i < lines.length; i++) {
    if (lines[i] === "" || lines[i].startsWith("$")) {
      continue;
    }
    if (lines[i].startsWith("*") && lines[i].length > 1) {
      parts.push(tmp);
      tmp = [];
    } else {
      tmp.push(lines[i]);
    }
  }
  parts.push(tmp);

  return parts;
}

export function simpleString(str: string) {
  return `+${str}\r\n`;
}

export function bulkString(str: string | undefined | null) {
  if (!str) {
    return "$-1\r\n";
  }
  return `$${str.length}\r\n${str}\r\n`;
}

export function respArray(...parts: string[]) {
  const res = `*${parts.length}\r\n`;
  return parts.reduce((res, part) => `${res}${bulkString(part)}`, res);
}

export function parseOutputList(parts: string[]) {
  if (parts.length === 1) {
    return simpleString(parts[0]);
  } else {
    return respArray(...parts);
  }
}
export function encodeInt(x: number): string {
  return `:${x}\r\n`;
}
export function parseOutputString(str: string) {
  const parts = str.split(" ");
  return parseOutputList(parts);
}

export function RDBString(emptyRdbContentHex) {
  const contentBuf = Buffer.from(emptyRdbContentHex, "hex");
  return Buffer.concat([Buffer.from(`$${contentBuf.length}\r\n`), contentBuf]);
}
