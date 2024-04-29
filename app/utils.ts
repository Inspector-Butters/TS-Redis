import { Instance } from "./instance.ts";
import {
  RDBString,
  bulkString,
  parseOutputList,
  parseOutputString,
  parseRespCommand,
  respArray,
  simpleString,
} from "./resp.ts";

const emptyRDB: string =
  "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

function parseCommand(
  instance: Instance,
  str?: string,
  commandsList?: string[]
): any[] {
  if (commandsList && !str) {
    const [cmd, ...args] = commandsList;
    return handleCommand(cmd, args, instance);
  } else if (str && !commandsList) {
    const [cmd, ...args] = parseRespCommand(str);
    return handleCommand(cmd, args, instance);
  } else {
    throw new Error("No command provided");
  }
}

function handleCommand(cmd: string, args: string[], instance: Instance): any[] {
  switch (cmd.toUpperCase()) {
    case "PING": {
      return [-1, simpleString("PONG")];
    }
    case "REPLCONF": {
      if (args[0].toLowerCase() === "getack" && args[1] === "*") {
        return [
          2,
          parseOutputString(`REPLCONF ACK ${instance.offsetCount - 37}`),
        ];
      } else {
        return [-1, simpleString("OK")];
      }
    }
    case "PSYNC": {
      if (args[0] === "?" && args[1] === "-1") {
        return [
          -1,
          simpleString(
            `FULLRESYNC ${instance.replicationId} ${instance.replicationOffset}`
          ),
          RDBString(emptyRDB),
          // parseOutputString("REPLCONF GETACK *"),
        ];
      }
    }
    case "ECHO": {
      return [-1, parseOutputList(args)];
    }
    case "SET": {
      const [key, val, px, exp] = args;
      const isExpireable: boolean = px === "px";

      if (isExpireable) {
        instance.cache.set(key, val, true, parseInt(exp));
      } else {
        instance.cache.set(key, val, false, 0);
      }
      return [1, simpleString("OK")];
    }
    case "GET": {
      const [key] = args;
      const value = instance.cache.get(key);
      return [-1, bulkString(value)];
    }
    case "INFO": {
      if (args[0].toUpperCase() == "REPLICATION") {
        return [-1, bulkString(instance.replicationInfo)];
      }
    }
    default:
      return [-1, simpleString("OK")];
  }
}

export { emptyRDB, parseCommand };
