import * as net from "node:net";
import { parseArgs, ParseArgsConfig } from "node:util";
import { Instance } from "./instance.ts";
import {
  bulkString,
  parseOutputList,
  parseRespCommand,
  RDBString,
  simpleString,
} from "./resp.ts";
import { CustomCache } from "./cache.ts";

const emptyRDB: string =
  "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

async function main() {
  const options: ParseArgsConfig["options"] = {
    port: {
      type: "string",
      default: undefined,
    },
    replicaof: {
      type: "string",
      default: undefined,
    },
  };

  const { values, positionals } = parseArgs({
    options,
    allowPositionals: true,
  });

  const port = values["port"] ? parseInt(values["port"].toString()) : 6379;
  const masterHost = values["replicaof"] as string | undefined;

  let instance: Instance;
  let cache: CustomCache = new CustomCache();

  if (masterHost !== undefined) {
    if (positionals.length === 0) {
      console.error("Master port missing: cannot continue progam");
      process.exit(1);
    }
    const masterPort = positionals[0];
    instance = Instance.initSlave(masterHost, masterPort);
    instance.initiateHandshake(port);
  } else {
    instance = Instance.initMaster();
  }

  const server: net.Server = net.createServer((connection: net.Socket) => {
    connection.on("data", (data: Buffer) => {
      const command: string = data.toString().trim();
      addReplicaConnection(command, connection);
      const [type, ...result] = parseCommand(command);
      if (type === 1) {
        for (const replica of instance.replicaConnections) {
          replica.write(data);
        }
      }
      for (const res of result) {
        connection.write(res);
      }
      return;
    });
  });

  server.listen(port, "127.0.0.1");
  console.log(`Server listening on port ${port}`);

  function addReplicaConnection(data: string, connection: net.Socket) {
    const [cmd, ...args] = parseRespCommand(data);
    if (cmd.toUpperCase() === "REPLCONF" && args[0] === "listening-port") {
      instance.replicaConnections.push(connection);
      console.log("Replica connection added");
    }
  }

  function parseCommand(str: string): any[] {
    const [cmd, ...args] = parseRespCommand(str);
    console.log("parsed command", cmd, args);

    switch (cmd.toUpperCase()) {
      case "PING": {
        return [-1, simpleString("PONG")];
      }
      case "REPLCONF": {
        return [-1, simpleString("OK")];
      }
      case "PSYNC": {
        if (args[0] === "?" && args[1] === "-1") {
          return [
            -1,
            simpleString(
              `FULLRESYNC ${instance.replicationId} ${instance.replicationOffset}`
            ),
            RDBString(emptyRDB),
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
          cache.set(key, val, true, parseInt(exp));
        } else {
          cache.set(key, val, false, 0);
        }
        return [1, simpleString("OK")];
      }
      case "GET": {
        const [key] = args;
        const value = cache.get(key);
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
}

main();
