import * as net from "node:net";
import { parseArgs, ParseArgsConfig } from "node:util";
import { Instance } from "./instance.ts";
import {
  parseRespCommand,
} from "./resp.ts";
import { parseCommand } from "./utils.ts";

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
      console.log("starting command processing for ", instance.role, JSON.stringify(data.toString()));
      const command: string = data.toString().trim();
      if (instance.isMaster) {
        addReplicaConnection(command, connection);
      }
      const [type, ...result] = parseCommand(command, instance);
      if (type === 1 && instance.isMaster) {
        for (const replica of instance.replicaConnections) {
          console.log("Writing to replica", data.toString());
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
}

main();
