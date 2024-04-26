import net from "node:net";
import { randomUUID } from "node:crypto";
import {
  bulkString,
  parseOutputString,
  parseRespCommand,
  respArray,
  simpleString,
} from "./resp.ts";

enum instanceRole {
  MASTER = "master",
  SLAVE = "slave",
}

export class Instance {
  config: Record<string, any> = {};

  constructor(
    readonly role: instanceRole,
    readonly replicationId: string,
    readonly replicationOffset: number,
    config?: Record<string, any>
  ) {
    if (config) {
      this.config = config;
    }
    console.log(`Instance created with role: ${role}`);
  }

  get isMaster() {
    return this.role === instanceRole.MASTER;
  }

  get isSlave() {
    return this.role === instanceRole.SLAVE;
  }

  static initMaster() {
    const id: string = randomUUID().replace(/-/g, "");
    console.log(`Initiating master with id: ${id}`);
    return new Instance(instanceRole.MASTER, id, 0);
  }

  static initSlave(masterHost: string, masterPort: string) {
    const id: string = randomUUID().replace(/-/g, "");
    console.log(
      `Initiating slave with connection to master: ${masterHost}:${masterPort}`
    );
    return new Instance(instanceRole.SLAVE, id, 0, {
      masterHost: masterHost,
      masterPort: masterPort,
    });
  }

  get replicationInfo() {
    return `# Replication
role:${this.role}
master_replid:${this.replicationId}
master_repl_offset:${this.replicationOffset}
`;
  }

  initiateHandshake(port: number) {
    console.log("Initiating handshake");
    let step = 1;

    const sock = net.createConnection(
      {
        host: this.config["masterHost"],
        port: this.config["masterPort"],
      },
      () => {
        console.log("Connected to master");
        sock.write(respArray("PING"));
      }
    );

    sock.on("data", (data: Buffer) => {
      const resp = parseRespCommand(data.toString().trim());
      console.log("Received data from master", data.toString());

      if (step === 1 && resp[0].toUpperCase() == "PONG") {
        step++;
        sock.write(
          parseOutputString(`REPLCONF listening-port ${port.toString()}`)
        );
      }
      if (step === 2 && resp[0].toUpperCase() == "OK") {
        step++;
        sock.write(parseOutputString("REPLCONF capa psync2"));
      }
    });
  }
}
