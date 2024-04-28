import net from "node:net";
import { randomUUID } from "node:crypto";
import { parseOutputString, respArray } from "./resp.ts";
import { CustomCache } from "./cache.ts";
import { parseCommand } from "./utils.ts";

enum instanceRole {
  MASTER = "master",
  SLAVE = "slave",
}

export class Instance {
  config: Record<string, any> = {};
  replicaConnections: net.Socket[] = [];
  cache: CustomCache = new CustomCache();

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
      // console.log("Received data from master", data.toString());

      if (step > 6) {
        // const command: string = data.toString().trim();
        // console.log("SLAVE KIRI RECIEVED COMMAND FROM MASTER", JSON.stringify(data.toString()));
        const commands: string[] = data.toString().trim().split("*");
        for (let i = 1; i < commands.length; i++) {
          console.log(
            "SLAVE RECIEVED COMMAND FROM MASTER",
            JSON.stringify("*".concat(commands[i]))
          );
          parseCommand("*".concat(commands[i]), this);
        }
        return;
      }

      if (step === 1 && data.toString() == "+PONG\r\n") {
        console.log("PONG received");
        step++;
        sock.write(
          parseOutputString(`REPLCONF listening-port ${port.toString()}`)
        );
        return;
      }
      if (step === 2 && data.toString() == "+OK\r\n") {
        console.log("REPLCONF received");
        step++;
        sock.write(parseOutputString("REPLCONF capa psync2"));
        return;
      }
      if (step === 3 && data.toString() == "+OK\r\n") {
        console.log("REPLCONF received");
        step++;
        sock.write(parseOutputString("PSYNC ? -1"));
        return;
      }
      if (step === 4 && data.toString().startsWith("+FULLRESYNC")) {
        console.log("FULLRESYNC received");
        step++;
        return;
      }
      if (step === 5) {
        step++;
        console.log("RDB received");
        return;
      }
      if (step === 6) {
        step++;
        sock.write(parseOutputString("REPLCONF ACK 0"));
        return;
      }
    });
  }
}
