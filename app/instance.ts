import net from "node:net";
import { randomUUID } from "node:crypto";
import { parseOutputString, respArray } from "./resp.ts";
import { CustomCache } from "./cache.ts";
import { parseCommand } from "./utils.ts";
import { json } from "stream/consumers";

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
    const States = {
      START: "start",
      PING: "ping",
      REPLCONF_PORT: "replconf_port",
      REPLCONF_CAPA: "replconf_capa",
      PSYNC: "psync",
      FULLRESYNC: "fullresync",
      RDB: "rdb",
      GETACK: "getack",
      COMMAND: "command",
    }
    let HandshakeState = States.START;

    const sock = net.createConnection(
      {
        host: this.config["masterHost"],
        port: this.config["masterPort"],
      },
      () => {
        console.log("Connected to master");
        sock.write(respArray("PING"));
        HandshakeState = States.PING;
      }
    );

    sock.on("data", (data: Buffer) => {
      // let stringData: string = data.toString().trim();
      console.log("Received data from master", JSON.stringify(data.toString()));

      switch (HandshakeState) {
        case States.PING: {
          if (data.toString() !== "+PONG\r\n") {
            console.error("Unexpected response from master PING", JSON.stringify(data));
            process.exit(1);
          }
          console.log("PONG received");
          sock.write(
            parseOutputString(`REPLCONF listening-port ${port.toString()}`)
          );
          HandshakeState = States.REPLCONF_PORT;
          return;
        }
        case States.REPLCONF_PORT: {
          if (data.toString() !== "+OK\r\n") {
            console.error("Unexpected response from master REPLCONF", data.toString());
            process.exit(1);
          }
          console.log("REPLCONF received");
          sock.write(parseOutputString("REPLCONF capa psync2"));
          HandshakeState = States.REPLCONF_CAPA;
          return;
        }
        case States.REPLCONF_CAPA: {
          if (data.toString() !== "+OK\r\n") {
            console.error("Unexpected response from master REPLCONF CAPA", data.toString());
            process.exit(1);
          }
          console.log("REPLCONF received");
          sock.write(parseOutputString("PSYNC ? -1"));
          HandshakeState = States.PSYNC;
          return;
        }
        case States.PSYNC: {
          if (!data.toString().startsWith("+FULLRESYNC")) {
            console.error("Unexpected response from master PSYNC", data.toString());
            process.exit(1);
          }
          console.log("FULLRESYNC received");
          HandshakeState = States.RDB;
          const tmpdata: string[] = data.toString().split("\r\n");
          data = Buffer.from(tmpdata.slice(1).join("\r\n"));
        }
        case States.RDB: {
          if (!data.toString().startsWith("$")) {
            console.error("Unexpected response from master RDB", data.toString());
            break;
          }
          const rdbSizeString: string = data.toString().split("\\")[0].split("$")[1];
          const rdbSize = parseInt(rdbSizeString);
          const dbdatasize = 92;
          const dbdata = data.toString().substring(0, dbdatasize);
          console.log("RDB received");
          HandshakeState = States.GETACK;
          console.log("testing the data", JSON.stringify(data.toString()), "sub", JSON.stringify(data.toString().substring(dbdatasize)), "sizeall", dbdatasize);
          data = Buffer.from(data.toString().substring(dbdatasize));
          console.log("sending data to next stage", JSON.stringify(data.toString()));
        }
        case States.GETACK: {
          if (!data.toString().toLowerCase().startsWith("*3\r\n$8\r\nreplconf")) {
            console.error("Unexpected response from master GETACK", JSON.stringify(data.toString()));
            break;
          }
          console.log("GETACK received");
          sock.write(parseOutputString("REPLCONF ACK 0"));
          HandshakeState = States.COMMAND;

          const cmdLen = "*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n".length;
          data = Buffer.from(data.toString().substring(cmdLen));
        }
        case States.COMMAND: {
          const commands: string[] = data.toString().split("*");
          for (let i = 1; i < commands.length; i++) {
            console.log(
              "SLAVE RECIEVED COMMAND FROM MASTER",
              JSON.stringify("*".concat(commands[i]))
            );
            parseCommand("*".concat(commands[i]), this);
          }
          break;
        }
        default: {
          console.error("Unexpected state");
        }
      }
    });
  }
}
