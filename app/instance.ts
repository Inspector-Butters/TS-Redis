import net from "node:net";
import { randomUUID } from "node:crypto";
import { parseMultiRespCommand, parseOutputString, respArray } from "./resp.ts";
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
  offsetCount: number = 0;

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
    };
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
            console.error(
              "Unexpected response from master PING",
              JSON.stringify(data)
            );
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
            console.error(
              "Unexpected response from master REPLCONF",
              data.toString()
            );
            process.exit(1);
          }
          console.log("REPLCONF received");
          sock.write(parseOutputString("REPLCONF capa psync2"));
          HandshakeState = States.REPLCONF_CAPA;
          return;
        }
        case States.REPLCONF_CAPA: {
          if (data.toString() !== "+OK\r\n") {
            console.error(
              "Unexpected response from master REPLCONF CAPA",
              data.toString()
            );
            process.exit(1);
          }
          console.log("REPLCONF received");
          sock.write(parseOutputString("PSYNC ? -1"));
          HandshakeState = States.PSYNC;
          return;
        }
        case States.PSYNC: {
          if (!data.toString().startsWith("+FULLRESYNC")) {
            console.error(
              "Unexpected response from master PSYNC",
              data.toString()
            );
            process.exit(1);
          }
          console.log("FULLRESYNC received");
          HandshakeState = States.RDB;
          const tmpdata: string[] = data.toString().split("\r\n");
          data = Buffer.from(tmpdata.slice(1).join("\r\n"));
        }
        case States.RDB: {
          if (!data.toString().startsWith("$")) {
            console.error(
              "Unexpected response from master RDB",
              data.toString()
            );
            break;
          }
          const rdbSizeString: string = data
            .toString()
            .split("\\")[0]
            .split("$")[1];
          const rdbSize = parseInt(rdbSizeString);
          const dbdatasize = 92;
          const dbdata = data.toString().substring(0, dbdatasize);
          console.log("RDB received");
          HandshakeState = States.COMMAND;
          // console.log("testing the data", JSON.stringify(data.toString()), "sub", JSON.stringify(data.toString().substring(dbdatasize)), "sizeall", dbdatasize);
          data = Buffer.from(data.toString().substring(dbdatasize));
          if (data.toString().length > 0) {
            console.log(
              "sending data to next stage",
              JSON.stringify(data.toString())
            );
          } else {
            break;
          }
        }

        case States.COMMAND: {
          this.offsetCount += data.toString().length;
          const commands: string[][] = parseMultiRespCommand(data.toString());
          for (let i = 0; i < commands.length; i++) {
            console.log(
              "SLAVE RECIEVED COMMAND FROM MASTER",
              JSON.stringify(commands[i])
            );
            const [type, ...result] = parseCommand(
              this,
              undefined,
              commands[i]
            );
            if (type === 2) {
              for (const res of result) {
                sock.write(res);
              }
            }
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
